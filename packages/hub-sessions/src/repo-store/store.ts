import fs from "node:fs";
import path from "node:path";
import git, { type TreeEntry } from "isomorphic-git";
import { createSSHSignature } from "@intx/crypto";
import {
  initRepo as storageInitRepo,
  createDeployPack,
  receivePackObjects,
  collectReachableObjects,
  maybeGC,
  type CommitSigner,
  type GCPolicy,
} from "@intx/storage-isogit/node";
import { hasCode } from "@intx/types";
import { getLogger } from "@intx/log";
import type {
  AuthorizeFn,
  CommittedReads,
  CommittedTreeEntry,
  InitRepoOpts,
  KindHandler,
  NewlyTerminalRun,
  Principal,
  RefEntry,
  RepoAction,
  RepoId,
  RepoKind,
  RepoStore,
  RepoStoreSubscribeEvent,
  TreeContent,
  WriteResult,
  WriteTreeDeltaArgs,
  WriteTreePreservingPrefixArgs,
} from "./types";
import { SAFE_REPO_ID } from "./types";

const DEFAULT_SUBSCRIBE_BUFFER_LIMIT = 1024;

type SubscribeEntry = { seq: number; event: unknown };

type SubscriberState = {
  bufferLimit: number;
  buffer: SubscribeEntry[];
  closed: boolean;
  error: Error | null;
  waiter: ((value: IteratorResult<SubscribeEntry>) => void) | null;
};

const AUTHOR = {
  name: "interchange-hub",
  email: "hub@interchange.local",
};

const logger = getLogger(["hub", "repo-store"]);

type SigningKey = { privateKey: Uint8Array; publicKey: Uint8Array };

/**
 * In-process push-serialization lock, keyed by `${kind}/${id}`. Each
 * entry holds the tail of the chain of in-flight critical sections for
 * that repo; the next acquirer awaits the current tail and replaces it.
 * The tail-check on release prevents the map leaking entries once the
 * chain drains.
 *
 * Single-process assumption: this only serializes operations inside one
 * hub instance. Cross-process writers (a second replica, an external
 * git client) would need a filesystem-backed lock; the migration path
 * is swapping `withRepoLock`'s body for an FS-lock acquire/release.
 */
const locks = new Map<string, Promise<void>>();

async function withRepoLock<T>(
  repoId: RepoId,
  fn: () => Promise<T>,
): Promise<T> {
  const key = `${repoId.kind}/${repoId.id}`;
  const previous = locks.get(key) ?? Promise.resolve();
  let releaseFn: () => void = () => undefined;
  const tail = new Promise<void>((resolve) => {
    releaseFn = resolve;
  });
  locks.set(key, tail);
  try {
    await previous;
    return await fn();
  } finally {
    if (locks.get(key) === tail) {
      locks.delete(key);
    }
    releaseFn();
  }
}

export type CreateRepoStoreConfig = {
  dataDir: string;
  signingKey: SigningKey;
  /**
   * Handler map keyed by repo kind; the substrate throws at request
   * time when a kind has no registered handler, so callers may omit
   * kinds they do not service.
   */
  handlers: Partial<Record<RepoKind, KindHandler>>;
  authorize: AuthorizeFn;
  /**
   * Optional per-repo signing callback: when it returns a `CommitSigner`
   * for the `repoId`, the genesis commit is authored as `interchange-hub`
   * and signed; otherwise the unsigned harness-authored genesis is used.
   */
  signingCallback?: (repoId: RepoId) => CommitSigner | undefined;
  /**
   * Optional write-path GC policy: applied under the repo lock after
   * every successful write to a kind in `kinds`. Omitted, the substrate
   * never reclaims. The allowlist keeps the policy scoped to the kinds
   * the caller intends.
   */
  gc?: GCPolicy & { kinds: readonly RepoKind[] };
};

export function createRepoStore(config: CreateRepoStoreConfig): RepoStore {
  const { dataDir, signingKey, handlers, authorize, signingCallback, gc } =
    config;

  // Per-repo-directory isomorphic-git memoization cache, threaded through
  // every index-touching and object-reading call so parsed indexes and
  // packfile indexes are reused across the repo's serialized writes. Pure
  // accelerator, never a second source of truth: the on-disk repo stays
  // authoritative (the index is persisted per mutation, objects are
  // content-addressed, refs are never cached). Bounds: a dir's cache is
  // rebuilt after GIT_CACHE_MAX_OPS ops and at most GIT_CACHE_MAX_REPOS
  // dirs are held (LRU evicted). `invalidateGitCache` drops a dir's cache
  // after an out-of-band mutation that bypassed it (a received pack).
  const GIT_CACHE_MAX_OPS = 8192;
  const GIT_CACHE_MAX_REPOS = 256;
  type RepoGitCache = { cache: object; ops: number };
  const gitCaches = new Map<string, RepoGitCache>();
  function cacheFor(dir: string): object {
    let entry = gitCaches.get(dir);
    if (entry === undefined) {
      entry = { cache: {}, ops: 0 };
    } else {
      // Re-insert so this dir ranks most-recently-used for the LRU
      // eviction below; rebuild once it has spent its op budget.
      gitCaches.delete(dir);
      if (entry.ops >= GIT_CACHE_MAX_OPS) entry = { cache: {}, ops: 0 };
    }
    entry.ops += 1;
    gitCaches.set(dir, entry);
    while (gitCaches.size > GIT_CACHE_MAX_REPOS) {
      const lru = gitCaches.keys().next().value;
      if (lru === undefined) break;
      gitCaches.delete(lru);
    }
    return entry.cache;
  }
  function invalidateGitCache(dir: string): void {
    gitCaches.delete(dir);
  }

  // Per-(repoId, ref) seq cache: the tip's seq, so the next commit gets
  // `cached + 1`. Cleared on any failure inside the update path so a
  // half-applied state never poisons future reads.
  const seqCache = new Map<string, number>();

  // Per-(repoId, ref) subscriber set; each subscriber owns its buffer,
  // filter, and waiter so concurrent subscribers do not interfere.
  const subscribers = new Map<string, Set<SubscriberState>>();

  // Repo-scoped cache key shared by the per-repoId caches below
  // (existing-commit set, and any future per-repo bookkeeping).
  function indexCacheKey(repoId: RepoId): string {
    return `${repoId.kind}/${repoId.id}`;
  }

  // Per-commit reachable-object cache for createPack's first-parent
  // walk; without it the walk recomputes every ancestor's reachable set
  // on every push, scaling O(N^2) in commit count.
  const chainReachabilityCache = new Map<string, string[]>();

  // Per-repoId set of every commit OID currently reachable from any
  // branch or tag, so receivePack skips validating commits the receiver
  // already had. Incrementally extended on each ref update; lazily
  // initialised on first receivePack to avoid a cold-start scan for
  // writeTree-only repos.
  const existingCommitsCache = new Map<string, Set<string>>();

  // Per-(repoId, ref) cursor of the commit the receiver acked for an
  // incremental workflow-run pack. Advanced in `commitPackedTip` on the
  // receiver's ack — NOT when `createPack` builds the pack — so a
  // transfer cancelled before its ack re-ships the un-acked commits
  // instead of stranding the receiver with a dangling parent. Keyed per
  // ref so writes to different refs on the same repo do not interfere.
  const lastPackedTip = new Map<string, string>();
  function lastPackedTipKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }

  function refKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }

  // Count commits reachable from `ref` (0 when the ref does not exist).
  // `git.log` returns newest-first; the count is the seq the next
  // commit would land at (the current tip's seq is `count - 1`).
  async function countCommits(dir: string, ref: string): Promise<number> {
    try {
      const entries = await git.log({ fs, dir, cache: cacheFor(dir), ref });
      return entries.length;
    } catch (err) {
      if (hasCode(err) && err.code === "NotFoundError") return 0;
      throw err;
    }
  }

  // Walk the ref's history oldest-first, seq 0 at the root; emits the
  // same substrate-level event shape as the live path.
  async function replayHistory(
    dir: string,
    ref: string,
  ): Promise<SubscribeEntry[]> {
    let entries: Awaited<ReturnType<typeof git.log>>;
    try {
      entries = await git.log({ fs, dir, cache: cacheFor(dir), ref });
    } catch (err) {
      if (hasCode(err) && err.code === "NotFoundError") return [];
      throw err;
    }
    const reversed = [...entries].reverse();
    const out: SubscribeEntry[] = [];
    let prev: string | null = null;
    for (let i = 0; i < reversed.length; i++) {
      const entry = reversed[i];
      if (entry === undefined) throw new Error("unreachable");
      const event: RepoStoreSubscribeEvent = {
        type: "ref.updated",
        ref,
        oldSha: prev,
        newSha: entry.oid,
      };
      out.push({ seq: i, event });
      prev = entry.oid;
    }
    return out;
  }

  function deliverToSubscriber(sub: SubscriberState, entry: SubscribeEntry) {
    if (sub.closed) return;
    if (sub.waiter !== null) {
      const w = sub.waiter;
      sub.waiter = null;
      w({ value: entry, done: false });
      return;
    }
    if (sub.buffer.length >= sub.bufferLimit) {
      sub.error = new Error(
        `subscribe_buffer_overrun: subscriber exceeded bufferLimit=${String(sub.bufferLimit)}`,
      );
      sub.closed = true;
      return;
    }
    sub.buffer.push(entry);
  }

  // Inside the per-repo lock after a successful ref update: compute the
  // new tip's seq (bumped from cache or walked from the log), then fan
  // the event out to the ref's subscribers. Subscriber delivery errors
  // stay on the subscriber, never the ref-update path.
  async function emitRefUpdate(
    repoId: RepoId,
    ref: string,
    oldSha: string | null,
    newSha: string,
  ): Promise<void> {
    const key = refKey(repoId, ref);
    let seq: number;
    const cached = seqCache.get(key);
    if (cached !== undefined) {
      seq = cached + 1;
    } else {
      const dir = repoDir(repoId);
      const count = await countCommits(dir, ref);
      // `count` is the number of commits including the one we just
      // produced. The tip's seq is `count - 1`.
      seq = Math.max(0, count - 1);
    }
    seqCache.set(key, seq);

    const event: RepoStoreSubscribeEvent = {
      type: "ref.updated",
      ref,
      oldSha,
      newSha,
    };
    const entry: SubscribeEntry = { seq, event };
    const set = subscribers.get(key);
    if (set === undefined) return;
    for (const sub of set) deliverToSubscriber(sub, entry);
  }

  function handlerFor(repoId: RepoId): KindHandler {
    const handler = handlers[repoId.kind];
    if (handler === undefined) {
      throw new Error(`no handler registered for kind: ${repoId.kind}`);
    }
    return handler;
  }

  function signerFor(repoId: RepoId): CommitSigner | undefined {
    return signingCallback === undefined ? undefined : signingCallback(repoId);
  }

  function repoDir(repoId: RepoId): string {
    if (!SAFE_REPO_ID.test(repoId.id)) {
      throw new Error(`repo_id_invalid: ${repoId.id}`);
    }
    const handler = handlerFor(repoId);
    return path.join(dataDir, handler.directoryPrefix, repoId.id);
  }

  // Write-path reclaim for allowlisted kinds, inside the per-repo lock;
  // a reclaim failure logs rather than failing the already-durable write.
  async function maybeRunGC(repoId: RepoId): Promise<void> {
    if (gc === undefined || !gc.kinds.includes(repoId.kind)) return;
    await maybeGC(repoDir(repoId), gc);
  }

  function gateAccess(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    action: RepoAction,
  ): void {
    const verdict = authorize(principal, repoId, ref, action);
    if (!verdict.allowed) {
      throw new Error(`authorize_denied: ${verdict.reason}`);
    }
  }

  function validateClearPrefix(clearPrefix: string): void {
    const malformed =
      clearPrefix.length === 0 ||
      !clearPrefix.endsWith("/") ||
      clearPrefix.startsWith("/") ||
      clearPrefix.split("/").includes("..");
    if (malformed) {
      throw new Error(`clear_prefix_invalid: ${clearPrefix}`);
    }
  }

  // Delta path shape: puts are files (no trailing slash); deletes are an
  // exact file or a subtree prefix (trailing slash allowed). Both reject
  // empties, absolute paths, and `..` traversal segments.
  function validateDeltaPath(p: string, isDelete: boolean): void {
    const malformed =
      p.length === 0 ||
      p.startsWith("/") ||
      p.split("/").includes("..") ||
      (!isDelete && p.endsWith("/"));
    if (malformed) {
      throw new Error(`delta_path_invalid: ${JSON.stringify(p)}`);
    }
  }

  // Reject an ambiguous delta loudly (put+delete of the same path, or a
  // put under a subtree-prefix delete) rather than let assembleTree pick
  // a silent winner.
  function assertDeltaUnambiguous(
    puts: Record<string, string | Uint8Array>,
    deletes: readonly string[],
  ): void {
    const deleteSet = new Set(deletes);
    const deletePrefixes = deletes.filter((d) => d.endsWith("/"));
    for (const p of Object.keys(puts)) {
      if (deleteSet.has(p)) {
        throw new Error(
          `delta_ambiguous: path ${JSON.stringify(p)} is in both puts and deletes`,
        );
      }
      for (const dp of deletePrefixes) {
        if (p.startsWith(dp)) {
          throw new Error(
            `delta_ambiguous: put ${JSON.stringify(p)} lands under deleted subtree ${JSON.stringify(dp)}`,
          );
        }
      }
    }
  }

  // When a delta declares a change scope, every put and delete must fall
  // under it, or a scope-scoped validation would skip a region the write
  // actually mutated. Undefined scope = validate-all.
  function assertDeltaScoped(
    puts: Record<string, string | Uint8Array>,
    deletes: readonly string[],
    changedPathPrefixes: ReadonlySet<string> | undefined,
  ): void {
    if (changedPathPrefixes === undefined) return;
    const covered = (p: string): boolean => {
      for (const prefix of changedPathPrefixes) {
        if (p === prefix || p.startsWith(prefix)) return true;
      }
      return false;
    };
    for (const p of Object.keys(puts)) {
      if (!covered(p)) {
        throw new Error(
          `delta_out_of_scope: put ${JSON.stringify(p)} is not under any changedPathPrefix`,
        );
      }
    }
    for (const d of deletes) {
      if (!covered(d)) {
        throw new Error(
          `delta_out_of_scope: delete ${JSON.stringify(d)} is not under any changedPathPrefix`,
        );
      }
    }
  }

  function storageOptsFor(
    repoId: RepoId,
    opts: InitRepoOpts | undefined,
  ): { signer?: CommitSigner; gitignore?: string } {
    const out: { signer?: CommitSigner; gitignore?: string } = {};
    const signer = signerFor(repoId);
    if (signer !== undefined) out.signer = signer;
    if (opts?.gitignore !== undefined) out.gitignore = opts.gitignore;
    return out;
  }

  async function initRepo(repoId: RepoId, opts?: InitRepoOpts): Promise<void> {
    await storageInitRepo(repoDir(repoId), storageOptsFor(repoId, opts));
  }

  function getRepoDir(repoId: RepoId): string {
    return repoDir(repoId);
  }

  async function listRefs(
    principal: Principal,
    repoId: RepoId,
  ): Promise<RefEntry[]> {
    gateAccess(principal, repoId, "*", "resolveRef");
    const dir = repoDir(repoId);
    const repoExists = await fs.promises
      .stat(path.join(dir, ".git"))
      .then(() => true)
      .catch(() => false);
    if (!repoExists) return [];

    const [branches, tags] = await Promise.all([
      git.listBranches({ fs, dir }),
      git.listTags({ fs, dir }),
    ]);

    const names: string[] = [];
    for (const b of branches) names.push(`refs/heads/${b}`);
    for (const t of tags) names.push(`refs/tags/${t}`);
    names.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

    const entries: RefEntry[] = [];
    for (const name of names) {
      try {
        const sha = await git.resolveRef({ fs, dir, ref: name });
        entries.push({ name, sha });
      } catch (err: unknown) {
        if (hasCode(err) && err.code === "NotFoundError") continue;
        throw err;
      }
    }
    return entries;
  }

  async function resolveHead(
    principal: Principal,
    repoId: RepoId,
  ): Promise<{ symbolicTarget: string; sha: string } | null> {
    gateAccess(principal, repoId, "*", "resolveRef");
    const dir = repoDir(repoId);
    const repoExists = await fs.promises
      .stat(path.join(dir, ".git"))
      .then(() => true)
      .catch(() => false);
    if (!repoExists) return null;

    const symbolicTarget = await git.currentBranch({ fs, dir, fullname: true });
    if (symbolicTarget === undefined) return null;

    const sha = await resolveRefSha(dir, symbolicTarget);
    if (sha === null) return null;

    return { symbolicTarget, sha };
  }

  async function resolveRefSha(
    dir: string,
    ref: string,
  ): Promise<string | null> {
    try {
      return await git.resolveRef({ fs, dir, ref });
    } catch (err: unknown) {
      if (hasCode(err) && err.code === "NotFoundError") {
        return null;
      }
      throw err;
    }
  }

  // Walk from a tree object's root to the entry at `relPath`, matching
  // `expectedType`; null when a segment is missing or the final entry's
  // type mismatches. `""` resolves to the root tree (tree expected only).
  async function resolveTreeOid(
    dir: string,
    rootTreeOid: string,
    relPath: string,
    expectedType: "blob" | "tree",
  ): Promise<string | null> {
    if (relPath === "") {
      return expectedType === "tree" ? rootTreeOid : null;
    }
    const segments = relPath.split("/").filter((s) => s !== "");
    let currentOid = rootTreeOid;
    for (let i = 0; i < segments.length; i++) {
      const segment = segments[i];
      if (segment === undefined) throw new Error("unreachable");
      const isLast = i === segments.length - 1;
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid: currentOid,
      });
      const entry = tree.find((e) => e.path === segment);
      if (entry === undefined) return null;
      if (isLast) {
        if (entry.type !== expectedType) return null;
        return entry.oid;
      }
      if (entry.type !== "tree") return null;
      currentOid = entry.oid;
    }
    return currentOid;
  }

  // Walk from a commit's root tree to the entry at `relPath`; backs
  // both `priorReadBlob` and `priorListDir` so a handler's validatePush
  // does not duplicate the tree-walk per call.
  async function resolveTreeEntry(
    dir: string,
    commitSha: string,
    relPath: string,
    expectedType: "blob" | "tree",
  ): Promise<string | null> {
    const { commit } = await git.readCommit({
      fs,
      dir,
      cache: cacheFor(dir),
      oid: commitSha,
    });
    return resolveTreeOid(dir, commit.tree, relPath, expectedType);
  }

  // Build the prior-side closures fed into a kind handler's validatePush.
  // `commitSha` null surfaces the no-prior-commit state (readBlob null,
  // listDir empty); real read failures bubble rather than let an
  // append-only check silently degrade into an accept.
  function buildPriorTreeClosures(
    dir: string,
    commitSha: string | null,
  ): {
    priorReadBlob: (path: string) => Promise<Uint8Array | null>;
    priorListDir: (path: string) => Promise<string[]>;
    priorListDirOids: (
      path: string,
    ) => Promise<{ name: string; oid: string }[]>;
    readBlobByOid: (oid: string) => Promise<Uint8Array>;
  } {
    // Reads any blob by its object id, cache-backed; independent of
    // `commitSha`, so available even on the no-prior-commit branch.
    const readBlobByOid = async (oid: string): Promise<Uint8Array> => {
      const { blob } = await git.readBlob({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return blob;
    };
    if (commitSha === null) {
      return {
        priorReadBlob: async () => null,
        priorListDir: async () => [],
        priorListDirOids: async () => [],
        readBlobByOid,
      };
    }
    const priorReadBlob = async (
      relPath: string,
    ): Promise<Uint8Array | null> => {
      const oid = await resolveTreeEntry(dir, commitSha, relPath, "blob");
      if (oid === null) return null;
      const { blob } = await git.readBlob({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return blob;
    };
    const priorListDir = async (relPath: string): Promise<string[]> => {
      const oid = await resolveTreeEntry(dir, commitSha, relPath, "tree");
      if (oid === null) return [];
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return tree.map((e) => e.path);
    };
    // Same walk as `priorListDir` but carries each child's git object
    // id out of the listing, so a handler can prove a retained entry
    // byte-unchanged by OID without re-reading the blob.
    const priorListDirOids = async (
      relPath: string,
    ): Promise<{ name: string; oid: string }[]> => {
      const oid = await resolveTreeEntry(dir, commitSha, relPath, "tree");
      if (oid === null) return [];
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return tree.map((e) => ({ name: e.path, oid: e.oid }));
    };
    return { priorReadBlob, priorListDir, priorListDirOids, readBlobByOid };
  }

  // Build the prospective-tree closures one commit's validatePush sees,
  // parameterised on the commit OID so the substrate can walk per-commit
  // during a multi-commit pack.
  async function buildCommitTreeClosures(
    dir: string,
    commitSha: string,
  ): Promise<{
    topLevelTreePaths: string[];
    readBlob: (path: string) => Promise<Uint8Array>;
    listDir: (path: string) => Promise<string[]>;
  }> {
    const { commit } = await git.readCommit({
      fs,
      dir,
      cache: cacheFor(dir),
      oid: commitSha,
    });
    const { tree: rootTree } = await git.readTree({
      fs,
      dir,
      cache: cacheFor(dir),
      oid: commit.tree,
    });
    const topLevelTreePaths = rootTree.map((e) => e.path);
    const readBlob = async (relPath: string): Promise<Uint8Array> => {
      const oid = await resolveTreeEntry(dir, commitSha, relPath, "blob");
      if (oid === null) {
        throw new Error(
          `readBlob: path ${relPath} not found in commit ${commitSha} tree`,
        );
      }
      const { blob } = await git.readBlob({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return blob;
    };
    const listDir = async (relPath: string): Promise<string[]> => {
      if (relPath === "") return rootTree.map((e) => e.path);
      const oid = await resolveTreeEntry(dir, commitSha, relPath, "tree");
      // An absent directory lists as empty, matching the writeTree and
      // prior-side listDir: a handler walking an optional or scoped-but-
      // absent subtree hits its own guard and produces a clean
      // `path_violation` reason instead of a raw substrate throw. Real
      // read faults still bubble.
      if (oid === null) return [];
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return tree.map((e) => e.path);
    };
    return { topLevelTreePaths, readBlob, listDir };
  }

  // Read a tree's child entries as a name->oid map; null when the path
  // is absent or not a tree, so the diff treats "subtree gained/lost"
  // uniformly with "subtree changed".
  async function readTreeEntryMap(
    dir: string,
    commitSha: string,
    relPath: string,
  ): Promise<Map<string, string> | null> {
    const oid =
      relPath === ""
        ? (
            await git.readCommit({
              fs,
              dir,
              cache: cacheFor(dir),
              oid: commitSha,
            })
          ).commit.tree
        : await resolveTreeEntry(dir, commitSha, relPath, "tree");
    if (oid === null) return null;
    const { tree } = await git.readTree({ fs, dir, cache: cacheFor(dir), oid });
    const out = new Map<string, string>();
    for (const e of tree) out.set(e.path, e.oid);
    return out;
  }

  // Tree-entry modes an admitted tree must not carry: a symlink can
  // escape its directory at checkout; a submodule gitlink is always
  // dangling (a pushed pack ships no submodule object).
  const GIT_MODE_SYMLINK = "120000";
  const GIT_MODE_SUBMODULE = "160000";

  // First disallowed-mode (symlink/submodule) entry in a commit's tree,
  // or null. Mirrors the materialization backstop but at the push
  // boundary, so a checkout-escaping entry is refused at admission.
  // `seen` dedupes shared subtree oids across one pack's commits.
  async function findDisallowedTreeMode(
    dir: string,
    treeOid: string,
    seen: Set<string>,
    prefix = "",
  ): Promise<{ path: string; kind: "symlink" | "submodule" } | null> {
    if (seen.has(treeOid)) return null;
    seen.add(treeOid);
    const { tree } = await git.readTree({
      fs,
      dir,
      cache: cacheFor(dir),
      oid: treeOid,
    });
    for (const entry of tree) {
      const entryPath = prefix === "" ? entry.path : `${prefix}/${entry.path}`;
      if (entry.mode === GIT_MODE_SYMLINK) {
        return { path: entryPath, kind: "symlink" };
      }
      if (entry.type === "commit" || entry.mode === GIT_MODE_SUBMODULE) {
        return { path: entryPath, kind: "submodule" };
      }
      if (entry.type === "tree") {
        const nested = await findDisallowedTreeMode(
          dir,
          entry.oid,
          seen,
          entryPath,
        );
        if (nested !== null) return nested;
      }
    }
    return null;
  }

  // Bound the paths a received-pack commit may have changed, as
  // repo-root-relative prefixes ending in `/`. Content-addressing means a
  // subtree whose oid is unchanged is byte-identical, so only differing
  // entries descend; `runs/` is descended one level further so a per-run
  // handler scopes to the exact `runs/<runId>/` directories that changed.
  // Unreadable parent (null) = unbounded change set, returns undefined.
  async function computeChangedPathPrefixes(
    dir: string,
    commitSha: string,
    parentSha: string | null,
  ): Promise<Set<string> | undefined> {
    if (parentSha === null) return undefined;
    const prefixes = new Set<string>();
    const newTop = (await readTreeEntryMap(dir, commitSha, "")) ?? new Map();
    const oldTop = (await readTreeEntryMap(dir, parentSha, "")) ?? new Map();
    const topNames = new Set<string>([...newTop.keys(), ...oldTop.keys()]);
    for (const name of topNames) {
      if (newTop.get(name) === oldTop.get(name)) continue;
      if (name === "runs") {
        const newRuns =
          (await readTreeEntryMap(dir, commitSha, "runs")) ?? new Map();
        const oldRuns =
          (await readTreeEntryMap(dir, parentSha, "runs")) ?? new Map();
        const runNames = new Set<string>([
          ...newRuns.keys(),
          ...oldRuns.keys(),
        ]);
        for (const runId of runNames) {
          if (newRuns.get(runId) === oldRuns.get(runId)) continue;
          prefixes.add(`runs/${runId}/`);
        }
        continue;
      }
      prefixes.add(`${name}/`);
    }
    return prefixes;
  }

  // Enumerate every commit OID reachable from any branch or tag — the
  // pre-pack "old" history boundary. Empty when the repo's `.git` has no
  // refs yet. Read failures bubble; a silent empty would re-validate
  // commits the kind handler already accepted.
  async function snapshotExistingCommits(dir: string): Promise<Set<string>> {
    const out = new Set<string>();
    const visit = async (start: string): Promise<void> => {
      const stack: string[] = [start];
      while (stack.length > 0) {
        const oid = stack.pop();
        if (oid === undefined) break;
        if (out.has(oid)) continue;
        let parsed: Awaited<ReturnType<typeof git.readCommit>>;
        try {
          parsed = await git.readCommit({ fs, dir, cache: cacheFor(dir), oid });
        } catch (err) {
          // A previously-received single-commit pack may leave the
          // tip's `parent` field pointing at a SHA the receiver has
          // never seen (the producer ships only the tip's tree, not
          // its ancestor commits). That dangling parent is structural
          // for this repo kind, not a corruption, so the walk stops
          // at the missing node instead of erroring.
          if (hasCode(err) && err.code === "NotFoundError") continue;
          throw err;
        }
        out.add(oid);
        for (const parent of parsed.commit.parent) {
          if (!out.has(parent)) stack.push(parent);
        }
      }
    };
    const branches = await git.listBranches({ fs, dir });
    for (const b of branches) {
      const sha = await resolveRefSha(dir, `refs/heads/${b}`);
      if (sha !== null) await visit(sha);
    }
    const tags = await git.listTags({ fs, dir });
    for (const t of tags) {
      const sha = await resolveRefSha(dir, `refs/tags/${t}`);
      if (sha !== null) await visit(sha);
    }
    return out;
  }

  // Walk parent links from `tipSha` to the new commits a pack just
  // published, oldest-first, tip last: readable, not in `existingCommits`,
  // and not the CAS-pinned `expectedOldSha`. A parent whose commit object
  // is absent (the deploy-pack shape packs only the tip, not its
  // ancestors) is the history boundary — the walk stops there. First
  // parent only; merge commits are refused as structural defects.
  async function collectNewCommits(
    dir: string,
    tipSha: string,
    expectedOldSha: string | null,
    existingCommits: ReadonlySet<string>,
  ): Promise<string[]> {
    const chain: string[] = [];
    let current: string = tipSha;
    while (true) {
      if (current === expectedOldSha) break;
      if (existingCommits.has(current)) break;
      let parsed: Awaited<ReturnType<typeof git.readCommit>>;
      try {
        parsed = await git.readCommit({
          fs,
          dir,
          cache: cacheFor(dir),
          oid: current,
        });
      } catch (err) {
        if (hasCode(err) && err.code === "NotFoundError") break;
        throw err;
      }
      chain.push(current);
      const parents = parsed.commit.parent;
      if (parents.length === 0) break;
      if (parents.length > 1) {
        throw new Error(
          `pack_walk_multi_parent: commit ${current} has ${String(parents.length)} parents; merge commits are not supported in repo-store packs`,
        );
      }
      const next = parents[0];
      if (next === undefined) throw new Error("unreachable");
      current = next;
    }
    return chain.reverse();
  }

  // Assemble a new root tree by splicing `puts` (repo-root-relative
  // path -> blob oid) and `deletes` onto the parent's root tree, reusing
  // every unchanged entry by oid; the result is committed directly, so
  // the on-disk index is never touched.
  //
  // A no-slash delete names an exact path and clears the blob there;
  // deleting a directory with one is `delete_type_mismatch`. A
  // trailing-slash delete names a subtree prefix and clears it;
  // trailing-slash over a base blob is `delete_type_mismatch`, unless a
  // put drives the same descent (a legitimate file-to-directory
  // replacement — the put wins). A put overrides a delete of the same
  // path; deleting an absent path is an idempotent no-op. Recursion is
  // scoped to the touched subtrees, so cost tracks the change size, not
  // the repo. Returns the new tree oid, or null when the subtree ends
  // up empty.
  async function assembleTree(
    dir: string,
    baseTreeOid: string | null,
    prefix: string,
    puts: ReadonlyMap<string, string>,
    deletes: ReadonlySet<string>,
  ): Promise<string | null> {
    // A subtree-delete naming exactly this node drops the base wholesale;
    // only puts under it survive.
    const cleared = prefix !== "" && deletes.has(prefix);
    const baseEntries = new Map<
      string,
      { mode: string; oid: string; type: TreeEntry["type"] }
    >();
    if (baseTreeOid !== null && !cleared) {
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid: baseTreeOid,
      });
      for (const e of tree) {
        baseEntries.set(e.path, { mode: e.mode, oid: e.oid, type: e.type });
      }
    }

    // Classify this level's changes: direct blob puts, exact-file
    // deletes, and subtrees a put or a delete descends into.
    const blobPutsHere = new Set<string>();
    const subtreeNames = new Set<string>();
    const subtreePutNames = new Set<string>();
    const fileDeletesHere = new Set<string>();
    for (const full of puts.keys()) {
      if (prefix !== "" && !full.startsWith(prefix)) continue;
      const rest = full.slice(prefix.length);
      if (rest.length === 0) continue;
      const slash = rest.indexOf("/");
      if (slash === -1) blobPutsHere.add(rest);
      else {
        const child = rest.slice(0, slash);
        subtreeNames.add(child);
        subtreePutNames.add(child);
      }
    }
    for (const del of deletes) {
      if (prefix !== "" && !del.startsWith(prefix)) continue;
      const rest = del.slice(prefix.length);
      if (rest.length === 0) continue; // del === prefix, handled by `cleared`
      const slash = rest.indexOf("/");
      if (slash === -1) fileDeletesHere.add(rest);
      else subtreeNames.add(rest.slice(0, slash));
    }

    // A name written both as a direct blob and as a directory (a put/base
    // `foo` plus a put or delete under `foo/`) is contradictory; the blob
    // branch would silently win, so reject loudly. Covers every caller —
    // writeTree's files, preserving-prefix's merge output, and the delta's
    // puts/deletes — since all funnel through this classification.
    for (const name of blobPutsHere) {
      if (subtreeNames.has(name)) {
        throw new Error(
          `tree_name_collision: ${JSON.stringify(
            prefix + name,
          )} is written both as a file and as a directory`,
        );
      }
    }

    const names = new Set<string>([
      ...baseEntries.keys(),
      ...blobPutsHere,
      ...subtreeNames,
    ]);
    const entries: TreeEntry[] = [];
    for (const name of names) {
      const full = prefix + name;
      const putOid = puts.get(full);
      if (putOid !== undefined) {
        // Puts override whatever the base held and any delete of the same
        // path, always as mode 100644 blobs. A future caller needing to
        // preserve another mode would have to carry it through `puts`.
        entries.push({
          mode: "100644",
          path: name,
          oid: putOid,
          type: "blob",
        });
        continue;
      }
      if (fileDeletesHere.has(name)) {
        const base = baseEntries.get(name);
        if (base !== undefined && base.type === "tree") {
          throw new Error(
            `delete_type_mismatch: ${JSON.stringify(
              prefix + name,
            )} is deleted as a file but is a directory in the base tree`,
          );
        }
        continue; // file removed
      }
      if (subtreeNames.has(name)) {
        const base = baseEntries.get(name);
        // A trailing-slash delete descending into a base blob
        // contradicts the base type — reject it, unless a put drives the
        // descent (`!subtreePutNames` carve-out): a `put` under `name/`
        // is a file-to-directory replacement, not a delete mismatch.
        // That replacement still dead-ends at working-tree
        // materialization (mkdir over the base file EEXISTs), a separate
        // unreachable limitation that must not be masked by mislabeling
        // the put as a delete mismatch.
        if (
          base !== undefined &&
          base.type === "blob" &&
          !subtreePutNames.has(name)
        ) {
          throw new Error(
            `delete_type_mismatch: ${JSON.stringify(
              prefix + name,
            )} is deleted as a subtree but is a file in the base tree`,
          );
        }
        const baseChildOid =
          base !== undefined && base.type === "tree" ? base.oid : null;
        const childOid = await assembleTree(
          dir,
          baseChildOid,
          `${full}/`,
          puts,
          deletes,
        );
        if (childOid !== null) {
          entries.push({
            mode: "040000",
            path: name,
            oid: childOid,
            type: "tree",
          });
        }
        continue;
      }
      const base = baseEntries.get(name);
      if (base === undefined) continue;
      entries.push({
        mode: base.mode,
        path: name,
        oid: base.oid,
        type: base.type,
      });
    }
    if (entries.length === 0) return null;
    return await git.writeTree({ fs, dir, tree: entries });
  }

  // Prospective-side validatePush closures over the assembled tree oid,
  // so a handler sees exactly the tree the commit will carry. Assembly
  // wrote only unreferenced objects, so reading them advances nothing.
  function buildTreeReadClosures(
    dir: string,
    rootTreeOid: string,
  ): {
    topLevelTreePaths: () => Promise<string[]>;
    readBlob: (relPath: string) => Promise<Uint8Array>;
    listDir: (relPath: string) => Promise<string[]>;
    listDirOids: (relPath: string) => Promise<{ name: string; oid: string }[]>;
  } {
    const topLevelTreePaths = async (): Promise<string[]> => {
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid: rootTreeOid,
      });
      return tree.map((e) => e.path);
    };
    const readBlob = async (relPath: string): Promise<Uint8Array> => {
      const oid = await resolveTreeOid(dir, rootTreeOid, relPath, "blob");
      if (oid === null) {
        throw new Error(
          `readBlob: path ${relPath} not present in prospective tree`,
        );
      }
      const { blob } = await git.readBlob({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return blob;
    };
    const listDir = async (relPath: string): Promise<string[]> => {
      const oid =
        relPath === ""
          ? rootTreeOid
          : await resolveTreeOid(dir, rootTreeOid, relPath, "tree");
      if (oid === null) return [];
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return tree.map((e) => e.path);
    };
    // Same walk as `listDir` but carries each child's git object id out
    // of the assembled tree's listing, mirroring the prior side's
    // `priorListDirOids`, so a handler can prove byte-unchanged retention
    // by OID without re-reading every blob.
    const listDirOids = async (
      relPath: string,
    ): Promise<{ name: string; oid: string }[]> => {
      const oid =
        relPath === ""
          ? rootTreeOid
          : await resolveTreeOid(dir, rootTreeOid, relPath, "tree");
      if (oid === null) return [];
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return tree.map((e) => ({ name: e.path, oid: e.oid }));
    };
    return { topLevelTreePaths, readBlob, listDir, listDirOids };
  }

  // Unlocked body of writeTree; the caller holds the per-repo lock.
  // Extracted so writeTreePreservingPrefix can run its read-then-merge
  // step under the same lock without nested acquisitions.
  async function writeTreeUnderLock(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    w: {
      files: Record<string, string | Uint8Array>;
      deletes: ReadonlySet<string>;
      changedPathPrefixes: ReadonlySet<string> | undefined;
      message: string;
    },
    // When present, the parent tip already resolved under this lock: the
    // delta path pins it once and hands the SAME oid to both computeDelta
    // (its dedup reads) and this assembly, so the dedup snapshot, the
    // committed tree, and validation are provably one pre-image. `{ sha }`
    // wraps the value so a genuinely-null pin is distinct from "not
    // provided".
    pinnedParent?: { sha: string | null },
  ): Promise<WriteResult> {
    const dir = repoDir(repoId);
    await storageInitRepo(dir, storageOptsFor(repoId, undefined));

    const handler = handlerFor(repoId);

    // Assemble the commit's tree directly and commit that tree oid, never
    // staging into the on-disk index (a single repo-global structure
    // shared across refs whose rebuild cost scaled with history). Splicing
    // from the parent's root tree keeps the per-commit cost tracking the
    // change rather than the accumulated history.

    // Pin the parent under the lock so the splice runs against the ref's
    // tip, race-free; a ref that does not yet exist starts from empty and
    // parents on HEAD.
    const parentCommitSha =
      pinnedParent !== undefined
        ? pinnedParent.sha
        : await resolveRefSha(dir, ref);
    let baseRootTreeOid: string | null = null;
    if (parentCommitSha !== null) {
      const { commit } = await git.readCommit({
        fs,
        dir,
        cache: cacheFor(dir),
        oid: parentCommitSha,
      });
      baseRootTreeOid = commit.tree;
    }

    // Write the put blobs, then splice them and `w.deletes` onto the
    // parent root tree. writeBlob/writeTree emit unreferenced objects;
    // nothing the ref can reach moves until the commit lands.
    const puts = new Map<string, string>();
    for (const [relPath, contents] of Object.entries(w.files)) {
      const bytes =
        typeof contents === "string"
          ? new TextEncoder().encode(contents)
          : contents;
      const oid = await git.writeBlob({ fs, dir, blob: bytes });
      puts.set(relPath, oid);
    }
    const assembled = await assembleTree(
      dir,
      baseRootTreeOid,
      "",
      puts,
      w.deletes,
    );
    const newRootTreeOid =
      assembled ?? (await git.writeTree({ fs, dir, tree: [] }));

    // validatePush sees the full prospective tree via closures over the
    // assembled tree oid and the prior tree over the parent commit;
    // `w.changedPathPrefixes` is the handler's scoping hint.
    const { priorReadBlob, priorListDir, priorListDirOids } =
      buildPriorTreeClosures(dir, parentCommitSha);
    const prospective = buildTreeReadClosures(dir, newRootTreeOid);
    const changedPathPrefixes = w.changedPathPrefixes;
    // No disallowed-mode (symlink/submodule) gate here on purpose: the
    // substrate mode gate lives on the pack-receive path. This write path
    // cannot produce those modes — assembleTree writes puts as 100644 and
    // passes base entries through unchanged, and no ingress admits them —
    // so a walk here would gate a tree that cannot carry them.
    const validation = await handler.validatePush({
      repoId,
      ref,
      principal,
      topLevelTreePaths: await prospective.topLevelTreePaths(),
      readBlob: prospective.readBlob,
      listDir: prospective.listDir,
      listDirOids: prospective.listDirOids,
      priorReadBlob,
      priorListDir,
      priorListDirOids,
      changedPathPrefixes,
    });
    if (!validation.ok) {
      // Nothing was staged and no ref advanced — the assembly wrote only
      // unreferenced objects, which the next GC reclaims — so the refusal
      // just surfaces.
      throw new Error(`path_violation: ${validation.reason}`);
    }

    // Materialize the working tree for the touched paths only (O(change)):
    // remove each deleted path, then write each put file. Some consumers
    // (the workflow-run claim-check scan) read these files straight from
    // disk, so the working tree must mirror the committed change. Runs
    // after validation passes, so a rejected push leaves the working tree
    // untouched. `rm` with `force` no-ops a missing path; `recursive`
    // covers both a file delete and a subtree-prefix delete.
    for (const del of w.deletes) {
      await fs.promises.rm(path.join(dir, del), {
        recursive: true,
        force: true,
      });
    }
    for (const [relPath, contents] of Object.entries(w.files)) {
      const fullPath = path.join(dir, relPath);
      await fs.promises.mkdir(path.dirname(fullPath), { recursive: true });
      await fs.promises.writeFile(fullPath, contents);
    }

    // Parent is the pinned tip, or HEAD for a first write; `oldSha` is
    // null only when the ref truly does not exist.
    const oldSha = parentCommitSha;
    const parentSha =
      parentCommitSha ?? (await git.resolveRef({ fs, dir, ref: "HEAD" }));

    const commitSha = await git.commit({
      fs,
      dir,
      cache: cacheFor(dir),
      tree: newRootTreeOid,
      message: w.message,
      author: AUTHOR,
      parent: [parentSha],
      ref,
      signingKey: "sshsig",
      onSign: async ({ payload }) => ({
        signature: await createSSHSignature(
          payload,
          signingKey.privateKey,
          signingKey.publicKey,
        ),
      }),
    });

    const cachedExisting = existingCommitsCache.get(indexCacheKey(repoId));
    if (cachedExisting !== undefined) cachedExisting.add(commitSha);
    await handler.onRefUpdated({ repoId, ref, oldSha, newSha: commitSha });
    await emitRefUpdate(repoId, ref, oldSha, commitSha);

    await maybeRunGC(repoId);

    return { commitSha, newlyTerminalRuns: validation.newlyTerminalRuns ?? [] };
  }

  // Normalize `TreeContent` into the puts/deletes/scope shape: a
  // clearPrefix becomes a single subtree-delete and the change scope;
  // otherwise a purely-additive write validated in full.
  function normalizeTreeContent(content: TreeContent): {
    files: Record<string, string | Uint8Array>;
    deletes: ReadonlySet<string>;
    changedPathPrefixes: ReadonlySet<string> | undefined;
    message: string;
  } {
    if (content.clearPrefix !== undefined) {
      validateClearPrefix(content.clearPrefix);
      return {
        files: content.files,
        deletes: new Set([content.clearPrefix]),
        changedPathPrefixes: new Set([content.clearPrefix]),
        message: content.message,
      };
    }
    return {
      files: content.files,
      deletes: new Set<string>(),
      changedPathPrefixes: undefined,
      message: content.message,
    };
  }

  async function writeTree(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    content: TreeContent,
  ): Promise<WriteResult> {
    gateAccess(principal, repoId, ref, "writeTree");

    // The lock spans the entire writeTree body — assembly, validatePush,
    // commit, onRefUpdated — serializing post-update consumers against
    // the same ref's next writer.
    return withRepoLock(repoId, () =>
      writeTreeUnderLock(principal, repoId, ref, normalizeTreeContent(content)),
    );
  }

  // Blobs directly under `prefix` at `ref`, as repo-root-relative paths
  // including the prefix; the empty map covers the ref-missing /
  // prefix-missing first-write states.
  async function readPrefixBlobs(
    repoId: RepoId,
    ref: string,
    prefix: string,
  ): Promise<Map<string, Uint8Array>> {
    const dir = repoDir(repoId);
    const out = new Map<string, Uint8Array>();
    const repoExists = await fs.promises
      .stat(path.join(dir, ".git"))
      .then(() => true)
      .catch(() => false);
    if (!repoExists) return out;
    const commitSha = await resolveRefSha(dir, ref);
    if (commitSha === null) return out;
    const { commit } = await git.readCommit({
      fs,
      dir,
      cache: cacheFor(dir),
      oid: commitSha,
    });
    let currentOid = commit.tree;
    const segments = prefix
      .replace(/\/$/, "")
      .split("/")
      .filter((s) => s !== "");
    for (const segment of segments) {
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid: currentOid,
      });
      const entry = tree.find((e) => e.path === segment);
      if (entry === undefined || entry.type !== "tree") {
        return out;
      }
      currentOid = entry.oid;
    }
    const { tree } = await git.readTree({
      fs,
      dir,
      cache: cacheFor(dir),
      oid: currentOid,
    });
    // One tree read plus one readBlob per entry; fine at current scale
    // (single-digit tarballs per registry), the obvious optimization
    // target when a registry grows.
    for (const entry of tree) {
      if (entry.type !== "blob") continue;
      const { blob } = await git.readBlob({
        fs,
        dir,
        cache: cacheFor(dir),
        oid: commitSha,
        filepath: `${prefix}${entry.path}`,
      });
      out.set(`${prefix}${entry.path}`, blob);
    }
    return out;
  }

  async function writeTreePreservingPrefix(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    args: WriteTreePreservingPrefixArgs,
  ): Promise<WriteResult> {
    gateAccess(principal, repoId, ref, "writeTree");
    validateClearPrefix(args.preservePrefix);
    return withRepoLock(repoId, async () => {
      // Read and merge inside the lock so concurrent callers targeting
      // the same prefix observe each other's commits in serial order —
      // no lost-update window between the read and the writeTree.
      await storageInitRepo(repoDir(repoId), storageOptsFor(repoId, undefined));
      const existing = await readPrefixBlobs(repoId, ref, args.preservePrefix);
      const files = await args.merge(existing);
      return writeTreeUnderLock(principal, repoId, ref, {
        files,
        deletes: new Set([args.preservePrefix]),
        changedPathPrefixes: new Set([args.preservePrefix]),
        message: args.message,
      });
    });
  }

  // Commit a targeted delta: `computeDelta` runs under the per-repo lock
  // against the pinned parent tip and returns the exact files to put and
  // paths to delete; everything else is carried forward by oid. Unlike
  // writeTreePreservingPrefix (which clears and rebuilds a whole prefix),
  // a delta touches only the entries it names, so mutating one file in a
  // large directory does not re-hash the untouched siblings.
  // `changedPathPrefixes` is the caller-supplied scoping hint for the
  // touched region, since a delta has no single clear-prefix to derive it
  // from.
  async function writeTreeDelta(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    args: WriteTreeDeltaArgs,
  ): Promise<WriteResult> {
    gateAccess(principal, repoId, ref, "writeTree");
    return withRepoLock(repoId, async () => {
      const dir = repoDir(repoId);
      await storageInitRepo(dir, storageOptsFor(repoId, undefined));
      // Pin the parent tip once under the lock and hand the same oid to
      // computeDelta's dedup reads and the assembly below — one pre-image,
      // no lost-update window, no second resolve that could observe a
      // different tip.
      const parentCommitSha = await resolveRefSha(dir, ref);
      const { priorListDirOids, readBlobByOid } = buildPriorTreeClosures(
        dir,
        parentCommitSha,
      );
      const delta = await args.computeDelta(parentCommitSha, {
        readBlobByOid,
        listDirOids: priorListDirOids,
      });
      for (const p of Object.keys(delta.puts)) validateDeltaPath(p, false);
      for (const d of delta.deletes) validateDeltaPath(d, true);
      assertDeltaUnambiguous(delta.puts, delta.deletes);
      assertDeltaScoped(delta.puts, delta.deletes, args.changedPathPrefixes);
      return writeTreeUnderLock(
        principal,
        repoId,
        ref,
        {
          files: delta.puts,
          deletes: new Set(delta.deletes),
          changedPathPrefixes: args.changedPathPrefixes,
          message: args.message,
        },
        { sha: parentCommitSha },
      );
    });
  }

  async function receivePack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
    expectedOldSha: string | null,
  ): Promise<NewlyTerminalRun[]> {
    gateAccess(principal, repoId, ref, "receivePack");

    // The lock spans the entire receivePack body — packfile index, CAS
    // check against `expectedOldSha`, per-commit validation, ref write,
    // onRefUpdated. `oldSha` comes from receivePackObjects so the hook
    // sees the same pre-image the CAS read observed.
    return withRepoLock(repoId, async () => {
      const dir = repoDir(repoId);
      await storageInitRepo(dir, storageOptsFor(repoId, undefined));

      const handler = handlerFor(repoId);
      const transferId = crypto.randomUUID().replace(/-/g, "");

      const existingKey = indexCacheKey(repoId);
      let existingCommits = existingCommitsCache.get(existingKey);
      if (existingCommits === undefined) {
        existingCommits = await snapshotExistingCommits(dir);
        existingCommitsCache.set(existingKey, existingCommits);
      }
      const newCommitsFromPack: string[] = [];
      const newlyTerminalRuns: NewlyTerminalRun[] = [];

      const oldSha = await receivePackObjects(
        dir,
        pack,
        ref,
        commitSha,
        transferId,
        expectedOldSha,
        // A pack may carry more than one new commit (e.g. supervisor
        // bootstrap batching enqueue + dequeue before the hub has the
        // workflow-run repo bootstrapped). The prior-tree closures must
        // point at each commit's own parent, so walk the parent chain
        // from tip back to the pre-pack history and validate each new
        // commit oldest-first; a single-commit pack collapses to the
        // tip-only path.
        async () => {
          const newCommits = await collectNewCommits(
            dir,
            commitSha,
            expectedOldSha,
            existingCommits,
          );
          newCommitsFromPack.push(...newCommits);
          // Shared across the pack's commits so a subtree common to
          // several is walked once by the disallowed-mode gate below.
          const seenTreeOids = new Set<string>();
          for (const newCommit of newCommits) {
            const { commit: parsed } = await git.readCommit({
              fs,
              dir,
              cache: cacheFor(dir),
              oid: newCommit,
            });
            const parents = parsed.parent;
            const declaredParent =
              parents.length === 0 ? null : (parents[0] ?? null);
            // A commit declaring a parent the receiver lacks cannot be
            // validated against its true prior tree; silently degrading
            // to "empty prior" would let an overwrite of an immutable
            // prior-tree entry slip through. The workflow-run producer
            // ships the full parent chain, so this is rejected outright
            // for that kind; other kinds ship deploy-shape packs (tip +
            // tree) whose handlers do not read prior bytes, so a
            // dangling parent collapses to the no-prior path for them.
            let parentSha: string | null = null;
            if (declaredParent !== null) {
              try {
                await git.readCommit({
                  fs,
                  dir,
                  cache: cacheFor(dir),
                  oid: declaredParent,
                });
                parentSha = declaredParent;
              } catch (err) {
                if (!hasCode(err) || err.code !== "NotFoundError") throw err;
                if (repoId.kind === "workflow-run") {
                  throw new Error(
                    `pack_walk_dangling_parent: commit ${newCommit} declares parent ${declaredParent} which is neither in the receiver's store nor in the pack`,
                  );
                }
              }
            }
            const { priorReadBlob, priorListDir, priorListDirOids } =
              buildPriorTreeClosures(dir, parentSha);
            const { topLevelTreePaths, readBlob, listDir } =
              await buildCommitTreeClosures(dir, newCommit);
            const changedPathPrefixes = await computeChangedPathPrefixes(
              dir,
              newCommit,
              parentSha,
            );
            // Admission gate for EVERY kind, before the per-kind
            // validatePush: refuse a tree carrying a symlink (which can
            // escape its directory at checkout) or a submodule (a
            // dangling gitlink) so the ref never advances. Per-commit,
            // so an intermediate commit adding a symlink a later commit
            // deletes is still caught.
            const disallowed = await findDisallowedTreeMode(
              dir,
              parsed.tree,
              seenTreeOids,
            );
            if (disallowed !== null) {
              // The substrate owns the `path_violation:` prefix on the
              // thrown message, so the reason omits it (matching every
              // handler reason).
              const reason = `${disallowed.kind} at ${disallowed.path} is not allowed in a pushed asset tree (commit ${newCommit})`;
              logger.debug`validatePush rejected ${repoId.kind}/${repoId.id} on ${ref} at commit ${newCommit}: ${reason}`;
              return { ok: false, reason };
            }
            const result = await handler.validatePush({
              repoId,
              ref,
              principal,
              topLevelTreePaths,
              readBlob,
              listDir,
              priorReadBlob,
              priorListDir,
              priorListDirOids,
              changedPathPrefixes,
            });
            if (!result.ok) {
              logger.debug`validatePush rejected ${repoId.kind}/${repoId.id} on ${ref} at commit ${newCommit}: ${result.reason}`;
              return { ok: false, reason: result.reason };
            }
            if (result.newlyTerminalRuns !== undefined) {
              newlyTerminalRuns.push(...result.newlyTerminalRuns);
            }
          }
          return true;
        },
      );

      // receivePackObjects wrote new objects and advanced the ref
      // straight to disk without threading the memoization cache, so
      // drop the dir's cache; the next read rebuilds against the packed
      // objects and the new tip.
      invalidateGitCache(dir);
      for (const sha of newCommitsFromPack) existingCommits.add(sha);
      await handler.onRefUpdated({ repoId, ref, oldSha, newSha: commitSha });
      await emitRefUpdate(repoId, ref, oldSha, commitSha);

      await maybeRunGC(repoId);

      return newlyTerminalRuns;
    });
  }

  async function createPack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    gateAccess(principal, repoId, ref, "createPack");
    // `createDeployPack` packs only the tip commit + its tree, which
    // covers the deploy-pack shape every other kind ships (the receiver
    // starts from genesis and applies the tree wholesale). The
    // workflow-run kind ships incrementally and the receiver needs to
    // validate per-commit transitions against the sender's prior tree, so
    // its pack must carry the full parent chain from the ref's tip.
    if (repoId.kind === "workflow-run") {
      const dir = repoDir(repoId);
      const commitSha = await git.resolveRef({ fs, dir, ref });
      const tipKey = lastPackedTipKey(repoId, ref);
      const stopAt = lastPackedTip.get(tipKey) ?? null;
      const oids = await collectChainReachableObjects(dir, commitSha, stopAt);
      const result = await git.packObjects({
        fs,
        dir,
        cache: cacheFor(dir),
        oids,
        write: false,
      });
      if (result.packfile === undefined) {
        throw new Error(
          `packObjects returned no packfile for ref "${ref}" (${commitSha})`,
        );
      }
      // The cursor is NOT advanced here. Building a pack only produces
      // bytes; the shipped tip advances in `commitPackedTip` once the
      // receiver acks the transfer, so a cancelled-before-ack transfer
      // leaves the cursor where it was and the next `createPack` re-ships
      // the un-acked commits — the receiver still gets a self-consistent
      // chain instead of a pack whose base commit it never received.
      return { pack: result.packfile, commitSha, ref };
    }
    const { pack, commitSha } = await createDeployPack(repoDir(repoId), ref);
    return { pack, commitSha, ref };
  }

  // Advance the incremental-pack cursor for `(repoId, ref)` to
  // `commitSha`, called once the receiver acks the transfer that shipped
  // it. Gating the advance on the ack — not on building the pack — lets
  // a cancelled-before-ack transfer be re-shipped cleanly: the cursor
  // stays put, so the next `createPack` re-includes the un-acked commits.
  // No-op for every kind that does not ship incremental packs.
  function commitPackedTip(repoId: RepoId, ref: string, commitSha: string) {
    if (repoId.kind !== "workflow-run") return;
    lastPackedTip.set(lastPackedTipKey(repoId, ref), commitSha);
  }

  // Collect every object OID reachable from `tipSha` and every ancestor
  // commit along its first-parent chain — what the upload-pack layer's
  // negotiated walker would produce when the requester advertises no
  // `haves`. The walk stops at the first parent whose commit object is
  // not in the local store: by definition not part of local history.
  async function collectChainReachableObjects(
    dir: string,
    tipSha: string,
    stopAt: string | null,
  ): Promise<string[]> {
    const seen = new Set<string>();
    let current: string | null = tipSha;
    while (current !== null) {
      if (current === stopAt) break;
      let perCommit = chainReachabilityCache.get(current);
      if (perCommit === undefined) {
        perCommit = await collectReachableObjects(dir, current);
        chainReachabilityCache.set(current, perCommit);
      }
      for (const o of perCommit) seen.add(o);
      let parsed: Awaited<ReturnType<typeof git.readCommit>>;
      try {
        parsed = await git.readCommit({
          fs,
          dir,
          cache: cacheFor(dir),
          oid: current,
        });
      } catch (err) {
        if (hasCode(err) && err.code === "NotFoundError") break;
        throw err;
      }
      const parents = parsed.commit.parent;
      if (parents.length === 0) break;
      const next = parents[0];
      if (next === undefined) throw new Error("unreachable");
      current = next;
    }
    return Array.from(seen);
  }

  async function resolveRef(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<string | null> {
    gateAccess(principal, repoId, ref, "resolveRef");
    return resolveRefSha(repoDir(repoId), ref);
  }

  // Build the committed-read closures pinned to a commit. Shared by the
  // by-ref (`openCommittedReads`) and by-commit
  // (`openCommittedReadsAtCommit`) entrypoints, which differ only in how
  // they obtain and validate the commit SHA.
  function committedReadsAt(dir: string, commitSha: string): CommittedReads {
    const { readBlobByOid } = buildPriorTreeClosures(dir, commitSha);
    const treeOid = async (relPath: string): Promise<string | null> => {
      // "." and "" both name the root tree; normalize "." to "".
      const normalized = relPath === "." ? "" : relPath;
      return resolveTreeEntry(dir, commitSha, normalized, "tree");
    };
    const listDir = async (relPath: string): Promise<CommittedTreeEntry[]> => {
      const oid = await treeOid(relPath);
      if (oid === null) return [];
      const { tree } = await git.readTree({
        fs,
        dir,
        cache: cacheFor(dir),
        oid,
      });
      return tree.map((e) => ({ name: e.path, oid: e.oid, type: e.type }));
    };
    return { listDir, readBlobByOid, treeOid };
  }

  async function repoHasGitDir(dir: string): Promise<boolean> {
    return fs.promises
      .stat(path.join(dir, ".git"))
      .then(() => true)
      .catch(() => false);
  }

  async function openCommittedReads(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<CommittedReads | null> {
    gateAccess(principal, repoId, ref, "resolveRef");
    const dir = repoDir(repoId);
    if (!(await repoHasGitDir(dir))) return null;
    const commitSha = await resolveRefSha(dir, ref);
    if (commitSha === null) return null;
    return committedReadsAt(dir, commitSha);
  }

  async function openCommittedReadsAtCommit(
    principal: Principal,
    repoId: RepoId,
    commitSha: string,
  ): Promise<CommittedReads | null> {
    // No single ref to gate on; use the same `"*"`/`resolveRef`
    // bulk-read gate that `listRefs` and `resolveHead` use.
    gateAccess(principal, repoId, "*", "resolveRef");
    if (!/^[0-9a-f]{40}$/.test(commitSha)) {
      throw new Error(`commit_sha_invalid: ${commitSha}`);
    }
    const dir = repoDir(repoId);
    if (!(await repoHasGitDir(dir))) return null;
    // Confirm the commit object is present so a pruned commit reads as
    // a clean null rather than a lazy throw on the first tree walk.
    const present = await git
      .readCommit({ fs, dir, cache: cacheFor(dir), oid: commitSha })
      .then(() => true)
      .catch((err: unknown) => {
        if (hasCode(err) && err.code === "NotFoundError") return false;
        throw err;
      });
    if (!present) return null;
    return committedReadsAt(dir, commitSha);
  }

  function subscribe(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    opts: {
      signal: AbortSignal;
      from: "head" | { seq: number };
      bufferLimit?: number;
    },
  ): AsyncIterableIterator<{ seq: number; event: unknown }> {
    gateAccess(principal, repoId, ref, "resolveRef");

    const bufferLimit = opts.bufferLimit ?? DEFAULT_SUBSCRIBE_BUFFER_LIMIT;
    if (!Number.isInteger(bufferLimit) || bufferLimit <= 0) {
      throw new Error(
        `subscribe_buffer_limit_invalid: ${String(opts.bufferLimit)}`,
      );
    }

    const sub: SubscriberState = {
      bufferLimit,
      buffer: [],
      closed: false,
      error: null,
      waiter: null,
    };

    const key = refKey(repoId, ref);
    let set = subscribers.get(key);
    if (set === undefined) {
      set = new Set();
      subscribers.set(key, set);
    }
    set.add(sub);

    const removeSubscriber = () => {
      const current = subscribers.get(key);
      if (current === undefined) return;
      current.delete(sub);
      if (current.size === 0) subscribers.delete(key);
    };

    const finish = () => {
      if (sub.closed) {
        // Already closed by abort or error; still flush any waiter so
        // the consumer's pending `next()` resolves promptly.
      }
      sub.closed = true;
      removeSubscriber();
      if (sub.waiter !== null) {
        const w = sub.waiter;
        sub.waiter = null;
        w({ value: undefined, done: true });
      }
    };

    const onAbort = () => {
      sub.closed = true;
      removeSubscriber();
      if (sub.waiter !== null) {
        const w = sub.waiter;
        sub.waiter = null;
        w({ value: undefined, done: true });
      }
    };

    if (opts.signal.aborted) {
      // Aborted before any work: return an iterator that ends
      // immediately, registering then removing the subscriber for
      // symmetry with the live path's cleanup.
      onAbort();
    } else {
      opts.signal.addEventListener("abort", onAbort, { once: true });
    }

    // Replay queue: history events the iterator surfaces before falling
    // through to the live buffer. Filled synchronously by the first
    // `next()` call, then drained one entry per next().
    let replayQueue: SubscribeEntry[] | null = null;
    let replayPrimed = false;

    async function primeReplay(): Promise<void> {
      replayPrimed = true;
      const dir = repoDir(repoId);
      if (opts.from === "head") {
        // Seed the seq cache so the next commit carries the correct seq
        // even if nothing populated it; `from: "head"` subscribers see
        // only new commits — there is no history to replay.
        const cached = seqCache.get(key);
        if (cached === undefined) {
          const count = await countCommits(dir, ref);
          if (count > 0) seqCache.set(key, count - 1);
        }
        replayQueue = [];
        return;
      }
      const all = await replayHistory(dir, ref);
      // Seed the seq cache from the replay so live deliveries
      // continue the seq sequence correctly.
      if (all.length > 0) {
        const last = all[all.length - 1];
        if (last === undefined) throw new Error("unreachable");
        seqCache.set(key, last.seq);
      }
      const fromSeq = opts.from.seq;
      replayQueue = all.filter((e) => e.seq >= fromSeq);
    }

    const iterator: AsyncIterableIterator<SubscribeEntry> = {
      [Symbol.asyncIterator]() {
        return iterator;
      },
      async next(): Promise<IteratorResult<SubscribeEntry>> {
        if (!replayPrimed) {
          try {
            await primeReplay();
          } catch (err) {
            finish();
            throw err;
          }
        }
        if (replayQueue !== null && replayQueue.length > 0) {
          const entry = replayQueue.shift();
          if (entry === undefined) throw new Error("unreachable");
          return { value: entry, done: false };
        }
        if (sub.buffer.length > 0) {
          const entry = sub.buffer.shift();
          if (entry === undefined) throw new Error("unreachable");
          return { value: entry, done: false };
        }
        if (sub.error !== null) {
          const err = sub.error;
          sub.error = null;
          finish();
          throw err;
        }
        if (sub.closed) {
          finish();
          return { value: undefined, done: true };
        }
        return new Promise<IteratorResult<SubscribeEntry>>((resolve) => {
          sub.waiter = resolve;
        });
      },
      async return(): Promise<IteratorResult<SubscribeEntry>> {
        opts.signal.removeEventListener("abort", onAbort);
        finish();
        return { value: undefined, done: true };
      },
      async throw(err: unknown): Promise<IteratorResult<SubscribeEntry>> {
        opts.signal.removeEventListener("abort", onAbort);
        finish();
        throw err;
      },
    };

    return iterator;
  }

  return {
    initRepo,
    writeTree,
    writeTreePreservingPrefix,
    writeTreeDelta,
    receivePack,
    createPack,
    commitPackedTip,
    resolveRef,
    listRefs,
    resolveHead,
    getRepoDir,
    openCommittedReads,
    openCommittedReadsAtCommit,
    subscribe,
  };
}
