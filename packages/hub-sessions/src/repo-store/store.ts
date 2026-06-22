import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { createSSHSignature } from "@intx/crypto-node";
import {
  initRepo as storageInitRepo,
  createDeployPack,
  receivePackObjects,
  collectReachableObjects,
  type CommitSigner,
} from "@intx/storage-isogit";
import { hasCode } from "@intx/types";
import { getLogger } from "@intx/log";
import type {
  AuthorizeFn,
  InitRepoOpts,
  KindHandler,
  Principal,
  RefEntry,
  RepoAction,
  RepoId,
  RepoKind,
  RepoStore,
  RepoStoreSubscribeEvent,
  TreeContent,
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
 * In-process push-serialization lock. Keyed by `${kind}/${id}`. Each
 * entry holds the tail of the chain of in-flight critical sections for
 * that repo; the next acquirer awaits the current tail and replaces it
 * with its own pending completion. On release the tail-check
 * (`if (locks.get(key) === myTail) locks.delete(key)`) prevents the map
 * from leaking entries once the chain drains.
 *
 * Single-process assumption: this protects against concurrent operations
 * inside a single hub instance. Cross-process serialization (e.g. a
 * second hub replica or an external git client touching the same
 * on-disk repo) would need a filesystem-backed lock; the migration path
 * is to swap the body of `withRepoLock` for an FS-lock acquire/release
 * around the same critical section.
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
   * Handler map keyed by repo kind. The substrate throws at request
   * time when a kind has no registered handler, so callers may omit
   * kinds they do not service (e.g. a per-asset-kind store that only
   * registers `skill`).
   */
  handlers: Partial<Record<RepoKind, KindHandler>>;
  authorize: AuthorizeFn;
  /**
   * Optional per-repo signing callback. When supplied and the callback
   * returns a `CommitSigner` for the given `repoId`, the substrate
   * passes that signer to `initRepo` so the genesis commit is authored
   * as `interchange-hub` and signed. When the callback returns
   * `undefined`, or the field is omitted entirely, the substrate falls
   * back to the unsigned harness-authored genesis.
   */
  signingCallback?: (repoId: RepoId) => CommitSigner | undefined;
};

export function createRepoStore(config: CreateRepoStoreConfig): RepoStore {
  const { dataDir, signingKey, handlers, authorize, signingCallback } = config;

  // Per-(repoId, ref) seq cache. Value is the seq of the ref's
  // current tip; the next commit on the ref gets `cached + 1`. The
  // cache is populated lazily — on each ref update we either bump
  // the cached value or recompute it from `git.log` walk. Cleared
  // on any failure inside the update path so a half-applied state
  // never poisons future reads.
  const seqCache = new Map<string, number>();

  // Per-(repoId, ref) subscriber set. Each subscriber holds its own
  // buffer, kind filter, and waiter callback so concurrent
  // subscribers do not interfere with each other.
  const subscribers = new Map<string, Set<SubscriberState>>();

  // Per-repoId cache of "the ref whose tree currently matches the
  // on-disk index". When the next writeTreeUnderLock call targets
  // the same `(repoId, ref)` pair as the last successful commit, the
  // index already mirrors that ref's tip and the resetIndexToRef
  // pass is a no-op — skip it. The cache is invalidated on any
  // commit, ref update, or pack receive that advances or replaces
  // the ref.
  const indexRefCache = new Map<string, string>();
  function indexCacheKey(repoId: RepoId): string {
    return `${repoId.kind}/${repoId.id}`;
  }

  // Per-commit reachable-object cache. createPack for a workflow-run
  // ref walks the first-parent chain and unions every commit's
  // reachable objects so the receiver gets the full history needed to
  // validate per-commit transitions. Without this cache the walk
  // recomputes the reachable set for every ancestor on every push, so
  // a long-running deployment's createPack cost scales as O(N^2) in
  // the number of commits.
  const chainReachabilityCache = new Map<string, string[]>();

  // Per-repoId cache of "every commit OID currently reachable from
  // any branch or tag in the repo". The substrate uses this in
  // `receivePack` to skip per-commit validation for commits the
  // receiver already had before the pack arrived. Computing the set
  // fresh on every receive scans every ref's parent chain — for a
  // long-running workflow-run repo this scales with history depth.
  // Caching it as a flat set and incrementally extending it on each
  // ref update lets receivePack pay O(new commits) instead of
  // O(history) per call. The cache is initialised lazily on first
  // receivePack to avoid the cold-start scan for repos that only
  // ever write via writeTree.
  const existingCommitsCache = new Map<string, Set<string>>();

  // Per-(repoId, ref) cache of "the commit OID createPack last packed
  // into a workflow-run pack". After the first successful push lands
  // a ref's tip on the receiver, every subsequent push only needs to
  // carry the commits added since then — the parent chain older than
  // this tip is already on the receiver, so re-shipping it wastes
  // pack bytes and inflates `receivePack` time per push. The cache
  // advances on every createPack call so the substrate's view of
  // "what has been shipped" stays in sync with the wrapper's
  // serialised push pipeline. A failed push leaves the cache pointing
  // at the unshipped tip; the wrapper's bootstrap retry re-sends the
  // same pack bytes and the cache stays consistent. The cache key is
  // `${repoId.kind}/${repoId.id}/${ref}` so writes against different
  // refs on the same repo (events vs the workflow-run ref) do not
  // interfere.
  const lastPackedTip = new Map<string, string>();
  function lastPackedTipKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }

  function refKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }

  // Count commits reachable from `ref`. Returns 0 when the ref does
  // not yet exist. `git.log` returns newest-first; the count gives
  // the seq the next commit would land at if added now (the current
  // tip's seq is `count - 1`).
  async function countCommits(dir: string, ref: string): Promise<number> {
    try {
      const entries = await git.log({ fs, dir, ref });
      return entries.length;
    } catch (err) {
      if (hasCode(err) && err.code === "NotFoundError") return 0;
      throw err;
    }
  }

  // Walk the ref's commit history oldest-first, assigning seq 0 to
  // the root commit and incrementing toward HEAD. Each entry's
  // `event` is the substrate-level commit descriptor — same shape
  // the live path emits, so replay and live are vocabulary-identical.
  async function replayHistory(
    dir: string,
    ref: string,
  ): Promise<SubscribeEntry[]> {
    let entries: Awaited<ReturnType<typeof git.log>>;
    try {
      entries = await git.log({ fs, dir, ref });
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

  // Called from inside the per-repo lock immediately after a
  // successful ref update. Computes the new tip's seq (either by
  // bumping the cached value or by walking the log), then fans the
  // event out to every subscriber registered for this (repoId, ref).
  // Errors raised by individual subscriber delivery (e.g. buffer
  // overrun) are captured on the subscriber's state so the
  // ref-update path itself is never destabilised by a slow consumer.
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

  async function clearIndexPrefix(dir: string, prefix: string): Promise<void> {
    // `filepaths: [prefix]` scopes statusMatrix to the cleared subtree
    // instead of walking every tracked file in the repo and discarding
    // the non-prefix rows. The narrowed call returns status rows only
    // for files under `prefix` -- exactly the set this loop then
    // removes -- so the removal set is identical while the per-commit
    // walk becomes O(prefix) rather than O(repo). A prefix with no
    // tracked entries (a first write) yields an empty matrix and clears
    // nothing.
    const matrix = await git.statusMatrix({ fs, dir, filepaths: [prefix] });
    for (const row of matrix) {
      const filepath = row[0];
      if (filepath.startsWith(prefix)) {
        await git.remove({ fs, dir, filepath });
      }
    }
  }

  async function writeFileEntry(
    dir: string,
    relPath: string,
    contents: string | Uint8Array,
  ): Promise<void> {
    const fullPath = path.join(dir, relPath);
    await fs.promises.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.promises.writeFile(fullPath, contents);
    await git.add({ fs, dir, filepath: relPath });
  }

  // Read the set of paths present in the tree at the given ref so a
  // post-validation rollback can distinguish genuinely-new staged paths
  // (which the rollback must unlink) from paths that existed at the
  // ref's tip and were merely overwritten by the staging pass (which
  // the rollback must restore, not delete).
  async function refPathSet(dir: string, ref: string): Promise<Set<string>> {
    let sha: string;
    try {
      sha = await git.resolveRef({ fs, dir, ref });
    } catch (err) {
      if (hasCode(err) && err.code === "NotFoundError") {
        return new Set();
      }
      throw err;
    }
    const entries = await git.listFiles({ fs, dir, ref: sha });
    return new Set(entries);
  }

  // Undo the staged writes from a failed writeTreeUnderLock pass. For
  // each newly-staged file: if the push target ref held that path,
  // restore it from the ref's tip so the working tree and index match
  // the ref bit-for-bit; otherwise drop the file from disk and the
  // index because it had no prior presence to restore. Then re-checkout
  // any prefix that the clearPrefix step removed wholesale. The
  // combination leaves the working tree and index bit-identical to the
  // ref for every path the failed pass touched, so a subsequent
  // legitimate push does not pick up half-applied state and does not
  // lose ref content that the rejected push happened to overwrite
  // outside `clearedPrefix`.
  async function resetWorkingTreeToRef(
    dir: string,
    ref: string,
    args: {
      stagedFiles: string[];
      clearedPrefix: string | undefined;
      refPaths: ReadonlySet<string>;
    },
  ): Promise<void> {
    const toRestore: string[] = [];
    for (const relPath of args.stagedFiles) {
      if (args.refPaths.has(relPath)) {
        toRestore.push(relPath);
        continue;
      }
      try {
        await git.remove({ fs, dir, filepath: relPath });
      } catch (err) {
        if (!hasCode(err) || err.code !== "NotFoundError") throw err;
      }
      try {
        await fs.promises.unlink(path.join(dir, relPath));
      } catch (err) {
        if (!hasCode(err) || err.code !== "ENOENT") throw err;
      }
    }
    if (toRestore.length > 0) {
      // Read each ref-existing path's blob directly and rewrite the
      // working-tree file plus stage it. `git.checkout` with the
      // `filepaths` narrowing does not consistently re-materialize a
      // working-tree file that the rollback flow has already touched
      // (isomorphic-git treats the narrowed checkout as a subtree-walk
      // hint and leaves the working tree untouched for paths already
      // present in the index), so the rollback restores each path
      // explicitly from the ref's tree.
      let refSha: string | undefined;
      try {
        refSha = await git.resolveRef({ fs, dir, ref });
      } catch (err) {
        if (!hasCode(err) || err.code !== "NotFoundError") throw err;
      }
      if (refSha !== undefined) {
        for (const relPath of toRestore) {
          let blob: Uint8Array;
          try {
            const result = await git.readBlob({
              fs,
              dir,
              oid: refSha,
              filepath: relPath,
            });
            blob = result.blob;
          } catch (err) {
            if (hasCode(err) && err.code === "NotFoundError") continue;
            throw err;
          }
          const fullPath = path.join(dir, relPath);
          await fs.promises.mkdir(path.dirname(fullPath), { recursive: true });
          await fs.promises.writeFile(fullPath, blob);
          await git.add({ fs, dir, filepath: relPath });
        }
      }
    }
    if (args.clearedPrefix !== undefined) {
      // Re-checkout the prefix from HEAD so the files the clearPrefix
      // step removed reappear in the working tree and the index.
      // `force: true` overwrites any leftover writes from the failed
      // pass; the filepaths array narrows the checkout to just the
      // prefix so we do not perturb unrelated paths.
      try {
        await git.checkout({
          fs,
          dir,
          ref: "HEAD",
          force: true,
          filepaths: [args.clearedPrefix],
        });
      } catch (err) {
        if (!hasCode(err) || err.code !== "NotFoundError") throw err;
      }
    }
  }

  // Walk from a commit's root tree to the tree-or-blob entry at
  // `relPath`. Returns `null` when any path segment is missing, or
  // when the final entry does not match `expectedType`. Used to back
  // both `priorReadBlob` and `priorListDir` so a handler can inspect
  // the parent commit's tree from inside validatePush without each
  // call duplicating the tree-walk.
  async function resolveTreeEntry(
    dir: string,
    commitSha: string,
    relPath: string,
    expectedType: "blob" | "tree",
  ): Promise<string | null> {
    const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
    if (relPath === "") {
      return expectedType === "tree" ? commit.tree : null;
    }
    const segments = relPath.split("/").filter((s) => s !== "");
    let currentOid = commit.tree;
    for (let i = 0; i < segments.length; i++) {
      const segment = segments[i];
      if (segment === undefined) throw new Error("unreachable");
      const isLast = i === segments.length - 1;
      const { tree } = await git.readTree({ fs, dir, oid: currentOid });
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

  // Build the `(priorReadBlob, priorListDir)` pair fed into a kind
  // handler's validatePush. When `commitSha` is `null`, both closures
  // surface the "ref had no prior commit" state — readBlob returns
  // null, listDir returns an empty array. When non-null, they read
  // against that commit's tree. Read failures on a path that does
  // exist (an EIO mid-walk) surface as a thrown error so a handler's
  // append-only check cannot silently degrade into an accept.
  function buildPriorTreeClosures(
    dir: string,
    commitSha: string | null,
  ): {
    priorReadBlob: (path: string) => Promise<Uint8Array | null>;
    priorListDir: (path: string) => Promise<string[]>;
  } {
    if (commitSha === null) {
      return {
        priorReadBlob: async () => null,
        priorListDir: async () => [],
      };
    }
    const priorReadBlob = async (
      relPath: string,
    ): Promise<Uint8Array | null> => {
      const oid = await resolveTreeEntry(dir, commitSha, relPath, "blob");
      if (oid === null) return null;
      const { blob } = await git.readBlob({ fs, dir, oid });
      return blob;
    };
    const priorListDir = async (relPath: string): Promise<string[]> => {
      const oid = await resolveTreeEntry(dir, commitSha, relPath, "tree");
      if (oid === null) return [];
      const { tree } = await git.readTree({ fs, dir, oid });
      return tree.map((e) => e.path);
    };
    return { priorReadBlob, priorListDir };
  }

  // Build the `(readBlob, listDir, topLevelTreePaths)` triple a kind
  // handler's validatePush sees for the prospective tree of a given
  // commit. Mirrors the tip-only closures the storage layer builds for
  // its validateTree callback, except parameterised on the commit OID
  // so the substrate can walk per-commit during a multi-commit pack.
  async function buildCommitTreeClosures(
    dir: string,
    commitSha: string,
  ): Promise<{
    topLevelTreePaths: string[];
    readBlob: (path: string) => Promise<Uint8Array>;
    listDir: (path: string) => Promise<string[]>;
  }> {
    const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
    const { tree: rootTree } = await git.readTree({
      fs,
      dir,
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
      const { blob } = await git.readBlob({ fs, dir, oid });
      return blob;
    };
    const listDir = async (relPath: string): Promise<string[]> => {
      if (relPath === "") return rootTree.map((e) => e.path);
      const oid = await resolveTreeEntry(dir, commitSha, relPath, "tree");
      // An absent directory lists as empty, matching the writeTree-path
      // `listDir` and the prior-tree closures: every `listDir` the
      // substrate hands a kind handler returns `[]` for a missing path,
      // so a handler that walks an optional or scoped-but-absent subtree
      // (e.g. workflow-run's per-run scoped walk over a run the commit
      // dropped) hits its own empty-directory guard and produces a clean
      // `path_violation` reason instead of a raw substrate throw. A real
      // read fault inside `git.readTree` below still bubbles.
      if (oid === null) return [];
      const { tree } = await git.readTree({ fs, dir, oid });
      return tree.map((e) => e.path);
    };
    return { topLevelTreePaths, readBlob, listDir };
  }

  // Read a tree's child entries as a name->oid map. Returns null when
  // the path is absent or is not a tree at the given commit, so the
  // diff below treats "subtree gained/lost" uniformly with "subtree
  // changed".
  async function readTreeEntryMap(
    dir: string,
    commitSha: string,
    relPath: string,
  ): Promise<Map<string, string> | null> {
    const oid =
      relPath === ""
        ? (await git.readCommit({ fs, dir, oid: commitSha })).commit.tree
        : await resolveTreeEntry(dir, commitSha, relPath, "tree");
    if (oid === null) return null;
    const { tree } = await git.readTree({ fs, dir, oid });
    const out = new Map<string, string>();
    for (const e of tree) out.set(e.path, e.oid);
    return out;
  }

  // Bound the set of paths a received-pack commit may have changed
  // relative to its parent, as repo-root-relative POSIX prefixes ending
  // in `/`. Git is content-addressed: a subtree whose object id is
  // unchanged between the two commits is byte-identical, so the diff
  // only descends into entries whose oid differs. Top-level entries that
  // differ are emitted as `<name>/`; the `runs/` subtree is descended
  // one level further so a per-run handler can scope to the exact
  // `runs/<runId>/` directories that changed rather than re-validating
  // every run. `parentSha === null` (no readable parent) means the
  // substrate cannot bound the change set, so it returns `undefined`
  // and the handler validates the whole tree.
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

  // Enumerate every commit OID reachable from any branch or tag in
  // the repo. The substrate uses this to define the set of "old"
  // commits — anything reachable via an existing ref existed before
  // the in-flight pack landed, so a pack-walk that reaches one of
  // these has crossed the boundary between new and prior history.
  // Returns an empty set when the repo's `.git` directory has no refs
  // yet (a freshly-initialised repo with HEAD pointing at an unborn
  // branch). Read failures bubble; a silent empty would let the walk
  // re-validate commits the kind handler already accepted.
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
          parsed = await git.readCommit({ fs, dir, oid });
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

  // Walk parent links from `tipSha` back to identify the new commits
  // a pack just published, returning the chain in topological order —
  // oldest new commit first, tip last. A commit is considered "new"
  // when it is readable from the object store, was not present before
  // the pack arrived (not in `existingCommits`), and does not match
  // the CAS-pinned `expectedOldSha`. When the parent's commit object
  // is not in the store at all — the common case for the deploy-pack
  // shape `createDeployPack` produces, which packs only the tip's
  // tree and not its ancestor commits — that absence is the
  // boundary: history older than the readable commit is opaque from
  // this pack's perspective, so the walker stops without pushing the
  // unreadable parent.
  //
  // The walk chases the first parent only; multi-parent merge commits
  // are not produced by any current writer of repo-store-managed
  // kinds, so a multi-parent commit in a received pack is a
  // structural defect the substrate refuses outright. Each returned
  // commit is one the substrate must validate against its predecessor
  // before the ref advances.
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
        parsed = await git.readCommit({ fs, dir, oid: current });
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

  // Reset the index so it bit-matches the tree at `ref`'s tip
  // without re-materialising the working tree. The substrate uses
  // the index — not the working tree — when it builds the next
  // commit's tree via `git.commit({ref})`, so re-pointing index
  // entries at the ref-tree's oids is correct and substantially
  // cheaper than walking disk for every path. When the ref does not
  // yet exist the index is cleared so the next staging pass starts
  // from an empty repo state.
  //
  // isomorphic-git's index is a single repo-global structure shared
  // across refs, and `git.add` mutates it in place; without this
  // reset, every commit's tree is the union of `ref`'s tree and
  // whatever was last staged for a different ref. The reset reads
  // `ref` (the push target) rather than HEAD because HEAD may point
  // at a different branch the repo was initialised on, so seeding
  // from HEAD would mis-construct trees that target a named ref.
  async function resetIndexToRef(dir: string, ref: string): Promise<void> {
    let refSha: string | null;
    try {
      refSha = await git.resolveRef({ fs, dir, ref });
    } catch (err) {
      if (hasCode(err) && err.code === "NotFoundError") {
        refSha = null;
      } else {
        throw err;
      }
    }
    const indexFiles = new Set(await git.listFiles({ fs, dir }));
    if (refSha === null) {
      for (const filepath of indexFiles) {
        try {
          await git.remove({ fs, dir, filepath });
        } catch (err) {
          if (!hasCode(err) || err.code !== "NotFoundError") throw err;
        }
      }
      return;
    }
    const refBlobs = await walkRefBlobs(dir, refSha);
    for (const filepath of indexFiles) {
      if (refBlobs.has(filepath)) continue;
      try {
        await git.remove({ fs, dir, filepath });
      } catch (err) {
        if (!hasCode(err) || err.code !== "NotFoundError") throw err;
      }
    }
    for (const [filepath, oid] of refBlobs) {
      await git.updateIndex({ fs, dir, filepath, oid, add: true });
    }
  }

  // Walk every blob entry reachable from a commit's tree, returning
  // a (filepath -> blob oid) map. Avoids the O(n^2) cost of pairing
  // `git.listFiles` with per-file `resolveTreeEntry` calls — both of
  // which walk the tree top-to-bottom — by doing a single recursive
  // walk and emitting blob entries as we encounter them.
  async function walkRefBlobs(
    dir: string,
    commitSha: string,
  ): Promise<Map<string, string>> {
    const out = new Map<string, string>();
    const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
    const recurse = async (treeOid: string, prefix: string): Promise<void> => {
      const { tree } = await git.readTree({ fs, dir, oid: treeOid });
      for (const entry of tree) {
        const childPath =
          prefix === "" ? entry.path : `${prefix}/${entry.path}`;
        if (entry.type === "blob") {
          out.set(childPath, entry.oid);
        } else if (entry.type === "tree") {
          await recurse(entry.oid, childPath);
        }
      }
    };
    await recurse(commit.tree, "");
    return out;
  }

  // Unlocked body of writeTree. The caller is responsible for
  // acquiring the per-repo lock before invoking this and for not
  // releasing it until the returned promise settles. Extracted so
  // writeTreePreservingPrefix can run a read-then-merge step under the
  // same lock without holding two nested acquisitions.
  async function writeTreeUnderLock(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    content: TreeContent,
  ): Promise<{ commitSha: string }> {
    const dir = repoDir(repoId);
    await storageInitRepo(dir, storageOptsFor(repoId, undefined));

    const handler = handlerFor(repoId);

    // Reset the working tree and index to the target ref's tip so a
    // commit to ref A does not pick up index state left behind by a
    // prior commit to ref B. isomorphic-git's index is a single
    // repo-global structure shared across refs, and `git.add` /
    // `writeFileEntry` mutate it in place; without this reset, every
    // commit's tree is the union of `ref`'s tree and whatever was last
    // staged for a different ref. The reset reads `ref` (the push
    // target) rather than HEAD: writes target a named ref, and HEAD
    // may point at a different branch the repo was initialized on, so
    // checking out HEAD would seed the new commit from the wrong tip.
    //
    // Skip the reset when the index cache says the on-disk index
    // already mirrors `ref` — i.e. the previous commit on this repo
    // also targeted this ref. The cache is invalidated on every
    // successful commit so a write to a different ref always pays the
    // reset cost on the next return.
    const indexKey = indexCacheKey(repoId);
    if (indexRefCache.get(indexKey) !== ref) {
      await resetIndexToRef(dir, ref);
      indexRefCache.set(indexKey, ref);
    }

    // Snapshot the target ref's path set before the clearPrefix +
    // writeFileEntry pass mutates the index. A post-validation rollback
    // uses this set to decide whether each staged path needs to be
    // restored from the ref's tip (it existed there and was
    // overwritten) or unlinked (it had no prior presence). Captured
    // here, not inside resetWorkingTreeToHead, because the rollback
    // runs after the index has been mutated. The snapshot reads `ref`
    // (the push target) rather than HEAD: writes target a named ref
    // and HEAD may point at a different branch the repo was
    // initialized on, so reading HEAD would mis-classify every staged
    // path as new and fall back to the unlink path.
    const refPaths = await refPathSet(dir, ref);

    if (content.clearPrefix !== undefined) {
      validateClearPrefix(content.clearPrefix);
      await clearIndexPrefix(dir, content.clearPrefix);
      await fs.promises.rm(path.join(dir, content.clearPrefix), {
        recursive: true,
        force: true,
      });
    }

    for (const [relPath, contents] of Object.entries(content.files)) {
      await writeFileEntry(dir, relPath, contents);
    }

    // The prior tree is whatever the ref points at the moment
    // validatePush runs — i.e. the parent of the commit we are about
    // to produce. `refPaths` above was snapshotted from the same
    // commit, so both reads observe the same pre-image.
    const priorCommitSha = await resolveRefSha(dir, ref);
    const { priorReadBlob, priorListDir } = buildPriorTreeClosures(
      dir,
      priorCommitSha,
    );

    // The prospective tree is the actual commit `git.commit` will
    // produce: the prior tree, minus paths cleared by `clearPrefix`,
    // unioned with paths newly staged via `writeFileEntry`. The
    // closures handed to validatePush must reflect that union — not
    // just `content.files` — so a kind handler that walks the prior
    // tree (workflow-run's append-only event check) can see every
    // path the commit will carry. Building the closures from
    // `content.files` alone would advertise only the writer's
    // explicitly-staged paths, hiding prior-tree paths the writer
    // did not touch and tripping prior-vs-prospective comparisons
    // that the substrate does not actually delete.
    const clearedPrefix = content.clearPrefix;
    const survivingPriorPaths: string[] = [];
    for (const p of refPaths) {
      if (clearedPrefix !== undefined && p.startsWith(clearedPrefix)) continue;
      if (Object.prototype.hasOwnProperty.call(content.files, p)) continue;
      survivingPriorPaths.push(p);
    }
    const prospectivePaths = new Set<string>([
      ...Object.keys(content.files),
      ...survivingPriorPaths,
    ]);
    const survivingPriorPathSet = new Set(survivingPriorPaths);
    const topLevelTreePaths = Array.from(
      new Set(
        Array.from(prospectivePaths, (p) => {
          const slash = p.indexOf("/");
          return slash === -1 ? p : p.substring(0, slash);
        }),
      ),
    );
    const readBlob = async (relPath: string): Promise<Uint8Array> => {
      const entry = content.files[relPath];
      if (entry !== undefined) {
        if (typeof entry === "string") {
          return new TextEncoder().encode(entry);
        }
        return entry;
      }
      if (survivingPriorPathSet.has(relPath)) {
        const blob = await priorReadBlob(relPath);
        if (blob === null) {
          throw new Error(
            `readBlob: path ${relPath} listed in prior tree but unreadable from prior commit ${String(priorCommitSha)}`,
          );
        }
        return blob;
      }
      throw new Error(
        `readBlob: path ${relPath} not present in prospective tree`,
      );
    };
    const listDir = async (dirPath: string): Promise<string[]> => {
      const prefix = dirPath === "" ? "" : `${dirPath}/`;
      const names = new Set<string>();
      for (const p of prospectivePaths) {
        if (prefix !== "" && !p.startsWith(prefix)) continue;
        const rest = p.slice(prefix.length);
        if (rest.length === 0) continue;
        const slash = rest.indexOf("/");
        names.add(slash === -1 ? rest : rest.substring(0, slash));
      }
      return Array.from(names);
    };
    // A writeTree carrying a `clearPrefix` mutates only paths under that
    // prefix: the clearPrefix step removes the prefix's prior entries and
    // `writeFileEntry` restages the writer's files, while every other
    // prior path is carried forward verbatim in `survivingPriorPaths`
    // above. So the change set the handler may scope to is exactly
    // `{clearPrefix}`. Without a clearPrefix the write is purely additive
    // over an arbitrary set of paths the substrate does not summarise, so
    // leave the change set unbounded (validate-all).
    const changedPathPrefixes =
      clearedPrefix === undefined ? undefined : new Set([clearedPrefix]);
    const validation = await handler.validatePush({
      repoId,
      ref,
      principal,
      topLevelTreePaths,
      readBlob,
      listDir,
      priorReadBlob,
      priorListDir,
      changedPathPrefixes,
    });
    if (!validation.ok) {
      // Roll the working tree and index back to HEAD before
      // surfacing the validation refusal. Otherwise the staged
      // writeFileEntry / clearPrefix steps above leave files and
      // index entries on disk that the next legitimate push would
      // either include silently (statusMatrix re-reports them) or
      // trip a second validation refusal against. The reset is
      // best-effort but logged loudly: a failure here means the
      // repo is in a half-applied state the operator needs to fix
      // by hand, which is strictly better than the silent-leak
      // alternative.
      try {
        await resetWorkingTreeToRef(dir, ref, {
          stagedFiles: Object.keys(content.files),
          clearedPrefix: content.clearPrefix,
          refPaths,
        });
      } catch (resetErr) {
        logger.warn`repo-store rollback after validation refusal failed for ${repoId.kind}/${repoId.id}; the repo is in a half-applied state and may need manual cleanup — ${resetErr instanceof Error ? resetErr.message : String(resetErr)}`;
      }
      throw new Error(`path_violation: ${validation.reason}`);
    }

    // Resolve the previous ref value for the post-update hook (precise:
    // null only when the ref truly doesn't exist) and the parent SHA for
    // the new commit (best-effort: any error falls back to HEAD, so a
    // first write on a never-touched ref produces a commit parented on
    // the repo's initial commit instead of failing).
    const oldSha = await resolveRefSha(dir, ref);
    let parentSha: string;
    try {
      parentSha = await git.resolveRef({ fs, dir, ref });
    } catch {
      parentSha = await git.resolveRef({ fs, dir, ref: "HEAD" });
    }

    const commitSha = await git.commit({
      fs,
      dir,
      message: content.message,
      author: AUTHOR,
      parent: [parentSha],
      ref,
      signingKey: "sshsig",
      onSign: async ({ payload }) => ({
        signature: createSSHSignature(
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

    return { commitSha };
  }

  async function writeTree(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    content: TreeContent,
  ): Promise<{ commitSha: string }> {
    gateAccess(principal, repoId, ref, "writeTree");

    // The lock spans the entire substrate body of writeTree: index
    // mutation, validatePush, commit, and the onRefUpdated hook. Holding
    // the lock through onRefUpdated keeps post-update consumers
    // serialized against the same ref's next writer.
    return withRepoLock(repoId, () =>
      writeTreeUnderLock(principal, repoId, ref, content),
    );
  }

  // Enumerate every blob directly under `prefix` in the tree at
  // `ref`, returning a map from repo-root-relative path (including the
  // prefix) to bytes. The empty map covers the ref-missing /
  // prefix-missing cases — both legitimate first-write states for the
  // prefix-preserving primitive.
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
    const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
    let currentOid = commit.tree;
    const segments = prefix
      .replace(/\/$/, "")
      .split("/")
      .filter((s) => s !== "");
    for (const segment of segments) {
      const { tree } = await git.readTree({ fs, dir, oid: currentOid });
      const entry = tree.find((e) => e.path === segment);
      if (entry === undefined || entry.type !== "tree") {
        return out;
      }
      currentOid = entry.oid;
    }
    const { tree } = await git.readTree({ fs, dir, oid: currentOid });
    // N+1 isomorphic-git round-trips: one tree read plus one
    // readBlob per blob entry. Acceptable at the current scale
    // (single-digit tarballs per registry); when a registry grows
    // to hundreds of entries this becomes the obvious optimization
    // target — readBlob can take the entry oid directly, avoiding
    // the per-call path resolution.
    for (const entry of tree) {
      if (entry.type !== "blob") continue;
      const { blob } = await git.readBlob({
        fs,
        dir,
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
  ): Promise<{ commitSha: string }> {
    gateAccess(principal, repoId, ref, "writeTree");
    validateClearPrefix(args.preservePrefix);
    return withRepoLock(repoId, async () => {
      // Reading and merging both happen inside the lock so two
      // concurrent callers targeting the same prefix observe each
      // other's commits in serial order — no lost-update window
      // between the read and the writeTree.
      await storageInitRepo(repoDir(repoId), storageOptsFor(repoId, undefined));
      const existing = await readPrefixBlobs(repoId, ref, args.preservePrefix);
      const files = await args.merge(existing);
      return writeTreeUnderLock(principal, repoId, ref, {
        files,
        clearPrefix: args.preservePrefix,
        message: args.message,
      });
    });
  }

  async function receivePack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
    expectedOldSha: string | null,
  ): Promise<void> {
    gateAccess(principal, repoId, ref, "receivePack");

    // The lock spans the entire substrate body of receivePack: the
    // packfile index, the CAS check against `expectedOldSha`, the
    // validateTree hook, the ref write, and the onRefUpdated hook.
    // `oldSha` for onRefUpdated is taken from the receivePackObjects
    // return value so the post-update hook sees the same pre-image the
    // CAS read observed, without a second resolveRef.
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

      const oldSha = await receivePackObjects(
        dir,
        pack,
        ref,
        commitSha,
        transferId,
        expectedOldSha,
        // A pack may carry more than one new commit (e.g. supervisor
        // bootstrap that batches enqueue + dequeue before the hub has
        // the workflow-run repo bootstrapped). The kind handler's
        // prior-tree closures must point at THAT commit's parent — not
        // the ref's tip before the pack arrived — so an intra-pack
        // transition (inbox in commit N, processing in commit N+1) is
        // validated against the right pre-image. We walk the parent
        // chain from tip back to the first commit already present in
        // the pre-pack history, then call validatePush once per new
        // commit in topological (oldest-first) order. A single-commit
        // pack collapses to the same behaviour as the tip-only path.
        async () => {
          const newCommits = await collectNewCommits(
            dir,
            commitSha,
            expectedOldSha,
            existingCommits,
          );
          newCommitsFromPack.push(...newCommits);
          for (const newCommit of newCommits) {
            const { commit: parsed } = await git.readCommit({
              fs,
              dir,
              oid: newCommit,
            });
            const parents = parsed.parent;
            const declaredParent =
              parents.length === 0 ? null : (parents[0] ?? null);
            // When the commit declares a parent the receiver does not
            // have in its object store, the substrate cannot
            // reconstruct the prior tree the kind handler would
            // validate against. For kinds whose handler reads prior
            // bytes to enforce append-only invariants (workflow-run),
            // silently degrading to "empty prior" lets a path in the
            // new commit that overwrites an immutable prior-tree
            // entry slip through unchecked — append-only enforcement
            // accepts a brand-new path, not a path that contradicts
            // an entry the handler cannot read. The workflow-run
            // `createPack` above ships the full parent chain, so this
            // branch is unreachable on the production pack-push path
            // for that kind; a producer that does ship an incomplete
            // chain is rejected outright.
            //
            // Other kinds ship deploy-shape packs (tip commit + tree
            // only) as their normal flow and their handlers do not
            // read prior bytes, so a dangling parent there is
            // structurally normal and the substrate continues to
            // collapse to the no-prior path.
            let parentSha: string | null = null;
            if (declaredParent !== null) {
              try {
                await git.readCommit({ fs, dir, oid: declaredParent });
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
            const { priorReadBlob, priorListDir } = buildPriorTreeClosures(
              dir,
              parentSha,
            );
            const { topLevelTreePaths, readBlob, listDir } =
              await buildCommitTreeClosures(dir, newCommit);
            const changedPathPrefixes = await computeChangedPathPrefixes(
              dir,
              newCommit,
              parentSha,
            );
            const result = await handler.validatePush({
              repoId,
              ref,
              principal,
              topLevelTreePaths,
              readBlob,
              listDir,
              priorReadBlob,
              priorListDir,
              changedPathPrefixes,
            });
            if (!result.ok) {
              logger.debug`validatePush rejected ${repoId.kind}/${repoId.id} on ${ref} at commit ${newCommit}: ${result.reason}`;
              return { ok: false, reason: result.reason };
            }
          }
          return true;
        },
      );

      // receivePack does not touch the index — it goes straight from
      // pack bytes to ref pointer — so the index now points at
      // whatever was last committed (possibly a different ref). The
      // index cache must be cleared so the next writeTreeUnderLock
      // resets the index against the chosen target ref.
      indexRefCache.delete(indexCacheKey(repoId));
      for (const sha of newCommitsFromPack) existingCommits.add(sha);
      await handler.onRefUpdated({ repoId, ref, oldSha, newSha: commitSha });
      await emitRefUpdate(repoId, ref, oldSha, commitSha);
    });
  }

  async function createPack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    gateAccess(principal, repoId, ref, "createPack");
    // `createDeployPack` packs only the tip commit + its tree, which
    // covers the deploy-pack shape every other kind ships (the
    // receiver starts from genesis and applies the tree wholesale).
    // The workflow-run kind ships incrementally and the receiver
    // (the hub) needs to validate per-commit transitions against the
    // sender's prior tree, so the pack must carry the full parent
    // chain from the supplied ref's tip. Walking the chain here keeps
    // the kind-specific branching at the substrate boundary so the
    // kind handler stays receive-side only.
    if (repoId.kind === "workflow-run") {
      const dir = repoDir(repoId);
      const commitSha = await git.resolveRef({ fs, dir, ref });
      const tipKey = lastPackedTipKey(repoId, ref);
      const stopAt = lastPackedTip.get(tipKey) ?? null;
      const oids = await collectChainReachableObjects(dir, commitSha, stopAt);
      const result = await git.packObjects({
        fs,
        dir,
        oids,
        write: false,
      });
      if (result.packfile === undefined) {
        throw new Error(
          `packObjects returned no packfile for ref "${ref}" (${commitSha})`,
        );
      }
      lastPackedTip.set(tipKey, commitSha);
      return { pack: result.packfile, commitSha, ref };
    }
    const { pack, commitSha } = await createDeployPack(repoDir(repoId), ref);
    return { pack, commitSha, ref };
  }

  // Collect every object OID reachable from `tipSha` and from every
  // ancestor commit along its first-parent chain. Mirrors what the
  // upload-pack layer's negotiated walker produces when the requester
  // advertises no `haves` but is sized for the substrate's own use:
  // a workflow-run pack push from the supervisor needs to carry every
  // commit the hub does not yet have, and the substrate has no
  // negotiation channel to ask the hub what it already has. The walk
  // stops at the first parent whose commit object is not in the local
  // store — that commit is by definition not part of the local
  // supervisor's history and so cannot be a relevant ancestor.
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
        parsed = await git.readCommit({ fs, dir, oid: current });
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
        // Already closed by abort or error. Still flush any waiter
        // so the consumer's pending `next()` resolves promptly.
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
      // Aborted before any work — return an iterator that yields
      // {done: true} immediately. The subscriber is registered then
      // removed for symmetry with the live path's cleanup.
      onAbort();
    } else {
      opts.signal.addEventListener("abort", onAbort, { once: true });
    }

    // Replay queue: events sourced from history that the iterator
    // surfaces before falling through to the live buffer. Filled
    // synchronously by the first `next()` call (replayHistory does
    // the git.log walk), then drained one entry per next().
    let replayQueue: SubscribeEntry[] | null = null;
    let replayPrimed = false;

    async function primeReplay(): Promise<void> {
      replayPrimed = true;
      const dir = repoDir(repoId);
      if (opts.from === "head") {
        // Seed the seq cache so the next commit on the ref carries
        // the correct seq even if no prior commit had ever populated
        // it. Subscribers that come in with `from: "head"` see only
        // new commits — there is no history to replay.
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
    receivePack,
    createPack,
    resolveRef,
    listRefs,
    resolveHead,
    getRepoDir,
    subscribe,
  };
}
