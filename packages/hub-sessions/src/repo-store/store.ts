import fs from "node:fs";
import path from "node:path";
import git, {
  type CommitObject,
  type FsClient,
  type TreeEntry,
} from "isomorphic-git";
import { createSSHSignature } from "@intx/crypto";
import {
  initRepo as storageInitRepo,
  createDeployPack,
  collectReachableObjects,
  HUB_REF_NAMESPACE,
  maybeGC,
  removeStagedPacks,
  stagePack,
  syncDirectoryDurable,
  syncRemovedPathDurable,
  type CommitSigner,
  type GCPolicy,
  type StagedPack,
} from "@intx/storage-isogit/node";
import { hasCode } from "@intx/types/has-code";
import { getLogger } from "@intx/log";
import type {
  AuthorizeFn,
  CommittedReads,
  CommittedTreeEntry,
  InitRepoOpts,
  KindHandler,
  NewlyTerminalRun,
  PackCommitResult,
  PreparedReceivePack,
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
import { RepoRemovedDuringReceiveError, SAFE_REPO_ID } from "./types";

const DEFAULT_SUBSCRIBE_BUFFER_LIMIT = 1024;
const DEFINITION_REF_NAMESPACE = `${HUB_REF_NAMESPACE}/workflow-definitions`;

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

/**
 * Repo dirs whose leftover staged packs this process has removed. A
 * staged pack lives only as long as the receive that holds it, so one
 * this process did not make was left by a crash, and nothing can name
 * it again. Process-wide like `locks`, because two stores over one data
 * dir share its repos.
 */
const cleanedStagingDirs = new Set<string>();

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
  /**
   * Optional write-path garbage-collection policy. When supplied, after
   * every successful write under the repo lock the substrate applies the
   * shared reclaim policy to repos whose kind is in `kinds`. Omitted
   * entirely, the substrate never reclaims and never warns. The `kinds`
   * allowlist keeps the policy scoped to the kinds the caller intends
   * (e.g. `agent-state`) rather than every kind the store happens to
   * service.
   */
  gc?: GCPolicy & { kinds: readonly RepoKind[] };
};

/**
 * A `RepoStore` that owns its repositories on local disk and can remove one.
 */
export type LocalRepoStore = RepoStore & {
  /**
   * Delete a repository and forget everything the store keeps in memory about
   * it, under its lock, so a repository created again at the same id starts
   * from nothing. A missing repository is not an error. `last` names an entry
   * of the repository's directory removed only after every other one, so a
   * crash partway through leaves it in place. Removal is durable before
   * returning; sync failures propagate.
   */
  removeRepo(repoId: RepoId, opts?: { readonly last?: string }): Promise<void>;
  /**
   * Receive a pack in two steps, so the slow validation holds no lock.
   * Under the repo lock, stage the pack where no reader finds it and find
   * its new commits; then validate each commit with no lock held. A pack
   * whose tip the ref already holds (its tip or a first-parent ancestor)
   * stages nothing and answers `already_applied`.
   *
   * The returned `commit()` takes the repo lock, publishes the pack, and
   * moves `ref` to `commitSha`, with the live tip as the old value. The
   * ref can move while the pack validates, so there is no
   * `expectedOldSha`: when the live tip already holds `commitSha`,
   * `commit()` answers `already_applied`; when the live tip is a commit
   * of this pack, it returns only the runs of the later commits. Any
   * other live tip gets the pack tip, because the sender is authoritative
   * for the shape of its history.
   *
   * `discard()` takes the repo lock and removes the staged pack. It does
   * nothing when called again or once `commit()` is called. `commit()`
   * leaves nothing staged, and fails when called again or after
   * `discard()`. A failed validation removes the staged pack under the
   * repo lock before it throws. Throws
   * `RepoRemovedDuringReceiveError`, from the validation or from
   * `commit()`, when `removeRepo` removed the repo after the pack was
   * staged.
   *
   * Only for the workflow-run kind, and only while GC does not reclaim
   * that kind. The validation runs with no repo lock, so the kind's
   * validatePush must hold no state for onRefUpdated, and no pack may
   * be removed during the validation. Throws for any other kind, and
   * for a kind that GC reclaims.
   */
  prepareReceivePack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
  ): Promise<PreparedReceivePack>;
};

export function createRepoStore(config: CreateRepoStoreConfig): LocalRepoStore {
  const { dataDir, signingKey, handlers, authorize, signingCallback, gc } =
    config;

  // Per-repo-directory isomorphic-git memoization cache. The store
  // threads that dir's cache through every index-touching and
  // object-reading git.* call, so isomorphic-git reuses the parsed
  // on-disk index across the repo's serialized write sequence instead
  // of re-reading and re-parsing it on every git.add / remove /
  // updateIndex / commit / listFiles, and reuses loaded packfiles across
  // object reads. The index-free tree assembly (git.writeTree /
  // git.writeBlob) uses no index and threads no cache. Every index
  // mutation runs under withRepoLock, so the parsed index in a
  // long-lived per-repo cache never races a concurrent mutator. Object
  // reads can share the cache without the lock: a prepared pack receive
  // validates unlocked, and a loaded pack is only read, never changed.
  //
  // For objects, isomorphic-git caches packfiles, not objects. Its
  // `PackfileCache` maps a pack's .idx filename to the parsed index, the
  // .pack bytes (read on first use) and the promise of the one SHA-1
  // check of those bytes. Each loaded pack also keeps some parsed objects
  // by their offset in that pack. Loose objects are not cached.
  //
  // The cache stays a pure accelerator, never a second source of truth,
  // because the on-disk repo remains authoritative on every axis:
  // isomorphic-git persists the index to disk on each dirty mutation and
  // re-reads it on a stat mismatch; every packed read lists objects/pack
  // again, so a new pack is found and a removed pack is not consulted;
  // and refs are not cached at all — resolveRef and friends take no
  // cache — so the cache can never serve a stale ref tip.
  //
  // The cache keys a pack by its filename, so it depends on pack names
  // that never repeat with different bytes. @intx/storage-isogit names a
  // received pack `pack-recv-<transferId>`, where the receive draws a
  // random transferId, and a GC repack `pack-recv-gc-<uuid>`. Thus a
  // pack receive needs no invalidation after it writes objects and moves
  // a ref. A receive validates a pack through a view that shows the
  // staged pack under the name it gets when published, so an entry
  // loaded during the validation is the same entry after the publish.
  // An entry for a rejected (discarded) or GC-replaced pack stays until
  // the next budget rebuild, but the directory listing does not name
  // that pack, so no read uses the entry again. These
  // properties also make it safe to drop a dir's cache at any instant:
  // the next call just re-reads from disk.
  //
  // Two bounds keep a long-lived store from retaining parsed packfiles
  // (and their pack bytes) without limit: a dir's cache is rebuilt once
  // it has served GIT_CACHE_MAX_OPS calls, and the store holds at most
  // GIT_CACHE_MAX_REPOS dir caches, evicting the least-recently-used. A
  // pack receive takes its dir's cache once for its stage and once for
  // its validation and uses it for all of the reads in each, so a long
  // validation loads and verifies each pack once. A prepared receive
  // can take it once more at its commit, to check if the live ref
  // already holds the pack tip. Each step charges its calls to the
  // entry when it ends (`chargeGitCache`), so the budget still bounds
  // the cache between receives. When a validation takes the entry over
  // the budget, the next call that takes the dir's cache rebuilds it.
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
  // Add a finished receive's git calls to its dir's entry. Nothing is
  // charged when the entry no longer holds that cache: an LRU eviction
  // or a budget rebuild during the receive already dropped it.
  function chargeGitCache(dir: string, cache: object, ops: number): void {
    const entry = gitCaches.get(dir);
    if (entry !== undefined && entry.cache === cache) entry.ops += ops;
  }

  // A parsed tree with its entries also keyed by name, so a path
  // segment resolves by one lookup instead of a scan of the entries. A
  // flat directory such as `runs/<runId>/events/` grows by one entry per
  // commit, and a receive resolves many paths through it.
  type ParsedTree = {
    entries: TreeEntry[];
    byName: ReadonlyMap<string, TreeEntry>;
  };

  // The object reads that the tree resolvers and closure builders below
  // go through. `cache` gives the isomorphic-git cache for one call.
  type ObjectReads = {
    cache: () => object;
    readCommit: (oid: string) => Promise<CommitObject>;
    readTree: (oid: string) => Promise<ParsedTree>;
    readBlob: (oid: string) => Promise<Uint8Array>;
  };

  // `gitFs` is the client the reads go through: the repo's own `fs`, or
  // a staged pack's view, which also finds the objects of that pack.
  function readsWith(
    dir: string,
    gitFs: FsClient,
    cache: () => object,
  ): ObjectReads {
    return {
      cache,
      readCommit: async (oid) =>
        (await git.readCommit({ fs: gitFs, dir, cache: cache(), oid })).commit,
      readTree: async (oid) => {
        const { tree } = await git.readTree({
          fs: gitFs,
          dir,
          cache: cache(),
          oid,
        });
        return {
          entries: tree,
          byName: new Map(tree.map((e) => [e.path, e])),
        };
      },
      readBlob: async (oid) =>
        (await git.readBlob({ fs: gitFs, dir, cache: cache(), oid })).blob,
    };
  }

  // Reads outside a pack receive. Each call takes the dir's cache from
  // `cacheFor`, so each call counts against the op budget.
  function dirReads(dir: string): ObjectReads {
    return readsWith(dir, fs, () => cacheFor(dir));
  }

  // One isomorphic-git cache for a span of reads: a step of a pack
  // receive, or a walk that numbers a ref update or seeds a
  // subscriber. The span loads and SHA-1-verifies each pack once,
  // however many reads it makes. `fn` gets the cache as a thunk that
  // counts each call, and the count is charged to the dir's entry when
  // the span ends.
  async function withReceiveCache<T>(
    dir: string,
    fn: (cache: () => object) => Promise<T>,
  ): Promise<T> {
    const cache = cacheFor(dir);
    let ops = 0;
    try {
      return await fn(() => {
        ops += 1;
        return cache;
      });
    } finally {
      chargeGitCache(dir, cache, ops);
    }
  }

  // Reads for the validation of one pack receive. The parsed commits
  // and trees are kept, but only for a window of two commits: the commit
  // under validation and its parent, which the prior-side reads use.
  // `advance` moves the window to the next commit. A memo that kept
  // every commit would grow as the square of the history: a flat
  // `runs/<runId>/events/` directory has a new tree version in each
  // commit.
  function receiveReads(
    dir: string,
    gitFs: FsClient,
    cache: () => object,
  ): { reads: ObjectReads; advance: () => void } {
    const raw = readsWith(dir, gitFs, cache);
    const commits = windowedMemo(raw.readCommit);
    const trees = windowedMemo(raw.readTree);
    return {
      reads: { ...raw, readCommit: commits.get, readTree: trees.get },
      advance: () => {
        commits.advance();
        trees.advance();
      },
    };
  }

  // Memoize `load` for the current window and the window before it. A
  // hit in the earlier window is copied to the current one, so a value
  // that each commit reads loads once, not once per two commits.
  function windowedMemo<T>(load: (oid: string) => Promise<T>): {
    get: (oid: string) => Promise<T>;
    advance: () => void;
  } {
    let current = new Map<string, T>();
    let previous = new Map<string, T>();
    return {
      get: async (oid) => {
        const hit = current.get(oid) ?? previous.get(oid);
        if (hit !== undefined) {
          current.set(oid, hit);
          return hit;
        }
        const loaded = await load(oid);
        current.set(oid, loaded);
        return loaded;
      },
      advance: () => {
        previous = current;
        current = new Map();
      },
    };
  }

  // Per-(repoId, ref) seq cache: a commit and its seq. In a complete
  // history the seq of a commit is its depth (see `commitDepth`), which
  // no later write can change, so an entry stays true for its commit
  // after the ref moves on, and only shortens the next walk: a walk
  // that does not meet the cached commit counts to the root. In a
  // history with a missing parent the entry is the base the next seq
  // advances from, so that history's seq starts again once the entry
  // goes. An update with no subscriber computes no seq and deletes the
  // entry, and `primeReplay` seeds it again, with no lock, for the next
  // subscriber, except on a kind that GC reclaims (see `seedFromTip`).
  // A kept entry for an older tip would make that seed skip, and the
  // next update would walk every commit since that tip under the lock.
  // An update that lands after a subscriber registers and before its
  // seed runs still walks to the root under the lock.
  type CachedSeq = { tip: string; seq: number };
  const seqCache = new Map<string, CachedSeq>();

  // Per-(repoId, ref) subscriber set. Each subscriber holds its own
  // buffer, kind filter, and waiter callback so concurrent
  // subscribers do not interfere with each other.
  const subscribers = new Map<string, Set<SubscriberState>>();

  // Repo-scoped cache key shared by the per-repoId caches below
  // (existing-commit set, and any future per-repo bookkeeping).
  function indexCacheKey(repoId: RepoId): string {
    return `${repoId.kind}/${repoId.id}`;
  }

  // Per-commit reachable-object cache, keyed by repository and commit so
  // removing a repository can drop its entries. createPack for a
  // workflow-run ref walks the first-parent chain and unions every commit's
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
  // ever write via writeTree. A receive into a repo with no entry
  // scans the history before it takes the repo lock (see
  // `prebuildExistingCommits`), so the lock span reads only the
  // commits that writes added during the scan.
  const existingCommitsCache = new Map<string, Set<string>>();

  // Per-repoId token that a prebuilt existing-commit set carries.
  // `removeRepo` deletes the token, so a set built from a removed
  // repository never matches the repository created again at that id.
  const existingCommitsEpochs = new Map<string, object>();

  // Per-repoId scan before the lock, kept until a locked step installs
  // the repo's set, so every receive that starts before then shares one
  // scan. A scan that fails is dropped at once, so the next receive
  // scans again.
  const existingCommitsScans = new Map<
    string,
    Promise<PrebuiltExistingCommits | null>
  >();

  // Per-(repoId, ref) cache of "the commit OID the receiver has acked
  // for a workflow-run pack". After the first push lands a ref's tip on
  // the receiver, every subsequent push only needs to carry the commits
  // added since then — the parent chain older than this tip is already
  // on the receiver, so re-shipping it wastes pack bytes and inflates
  // `receivePack` time per push. The cursor advances in
  // `commitPackedTip`, which the push pipeline calls once the receiver
  // acks the transfer — NOT when `createPack` builds the pack. Advancing
  // on build would strand the receiver whenever a transfer is cancelled
  // before its ack (a reconnect drops the in-flight push): the next
  // rebuild would walk back only to the un-acked commit and omit it, so
  // the receiver would reject the pack with a dangling parent forever.
  // Gating the advance on the ack keeps a cancelled transfer cleanly
  // re-shippable. The cache key is `${repoId.kind}/${repoId.id}/${ref}`
  // so writes against different refs on the same repo (events vs the
  // workflow-run ref) do not interfere.
  const lastPackedTip = new Map<string, string>();
  function lastPackedTipKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }

  function refKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }

  // The seq of `sha`: the number of first-parent steps from it to its
  // root commit, so a root has seq 0. The store writes no merge
  // commits (writeTree commits one parent and a receive rejects a
  // commit with more), so this is the position `replayHistory` gives
  // the commit. The walk stops early at `known`, a commit whose seq it
  // already has. A history with a missing parent, as a deploy pack or
  // GC leaves it, has no replay entries to match. A walk that meets the
  // missing parent counts the readable commits it walked: on from
  // `known` when there is one, so a gap between two received tips
  // still advances the seq, and from 0 at the oldest readable commit
  // when there is none. Returns null only when `sha` itself is missing.
  async function commitDepth(
    dir: string,
    cache: () => object,
    sha: string,
    known: CachedSeq | undefined,
  ): Promise<number | null> {
    let steps = 0;
    let current = sha;
    while (true) {
      if (known !== undefined && current === known.tip) {
        return known.seq + steps;
      }
      let parent: string | undefined;
      try {
        const { commit } = await git.readCommit({
          fs,
          dir,
          cache: cache(),
          oid: current,
        });
        parent = commit.parent[0];
      } catch (err) {
        if (!hasCode(err) || err.code !== "NotFoundError") throw err;
        if (steps === 0) return null;
        return known === undefined ? steps - 1 : known.seq + steps;
      }
      if (parent === undefined) return steps;
      steps += 1;
      current = parent;
    }
  }

  // Walk the ref's commit history oldest-first, assigning seq 0 to
  // the root commit and incrementing toward HEAD. Each entry's
  // `event` is the substrate-level commit descriptor — same shape
  // the live path emits, so replay and live are vocabulary-identical.
  // `tip` is the newest commit of the replay, null when it is empty.
  async function replayHistory(
    dir: string,
    ref: string,
  ): Promise<{ entries: SubscribeEntry[]; tip: string | null }> {
    let entries: Awaited<ReturnType<typeof git.log>>;
    try {
      entries = await git.log({ fs, dir, cache: cacheFor(dir), ref });
    } catch (err) {
      if (hasCode(err) && err.code === "NotFoundError") {
        return { entries: [], tip: null };
      }
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
    return { entries: out, tip: prev };
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
  // successful ref update. With no subscriber for (repoId, ref) it
  // drops the cached seq and computes nothing, so the update reads no
  // history under the lock. Otherwise it computes the new tip's seq
  // with a walk that stops at the cached commit, so an update that
  // extends the cached tip reads only the commits it adds, and fans
  // the event out to every subscriber.
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
    const set = subscribers.get(key);
    if (set === undefined) {
      seqCache.delete(key);
      return;
    }
    const dir = repoDir(repoId);
    const seq = await withReceiveCache(dir, (cache) =>
      commitDepth(dir, cache, newSha, seqCache.get(key)),
    );
    // The ref write before this call put `newSha` in the store, so a
    // missing tip means the store lost an object it just wrote.
    if (seq === null) {
      throw new Error(
        `ref_update_tip_missing: ${newSha} on ${ref} for ${repoId.kind}/${repoId.id}`,
      );
    }
    seqCache.set(key, { tip: newSha, seq });

    const event: RepoStoreSubscribeEvent = {
      type: "ref.updated",
      ref,
      oldSha,
      newSha,
    };
    const entry: SubscribeEntry = { seq, event };
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

  // GC runs on the write path of a repo whose kind is in `gc.kinds`. It
  // removes packs and loose objects, and under `tip-only` retention it
  // drops history, so a read of that repo with no repo lock can find
  // objects gone partway through.
  function isReclaimedByGC(repoId: RepoId): boolean {
    return gc !== undefined && gc.kinds.includes(repoId.kind);
  }

  // Write-path reclaim. Called at the end of every successful write for an
  // allowlisted kind, from inside the substrate's `withRepoLock`. The shared
  // evaluator acquires the storage per-directory lock, applies the policy
  // (reclaim over threshold, warn over byte budget), and logs rather than
  // propagates a reclaim failure — the triggering write has already
  // committed durably, so failing the caller would misreport it.
  async function maybeRunGC(repoId: RepoId): Promise<void> {
    // Not `isReclaimedByGC`: the check is inline so `gc` narrows for the
    // `maybeGC` call below.
    if (gc === undefined || !gc.kinds.includes(repoId.kind)) return;
    await maybeGC(repoDir(repoId), gc);
  }

  function gateAccess(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    action: RepoAction,
  ): void {
    const nonHubWrite =
      principal.kind !== "hub" &&
      (action === "writeTree" || action === "receivePack");
    // A case-insensitive filesystem also folds non-ASCII letters that
    // toLowerCase keeps (APFS stores `refſ` as `refs`), so only ASCII names
    // compare reliably against the Hub namespace below.
    if (nonHubWrite && /[\u0080-\uffff]/.test(ref)) {
      throw new Error(
        "authorize_denied: only the Hub may write non-ASCII ref names",
      );
    }
    // GC keeps these refs alive, and a loose ref at a namespace root would
    // block all retained children. Compare case-insensitively: on a
    // case-insensitive filesystem a differently cased ref is the same file.
    const lowerRef = ref.toLowerCase();
    if (
      nonHubWrite &&
      (lowerRef === HUB_REF_NAMESPACE ||
        lowerRef.startsWith(`${HUB_REF_NAMESPACE}/`))
    ) {
      throw new Error(
        `authorize_denied: ${HUB_REF_NAMESPACE} refs are Hub-owned`,
      );
    }
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

  // Validate a delta write path. A put is always a file (no trailing
  // slash); a delete is either an exact file or a subtree prefix
  // (trailing slash allowed). Both reject empties, absolute paths, and
  // any `..` traversal segment.
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

  // Reject an ambiguous delta rather than silently pick a winner: a path
  // that is both put and deleted, or a put that lands under a
  // subtree-prefix delete. assembleTree would let the put win in both
  // cases; making the contradiction loud keeps a caller from committing a
  // tree that does not match its stated intent.
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
  // under it — otherwise a handler that scopes validation to
  // `changedPathPrefixes` would skip a region the write actually mutated,
  // an exactly-once hole. An undefined scope means validate-all, so there
  // is nothing to under-cover.
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

  // The genesis is several writes, and its commit lands on whatever
  // `main` holds by then. Under the lock no receive can move `main`
  // between those writes, so the genesis cannot land on top of a
  // received tip.
  async function initRepo(repoId: RepoId, opts?: InitRepoOpts): Promise<void> {
    await withRepoLock(repoId, () =>
      storageInitRepo(repoDir(repoId), storageOptsFor(repoId, opts)),
    );
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

  // Whether `commitSha` is `tipSha` or one of its first-parent
  // ancestors. `existingCommits` alone cannot answer this: it also
  // holds commits no ref reaches any more, such as a hub genesis that
  // a sidecar's first push replaced. The walk answers false at the first
  // commit in `stopAt`, which the caller knows cannot lead to
  // `commitSha`.
  async function refHoldsCommit(
    dir: string,
    cache: () => object,
    tipSha: string | null,
    commitSha: string,
    stopAt: ReadonlySet<string>,
  ): Promise<boolean> {
    let current = tipSha;
    while (current !== null) {
      if (current === commitSha) return true;
      if (stopAt.has(current)) return false;
      const { commit } = await git.readCommit({
        fs,
        dir,
        cache: cache(),
        oid: current,
      });
      current = commit.parent[0] ?? null;
    }
    return false;
  }

  // Walk from a tree object's root to the tree-or-blob entry at
  // `relPath`. Returns `null` when any path segment is missing, or when
  // the final entry does not match `expectedType`. `relPath === ""`
  // resolves to the root tree itself, and only when a tree is expected.
  async function resolveTreeOid(
    reads: ObjectReads,
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
      const entry = (await reads.readTree(currentOid)).byName.get(segment);
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

  // Walk from a commit's root tree to the tree-or-blob entry at
  // `relPath`.
  async function resolveTreeEntry(
    reads: ObjectReads,
    commitSha: string,
    relPath: string,
    expectedType: "blob" | "tree",
  ): Promise<string | null> {
    const commit = await reads.readCommit(commitSha);
    return resolveTreeOid(reads, commit.tree, relPath, expectedType);
  }

  // Path reads against one root tree. The prospective and prior
  // closures both use them, so the two sides resolve and list a path the
  // same way. A missing blob reads as null and a missing directory lists
  // as empty; each side decides what a missing blob means.
  function treePathReads(
    reads: ObjectReads,
    rootTreeOid: () => Promise<string>,
  ): {
    blobAt: (relPath: string) => Promise<Uint8Array | null>;
    entriesAt: (relPath: string) => Promise<TreeEntry[]>;
  } {
    const blobAt = async (relPath: string): Promise<Uint8Array | null> => {
      const root = await rootTreeOid();
      const oid = await resolveTreeOid(reads, root, relPath, "blob");
      if (oid === null) return null;
      return reads.readBlob(oid);
    };
    const entriesAt = async (relPath: string): Promise<TreeEntry[]> => {
      const root = await rootTreeOid();
      const oid = await resolveTreeOid(reads, root, relPath, "tree");
      if (oid === null) return [];
      return (await reads.readTree(oid)).entries;
    };
    return { blobAt, entriesAt };
  }

  // A directory listing with each child's git object id and entry type,
  // read straight from the tree. A handler uses the oid to prove a
  // retained entry byte-unchanged without re-reading it, and the type to
  // refuse a tree where it needs a blob.
  function listingOf(tree: TreeEntry[]): CommittedTreeEntry[] {
    return tree.map((e) => ({ name: e.path, oid: e.oid, type: e.type }));
  }

  // Build the `(priorReadBlob, priorListDir)` pair fed into a kind
  // handler's validatePush. When `commitSha` is `null`, both closures
  // surface the "ref had no prior commit" state — readBlob returns
  // null, listDir returns an empty array. When non-null, they read
  // against that commit's tree, which they resolve on first use. Read
  // failures on a path that does exist (an EIO mid-walk) surface as a
  // thrown error so a handler's append-only check cannot silently
  // degrade into an accept.
  function buildPriorTreeClosures(
    reads: ObjectReads,
    commitSha: string | null,
  ): {
    priorReadBlob: (path: string) => Promise<Uint8Array | null>;
    priorListDir: (path: string) => Promise<string[]>;
    priorListDirOids: (path: string) => Promise<CommittedTreeEntry[]>;
    readBlobByOid: (oid: string) => Promise<Uint8Array>;
  } {
    // Object ids are content addresses, so a read by id is independent
    // of `commitSha` and is available even on the no-prior-commit branch
    // (where callers have no ids to read).
    const readBlobByOid = reads.readBlob;
    if (commitSha === null) {
      return {
        priorReadBlob: async () => null,
        priorListDir: async () => [],
        priorListDirOids: async () => [],
        readBlobByOid,
      };
    }
    let rootTreeOid: string | undefined;
    const paths = treePathReads(reads, async () => {
      if (rootTreeOid === undefined) {
        rootTreeOid = (await reads.readCommit(commitSha)).tree;
      }
      return rootTreeOid;
    });
    return {
      priorReadBlob: paths.blobAt,
      priorListDir: async (relPath) =>
        (await paths.entriesAt(relPath)).map((e) => e.path),
      priorListDirOids: async (relPath) =>
        listingOf(await paths.entriesAt(relPath)),
      readBlobByOid,
    };
  }

  // Read a tree's child entries as a name->oid map. Returns null when
  // the path is absent or is not a tree under `rootTreeOid`, so the
  // diff below treats "subtree gained/lost" uniformly with "subtree
  // changed".
  async function readTreeEntryMap(
    reads: ObjectReads,
    rootTreeOid: string,
    relPath: string,
  ): Promise<Map<string, string> | null> {
    const oid = await resolveTreeOid(reads, rootTreeOid, relPath, "tree");
    if (oid === null) return null;
    const out = new Map<string, string>();
    for (const e of (await reads.readTree(oid)).entries) out.set(e.path, e.oid);
    return out;
  }

  // git tree-entry modes an admitted asset tree must not carry: a symlink can
  // escape its directory when the tree is checked out, and a submodule gitlink
  // is always a dangling reference (a pushed pack ships no submodule object).
  const GIT_MODE_SYMLINK = "120000";
  const GIT_MODE_SUBMODULE = "160000";

  // Walk a commit's tree and return the first entry whose mode is disallowed
  // (a symlink or a submodule), or the first name that a tree repeats, or
  // null when the tree is clean. This mirrors the materialization backstop in
  // `writeTreeToDisk`, but at the PUSH boundary so a directory-escaping
  // symlink (or a dangling gitlink) is refused at admission rather than only
  // caught later at checkout. A repeated name is refused because readers
  // resolve it to different entries: a name lookup keeps one entry and a
  // scan keeps another, so validation could pass one entry while a reader
  // serves the other. It reads trees only, never blobs; `seen` deduplicates
  // shared subtree oids across the commits of one pack so a subtree is
  // walked once.
  async function findDisallowedTreeMode(
    reads: ObjectReads,
    treeOid: string,
    seen: Set<string>,
    prefix = "",
  ): Promise<{
    path: string;
    kind: "symlink" | "submodule" | "repeated name";
  } | null> {
    if (seen.has(treeOid)) return null;
    seen.add(treeOid);
    const names = new Set<string>();
    for (const entry of (await reads.readTree(treeOid)).entries) {
      const entryPath = prefix === "" ? entry.path : `${prefix}/${entry.path}`;
      if (names.has(entry.path)) {
        return { path: entryPath, kind: "repeated name" };
      }
      names.add(entry.path);
      if (entry.mode === GIT_MODE_SYMLINK) {
        return { path: entryPath, kind: "symlink" };
      }
      if (entry.type === "commit" || entry.mode === GIT_MODE_SUBMODULE) {
        return { path: entryPath, kind: "submodule" };
      }
      if (entry.type === "tree") {
        const nested = await findDisallowedTreeMode(
          reads,
          entry.oid,
          seen,
          entryPath,
        );
        if (nested !== null) return nested;
      }
    }
    return null;
  }

  // Bound the set of paths a received-pack commit may have changed
  // relative to its parent, as repo-root-relative POSIX prefixes ending
  // in `/`. Git is content-addressed: a subtree whose object id is
  // unchanged between the two commits is byte-identical, so the diff
  // only descends into entries whose oid differs. Top-level entries that
  // differ are emitted as `<name>/`; the `runs/` subtree is descended
  // one level further so a per-run handler can scope to the exact
  // `runs/<runId>/` directories that changed rather than re-validating
  // every run. `parentTreeOid === null` (no readable parent) means the
  // substrate cannot bound the change set, so it returns `undefined`
  // and the handler validates the whole tree.
  async function computeChangedPathPrefixes(
    reads: ObjectReads,
    treeOid: string,
    parentTreeOid: string | null,
  ): Promise<Set<string> | undefined> {
    if (parentTreeOid === null) return undefined;
    const prefixes = new Set<string>();
    const newTop = (await readTreeEntryMap(reads, treeOid, "")) ?? new Map();
    const oldTop =
      (await readTreeEntryMap(reads, parentTreeOid, "")) ?? new Map();
    const topNames = new Set<string>([...newTop.keys(), ...oldTop.keys()]);
    for (const name of topNames) {
      if (newTop.get(name) === oldTop.get(name)) continue;
      if (name === "runs") {
        const newRuns =
          (await readTreeEntryMap(reads, treeOid, "runs")) ?? new Map();
        const oldRuns =
          (await readTreeEntryMap(reads, parentTreeOid, "runs")) ?? new Map();
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
  //
  // `seed` is a set an earlier scan of the same repository built; the
  // scan adds to it and returns it. The walk skips a commit already in
  // the set, and a scan adds every readable ancestor of each commit it
  // adds, so a rescan reads only the commits a write added since.
  async function snapshotExistingCommits(
    dir: string,
    cache: () => object,
    seed: Set<string>,
  ): Promise<Set<string>> {
    const out = seed;
    const visit = async (start: string): Promise<void> => {
      const stack: string[] = [start];
      while (stack.length > 0) {
        const oid = stack.pop();
        if (oid === undefined) break;
        if (out.has(oid)) continue;
        let parsed: Awaited<ReturnType<typeof git.readCommit>>;
        try {
          parsed = await git.readCommit({ fs, dir, cache: cache(), oid });
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
  // a staged pack carries, returning the chain in topological order —
  // oldest new commit first, tip last. `gitFs` is the staged pack's
  // view, so the walk reads the pack's commits. A commit is considered
  // "new" when it is readable from the object store, was not present
  // before the pack arrived (not in `existingCommits`), and does not
  // match `priorTip`, the ref's tip under the lock. When the parent's
  // commit object is not in the store at all — the common case for the
  // deploy-pack shape `createDeployPack` produces, which packs only the
  // tip's tree and not its ancestor commits — that absence is the
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
    gitFs: FsClient,
    cache: () => object,
    tipSha: string,
    priorTip: string | null,
    existingCommits: ReadonlySet<string>,
  ): Promise<string[]> {
    const chain: string[] = [];
    let current: string = tipSha;
    while (true) {
      if (current === priorTip) break;
      if (existingCommits.has(current)) break;
      let parsed: Awaited<ReturnType<typeof git.readCommit>>;
      try {
        parsed = await git.readCommit({
          fs: gitFs,
          dir,
          cache: cache(),
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

  // Assemble a new root tree by splicing `puts` (repo-root-relative path
  // -> blob oid) and `deletes` onto the parent's root tree, reusing every
  // unchanged entry by oid. The result is committed directly via
  // `git.commit({ tree })`, so the on-disk index is never touched.
  //
  // `deletes` is a set of repo-root-relative paths, and removal must
  // match the base entry's type. A no-slash entry names an exact path
  // and clears the blob there; deleting a path that is a directory in
  // the base is rejected with `delete_type_mismatch` (there is no
  // git-rm-style recursive drop). A trailing-slash entry names a
  // subtree prefix and clears it (the clear-prefix shape); a
  // trailing-slash delete whose leading segment is a base blob is
  // rejected with `delete_type_mismatch`, unless a `put` drives the
  // same descent -- that is a legitimate file-to-directory replacement
  // and is accepted (the put wins). A `put` at a path overrides a
  // delete of the same path. A delete of a path absent from the parent
  // tree is an idempotent no-op (the entry is simply never emitted),
  // matching `git rm --ignore-unmatch` and the working-tree `rm` with
  // `force: true`.
  //
  // Recursion is scoped to the touched subtrees: a level is read and
  // rewritten only when a put lands under it or a delete removes within
  // it. Every entry the write does not touch — sibling subtrees AND
  // sibling blobs inside a subtree being touched — is carried forward by
  // its existing oid without a re-hash or a walk, so the per-commit cost
  // tracks the size of the change rather than the repo. Returns the new
  // tree oid, or `null` when the subtree ends up empty (the caller writes
  // an empty root tree when the whole repo empties).
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

    // Classify what changes at this level: direct blob puts, exact-file
    // deletes here, and subtrees a put or a delete descends into.
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
    // `foo` plus a put or delete under `foo/`) is contradictory. Left
    // implicit, the blob-put branch below wins and the subtree side is
    // silently dropped; reject it loudly instead. This covers every
    // caller — writeTree's `content.files`, writeTreePreservingPrefix's
    // merge output, and writeTreeDelta's puts/deletes — since all funnel
    // into this same classification.
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
        // A put overrides whatever the base held and any delete of the
        // same path. Puts are always regular files (mode 100644). If a
        // put ever overwrites a base entry with a different mode (an
        // executable 100755 blob, a symlink 120000, or a submodule), this
        // downgrades it to 100644 — the store only ever writes 100644
        // content blobs today, so no caller hits it, but a future caller
        // that needs to preserve an executable bit would have to carry
        // the mode through `puts` rather than assume 100644.
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
        // contradicts the base type -- reject it. The
        // `!subtreePutNames` carve-out lets a put-driven descent
        // through: a `put` under `name/` is a file-to-directory
        // replacement, not a delete mismatch, so it must not raise
        // delete_type_mismatch. That replacement still dead-ends at
        // working-tree materialization in writeTreeUnderLock (mkdir
        // over the base file EEXISTs) -- a separate limitation the
        // claim-check callers never reach, which must not be masked by
        // mislabeling a put as a delete mismatch.
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

  // Prospective-side validatePush closures sourced from a root tree oid,
  // so a handler sees exactly the tree the commit carries. The writeTree
  // path passes the assembled tree, whose blobs and trees are
  // unreferenced objects until the commit lands, so reading them here
  // advances nothing. The pack path passes each new commit's tree.
  function buildTreeReadClosures(
    reads: ObjectReads,
    rootTreeOid: string,
  ): {
    topLevelTreePaths: () => Promise<string[]>;
    readBlob: (relPath: string) => Promise<Uint8Array>;
    listDir: (relPath: string) => Promise<string[]>;
    listDirOids: (relPath: string) => Promise<CommittedTreeEntry[]>;
  } {
    const paths = treePathReads(reads, async () => rootTreeOid);
    const topLevelTreePaths = async (): Promise<string[]> =>
      (await reads.readTree(rootTreeOid)).entries.map((e) => e.path);
    const readBlob = async (relPath: string): Promise<Uint8Array> => {
      const blob = await paths.blobAt(relPath);
      if (blob === null) {
        throw new Error(
          `readBlob: path ${relPath} not present in prospective tree`,
        );
      }
      return blob;
    };
    // An absent directory lists as empty, matching the prior-tree
    // closures, so a handler that walks an optional or absent subtree
    // (e.g. workflow-run's per-run scoped walk over a run the commit
    // dropped) hits its own empty-directory guard instead of a substrate
    // throw. A real read fault still bubbles.
    const listDir = async (relPath: string): Promise<string[]> =>
      (await paths.entriesAt(relPath)).map((e) => e.path);
    // A handler validating a large retained subtree by its per-commit
    // delta (workflow-run's consumed dedup index) reads the prospective
    // OID straight from here instead of re-reading and hashing every
    // retained blob.
    const listDirOids = async (
      relPath: string,
    ): Promise<CommittedTreeEntry[]> =>
      listingOf(await paths.entriesAt(relPath));
    return { topLevelTreePaths, readBlob, listDir, listDirOids };
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
    w: {
      files: Record<string, string | Uint8Array>;
      deletes: ReadonlySet<string>;
      changedPathPrefixes: ReadonlySet<string> | undefined;
      message: string;
    },
    // When present, the parent tip already resolved under this lock. The
    // delta path pins it once and hands the SAME oid to both computeDelta
    // (its dedup reads) and this assembly, so the dedup snapshot, the
    // committed tree, and validation are provably one pre-image. Absent,
    // the write resolves its own (the writeTree / preserving-prefix path,
    // which has no prior read to keep consistent with). `{ sha }` wraps
    // the value so a genuinely-null pin is distinct from "not provided".
    pinnedParent?: { sha: string | null },
  ): Promise<WriteResult> {
    const dir = repoDir(repoId);
    await storageInitRepo(dir, storageOptsFor(repoId, undefined));

    const handler = handlerFor(repoId);

    // Assemble the commit's tree directly and commit that tree oid,
    // never staging into the on-disk index. The index is a single
    // repo-global structure shared across refs; routing writes through
    // it forced a full index rebuild on every events<->workflow-run ref
    // flip and re-serialized the whole index once per staged blob.
    // Splicing the tree from the parent's root tree — applying `w.files`
    // as puts and `w.deletes` as removals, reusing every untouched entry
    // by oid — keeps the per-commit cost tracking the change rather than
    // the accumulated history.

    // Pin the parent under the lock: the new commit's parent is the
    // ref's tip, and the splice runs against THAT commit's root tree,
    // read under the same lock, so the pre-image is race-free. A ref
    // that does not yet exist has no base tree (the splice starts from
    // empty) and parents on HEAD, matching the prior index-reset path
    // which seeded an empty index for a missing ref.
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
    // parent root tree. writeBlob and writeTree emit unreferenced
    // objects; nothing the ref can reach moves until the commit lands.
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
    // assembled tree oid; the prior-side closures read the parent commit
    // (`parentCommitSha`) exactly as before. `w.changedPathPrefixes` is
    // the handler's scoping hint — the prefixes this write may have
    // touched — and stays undefined for an unbounded (validate-all) write.
    const { priorReadBlob, priorListDir, priorListDirOids } =
      buildPriorTreeClosures(dirReads(dir), parentCommitSha);
    const prospective = buildTreeReadClosures(dirReads(dir), newRootTreeOid);
    const changedPathPrefixes = w.changedPathPrefixes;
    // No disallowed-mode (symlink/submodule) gate here on purpose: the
    // substrate mode gate lives on the pack-receive path. This write path
    // cannot introduce a `120000`/`160000` entry -- `assembleTree` writes puts
    // as `100644` and passes base-tree entries through unchanged, and no
    // ingress ever admits such a mode into a base tree (the pack path is now
    // gated; this path only writes `100644`). Adding a walk here would gate a
    // tree that cannot carry those modes.
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
      // A write validates one commit, so its parse cache lasts one call.
      validationScope: {},
    });
    if (!validation.ok) {
      // Nothing was staged and no ref advanced: the assembly wrote only
      // unreferenced blob and tree objects, which the next GC reclaims.
      // There is no half-applied index or working-tree state to roll
      // back, so the refusal just surfaces.
      throw new Error(`path_violation: ${validation.reason}`);
    }

    // Materialize the working tree for the paths this write touched:
    // remove each deleted path, then write each put file. The store's own
    // reads resolve through the object store, but some consumers (the
    // workflow-run claim-check processing scan) read these files straight
    // from disk, so the working tree must mirror the committed change.
    // Only the deleted paths and the put paths are touched, so this is
    // O(change), not O(repo). It runs only after validation passes, so a
    // rejected push leaves the working tree untouched — the failure
    // atomicity the old index rollback provided, now without any
    // rollback. `rm` with `force` no-ops a missing path and `recursive`
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

    // The parent is the pinned tip, or HEAD for a never-written ref so a
    // first write parents on the repo's initial commit. `oldSha` is
    // precise: null only when the ref truly does not exist.
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

  // Normalize a `TreeContent` (files + optional clearPrefix) into the
  // puts/deletes/scope shape writeTreeUnderLock consumes. A clearPrefix
  // becomes a single subtree-delete and the handler's change scope; its
  // absence is a purely-additive write validated in full.
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

    // The lock spans the entire substrate body of writeTree: tree
    // assembly, validatePush, commit, and the onRefUpdated hook. Holding
    // the lock through onRefUpdated keeps post-update consumers
    // serialized against the same ref's next writer.
    return withRepoLock(repoId, () =>
      writeTreeUnderLock(principal, repoId, ref, normalizeTreeContent(content)),
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
    const reads = dirReads(dir);
    const treeOid = await resolveTreeEntry(
      reads,
      commitSha,
      prefix.replace(/\/$/, ""),
      "tree",
    );
    if (treeOid === null) return out;
    // One tree read plus one blob read per blob entry, each blob read by
    // its oid from the listing.
    for (const entry of (await reads.readTree(treeOid)).entries) {
      if (entry.type !== "blob") continue;
      const blob = await reads.readBlob(entry.oid);
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
      // Reading and merging both happen inside the lock so two
      // concurrent callers targeting the same prefix observe each
      // other's commits in serial order — no lost-update window
      // between the read and the writeTree.
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
  // writeTreePreservingPrefix — which clears and rebuilds a whole prefix
  // from a full merge output — a delta touches only the entries it names,
  // so a caller that mutates one file in a large directory (a claim-check
  // move that adds one entry and deletes another) does not re-hash or
  // re-materialize the untouched siblings. `changedPathPrefixes` is the
  // handler's scoping hint for the touched region; the caller supplies it
  // because the delta has no single clear-prefix to derive it from.
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
      // Pin the parent tip ONCE under the lock and hand the same oid to
      // computeDelta's dedup reads and to the assembly below, so the
      // pre-image the delta is computed against is exactly the pre-image
      // it is committed against — no lost-update window, no second
      // resolve that could (in principle) observe a different tip.
      const parentCommitSha = await resolveRefSha(dir, ref);
      const { priorListDirOids, readBlobByOid } = buildPriorTreeClosures(
        dirReads(dir),
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

  // How a receive decides whether the ref may move. A one-call receive
  // holds the repo lock from stage to ref write, so the caller's
  // `expectedOldSha` still names the tip at the ref write. A prepared
  // receive validates with no lock held, so the ref can move before its
  // commit; commit decides from the live tip instead.
  type ReceiveMode =
    | { readonly kind: "expected"; readonly expectedOldSha: string | null }
    | { readonly kind: "prepared" };

  // A pack staged and walked under the repo lock, ready to validate.
  type StagedReceive = {
    readonly principal: Principal;
    readonly repoId: RepoId;
    readonly ref: string;
    readonly commitSha: string;
    readonly dir: string;
    readonly handler: KindHandler;
    readonly staged: StagedPack;
    // The repo's `existingCommitsCache` entry when the pack was staged.
    // `removeRepo` deletes that entry, so a different entry, or none,
    // means the repo this pack was staged in is gone.
    readonly incarnation: Set<string>;
    // The ref's tip when the pack was staged.
    readonly priorTip: string | null;
    // The pack's new commits, oldest first.
    readonly chain: readonly string[];
  };

  function repoStillHolds(s: StagedReceive): boolean {
    return existingCommitsCache.get(indexCacheKey(s.repoId)) === s.incarnation;
  }

  async function discardStaged(s: StagedReceive): Promise<void> {
    await s.staged.discard().catch((cause: unknown) => {
      // The pack is not in objects/pack, so no reader finds it. The
      // first receive for this dir in the next process removes it.
      logger.warn`Cannot remove staged pack for ${s.repoId.kind}/${s.repoId.id}: ${cause instanceof Error ? cause.message : String(cause)}`;
    });
  }

  // An existing-commit set built with no repo lock, the repo's token
  // when the build started, and the git cache the build read through
  // with its uncharged calls (see `chargePrebuiltScan`).
  type PrebuiltExistingCommits = {
    commits: Set<string>;
    epoch: object;
    cache: object;
    ops: number;
    charged: boolean;
  };

  // Scan a repo's history before a receive takes the repo lock, when
  // the repo has no existing-commit set yet. Answers null when the set
  // exists, when GC reclaims the kind (a scan with no lock could lose a
  // pack and leave commits out of the set), when the repo does not
  // exist yet, and when the scan fails: the scan races ref writes and
  // `removeRepo`, so a failure is not a fault of the receive, and the
  // locked scan in `stageReceive` reports a real one. A receive that
  // starts while a scan of the repo runs waits for that scan.
  function prebuildExistingCommits(
    repoId: RepoId,
  ): Promise<PrebuiltExistingCommits | null> {
    const key = indexCacheKey(repoId);
    if (existingCommitsCache.has(key) || isReclaimedByGC(repoId)) {
      return Promise.resolve(null);
    }
    const running = existingCommitsScans.get(key);
    if (running !== undefined) return running;
    let epoch = existingCommitsEpochs.get(key);
    if (epoch === undefined) {
      epoch = {};
      existingCommitsEpochs.set(key, epoch);
    }
    const scan = scanExistingCommits(repoId, epoch).then((prebuilt) => {
      if (prebuilt === null && existingCommitsScans.get(key) === scan) {
        existingCommitsScans.delete(key);
      }
      return prebuilt;
    });
    existingCommitsScans.set(key, scan);
    return scan;
  }

  async function scanExistingCommits(
    repoId: RepoId,
    epoch: object,
  ): Promise<PrebuiltExistingCommits | null> {
    const dir = repoDir(repoId);
    try {
      if (!(await repoHasGitDir(dir))) return null;
      const cache = cacheFor(dir);
      let ops = 0;
      const commits = await snapshotExistingCommits(
        dir,
        () => {
          ops += 1;
          return cache;
        },
        new Set(),
      );
      return { commits, epoch, cache, ops, charged: false };
    } catch (cause) {
      logger.debug`Scan of ${repoId.kind}/${repoId.id} before the repo lock failed; the receive scans under the lock: ${cause instanceof Error ? cause.message : String(cause)}`;
      return null;
    }
  }

  // Charge a prebuilt scan's git calls to its dir's cache entry once,
  // inside the locked span that follows it. A charge before the lock
  // could take the entry over its budget, and the locked span would
  // then start from an empty cache and load every pack index again
  // while it holds the lock.
  function chargePrebuiltScan(
    dir: string,
    prebuilt: PrebuiltExistingCommits | null,
  ): void {
    if (prebuilt === null || prebuilt.charged) return;
    prebuilt.charged = true;
    chargeGitCache(dir, prebuilt.cache, prebuilt.ops);
  }

  // The locked first step of every receive: resolve the tip, stage the
  // pack, and find its new commits. The caller holds the repo lock. In
  // the prepared mode it answers null, with nothing staged, when the ref
  // already holds the pack's tip. `prebuilt` is the caller's scan from
  // before it took the lock, used when the repo still has no set and
  // was not removed since.
  async function stageReceive(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
    mode: ReceiveMode & { kind: "expected" },
    prebuilt: PrebuiltExistingCommits | null,
  ): Promise<StagedReceive>;
  async function stageReceive(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
    mode: ReceiveMode,
    prebuilt: PrebuiltExistingCommits | null,
  ): Promise<StagedReceive | null>;
  async function stageReceive(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
    mode: ReceiveMode,
    prebuilt: PrebuiltExistingCommits | null,
  ): Promise<StagedReceive | null> {
    const dir = repoDir(repoId);
    await storageInitRepo(dir, storageOptsFor(repoId, undefined));
    const handler = handlerFor(repoId);
    return withReceiveCache(dir, async (cache) => {
      chargePrebuiltScan(dir, prebuilt);
      const existingKey = indexCacheKey(repoId);
      let existingCommits = existingCommitsCache.get(existingKey);
      if (existingCommits === undefined) {
        // Receives that shared one scan each seed with a copy, so a
        // scan below that fails partway leaves the shared set whole for
        // the next receive.
        const seed =
          prebuilt !== null &&
          existingCommitsEpochs.get(existingKey) === prebuilt.epoch
            ? new Set(prebuilt.commits)
            : new Set<string>();
        existingCommits = await snapshotExistingCommits(dir, cache, seed);
        existingCommitsCache.set(existingKey, existingCommits);
        existingCommitsScans.delete(existingKey);
      }
      if (!cleanedStagingDirs.has(dir)) {
        await removeStagedPacks(dir);
        cleanedStagingDirs.add(dir);
      }
      // The tip is read under the lock, not taken from the caller, which
      // read it outside the lock. A held tip is accepted before the pack
      // is staged or checked against `commitSha`: the store already
      // holds that commit, so the pack cannot change anything.
      const priorTip = await resolveRefSha(dir, ref);
      if (
        mode.kind === "prepared" &&
        existingCommits.has(commitSha) &&
        (await refHoldsCommit(dir, cache, priorTip, commitSha, new Set()))
      ) {
        return null;
      }

      const transferId = crypto.randomUUID().replace(/-/g, "");
      const staged = await stagePack(dir, pack, transferId);
      try {
        if (!staged.oids.includes(commitSha)) {
          throw new Error(
            `sha_mismatch: expected commit ${commitSha} not found in pack`,
          );
        }
        if (mode.kind === "expected" && priorTip !== mode.expectedOldSha) {
          const expected =
            mode.expectedOldSha === null ? "null" : mode.expectedOldSha;
          const observed = priorTip === null ? "null" : priorTip;
          throw new Error(
            `non_fast_forward: ref ${ref} expected ${expected} but found ${observed}`,
          );
        }
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
        //
        // The walk reads `existingCommits`, which a commit changes in
        // place, so it runs under the lock.
        const chain = await collectNewCommits(
          dir,
          staged.fs,
          cache,
          commitSha,
          priorTip,
          existingCommits,
        );
        return {
          principal,
          repoId,
          ref,
          commitSha,
          dir,
          handler,
          staged,
          incarnation: existingCommits,
          priorTip,
          chain,
        };
      } catch (err) {
        await staged.discard().catch(() => undefined);
        throw err;
      }
    });
  }

  // Validate each new commit of a staged pack against its own
  // first-parent tree, through the staged view. It needs no lock: each
  // result depends only on the commit and its parent, which no write
  // changes. Returns each commit's newly terminal runs, in chain order.
  async function validateStaged(
    s: StagedReceive,
  ): Promise<NewlyTerminalRun[][]> {
    const { principal, repoId, ref, dir, handler } = s;
    return withReceiveCache(dir, async (cache) => {
      const receive = receiveReads(dir, s.staged.fs, cache);
      const { reads } = receive;
      // Shared across the pack's commits so a subtree common to several is
      // walked once by the disallowed-mode gate below.
      const seenTreeOids = new Set<string>();
      // One scope for every commit of the pack, so a handler parses
      // a blob that many commits carry once per receive.
      const validationScope = {};
      const runs: NewlyTerminalRun[][] = [];
      for (const newCommit of s.chain) {
        // The memo window now holds this commit and its parent.
        receive.advance();
        const parsed = await reads.readCommit(newCommit);
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
        let parentTreeOid: string | null = null;
        if (declaredParent !== null) {
          try {
            parentTreeOid = (await reads.readCommit(declaredParent)).tree;
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
          buildPriorTreeClosures(reads, parentSha);
        const prospective = buildTreeReadClosures(reads, parsed.tree);
        const changedPathPrefixes = await computeChangedPathPrefixes(
          reads,
          parsed.tree,
          parentTreeOid,
        );
        // Admission gate for EVERY kind, before the per-kind validatePush:
        // an asset tree carrying a symlink (which can escape its directory
        // at checkout) or a submodule (a dangling gitlink the pack cannot
        // reproduce) is refused here, so the ref never advances. No kind
        // legitimately pushes either; `writeTreeToDisk` rejects both at
        // materialization as the defense-in-depth backstop. Per-commit, so
        // an intermediate commit that adds a symlink a later commit deletes
        // is still caught.
        const disallowed = await findDisallowedTreeMode(
          reads,
          parsed.tree,
          seenTreeOids,
        );
        if (disallowed !== null) {
          // The substrate owns the `path_violation:` prefix on the thrown
          // message, so the reason omits it (matching every handler reason).
          const reason = `${disallowed.kind} at ${disallowed.path} is not allowed in a pushed asset tree (commit ${newCommit})`;
          logger.debug`validatePush rejected ${repoId.kind}/${repoId.id} on ${ref} at commit ${newCommit}: ${reason}`;
          throw new Error(`path_violation: ${reason}`);
        }
        const result = await handler.validatePush({
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
          validationScope,
        });
        if (!result.ok) {
          logger.debug`validatePush rejected ${repoId.kind}/${repoId.id} on ${ref} at commit ${newCommit}: ${result.reason}`;
          throw new Error(`path_violation: ${result.reason}`);
        }
        runs.push(result.newlyTerminalRuns ?? []);
      }
      return runs;
    });
  }

  // The locked last step of every receive: publish the staged pack and
  // move the ref. The caller holds the repo lock. `runs` is the result
  // of `validateStaged`.
  async function commitStaged(
    s: StagedReceive,
    runs: readonly NewlyTerminalRun[][],
    mode: ReceiveMode,
  ): Promise<PackCommitResult> {
    const { repoId, ref, commitSha, dir, handler } = s;
    if (!repoStillHolds(s)) throw new RepoRemovedDuringReceiveError(repoId);
    const liveTip = await resolveRefSha(dir, ref);
    let firstNew = 0;
    if (mode.kind === "prepared") {
      // Another receive can move the ref while this one validates. A
      // live tip that already holds the pack tip leaves the ref where it
      // is. The walk stops at the prepare-time tip or at a commit of this
      // pack, because neither leads to the pack tip. A live tip on an
      // unrelated line walks to its root; the `existingCommits` check
      // first skips the walk when no ref has ever reached the pack tip.
      const stopAt = new Set(s.chain);
      if (s.priorTip !== null) stopAt.add(s.priorTip);
      if (
        s.incarnation.has(commitSha) &&
        (await withReceiveCache(dir, (cache) =>
          refHoldsCommit(dir, cache, liveTip, commitSha, stopAt),
        ))
      ) {
        return { kind: "already_applied" };
      }
      // A live tip inside this pack's chain already applied the commits
      // up to it, so only the later commits' runs are new. Any other live
      // tip takes the whole chain: the sender is authoritative for the
      // shape of its history.
      firstNew = liveTip === null ? 0 : s.chain.indexOf(liveTip) + 1;
    }

    // The store runs on the node runtime, which defines no flush, so no
    // flush step comes between the publish and the ref write.
    await s.staged.publish();
    await git.writeRef({ fs, dir, ref, value: commitSha, force: true });

    for (const sha of s.chain) s.incarnation.add(sha);
    await handler.onRefUpdated({
      repoId,
      ref,
      oldSha: liveTip,
      newSha: commitSha,
    });
    await emitRefUpdate(repoId, ref, liveTip, commitSha);
    return {
      kind: "applied",
      newlyTerminalRuns: runs.slice(firstNew).flat(),
    };
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
    const mode = { kind: "expected", expectedOldSha } as const;
    const prebuilt = await prebuildExistingCommits(repoId);

    // One lock span covers the stage, the CAS check against
    // `expectedOldSha`, the validation, the publish, the ref write, and
    // the onRefUpdated hook. The skill kind stages its index in
    // validatePush and promotes it in onRefUpdated, so no other write may
    // run between them. `oldSha` for onRefUpdated is the tip the CAS
    // check observed, read once under the lock.
    return withRepoLock(repoId, async () => {
      const s = await stageReceive(
        principal,
        repoId,
        ref,
        pack,
        commitSha,
        mode,
        prebuilt,
      );
      try {
        const runs = await validateStaged(s);
        const result = await commitStaged(s, runs, mode);
        await maybeRunGC(repoId);
        // A receive that checks `expectedOldSha` always moves the ref.
        return result.kind === "applied" ? result.newlyTerminalRuns : [];
      } finally {
        await discardStaged(s);
      }
    });
  }

  async function prepareReceivePack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
  ): Promise<PreparedReceivePack> {
    gateAccess(principal, repoId, ref, "receivePack");
    // The validation runs with no lock, and commit runs onRefUpdated
    // under a later lock. A kind whose validatePush stages state for
    // onRefUpdated (the skill kind does) needs one lock span over both,
    // so only the workflow-run kind, the one caller of this path, may
    // use it. The validation also depends on no pack being removed while
    // it reads. Only GC removes packs, so the kind must not be a GC kind.
    // For the same reason commit does not run GC.
    if (repoId.kind !== "workflow-run") {
      throw new Error(
        `prepareReceivePack: kind ${repoId.kind} is not workflow-run; a prepared receive validates with no repo lock, so it accepts only the workflow-run kind`,
      );
    }
    if (isReclaimedByGC(repoId)) {
      throw new Error(
        `prepareReceivePack: kind ${repoId.kind} is reclaimed by GC, which can remove a pack while a prepared receive reads it`,
      );
    }
    // No `expectedOldSha`: the ref can move while this receive validates,
    // so commit decides from the live tip instead of a CAS.
    const mode = { kind: "prepared" } as const;
    const prebuilt = await prebuildExistingCommits(repoId);
    const s = await withRepoLock(repoId, () =>
      stageReceive(principal, repoId, ref, pack, commitSha, mode, prebuilt),
    );
    if (s === null) return { kind: "already_applied" };

    let runs: NewlyTerminalRun[][];
    try {
      runs = await validateStaged(s);
    } catch (err) {
      // A repo removed during validation can fail its reads. That is not
      // a fault of the pack, so it gets its own error. `removeRepo`
      // changes the incarnation under the lock, so read it there. The
      // discard also takes the lock: a removal of the staging dir that
      // runs alongside the removal of the repo dir can leave part of the
      // repo dir behind.
      const removed = await withRepoLock(repoId, async () => {
        await discardStaged(s);
        return !repoStillHolds(s);
      });
      if (removed) {
        throw new RepoRemovedDuringReceiveError(repoId, { cause: err });
      }
      throw err;
    }

    // `commit()` claims the receive when it is called, not when it gets
    // the lock, so a `discard()` called after it does nothing while the
    // commit waits for the lock.
    let state: "open" | "committed" | "discarded" = "open";
    return {
      kind: "prepared",
      commit: async () => {
        if (state !== "open") {
          throw new Error(
            `prepared receive of ${commitSha} for ${repoId.kind}/${repoId.id} was already ${state}`,
          );
        }
        state = "committed";
        return withRepoLock(repoId, async () => {
          try {
            return await commitStaged(s, runs, mode);
          } finally {
            await discardStaged(s);
          }
        });
      },
      // The removal takes the repo lock for the same reason as the
      // discard of a failed validation.
      discard: async () => {
        if (state !== "open") return;
        state = "discarded";
        await withRepoLock(repoId, () => discardStaged(s));
      },
    };
  }

  async function createPack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pinned?: { commitSha: string; retain: boolean },
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    gateAccess(principal, repoId, ref, "createPack");
    if (pinned !== undefined) {
      if (
        principal.kind !== "hub" ||
        (repoId.kind !== "workflow" && repoId.kind !== "package-registry")
      ) {
        throw new Error(
          "Pinned definition packs require Hub access to a workflow or package-registry asset",
        );
      }
      if (!/^[a-f0-9]{40}$/.test(pinned.commitSha))
        throw new Error("Invalid pinned definition commit");
      return withRepoLock(repoId, async () => {
        const dir = repoDir(repoId);
        await git.readCommit({ fs, dir, oid: pinned.commitSha });
        if (pinned.retain) {
          // This ref keeps approved source bytes reachable after a branch is
          // replaced. Its name is its value, so retaining it is idempotent.
          const retainedRef = `${DEFINITION_REF_NAMESPACE}/${pinned.commitSha}`;
          await git.writeRef({
            fs,
            dir,
            ref: retainedRef,
            value: pinned.commitSha,
            force: true,
          });
        }
        const packed = await createDeployPack(dir, pinned.commitSha);
        return { ...packed, ref };
      });
    }
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
      const oids = await collectChainReachableObjects(
        repoId,
        dir,
        commitSha,
        stopAt,
      );
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
      // receiver acks the transfer that carried these commits. A
      // transfer a reconnect cancels before its ack thus leaves the
      // cursor where it was, so the next `createPack` re-includes the
      // un-acked commits and the receiver still gets a self-consistent
      // chain instead of a pack whose base commit it never received.
      return { pack: result.packfile, commitSha, ref };
    }
    const { pack, commitSha } = await createDeployPack(repoDir(repoId), ref);
    return { pack, commitSha, ref };
  }

  // Advance the incremental-pack cursor for `(repoId, ref)` to
  // `commitSha`. Called by the pack-push pipeline once the receiver has
  // acked the transfer that shipped `commitSha`. Gating the advance on
  // the ack — rather than advancing inside `createPack` when the pack
  // bytes are produced — is what lets a cancelled-before-ack transfer
  // (a reconnect drops the in-flight push) be re-shipped cleanly: the
  // cursor stays put, so the next `createPack` re-includes the un-acked
  // commits and the receiver gets a self-consistent chain. Only the
  // `workflow-run` kind ships incremental packs; for every other kind
  // `createPack` produces a self-contained deploy pack and the cursor
  // is unused, so this is a no-op there.
  function commitPackedTip(repoId: RepoId, ref: string, commitSha: string) {
    if (repoId.kind !== "workflow-run") return;
    lastPackedTip.set(lastPackedTipKey(repoId, ref), commitSha);
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
    repoId: RepoId,
    dir: string,
    tipSha: string,
    stopAt: string | null,
  ): Promise<string[]> {
    const seen = new Set<string>();
    let current: string | null = tipSha;
    while (current !== null) {
      if (current === stopAt) break;
      const cacheKey = `${indexCacheKey(repoId)}/${current}`;
      let perCommit = chainReachabilityCache.get(cacheKey);
      if (perCommit === undefined) {
        perCommit = await collectReachableObjects(dir, current);
        chainReachabilityCache.set(cacheKey, perCommit);
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
    const reads = dirReads(dir);
    const { readBlobByOid } = buildPriorTreeClosures(reads, commitSha);
    const treeOid = async (relPath: string): Promise<string | null> => {
      // "." and "" both name the root tree; resolveTreeEntry treats the
      // empty string as the root, so normalize "." to it.
      const normalized = relPath === "." ? "" : relPath;
      return resolveTreeEntry(reads, commitSha, normalized, "tree");
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
    // No single ref to gate on; use the same `"*"`/`resolveRef` bulk-read
    // gate that `listRefs` and `resolveHead` use.
    gateAccess(principal, repoId, "*", "resolveRef");
    if (!/^[0-9a-f]{40}$/.test(commitSha)) {
      throw new Error(`commit_sha_invalid: ${commitSha}`);
    }
    const dir = repoDir(repoId);
    if (!(await repoHasGitDir(dir))) return null;
    // Confirm the commit object is present so a pruned commit reads as a
    // clean `null` rather than a lazy throw on the first tree walk.
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

    // Seed an empty seq cache with the ref tip's depth, so an update
    // that lands after the seed walks under the repo lock only to that
    // tip, not the whole history. The seed runs with no repo lock, so an
    // update can cache its own entry first; the seed then leaves that
    // entry, which a history with a missing parent advances from. A
    // kind that GC reclaims gets no seed walk: GC can remove an object
    // during a walk with no lock, and the walk would count a history it
    // saw cut short. Its first update counts under the lock instead.
    async function seedFromTip(dir: string): Promise<void> {
      if (seqCache.has(key) || isReclaimedByGC(repoId)) return;
      const tip = await resolveRefSha(dir, ref);
      if (tip === null) return;
      const depth = await withReceiveCache(dir, (cache) =>
        commitDepth(dir, cache, tip, undefined),
      );
      if (depth !== null && !seqCache.has(key)) {
        seqCache.set(key, { tip, seq: depth });
      }
    }

    async function primeReplay(): Promise<void> {
      replayPrimed = true;
      const dir = repoDir(repoId);
      if (opts.from === "head") {
        // Subscribers that come in with `from: "head"` see only new
        // commits — there is no history to replay.
        await seedFromTip(dir);
        replayQueue = [];
        return;
      }
      const { entries: all, tip } = await replayHistory(dir, ref);
      // Seed an empty seq cache with the replay's tip. A replay with
      // entries read the whole history, so its seed holds for every
      // kind. A history that ends in a missing parent replays nothing,
      // so its seed comes from a walk of the readable history.
      const last = all.at(-1);
      if (last !== undefined && tip !== null) {
        if (!seqCache.has(key)) seqCache.set(key, { tip, seq: last.seq });
      } else {
        await seedFromTip(dir);
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

  async function removeRepo(
    repoId: RepoId,
    { last }: { readonly last?: string } = {},
  ): Promise<void> {
    const dir = repoDir(repoId);
    await withRepoLock(repoId, async () => {
      try {
        if (last !== undefined) {
          let entries: string[];
          try {
            entries = await fs.promises.readdir(dir);
          } catch (cause) {
            if (
              !(cause instanceof Error && "code" in cause) ||
              cause.code !== "ENOENT"
            )
              throw cause;
            entries = [];
          }
          for (const entry of entries) {
            if (entry === last) continue;
            await fs.promises.rm(path.join(dir, entry), {
              recursive: true,
              force: true,
            });
          }
          // Keep the recovery record until the other entries are durably gone.
          if (entries.length > 0) await syncDirectoryDurable(dir);
          await fs.promises.rm(path.join(dir, last), {
            recursive: true,
            force: true,
          });
        }
        await fs.promises.rm(dir, { recursive: true, force: true });
        await syncRemovedPathDurable(dir);
      } finally {
        // Failed deletion or sync may still have changed the repository on disk.
        invalidateGitCache(dir);
        // This delete is the repo's incarnation boundary: a prepared
        // receive holds the deleted set and finds a different entry, or
        // none, when it commits, and a set prebuilt before the delete
        // no longer matches the repo's token.
        existingCommitsCache.delete(indexCacheKey(repoId));
        existingCommitsEpochs.delete(indexCacheKey(repoId));
        existingCommitsScans.delete(indexCacheKey(repoId));
        cleanedStagingDirs.delete(dir);
        const refPrefix = `${indexCacheKey(repoId)}/`;
        for (const key of [...seqCache.keys()]) {
          if (key.startsWith(refPrefix)) seqCache.delete(key);
        }
        for (const key of [...lastPackedTip.keys()]) {
          if (key.startsWith(refPrefix)) lastPackedTip.delete(key);
        }
        for (const key of [...chainReachabilityCache.keys()]) {
          if (key.startsWith(refPrefix)) chainReachabilityCache.delete(key);
        }
      }
    });
  }

  return {
    initRepo,
    writeTree,
    writeTreePreservingPrefix,
    writeTreeDelta,
    receivePack,
    prepareReceivePack,
    createPack,
    commitPackedTip,
    resolveRef,
    listRefs,
    resolveHead,
    getRepoDir,
    openCommittedReads,
    openCommittedReadsAtCommit,
    subscribe,
    removeRepo,
  };
}
