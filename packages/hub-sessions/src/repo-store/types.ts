import { type } from "arktype";
import { RepoAction as RepoActionSchema } from "@intx/types/sidecar";
import type { RepoKind, RepoId, RepoAction } from "@intx/types/sidecar";

export type { RepoKind, RepoId, RepoAction };

/**
 * arktype validator for the `user` principal variant, exported so kind
 * handlers can narrow a principal structurally. The route layer already
 * resolved the grant verdict (`authz`); the handler only cross-checks it
 * against the bearer-token's claims (`tokenClaims`).
 */
export const UserPrincipal = type({
  kind: "'user'",
  principalId: "string",
  tenantId: "string",
  authz: {
    effect: "'allow' | 'deny'",
    resource: "string",
    grantVerb: "string",
  },
  tokenClaims: {
    refPattern: "string",
    actions: RepoActionSchema.array(),
    expiresAt: "number",
  },
});

export type UserPrincipal = typeof UserPrincipal.infer;

/** Valid `RepoId.id` shape; the substrate rejects mismatches with `repo_id_invalid:`. */
export const SAFE_REPO_ID = /^[a-zA-Z0-9_-]+$/;

/**
 * Discriminated-union extension point: the substrate needs only `kind`;
 * concrete shapes live in kind handlers, narrowed by `kind` checks plus
 * arktype validation.
 */
export type Principal = { readonly kind: string };

/**
 * Authorization callback for gated substrate operations (`writeTree`,
 * `receivePack`, `createPack`, `resolveRef`, bulk reads). An
 * `allowed: false` verdict throws `authorize_denied: <reason>`.
 * Bulk reads (`listRefs`, `resolveHead`) pass `"*"` as `ref`: they
 * have no single ref for a per-ref claim check; per-ref filtering is
 * the advertise-refs layer's job.
 */
export type AuthorizeFn = (
  principal: Principal,
  repoId: RepoId,
  ref: string,
  action: RepoAction,
) => { allowed: true } | { allowed: false; reason: string };

/**
 * A run whose terminal event is newly added by the commit under validation
 * (present in the prospective tree, absent from the prior). Detected
 * authoritatively by the kind handler so callers do not re-derive
 * terminal-ness from path shapes; `status` matches `workflow_run.status`;
 * `terminalEventJson` carries the raw event bytes. Already-terminal runs
 * carried forward unchanged are NOT newly terminal and do not appear.
 */
export type NewlyTerminalRun = {
  runId: string;
  status: "completed" | "failed" | "cancelled";
  terminalEventJson: string;
};

export type ValidatePushResult =
  | { ok: true; newlyTerminalRuns?: NewlyTerminalRun[] }
  | { ok: false; reason: string };

/**
 * Result of a writeTree commit: the new commit SHA plus the terminal runs
 * the kind handler detected (empty for handlers and commits that produce
 * none).
 */
export type WriteResult = {
  commitSha: string;
  newlyTerminalRuns: readonly NewlyTerminalRun[];
};

/**
 * Per-call `initRepo` options: `gitignore` overrides the genesis
 * `.gitignore` body (the asset REST handler supplies a richer one).
 */
export type InitRepoOpts = {
  gitignore?: string;
};

/** One `RepoStore.listRefs` entry: fully-qualified ref name and the SHA it resolves to. */
export type RefEntry = {
  readonly name: string;
  readonly sha: string;
};

export type TreeContent = {
  /** Repo-relative path to contents, written to the working tree and staged. */
  files: Record<string, string | Uint8Array>;
  /**
   * Subtree prefix to clear before staging: every tracked path under it
   * is removed and the directory deleted. Must end with `/`, no `..` or
   * absolute components. Unset = purely additive.
   */
  clearPrefix?: string;
  /** Commit message for the resulting commit. */
  message: string;
};

/**
 * Options for `writeTreePreservingPrefix`. The substrate enumerates
 * blobs under `preservePrefix` under the per-repo lock, calls `merge`,
 * and commits the returned set as the prefix's new value; concurrent
 * callers serialize so no one sees a stale pre-image.
 */
export type WriteTreePreservingPrefixArgs = {
  /**
   * Subtree prefix surfaced to `merge` and replaced by its return
   * value. Must end with `/`, no `..` or absolute components.
   */
  preservePrefix: string;
  /**
   * Called under the per-repo lock with the current blobs under
   * `preservePrefix`; returns the full replacement set (paths outside
   * the prefix pass through unchanged). May throw to abort; the
   * substrate releases the lock and propagates the error.
   */
  merge: (
    existing: ReadonlyMap<string, Uint8Array>,
  ) => Promise<Record<string, string | Uint8Array>>;
  /** Commit message for the resulting commit. */
  message: string;
};

/**
 * Options for `writeTreeDelta`. `computeDelta` runs under the per-repo
 * lock against the pinned parent tip (`parentCommitSha`, null when the
 * ref does not yet exist) and returns the exact files to put and paths
 * to delete; everything else is carried forward by oid. A delete ending
 * in `/` clears a subtree; other deletes clear a single file; a delete
 * whose base entry is the wrong type is rejected.
 *
 * `changedPathPrefixes` scopes validation to the touched region
 * (`undefined` = validate the whole tree). `prior` exposes
 * cache-backed reads of the pinned parent tree under the same lock.
 */
export type PriorDeltaReads = {
  readBlobByOid: (oid: string) => Promise<Uint8Array>;
  listDirOids: (path: string) => Promise<{ name: string; oid: string }[]>;
};

export type WriteTreeDeltaArgs = {
  computeDelta: (
    parentCommitSha: string | null,
    prior: PriorDeltaReads,
  ) => Promise<{
    puts: Record<string, string | Uint8Array>;
    deletes: readonly string[];
  }>;
  changedPathPrefixes: ReadonlySet<string> | undefined;
  message: string;
};

export interface KindHandler {
  kind: RepoKind;
  /**
   * On-disk directory under `dataDir` for repos of this kind, so the
   * substrate does not hard-code a `<kind>/<id>` path.
   */
  directoryPrefix: string;
  /**
   * Inspect a prospective commit's tree before the ref advances; return
   * `{ ok: false, reason }` to reject (thrown as `path_violation: <reason>`).
   * Runs on every receivePack and writeTree independently of authorize:
   * authorize gates access, validatePush enforces content rules.
   *
   * `topLevelTreePaths` lists the root's names; `readBlob`/`listDir` walk
   * the prospective tree by repo-root-relative POSIX path (empty string =
   * root). `priorReadBlob`/`priorListDir` mirror them against the parent
   * commit's tree (null/empty when the ref has no prior commit) so a
   * handler can enforce append-only invariants against the prior bytes.
   * `principal` is the push's principal, as fed to the authorize hook.
   * `changedPathPrefixes` bounds the paths the commit can have changed
   * (repo-root-relative prefixes ending in `/`); `undefined` means the
   * change set is unbounded and the whole tree must be validated.
   * `priorListDirOids`/`listDirOids` return child entries with their git
   * object ids so a handler can prove byte-unchanged retention by OID
   * equality without re-reading blobs (absent when there is no prior
   * commit / the path does not surface it).
   */
  validatePush: (args: {
    repoId: RepoId;
    ref: string;
    principal: Principal;
    topLevelTreePaths: string[];
    readBlob: (path: string) => Promise<Uint8Array>;
    listDir: (path: string) => Promise<string[]>;
    listDirOids?: (path: string) => Promise<{ name: string; oid: string }[]>;
    priorReadBlob: (path: string) => Promise<Uint8Array | null>;
    priorListDir: (path: string) => Promise<string[]>;
    priorListDirOids?: (
      path: string,
    ) => Promise<{ name: string; oid: string }[]>;
    changedPathPrefixes?: ReadonlySet<string> | undefined;
  }) => Promise<ValidatePushResult> | ValidatePushResult;
  /** Called after a successful ref update from any operation; `oldSha` is null when the ref did not exist before. */
  onRefUpdated: (args: {
    repoId: RepoId;
    ref: string;
    oldSha: string | null;
    newSha: string;
  }) => Promise<void> | void;
}

/**
 * One `CommittedReads.listDir` child: the entry's own path segment,
 * its git object id, and its git tree-entry kind.
 */
export type CommittedTreeEntry = {
  readonly name: string;
  readonly oid: string;
  readonly type: "blob" | "tree" | "commit";
};

/**
 * Cache-backed reads pinned to the commit a ref resolved to when
 * `openCommittedReads` was called; every read resolves through the git
 * object store, never the materialized working tree, so a concurrent
 * ref advance does not shift the snapshot. `listDir` returns direct
 * children (absent or non-tree paths list as `[]`); `readBlobByOid`
 * reads by object id and throws on a read fault rather than returning
 * an empty result.
 */
export type CommittedReads = {
  listDir(relPath: string): Promise<CommittedTreeEntry[]>;
  readBlobByOid(oid: string): Promise<Uint8Array>;
  /**
   * Git tree object id of the subtree at `relPath` ("" or "." = the
   * commit's root tree), or null when absent or non-tree. This is the
   * content identity a source closure freezes for a materialized package.
   */
  treeOid(relPath: string): Promise<string | null>;
};

export interface RepoStore {
  /**
   * Idempotent bookkeeping: create the repo directory and initialize
   * git when absent. Not authorize-gated (it can only produce an empty
   * repo); also called internally by `writeTree` and `receivePack`.
   */
  initRepo(repoId: RepoId, opts?: InitRepoOpts): Promise<void>;
  writeTree(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    content: TreeContent,
  ): Promise<WriteResult>;
  /**
   * Read-then-write for mutating one subtree against its current
   * contents. Enumerates blobs under `args.preservePrefix` under the
   * per-repo lock, calls `args.merge`, and commits the returned set;
   * concurrent callers serialize so the merge pre-image is always the
   * previous tip — no read-outside-the-lock window. `clearPrefix` and
   * the commit are handled internally; paths outside the prefix are
   * untouched.
   */
  writeTreePreservingPrefix(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    args: WriteTreePreservingPrefixArgs,
  ): Promise<WriteResult>;
  /**
   * Delta variant for mutating a few named entries in a large subtree
   * without re-materializing the untouched siblings: `computeDelta` runs
   * under the per-repo lock against the pinned parent tip and returns
   * the files to put and paths to delete; everything else is carried
   * forward by oid. Prefer over `writeTreePreservingPrefix` when the
   * untouched remainder is large (e.g. a claim-check move).
   */
  writeTreeDelta(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    args: WriteTreeDeltaArgs,
  ): Promise<WriteResult>;
  /**
   * Receive a packfile and advance `ref` to `commitSha`.
   *
   * `expectedOldSha` is a CAS guard under the per-repo lock: a SHA
   * string requires the ref to point there, `null` requires it not to
   * exist yet; on mismatch the call throws `non_fast_forward:` and
   * leaves the ref untouched. Resolve the old SHA via `resolveRef`
   * first; there is no force-write mode.
   *
   * Returns the runs the pack drove to a terminal event, detected
   * authoritatively by the kind handler, so callers can react without
   * re-deriving terminal-ness from the committed path shape.
   */
  receivePack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
    expectedOldSha: string | null,
  ): Promise<NewlyTerminalRun[]>;
  createPack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }>;
  /**
   * Commit the receiver-acked "last shipped tip" for an incremental
   * (`workflow-run`) pack ref, advancing the cursor `createPack` walks
   * back to. Called only once the receiver has acked the transfer that
   * shipped `commitSha`, so a cancelled transfer re-ships the un-acked
   * commits; building a pack must NOT advance the cursor itself. No-op
   * for kinds whose packs are not incremental.
   */
  commitPackedTip(repoId: RepoId, ref: string, commitSha: string): void;
  resolveRef(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<string | null>;
  /**
   * Enumerate the repo's refs (branches and tags), lexicographically
   * sorted. Gated under the same `resolveRef` action as `resolveRef`;
   * the substrate does not duplicate the check per ref. Empty list
   * when the on-disk repo does not yet exist.
   */
  listRefs(principal: Principal, repoId: RepoId): Promise<RefEntry[]>;
  /**
   * Resolve HEAD to its symbolic target plus the SHA that ref resolves
   * to. Gated under `resolveRef`. Returns null when the repo does not
   * exist, HEAD is detached, or the target ref is unborn. The
   * smart-HTTP advertise layer uses this to emit `symref=HEAD:<target>`
   * so stock `git clone` lands on a real branch.
   */
  resolveHead(
    principal: Principal,
    repoId: RepoId,
  ): Promise<{ symbolicTarget: string; sha: string } | null>;
  /**
   * Synchronously return the on-disk repo dir: the substrate's
   * `dataDir`, the kind handler's `directoryPrefix`, and the validated
   * `repoId.id`. Not authorize-gated (pure path computation); consumers
   * of the path remain gated through the substrate methods they call.
   */
  getRepoDir(repoId: RepoId): string;
  /**
   * Open cache-backed reads pinned to `ref`'s tip at call time,
   * resolving through the git object store — not the materialized
   * working tree, which a non-atomic post-commit materialization can
   * leave lagging. Gated under `resolveRef`. Null when the repo does
   * not exist or `ref` does not resolve to a commit.
   */
  openCommittedReads(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<CommittedReads | null>;
  /**
   * By-SHA counterpart of `openCommittedReads`: read the exact commit
   * a consumer already holds (e.g. a ref-update event's newSha), even
   * after the ref has advanced past it. Gated under `resolveRef`;
   * malformed SHAs throw `commit_sha_invalid`. Null when the repo does
   * not exist or the commit is missing from the object store (a
   * concurrent GC prune), so a diffing caller gets an empty view rather
   * than a mid-walk throw.
   */
  openCommittedReadsAtCommit(
    principal: Principal,
    repoId: RepoId,
    commitSha: string,
  ): Promise<CommittedReads | null>;
  /**
   * Tail a ref's commit log: one `{ seq, event }` entry per commit.
   * `seq` is zero-indexed at the root commit and stable across
   * restarts; the emitted `event` is the substrate-level commit
   * descriptor. On `opts.signal` abort the iterator ends cleanly and
   * the watcher slot is released on the same tick.
   *
   * Replay vs live: `from: { seq }` enumerates every prior commit with
   * seq >= the given number then goes live; `from: "head"` records the
   * tip at subscribe time and emits only commits that land strictly
   * after.
   *
   * Backpressure: events buffer in userspace up to `bufferLimit`
   * (default 1024); on overrun the iterator throws. Consumers that
   * cannot keep up are expected to abort.
   */
  subscribe(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    opts: {
      signal: AbortSignal;
      from: "head" | { seq: number };
      bufferLimit?: number;
    },
  ): AsyncIterableIterator<{ seq: number; event: unknown }>;
}

/**
 * Substrate-level event shape emitted by `RepoStore.subscribe` per
 * successful commit on a watched ref. Higher layers decode richer
 * vocabularies on top (see `subscribeKind`).
 */
export type RepoStoreSubscribeEvent = {
  readonly type: "ref.updated";
  readonly ref: string;
  readonly oldSha: string | null;
  readonly newSha: string;
};
