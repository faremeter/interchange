import { type } from "arktype";
import { RepoAction as RepoActionSchema } from "@intx/types/sidecar";
import type { RepoKind, RepoId, RepoAction } from "@intx/types/sidecar";

export type { RepoKind, RepoId, RepoAction };

/**
 * arktype validator for the `user` principal variant. The substrate
 * only requires the `kind` discriminant; kind handlers that accept
 * user-token-authenticated requests rely on this shape to
 * cross-check the pre-resolved authz verdict against the bearer
 * token's claims. The validator is exported alongside the type so
 * handlers can call it for a structural narrow without re-declaring
 * the shape.
 *
 * Field semantics:
 *   - `authz`: the pre-resolved grant verdict from the route layer.
 *     The kind handler does NOT re-query the grant store; it only
 *     sanity-checks that the verdict targets the right resource and
 *     grant verb, then defers to `effect`.
 *   - `tokenClaims`: the bearer-token's scope. The kind handler
 *     verifies the requested `(ref, action)` falls inside this scope
 *     synchronously (`actions.includes(action)`,
 *     `glob.match(refPattern, ref)`, `Date.now() < expiresAt`).
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

/**
 * Regex defining the shape of a valid `RepoId.id`. The substrate
 * validates against this at every public operation and throws an Error
 * prefixed with `"repo_id_invalid: "` on mismatch.
 */
export const SAFE_REPO_ID = /^[a-zA-Z0-9_-]+$/;

/**
 * Principal is a discriminated-union extension point. The substrate
 * requires only the `kind` discriminant; concrete principal shapes live
 * in kind-handler packages and are narrowed via `principal.kind === "..."`
 * checks plus arktype validation in the handler. No index signature is
 * declared here so that handlers do not need `as Type` casts to access
 * their own fields.
 */
export type Principal = { readonly kind: string };

/**
 * Authorization callback supplied to the repo-store. Called once per
 * substrate operation that requires gating (`writeTree`, `receivePack`,
 * `createPack`, `resolveRef`, plus the bulk-read variants below). The
 * substrate translates an `allowed: false` verdict into a thrown Error
 * prefixed with `"authorize_denied: "` carrying the supplied reason.
 *
 * The substrate passes the literal string `"*"` as `ref` when it calls
 * the authorize hook on behalf of the bulk-read methods `listRefs` and
 * `resolveHead`. Both methods enumerate refs across the whole repo and
 * have no single ref to feed into a per-ref claim check; the sentinel
 * lets kind handlers recognise the call and skip the per-ref
 * `refPattern` match while still gating on `action` (always
 * `"resolveRef"` for the bulk case) and expiry. Per-ref refPattern
 * filtering for the response payload is the responsibility of the
 * caller (the advertise-refs layer), not the authorize hook.
 */
export type AuthorizeFn = (
  principal: Principal,
  repoId: RepoId,
  ref: string,
  action: RepoAction,
) => { allowed: true } | { allowed: false; reason: string };

export type ValidatePushResult = { ok: true } | { ok: false; reason: string };

/**
 * Per-call options for `initRepo`. Currently a single override —
 * `gitignore` — that overrides the body written to `.gitignore` in
 * the genesis tree. When omitted, the substrate's default body is
 * used. The asset REST handler supplies a richer body that includes
 * OS/editor cruft, common build output, and `keys/`.
 */
export type InitRepoOpts = {
  gitignore?: string;
};

/**
 * A single ref entry returned by `RepoStore.listRefs`. `name` is the
 * fully-qualified ref name (`refs/heads/main`, `refs/tags/v1`, ...);
 * `sha` is the SHA-1 the ref currently resolves to.
 */
export type RefEntry = {
  readonly name: string;
  readonly sha: string;
};

export type TreeContent = {
  /**
   * Map of repo-relative path to file contents. Each entry is written
   * to the working tree and staged before commit.
   */
  files: Record<string, string | Uint8Array>;
  /**
   * Optional directory-subtree prefix to clear before staging. When
   * set, every tracked path beginning with this prefix is removed
   * from the git index and the corresponding directory on disk is
   * deleted before `files` is written. Must end with `/` and must
   * not contain `..` or absolute path components. When unset,
   * writeTree is purely additive.
   */
  clearPrefix?: string;
  /** Commit message for the resulting commit. */
  message: string;
};

/**
 * Per-call options for `RepoStore.writeTreePreservingPrefix`. The
 * substrate enumerates existing blobs under `preservePrefix` from the
 * current ref tip while holding the per-repo lock, calls the caller's
 * `merge` callback with those entries, and writes the returned set as
 * the new value of the prefix subtree. Two concurrent callers
 * targeting the same prefix serialize at the lock so neither one
 * observes a stale pre-image of the other's commit.
 */
export type WriteTreePreservingPrefixArgs = {
  /**
   * Directory-subtree prefix whose existing blobs are surfaced to
   * `merge` and then replaced wholesale by its return value. Must end
   * with `/` and must not contain `..` or absolute path components.
   */
  preservePrefix: string;
  /**
   * Called under the per-repo lock with the current set of blobs
   * directly under `preservePrefix` (keyed by repo-root-relative
   * path, including the prefix). Returns the full set of files the
   * substrate should write at the prefix; the prefix subtree is
   * cleared and replaced with this set in a single commit. Paths
   * outside the prefix are passed through unchanged. The callback may
   * throw to abort the write; the substrate releases the lock and
   * propagates the error.
   */
  merge: (
    existing: ReadonlyMap<string, Uint8Array>,
  ) => Promise<Record<string, string | Uint8Array>>;
  /** Commit message for the resulting commit. */
  message: string;
};

export interface KindHandler {
  kind: RepoKind;
  /**
   * On-disk directory under `dataDir` for repos of this kind. Allows
   * each kind to declare its own layout (e.g. "agents") so the
   * substrate does not hard-code a `<kind>/<id>` path.
   */
  directoryPrefix: string;
  /**
   * Inspect the prospective commit's tree before the ref is
   * advanced. Return `{ ok: false, reason }` to reject the write.
   * The substrate translates rejection into a thrown Error whose
   * message begins with `"path_violation: "`.
   *
   * Runs on every `receivePack` and every `writeTree` independently
   * of the authorize verdict: authorize gates access, validatePush
   * enforces content rules.
   *
   * `topLevelTreePaths` lists the names directly under the prospective
   * tree root. `readBlob` reads any blob in the prospective tree by
   * repo-root-relative POSIX path (e.g. `greet/SKILL.md`). `listDir`
   * enumerates the names directly under a tree-root-relative POSIX
   * directory path (no trailing slash, no leading slash); pass the
   * empty string to list the root. Handlers that only need path-level
   * checks can ignore `readBlob` and `listDir`.
   *
   * `priorReadBlob` and `priorListDir` mirror `readBlob` / `listDir`
   * against the parent commit's tree — the ref's tip at the moment
   * validatePush runs. Handlers use these to compare prospective
   * content against the immediately-prior bytes (e.g. enforcing
   * append-only invariants by rejecting any path whose prior bytes
   * differ from the prospective bytes). `priorReadBlob` returns
   * `null` when the path did not exist at the prior tree (or the
   * ref has no prior commit — first push). `priorListDir` returns
   * an empty array in the same cases.
   *
   * `principal` is the principal performing the push, the same value
   * fed to the `authorize` hook. Handlers use it for principal-vs-
   * payload cross-checks that a structural shape validator cannot
   * express (e.g. "only a `hub` principal may write a `CancelRequested`
   * whose origin is `hub-admin`").
   *
   * `changedPathPrefixes` is the set of repo-root-relative POSIX path
   * prefixes (each ending in `/`) under which this commit could have
   * mutated tree entries -- the cleared prefix for a `writeTree`
   * carrying a `clearPrefix`, or the subtrees whose object differs from
   * the parent commit for a received pack. Every path the commit can
   * have changed relative to its parent is under one of these prefixes;
   * any path outside them is carried forward byte-identical by the
   * substrate. It is `undefined` when the substrate cannot bound the
   * change set (no parent to diff against and no `clearPrefix`), in
   * which case the handler must validate the whole prospective tree. A
   * handler with per-subtree invariants that cannot be affected by a
   * commit outside that subtree (workflow-run's per-run append-only
   * log) uses this to skip re-validating subtrees the commit provably
   * did not touch; a handler with no such structure ignores it and
   * validates unconditionally.
   */
  validatePush: (args: {
    repoId: RepoId;
    ref: string;
    principal: Principal;
    topLevelTreePaths: string[];
    readBlob: (path: string) => Promise<Uint8Array>;
    listDir: (path: string) => Promise<string[]>;
    priorReadBlob: (path: string) => Promise<Uint8Array | null>;
    priorListDir: (path: string) => Promise<string[]>;
    changedPathPrefixes?: ReadonlySet<string> | undefined;
  }) => Promise<ValidatePushResult> | ValidatePushResult;
  /**
   * Fired after a successful ref update from any operation. `oldSha`
   * is `null` when the ref did not exist before the update.
   */
  onRefUpdated: (args: {
    repoId: RepoId;
    ref: string;
    oldSha: string | null;
    newSha: string;
  }) => Promise<void> | void;
}

export interface RepoStore {
  /**
   * Bookkeeping primitive. Idempotent. Creates the repo directory
   * and initializes git when not already present. Not gated by
   * `authorize`: the only state it can produce is an empty repo, so
   * the higher-level question of who may mint a new `<kind>/<id>`
   * lives at the caller. The substrate also calls `initRepo`
   * internally from `writeTree` and `receivePack`, so first-touch
   * operations succeed without an explicit init call.
   */
  initRepo(repoId: RepoId, opts?: InitRepoOpts): Promise<void>;
  writeTree(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    content: TreeContent,
  ): Promise<{ commitSha: string }>;
  /**
   * Read-then-write variant for use cases that mutate a single
   * directory subtree against its current contents (overwrite one
   * entry, delete one entry, augment by one entry). The substrate
   * enumerates blobs under `args.preservePrefix` while holding the
   * per-repo lock, invokes `args.merge` with those entries, and
   * commits the returned set as the new value of the prefix.
   *
   * Two concurrent callers targeting the same prefix serialize at the
   * lock, so the merge callback's pre-image is always the previous
   * commit's tip — there is no read-outside-the-lock window where one
   * caller could base its write on a stale view of the prefix.
   *
   * The substrate handles `clearPrefix` and the commit internally;
   * paths outside the prefix are untouched.
   */
  writeTreePreservingPrefix(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    args: WriteTreePreservingPrefixArgs,
  ): Promise<{ commitSha: string }>;
  /**
   * Receive a packfile and advance `ref` to `commitSha`.
   *
   * `expectedOldSha` is a compare-and-set guard the substrate runs
   * under the per-repo lock. Pass a SHA string to require the ref
   * currently points there; pass `null` to require the ref does not
   * yet exist. On mismatch the call throws with a `non_fast_forward:`
   * prefix and leaves the ref untouched.
   *
   * Callers that do not have the old SHA in hand should resolve it
   * via `resolveRef` first; the substrate exposes no force-write
   * mode because silently overwriting a losing concurrent update is
   * never the right behavior.
   */
  receivePack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
    expectedOldSha: string | null,
  ): Promise<void>;
  createPack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }>;
  resolveRef(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<string | null>;
  /**
   * Enumerate the repo's refs (branches and tags), lexicographically
   * sorted by name. The principal is gated under the same
   * `resolveRef` action that `resolveRef` itself enforces — the
   * substrate does not duplicate the check on a per-ref basis. When
   * the on-disk repo does not yet exist (the bookkeeping primitive
   * `initRepo` has never been called), the result is the empty list.
   */
  listRefs(principal: Principal, repoId: RepoId): Promise<RefEntry[]>;
  /**
   * Resolve HEAD into the ref it symbolically points at plus the SHA
   * that ref currently resolves to. The principal is gated under the
   * same `resolveRef` action that `resolveRef` and `listRefs`
   * enforce. Returns `null` when:
   *   - The on-disk repo does not yet exist (mirrors `listRefs`'s
   *     empty-list contract for uninitialised repos).
   *   - HEAD is detached (no symbolic target).
   *   - HEAD's symbolic target does not resolve (unborn ref).
   * The smart-HTTP advertise layer uses the result to emit
   * `symref=HEAD:<target>` so stock `git clone` lands on a real
   * branch instead of leaving the working tree unborn.
   */
  resolveHead(
    principal: Principal,
    repoId: RepoId,
  ): Promise<{ symbolicTarget: string; sha: string } | null>;
  /**
   * Synchronously return the on-disk directory backing the repo.
   * The path is the result of composing the substrate's `dataDir`,
   * the kind handler's `directoryPrefix`, and the validated
   * `repoId.id`. This carries no authorize gate: it is a pure path
   * computation. Consumers of the path (the smart-HTTP wire
   * handlers) remain authorize-gated through the substrate methods
   * they reach into for ref-listing and pack negotiation.
   */
  getRepoDir(repoId: RepoId): string;
  /**
   * Tail a ref's commit log. Returns an async iterator that emits
   * `{ seq, event }` entries: one per commit on the ref. `seq` is
   * zero-indexed at the ref's root commit and counts ancestors
   * walking forward to HEAD, so the same commit always carries the
   * same `seq` across restarts. The emitted `event` is the
   * substrate-level commit descriptor; consumers that need richer
   * shapes layer their own decoding on top.
   *
   * Cancellation: when `opts.signal` aborts, the iterator ends
   * cleanly (no throw from the consumer's `for await`). The
   * substrate releases the watcher slot on the same abort tick.
   *
   * Replay vs live:
   *   - `from: { seq: number }` enumerates every prior commit on the
   *     ref whose computed `seq` is >= the supplied number, then
   *     transitions to live mode and continues with new commits.
   *   - `from: "head"` records HEAD-of-ref at subscribe time and
   *     emits only commits that land strictly after.
   *
   * Backpressure: events are buffered in userspace bounded by
   * `bufferLimit` (default 1024). On overrun the iterator throws a
   * loud error; silent drop would corrupt audit. Consumers that
   * cannot keep up are expected to abort.
   *
   * The substrate's vocabulary is the ref-update envelope. Consumers
   * that need to filter on a richer event kind (e.g. a workflow-event
   * `type` discriminator committed at the new ref) layer a decoder on
   * top — see `subscribeKind` for the typed entrypoint that loads the
   * committed payload, narrows it with an arktype validator, and
   * applies a per-call kind filter.
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
 * Substrate-level event shape emitted by `RepoStore.subscribe`.
 * Each successful commit on a watched ref produces one event with
 * this shape. The substrate is schema-agnostic; higher layers that
 * want to surface richer event vocabularies build their own decoders
 * on top (see `subscribeKind` for the workflow-event entrypoint).
 */
export type RepoStoreSubscribeEvent = {
  readonly type: "ref.updated";
  readonly ref: string;
  readonly oldSha: string | null;
  readonly newSha: string;
};
