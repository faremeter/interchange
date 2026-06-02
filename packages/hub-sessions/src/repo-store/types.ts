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
 * prefixed with `"repo_id_invalid: "` on mismatch. Exposed so callers
 * that want to validate ids before constructing a `RepoId` (e.g. shims
 * that throw their own legacy error messages) can use the same rule.
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
   * `topLevelTreePaths` lists the names directly under the tree
   * root. `readBlob` reads any blob in the tree by repo-root-relative
   * POSIX path (e.g. `greet/SKILL.md`). Handlers that only need
   * path-level checks can ignore `readBlob`.
   */
  validatePush: (args: {
    repoId: RepoId;
    ref: string;
    topLevelTreePaths: string[];
    readBlob: (path: string) => Promise<Uint8Array>;
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
}
