import type { RepoKind, RepoId, RepoAction } from "@intx/types/sidecar";

export type { RepoKind, RepoId, RepoAction };

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

export type AuthorizeFn = (
  principal: Principal,
  repoId: RepoId,
  ref: string,
  action: RepoAction,
) => { allowed: true } | { allowed: false; reason: string };

export type ValidatePushResult = { ok: true } | { ok: false; reason: string };

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
  initRepo(repoId: RepoId): Promise<void>;
  writeTree(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    content: TreeContent,
  ): Promise<{ commitSha: string }>;
  receivePack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
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
}
