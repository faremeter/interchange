// Shared internals of closure materialization: the failure type, the fetch
// byte/time caps, and the small cross-cutting helpers. Split out so the
// registry-fetch, tarball-fetcher and store-layout modules can all depend on
// them without an import cycle.

/**
 * Maximum bytes a single registry tarball fetch will read before aborting. The
 * cap bounds the memory a malicious or misconfigured registry can force a
 * deploy to buffer.
 */
export const DEFAULT_MAX_REGISTRY_TARBALL_BYTES = 10 * 1024 * 1024;

/**
 * Default deadline for a single HTTP-registry tarball fetch, covering both the
 * request and the streamed body read. `readResponseWithLimit` consumes the body
 * through a manual reader loop, so the byte cap bounds size but nothing bounds
 * time: a registry that accepts the connection and then stalls mid-stream would
 * block the fetch -- and the closure materialization awaiting it --
 * indefinitely. The deadline is generous so a legitimately large tarball on a
 * slow link still completes within it. Callers that need a different bound pass
 * `registryFetchTimeoutMs` to `createTarballFetcher`.
 */
export const DEFAULT_REGISTRY_FETCH_TIMEOUT_MS = 120 * 1000;

/**
 * Categories of closure materialization failure. Each value names one failure
 * kind; a single category fires per failed attempt.
 *
 * - `tarball.missing`: a manifest entry's asset-sourced tarball is not present
 *   at the recorded path.
 * - `asset.mount.missing`: a `kind: "asset"` manifest entry names an `assetId`
 *   the asset mount map does not cover. That is a mismatch between the assets
 *   the closure names and those delivered, not a missing file.
 * - `integrity.mismatch`: fetched tarball bytes do not match the manifest's
 *   pinned SRI integrity.
 * - `registry.fetch.failed`: the configured registry refused or dropped the
 *   request for a tarball.
 * - `registry.unknown`: the manifest entry references a registry name not
 *   present in the sidecar's registry config.
 * - `registry.auth.failed`: the registry rejected the sidecar's credentials.
 * - `tarball.extract.failed`: reading, re-verifying, or unpacking the cached
 *   tarball failed.
 * - `git.materialization.failed`: a git-sourced entry could not be materialized
 *   from its checked-out subtree, or reached the tarball fetcher.
 * - `package.entry.invalid`: a closure entry could not be laid out: its
 *   package.json is malformed, a path escapes its root, its dependencies
 *   collide, or a hard dependency has no satisfying closure version.
 */
export type ClosureMaterializationErrorCategory =
  | "tarball.missing"
  | "asset.mount.missing"
  | "integrity.mismatch"
  | "registry.fetch.failed"
  | "registry.unknown"
  | "registry.auth.failed"
  | "tarball.extract.failed"
  | "git.materialization.failed"
  | "package.entry.invalid";

/** A closure-materialization failure carrying its category. */
export class ClosureMaterializationError extends Error {
  readonly category: ClosureMaterializationErrorCategory;

  constructor(opts: {
    category: ClosureMaterializationErrorCategory;
    message: string;
  }) {
    super(opts.message);
    this.name = "ClosureMaterializationError";
    this.category = opts.category;
  }
}

/**
 * Whether a `platform`/`os`/`cpu`-style allowlist matches `host`. A list with a
 * `!`-negated entry matches everything except the negated hosts; an unnegated
 * list matches only its members.
 */
export function platformListMatches(
  entries: readonly string[],
  host: string,
): boolean {
  const hasNegation = entries.some((e) => e.startsWith("!"));
  if (hasNegation) {
    return !entries.includes(`!${host}`);
  }
  return entries.includes(host);
}

export function isEEXIST(err: unknown): boolean {
  if (err === null || typeof err !== "object") return false;
  if (!("code" in err)) return false;
  return (err as { code: unknown }).code === "EEXIST";
}

export function isENOENT(err: unknown): boolean {
  if (err === null || typeof err !== "object") return false;
  if (!("code" in err)) return false;
  return (err as { code: unknown }).code === "ENOENT";
}

export function describeError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
