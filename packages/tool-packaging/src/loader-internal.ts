// Shared internals of the tool-package loader: the failure type, the fetch
// byte/time caps, and the small cross-cutting helpers. Split out so the
// registry-fetch and store-layout modules and `loader.ts` itself can all depend
// on them without an import cycle. `loader.ts` re-exports the public members
// (`ToolLoaderError`, the `DEFAULT_*` caps) so existing consumers are unaffected.

import type { DeployApplyErrorCategory } from "@intx/types/sidecar";

/**
 * Maximum bytes a single registry tarball fetch will read before aborting. The
 * cap bounds the memory a malicious or misconfigured registry can force a
 * deploy to buffer while the atomic-apply mirror replays it.
 */
export const DEFAULT_MAX_REGISTRY_TARBALL_BYTES = 10 * 1024 * 1024;

/**
 * Default deadline for a single HTTP-registry tarball fetch, covering
 * both the request and the streamed body read. The byte cap bounds
 * size but not time: a registry that stalls mid-stream would block the
 * fetch indefinitely. Generous so a large tarball on a slow link still
 * completes. Override via `registryFetchTimeoutMs` on `createToolLoader`.
 */
export const DEFAULT_REGISTRY_FETCH_TIMEOUT_MS = 120 * 1000;

/**
 * A tool-loader failure carrying a `DeployApplyErrorCategory` (and,
 * when attributable to a specific package, its name and version) so
 * the apply layer can map every loader failure.
 */
export class ToolLoaderError extends Error {
  readonly category: DeployApplyErrorCategory;
  readonly package:
    | { readonly name: string; readonly version: string }
    | undefined;

  constructor(opts: {
    category: DeployApplyErrorCategory;
    message: string;
    package?: { name: string; version: string };
  }) {
    super(opts.message);
    this.name = "ToolLoaderError";
    this.category = opts.category;
    this.package = opts.package;
  }
}

/**
 * Whether an npm-style allowlist matches `host`. A `!`-negated entry
 * matches everything except the negated host; an unnegated list matches
 * only its members.
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
