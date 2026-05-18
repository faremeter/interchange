// Shared context-directory layout for the agent-* examples.
//
// Every example persists its conversation under
// `<repo-root>/tmp/<example-name>/context/`. Centralising the layout
// here keeps the per-example packages from rebuilding the same
// fileURL → repo-root → `tmp/` chain, and ensures the directory
// naming convention is enforced rather than copied.

import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Resolve the repository root by walking three levels up from this
 * file's directory. The helper assumes it lives at
 * `examples/agent-common/src/paths.ts` and the other example packages
 * live at the same depth (`examples/<name>/src/...`), so the same
 * three-levels-up calculation works for callers that derive paths
 * relative to themselves.
 */
export function defaultRepoRoot(): string {
  const here = fileURLToPath(new URL(".", import.meta.url));
  return resolve(here, "..", "..", "..");
}

/**
 * Default `contextDir` for an example. Returns
 * `<repo-root>/tmp/<exampleName>/context`. The repo-wide `tmp/`
 * gitignore covers the directory; deleting the parent
 * `tmp/<exampleName>/` resets the example to a fresh state.
 */
export function defaultContextDir(
  exampleName: string,
  repoRoot: string = defaultRepoRoot(),
): string {
  return resolve(repoRoot, "tmp", exampleName, "context");
}
