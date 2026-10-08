// Shared context-directory layout for the agent-* examples.
//
// Every example persists its conversation under
// `<repo-root>/tmp/<example-name>/context/`. Centralising the layout
// here keeps the per-example packages from rebuilding the same
// fileURL-to-repo-root-to-`tmp/` chain and enforces the naming
// convention rather than copying it.

import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Resolve the repository root by walking three levels up from this
 * file's directory. The helper assumes it lives at
 * `examples/agent-common/src/paths.ts` and the other example packages
 * live at the same depth (`examples/<name>/src/...`).
 */
export function defaultRepoRoot(): string {
  const here = fileURLToPath(new URL(".", import.meta.url));
  return resolve(here, "..", "..", "..");
}

/**
 * Default `contextDir` for an example:
 * `<repo-root>/tmp/<exampleName>/context`. The repo-wide `tmp/`
 * gitignore covers the directory; deleting `tmp/<exampleName>/`
 * resets the example to a fresh state.
 */
export function defaultContextDir(
  exampleName: string,
  repoRoot: string = defaultRepoRoot(),
): string {
  return resolve(repoRoot, "tmp", exampleName, "context");
}
