// Default context directory location.
//
// The example persists conversation state under `<repo-root>/tmp/coding-
// agent/context/`. The top-level `tmp/` is gitignored by repository
// convention (`.gitignore`), and the `coding-agent/` subdirectory is left
// without a leading dot so it is visible in casual directory listings.

import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function defaultRepoRoot(): string {
  // This file lives at <repo>/examples/coding-agent/src/paths.ts.
  // The repo root is three levels up.
  const here = fileURLToPath(new URL(".", import.meta.url));
  return resolve(here, "..", "..", "..");
}

export function defaultContextDir(
  repoRoot: string = defaultRepoRoot(),
): string {
  return resolve(repoRoot, "tmp", "coding-agent", "context");
}
