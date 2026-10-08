// Pack a set of workspace packages into a scratch npm registry and install
// them into a scratch consumer — the shared middle of the publish load and
// tool-load smokes. Installing the set together resolves internal @intx/*
// deps to the just-packed tarballs rather than the public registry.

import { readdirSync } from "node:fs";
import { join } from "node:path";

/** Pack each package directory into `<scratch>/tarballs`, then `npm install`
 *  the whole set into `<scratch>/consumer`. Returns the consumer directory and
 *  the tarball count; `dist` must already be emitted, and `run` throws on any
 *  non-zero exit. */
export function packAndInstall(
  run: (cmd: string[], cwd: string) => void,
  scratch: string,
  packageDirs: string[],
  repoRoot: string,
): { consumer: string; tarballCount: number } {
  const tarballs = join(scratch, "tarballs");
  const consumer = join(scratch, "consumer");
  run(["mkdir", "-p", tarballs, consumer], repoRoot);
  for (const dir of packageDirs) {
    run(["bun", "pm", "pack", "--destination", tarballs, "--quiet"], dir);
  }
  const tgz = readdirSync(tarballs)
    .filter((f) => f.endsWith(".tgz"))
    .map((f) => join(tarballs, f));
  run(["npm", "init", "-y"], consumer);
  // No --silent: run() then surfaces npm's own 404 diagnostic on failure.
  run(["npm", "install", "--no-audit", "--no-fund", ...tgz], consumer);
  return { consumer, tarballCount: tgz.length };
}
