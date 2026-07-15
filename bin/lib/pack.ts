// Pack a set of workspace packages into a scratch npm registry and install
// them into a scratch consumer — the shared middle of the publish load smoke
// and the tool-load smoke. Installing the packed set together is what makes
// internal @intx/* dependencies resolve to the just-packed tarballs rather
// than the public registry (which still holds the broken 0.1.x).

import { readdirSync } from "node:fs";
import { join } from "node:path";

/** Pack each package directory into `<scratch>/tarballs`, then `npm install`
 *  the whole set into `<scratch>/consumer`. Returns the consumer directory
 *  (with the set installed) and the tarball count. `dist` must already be
 *  emitted for each package; `run` throws with captured output on any
 *  non-zero exit, so a pack or install failure surfaces its own diagnostic. */
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
  // No --silent: on failure the run() helper surfaces npm's own diagnostic
  // (which package at which version 404'd), the actionable detail.
  run(["npm", "install", "--no-audit", "--no-fund", ...tgz], consumer);
  return { consumer, tarballCount: tgz.length };
}
