// Bun-backed spawn edge for the one-shot workflow probe child.
//
// The probe executor takes a spawner and a binary path as required
// arguments. This module is the process that owns both: it resolves
// `bin/workflow-probe-child` and launches it with a fresh env.

import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

import type {
  ProbeChildHandle,
  ProbeChildSpawner,
} from "@intx/workflow-host/probe";

/**
 * Path of this package's `bin/workflow-probe-child`. Resolved at load so
 * the boot edge can hand the probe executor a concrete path.
 *
 * A dev-worktree build step (`make build-test-binaries`) bundles this
 * binary into `dist-test/workflow-probe-child.js`; a single-file bundle
 * boots faster than the extensionless shebang source because the whole
 * `@intx/*` module graph is loaded from one pre-bundled file instead of
 * being transpiled module-by-module. Prefer the bundle when it exists;
 * the source binary remains the fallback, so a worktree that never ran
 * the build step behaves exactly as before. The bundle is built from
 * this same source, so the executed code is identical either way.
 */
const BUNDLED_PROBE_BINARY = fileURLToPath(
  new URL("../dist-test/workflow-probe-child.js", import.meta.url),
);
export const SIDECAR_WORKFLOW_PROBE_CHILD_BINARY: string = existsSync(
  BUNDLED_PROBE_BINARY,
)
  ? BUNDLED_PROBE_BINARY
  : fileURLToPath(import.meta.resolve("../bin/workflow-probe-child"));

/**
 * Real `Bun.spawn`-backed probe-child spawner. The caller assembles a
 * fresh env (no `process.env` spread). Stdout is piped for the result
 * line, stdin is ignored, and stderr is inherited so child diagnostics
 * land on the sidecar's stderr.
 */
export const defaultProbeChildSpawner: ProbeChildSpawner = ({
  binaryPath,
  env,
}): ProbeChildHandle => {
  const proc = Bun.spawn([binaryPath], {
    stdio: ["ignore", "pipe", "inherit"],
    env,
  });
  return {
    pid: proc.pid,
    stdout: proc.stdout,
    kill(signal?: number | string): void {
      if (signal === undefined) {
        proc.kill();
        return;
      }
      if (typeof signal === "number") {
        proc.kill(signal);
        return;
      }
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the probe reaper passes "SIGTERM"/"SIGKILL"; Bun's runtime accepts the same "SIG*" strings, narrowed back at the boundary.
      proc.kill(signal as NodeJS.Signals);
    },
    exited: proc.exited,
  };
};
