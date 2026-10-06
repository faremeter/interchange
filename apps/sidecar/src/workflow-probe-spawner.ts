// Bun-backed spawn edge for the one-shot workflow probe child.
//
// The probe executor takes a spawner and a binary path as required
// arguments. This module is the process that owns both: it resolves
// `bin/workflow-probe-child` and launches it with a fresh env.

import { fileURLToPath } from "node:url";

import type {
  ProbeChildHandle,
  ProbeChildSpawner,
} from "@intx/workflow-host/probe";

/**
 * Path of this package's `bin/workflow-probe-child`. Resolved at load so
 * the boot edge can hand the probe executor a concrete path.
 */
export const SIDECAR_WORKFLOW_PROBE_CHILD_BINARY: string = fileURLToPath(
  import.meta.resolve("../bin/workflow-probe-child"),
);

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
