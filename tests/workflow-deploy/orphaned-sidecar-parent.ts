// Stand-in for a test worker in `fixture-sidecar-lifetime.test.ts`: it
// spawns a fixture sidecar against the hub port in argv, then two unrelated
// long-lived children the way a worker might (`Bun.spawn` and
// `node:child_process`), reports their pids and the sidecar's data dir on
// stdout, and waits for the test to kill it. The bystanders must not keep
// the sidecar's stdin pipe open after this process dies.

import { spawn } from "node:child_process";

import {
  createFixtureIdentity,
  startSidecarSubprocess,
} from "../hub-agent/lib/deploy-flow-env";

const hubPort = Number.parseInt(process.argv[2] ?? "", 10);
if (!Number.isInteger(hubPort)) {
  throw new Error(`orphaned-sidecar-parent: bad hub port ${process.argv[2]}`);
}

// The hub at `hubPort` refuses every upgrade, so the credentials are never
// checked.
const sidecar = await startSidecarSubprocess({
  hubPort,
  credentials: createFixtureIdentity().primary,
  registerTempDir: () => undefined,
});
const bunBystander = Bun.spawn(["sleep", "600"]);
const nodeBystander = spawn("sleep", ["600"], { stdio: "ignore" });
process.stdout.write(
  `${JSON.stringify({
    pid: sidecar.proc.pid,
    dataDir: sidecar.dataDir,
    bystanderPids: [bunBystander.pid, nodeBystander.pid],
  })}\n`,
);
await new Promise(() => undefined);
