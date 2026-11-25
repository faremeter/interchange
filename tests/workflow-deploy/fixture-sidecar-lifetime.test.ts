// The deploy-flow fixture's sidecar subprocess must not outlive the process
// that spawned it. A sidecar redials its hub forever, so an orphan keeps
// dialing a port the OS may since have given to another test's server.
//
// Harness justification: SPAWN-REAL. Parent death is a kernel event, so the
// test kills a real parent process and watches the real sidecar exit.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type } from "arktype";

import {
  pidAlive,
  startSidecarSubprocess,
  terminateSidecarSubprocess,
  waitFor,
} from "../hub-agent/lib/deploy-flow-env";

const PARENT_SCRIPT = path.resolve(
  import.meta.dir,
  "orphaned-sidecar-parent.ts",
);

const ParentReport = type({
  pid: "number",
  dataDir: "string",
  bystanderPids: "number[]",
});

// The sidecars dial this server, which refuses every upgrade, so they stay
// in their redial loop on a port the test owns rather than on one the OS
// could hand to another test. A dial also shows that a sidecar booted, so a
// test that sees one knows a later exit is not a boot failure.
let refusingHub: ReturnType<typeof Bun.serve>;
const dialWaiters: (() => void)[] = [];

function nextDial(): Promise<void> {
  return new Promise((resolve) => dialWaiters.push(resolve));
}

let refusingHubPort: number;
const tempDirs: string[] = [];

beforeAll(() => {
  refusingHub = Bun.serve({
    port: 0,
    fetch: () => {
      for (const resolve of dialWaiters.splice(0)) resolve();
      return new Response(null, { status: 404 });
    },
  });
  if (refusingHub.port === undefined) {
    throw new Error("refusing hub has no bound port");
  }
  refusingHubPort = refusingHub.port;
});

afterAll(async () => {
  await refusingHub.stop(true);
  for (const dir of tempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

async function readFirstLine(stream: ReadableStream<Uint8Array>) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";
  while (!text.includes("\n")) {
    const { done, value } = await reader.read();
    if (done) {
      throw new Error(`stream ended before a full line: ${text}`);
    }
    text += decoder.decode(value, { stream: true });
  }
  reader.releaseLock();
  return text.slice(0, text.indexOf("\n"));
}

describe("deploy-flow fixture sidecar lifetime", () => {
  test("a sidecar exits when the process that spawned it is killed", async () => {
    const dialed = nextDial();
    const parent = Bun.spawn(
      [
        "bun",
        "run",
        "--conditions=intx-src",
        PARENT_SCRIPT,
        String(refusingHubPort),
      ],
      { stdout: "pipe", stderr: "inherit" },
    );
    let sidecarPid: number | undefined;
    let bystanderPids: number[] = [];
    try {
      const report = ParentReport.assert(
        JSON.parse(await readFirstLine(parent.stdout)),
      );
      sidecarPid = report.pid;
      bystanderPids = report.bystanderPids;
      tempDirs.push(report.dataDir);
      await dialed;

      parent.kill("SIGKILL");
      await parent.exited;

      const pid = sidecarPid;
      await waitFor(() => !pidAlive(pid));
      expect(bystanderPids.every(pidAlive)).toBe(true);
    } finally {
      parent.kill("SIGKILL");
      for (const pid of [sidecarPid, ...bystanderPids]) {
        if (pid !== undefined && pidAlive(pid)) process.kill(pid, "SIGKILL");
      }
    }
  });

  test("a sidecar exits when its stdin closes", async () => {
    const dialed = nextDial();
    const sidecar = await startSidecarSubprocess({
      hubPort: refusingHubPort,
      registerTempDir: (dir) => tempDirs.push(dir),
    });
    try {
      await dialed;
      await sidecar.proc.stdin.end();
      expect(await sidecar.proc.exited).toBe(1);
    } finally {
      await terminateSidecarSubprocess(sidecar);
    }
  });
});
