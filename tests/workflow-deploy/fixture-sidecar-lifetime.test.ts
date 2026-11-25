// The deploy-flow fixture's sidecar subprocess must not outlive the process
// that spawned it. A sidecar redials its hub forever, so an orphan keeps
// dialing a port the OS may since have given to another test's server.
//
// Harness justification: SPAWN-REAL. Parent death is a kernel event, so the
// test kills a real parent process and watches the real sidecar exit.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type } from "arktype";

import {
  hasExited,
  listProcessSubtree,
  pidAlive,
  startDeployFlowEnv,
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

// The TCP sockets this process listens on, as `lsof` names them.
function listListeningSockets(): string[] {
  const result = Bun.spawnSync([
    "lsof",
    "-a",
    "-p",
    String(process.pid),
    "-iTCP",
    "-sTCP:LISTEN",
    "-nP",
    "-Fn",
  ]);
  return new TextDecoder()
    .decode(result.stdout)
    .split("\n")
    .filter((line) => line.startsWith("n"));
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

  test("a failed setup stops its servers and removes its temp dirs", async () => {
    // Every temp dir the fixture makes lands under this root, so an empty
    // root after the rejection shows the cleanup ran to its last step.
    const tempRoot = await fs.promises.mkdtemp(
      path.join(os.tmpdir(), "failed-setup-"),
    );
    tempDirs.push(tempRoot);
    const childrenBefore = new Set(listProcessSubtree(process.pid));
    const listenersBefore = new Set(listListeningSockets());
    // The refusing hub is already listening, so an empty listing would mean
    // `lsof` cannot see this process and the check below proves nothing.
    expect(
      [...listenersBefore].some((socket) =>
        socket.endsWith(`:${String(refusingHubPort)}`),
      ),
    ).toBe(true);
    const savedTmpdir = process.env["TMPDIR"];
    process.env["TMPDIR"] = tempRoot;
    try {
      // The sidecar rejects this value at boot and exits.
      await expect(
        startDeployFlowEnv({
          sidecarEnv: { SIDECAR_RECONNECT_DELAY_MS: "not-a-number" },
        }),
      ).rejects.toThrow(/sidecar exited .* before it connected/);
    } finally {
      if (savedTmpdir === undefined) {
        delete process.env["TMPDIR"];
      } else {
        process.env["TMPDIR"] = savedTmpdir;
      }
    }

    // The hub and the inference server run in this process, so a listening
    // socket the setup opened and left open is one of them.
    expect(
      listListeningSockets().filter((socket) => !listenersBefore.has(socket)),
    ).toEqual([]);
    expect(await fs.promises.readdir(tempRoot)).toEqual([]);
    // Filtered on liveness so the `ps` the walk itself spawns, which has
    // exited by now, does not count.
    const leftover = listProcessSubtree(process.pid).filter(
      (pid) => !childrenBefore.has(pid) && pidAlive(pid),
    );
    expect(leftover).toEqual([]);
  });

  test("teardown stops every sidecar the env started", async () => {
    const env = await startDeployFlowEnv();
    let tornDown = false;
    try {
      const secondDataDir = await fs.promises.mkdtemp(
        path.join(os.tmpdir(), "second-sidecar-"),
      );
      tempDirs.push(secondDataDir);
      const second = await env.startSidecar({
        identity: "second",
        dataDir: secondDataDir,
      });
      expect(second.dataDir).toBe(secondDataDir);
      await expect(env.startSidecar({ identity: "second" })).rejects.toThrow(
        /still running/,
      );

      // A restart: the test kills the second sidecar itself and starts a
      // replacement on its data dir, and teardown then terminates the killed
      // process a second time. The replacement is started twice at once, and
      // the start that finds the other still spawning is refused.
      await terminateSidecarSubprocess(second);
      const starts = await Promise.allSettled([
        env.startSidecar({ identity: "second", dataDir: secondDataDir }),
        env.startSidecar({ identity: "second", dataDir: secondDataDir }),
      ]);
      const [replacement, ...extra] = starts.flatMap((result) =>
        result.status === "fulfilled" ? [result.value] : [],
      );
      expect(extra).toEqual([]);
      const refusals = starts.flatMap((result) =>
        result.status === "rejected" ? [String(result.reason)] : [],
      );
      expect(refusals).toHaveLength(1);
      expect(refusals[0]).toMatch(/still starting/);
      if (replacement === undefined) {
        throw new Error("no replacement sidecar started");
      }
      expect(hasExited(env.sidecar)).toBe(false);

      await env.teardown();
      tornDown = true;
      expect(hasExited(env.sidecar)).toBe(true);
      expect(hasExited(replacement)).toBe(true);
      await expect(env.startSidecar()).rejects.toThrow(/after teardown/);
    } finally {
      if (!tornDown) await env.teardown();
    }
  });
});
