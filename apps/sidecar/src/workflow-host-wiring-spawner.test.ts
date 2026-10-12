// End-to-end exercise of the Bun.spawn-backed `defaultSubprocessSpawner`.
// The supervisor relies on four behaviours of the spawner:
//
//   1. The control channel surfaces NDJSON lines the child writes
//      to its stdout. A real child writes a signed `ready` envelope
//      here; the test stub writes a sentinel NDJSON line.
//   2. The handle's `exited` future resolves with the child's
//      terminal exit code.
//   3. A child that cannot launch (binary missing) makes the spawner
//      throw synchronously, so no handle is returned.
//   4. The child's runtime env is exactly the supervisor-supplied
//      env -- unrelated `process.env` entries do not leak in.

import {
  describe,
  test,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  spyOn,
} from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { createLogCapture } from "@intx/workflow-host/testing";

import { defaultSubprocessSpawner } from "./workflow-child-spawner";

let tmpRoot: string;
const logs = createLogCapture();

beforeAll(() => {
  logs.install();
});

afterAll(() => {
  logs.restore();
});

beforeEach(() => {
  logs.reset();
});

beforeAll(async () => {
  tmpRoot = await fs.mkdtemp(path.join(os.tmpdir(), "intx-spawner-test-"));
});

afterAll(async () => {
  await fs.rm(tmpRoot, { recursive: true, force: true });
});

// A killed process whose parent also died stays a zombie until init reaps
// it, which a container without an init never does, so a zombie counts as
// gone.
function processGone(pid: number): boolean {
  const ps = Bun.spawnSync(["ps", "-o", "stat=", "-p", String(pid)]);
  const state = ps.stdout.toString().trim();
  return state === "" || state.startsWith("Z");
}

function killGroupIfAlive(pid: number): void {
  try {
    process.kill(-pid, "SIGKILL");
  } catch (err) {
    if (!(err instanceof Error && "code" in err && err.code === "ESRCH"))
      throw err;
  }
}

async function writeChildScript(body: string): Promise<string> {
  const file = path.join(tmpRoot, `child-${String(Date.now())}.ts`);
  await fs.writeFile(file, body, "utf-8");
  return file;
}

describe("defaultSubprocessSpawner (Bun.spawn-backed)", () => {
  test("surfaces the child's NDJSON output on controlReader and resolves exited with the exit code", async () => {
    // Inline child:
    //   - reads CHILD_TOKEN out of the env and echoes it
    //   - reads SHOULD_NOT_LEAK to prove env isolation (not set by
    //     this test's spawner call, so the child must observe it
    //     as the sentinel string)
    //   - writes one byte sequence to fd 3 (event channel)
    //   - emits one NDJSON line on stdout
    //   - exits 0
    const childScript = `
import fs from "node:fs";
const token = process.env.CHILD_TOKEN ?? "MISSING";
const leaked = process.env.SHOULD_NOT_LEAK ?? "UNSET";
const event = new TextEncoder().encode("event-byte");
const eventStream = fs.createWriteStream("", { fd: 3 });
eventStream.write(event, () => {
  process.stdout.write(
    JSON.stringify({ probe: "ready", token, leaked }) + "\\n",
    () => process.exit(0),
  );
});
`;
    const scriptPath = await writeChildScript(childScript);

    // The wiring module's spawner is a bare Bun.spawn binding; the
    // production callsite invokes the binary directly. The script
    // path here points to a TypeScript file which requires Bun's
    // shebang to execute; the production binary at
    // `apps/sidecar/bin/workflow-child` is
    // `#!/usr/bin/env -S bun --conditions=intx-src`, so the wiring
    // module spawns it as a bare argv entry. The
    // test mirrors that by routing the spawn through a tiny
    // wrapper script that points at Bun's runtime.
    const wrapperPath = path.join(tmpRoot, `wrapper-${String(Date.now())}.sh`);
    await fs.writeFile(
      wrapperPath,
      `#!/bin/sh\nexec "${process.execPath}" "${scriptPath}"\n`,
      "utf-8",
    );
    await fs.chmod(wrapperPath, 0o755);

    // Set SHOULD_NOT_LEAK on the test process; the spawner must NOT
    // forward it to the child because the env arg below excludes
    // it. The child observes the unset variable as "UNSET".
    process.env.SHOULD_NOT_LEAK = "leaked";
    try {
      const handle = defaultSubprocessSpawner({
        binaryPath: wrapperPath,
        env: { CHILD_TOKEN: "spawner-test-value" },
      });

      const ctrlIter = handle.controlReader.read();
      const first = await ctrlIter.next();
      expect(first.done).toBeFalsy();
      if (first.value === undefined) {
        throw new Error("control reader yielded undefined first value");
      }
      const parsed: unknown = JSON.parse(first.value);
      if (
        typeof parsed !== "object" ||
        parsed === null ||
        !("probe" in parsed) ||
        !("token" in parsed) ||
        !("leaked" in parsed)
      ) {
        throw new Error(
          `child NDJSON payload missing expected fields: ${JSON.stringify(parsed)}`,
        );
      }
      expect(parsed.probe).toBe("ready");
      expect(parsed.token).toBe("spawner-test-value");
      expect(parsed.leaked).toBe("UNSET");

      // Drain one frame off the event channel. The child wrote one
      // byte sequence to fd 3 before exiting; the supervisor's
      // FrameReader contract yields one Uint8Array per kernel-
      // delivered chunk.
      const eventIter = handle.eventReader.read();
      const chunk = await eventIter.next();
      expect(chunk.done).toBeFalsy();
      if (chunk.value === undefined) {
        throw new Error("event reader yielded undefined value");
      }
      expect(new TextDecoder().decode(chunk.value)).toBe("event-byte");

      const code = await handle.exited;
      expect(code).toBe(0);
    } finally {
      delete process.env.SHOULD_NOT_LEAK;
    }
  });

  test("a child that exits before writing anything resolves exited with its code", async () => {
    // The production binary fails this way on a malformed env: it
    // throws in `parseSpawnTimeEnv` before writing `ready`. The
    // inline child below exits 7 without writing a single byte; the
    // spawner's `exited` future must observe the terminal code.
    const childScript = `process.exit(7);`;
    const scriptPath = await writeChildScript(childScript);
    const wrapperPath = path.join(
      tmpRoot,
      `wrapper-fail-${String(Date.now())}.sh`,
    );
    await fs.writeFile(
      wrapperPath,
      `#!/bin/sh\nexec "${process.execPath}" "${scriptPath}"\n`,
      "utf-8",
    );
    await fs.chmod(wrapperPath, 0o755);

    const handle = defaultSubprocessSpawner({
      binaryPath: wrapperPath,
      env: {},
    });
    const code = await handle.exited;
    expect(code).toBe(7);
  });

  test("Bun.spawn synchronous throws on a missing binary surface to the caller", () => {
    // A binary path that does not exist on disk surfaces as a
    // synchronous throw from `Bun.spawn`, which the supervisor's
    // `spawn(opts)` propagates to its caller; the throw is the
    // failure signal. Pin the synchronous-throw behaviour here so a
    // future Bun release that swaps to an async failure mode does
    // not silently change the supervisor's spawn-error surface.
    expect(() => {
      defaultSubprocessSpawner({
        binaryPath: "/nonexistent-binary-for-spawner-test",
        env: {},
      });
    }).toThrow();
  });

  test("kill() forwards the recycle path's SIGTERM and SIGKILL signals", async () => {
    // The supervisor's recycle path calls `handle.kill("SIGTERM")`
    // and then `handle.kill("SIGKILL")`. Assert both crossings
    // succeed; the bytes the supervisor sends are these exact
    // strings.
    const childScript = `
import fs from "node:fs";
process.stdout.write(JSON.stringify({ probe: "ready" }) + "\\n");
const eventStream = fs.createWriteStream("", { fd: 3 });
void eventStream;
await new Promise((r) => setTimeout(r, 5000));
process.exit(0);
`;
    const scriptPath = await writeChildScript(childScript);
    const wrapperPath = path.join(
      tmpRoot,
      `wrapper-kill-${String(Date.now())}.sh`,
    );
    await fs.writeFile(
      wrapperPath,
      `#!/bin/sh\nexec "${process.execPath}" "${scriptPath}"\n`,
      "utf-8",
    );
    await fs.chmod(wrapperPath, 0o755);

    const handle = defaultSubprocessSpawner({
      binaryPath: wrapperPath,
      env: {},
    });

    const iter = handle.controlReader.read();
    const ready = await iter.next();
    expect(ready.done).toBeFalsy();

    handle.kill("SIGTERM");
    handle.kill("SIGKILL");

    const code = await handle.exited;
    expect(typeof code).toBe("number");
  });

  // The grandchild inherits the control pipe, so the reader ends only once
  // every process holding it has exited.
  async function drainControl(
    lines: AsyncIterableIterator<string>,
  ): Promise<string[]> {
    const rest: string[] = [];
    for await (const line of lines) rest.push(line);
    return rest;
  }

  async function writeShellChild(name: string, body: string): Promise<string> {
    const file = path.join(tmpRoot, `${name}-${String(Date.now())}.sh`);
    await fs.writeFile(file, `#!/bin/sh\n${body}\n`, "utf-8");
    await fs.chmod(file, 0o755);
    return file;
  }

  test("kill() stops processes the child started", async () => {
    const binaryPath = await writeShellChild(
      "shell-kill",
      `/bin/sleep 60 &\necho '{"probe":"ready"}'\nwait`,
    );
    const handle = defaultSubprocessSpawner({ binaryPath, env: {} });
    const lines = handle.controlReader.read();

    const ready = await lines.next();
    expect(ready.value).toBe('{"probe":"ready"}');
    handle.kill("SIGTERM");

    await handle.exited;
    expect(await drainControl(lines)).toEqual([]);
  });

  test("exited settles only after processes the child left behind are stopped", async () => {
    const binaryPath = await writeShellChild(
      "shell-orphan",
      "/bin/sleep 60 &\nexit 0",
    );
    const handle = defaultSubprocessSpawner({ binaryPath, env: {} });

    expect(await handle.exited).toBe(0);
    expect(await drainControl(handle.controlReader.read())).toEqual([]);
  });

  test("kill() after exited settles signals nothing", async () => {
    const binaryPath = await writeShellChild("shell-exit", "exit 0");
    const handle = defaultSubprocessSpawner({ binaryPath, env: {} });
    await handle.exited;

    const kill = spyOn(process, "kill");
    try {
      handle.kill("SIGTERM");
      handle.kill("SIGKILL");
      handle.kill();
      expect(kill).not.toHaveBeenCalled();
    } finally {
      kill.mockRestore();
    }
  });

  test("reports the missing event channel fd when the group kill fails", () => {
    // On this path the spawner reads only `stdio`, and `pid` for the group
    // kill, so a fake process is enough and no real child needs cleaning up.
    const pid = 424242;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the stub returns only the fields the spawner reads on its missing-fd path
    const spawn = spyOn(Bun, "spawn").mockImplementation((() => ({
      pid,
      stdio: [undefined, undefined, undefined, undefined],
    })) as unknown as typeof Bun.spawn);
    const kill = spyOn(process, "kill").mockImplementation(() => {
      throw Object.assign(new Error("operation not permitted"), {
        code: "EPERM",
      });
    });
    try {
      expect(() =>
        defaultSubprocessSpawner({ binaryPath: "/bin/true", env: {} }),
      ).toThrow(/numeric fd/);
      expect(kill).toHaveBeenCalledWith(-pid, "SIGKILL");
    } finally {
      kill.mockRestore();
      spawn.mockRestore();
    }
    const failure = logs
      .errors()
      .find((message) => message.includes("that has no event channel"));
    expect(failure).toContain(String(pid));
    expect(failure).toContain("operation not permitted");
  });

  test("stops the child's process group when Bun.spawn returns no event channel fd", async () => {
    // The child starts a grandchild and records its pid, so the test can
    // prove the kill reached the whole group and not only the leader.
    const pidFile = path.join(
      tmpRoot,
      `no-fd-grandchild-${String(Date.now())}`,
    );
    const binaryPath = await writeShellChild(
      "shell-no-fd",
      `/bin/sleep 60 &\necho $! > "${pidFile}"\nwait`,
    );
    const realSpawn = Bun.spawn;
    let child: Bun.Subprocess | undefined;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the stub returns only the fields the spawner reads before its fd check
    const spawn = spyOn(Bun, "spawn").mockImplementation(((
      ...args: Parameters<typeof realSpawn>
    ) => {
      const real = realSpawn(...args);
      child = real;
      // The spawner is synchronous, so the grandchild must exist before
      // the stub returns for the kill to have something to reach.
      const deadline = Date.now() + 2_000;
      while (!existsSync(pidFile) || readFileSync(pidFile, "utf-8") === "") {
        if (Date.now() > deadline) throw new Error("grandchild never started");
        Bun.sleepSync(5);
      }
      return {
        pid: real.pid,
        exited: real.exited,
        stdio: [real.stdio[0], real.stdio[1], real.stdio[2], undefined],
      };
    }) as unknown as typeof Bun.spawn);
    try {
      try {
        expect(() => defaultSubprocessSpawner({ binaryPath, env: {} })).toThrow(
          /numeric fd/,
        );
      } finally {
        spawn.mockRestore();
      }
      if (child === undefined) throw new Error("Bun.spawn stub never ran");
      const exited = await Promise.race([
        child.exited.then(() => true),
        Bun.sleep(2_000).then(() => false),
      ]);
      if (!exited) throw new Error("child survived");
      expect(child.signalCode).toBe("SIGKILL");

      const grandchild = Number(readFileSync(pidFile, "utf-8").trim());
      const deadline = Date.now() + 2_000;
      while (!processGone(grandchild)) {
        if (Date.now() > deadline) throw new Error("grandchild survived");
        await Bun.sleep(5);
      }
    } finally {
      // A failed assertion must not leave the detached group running.
      if (child !== undefined) killGroupIfAlive(child.pid);
    }
  });
});
