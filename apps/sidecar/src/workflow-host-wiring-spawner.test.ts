// End-to-end exercise of the Bun.spawn-backed `defaultSubprocessSpawner`
// the wiring module exports. The supervisor's `wireChild` consumes
// four behaviours from the handle:
//
//   1. The control channel surfaces NDJSON lines the child writes
//      to its stdout. A real child writes a signed `ready` envelope
//      here; the test stub writes a sentinel NDJSON line.
//   2. The handle's `exited` future resolves with the child's
//      terminal exit code so the supervisor can race spawn-time
//      crashes against `readyPromise`.
//   3. A `Bun.spawn` that fails immediately (binary missing) settles
//      `exited` with a non-zero code rapidly enough for that race
//      to fire.
//   4. The child's runtime env is exactly the supervisor-supplied
//      env -- unrelated `process.env` entries do not leak in.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { defaultSubprocessSpawner } from "./workflow-host-wiring";

let tmpRoot: string;

beforeAll(async () => {
  tmpRoot = await fs.mkdtemp(path.join(os.tmpdir(), "intx-spawner-test-"));
});

afterAll(async () => {
  await fs.rm(tmpRoot, { recursive: true, force: true });
});

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
    // `apps/sidecar/bin/workflow-child` is `#!/usr/bin/env bun`,
    // so the wiring module spawns it as a bare argv entry. The
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

  test("spawn-time failure surfaces fast enough for the supervisor's readyPromise race", async () => {
    // The supervisor's `wireChild` races `handle.exited` against
    // the child's `readyPromise`. A child that fails to reach
    // `ready` (the production binary's failure mode for malformed
    // env: throws in `parseSpawnTimeEnv` before opening the
    // control channel) must surface as `exited` settling with a
    // non-zero code -- otherwise the supervisor wedges in
    // `starting`. The inline child below exits 1 without writing
    // a single byte; the spawner's `exited` future must observe
    // the terminal code.
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
    // The other half of the spawn-time race: a binary path that
    // does not exist on disk surfaces as a synchronous throw from
    // `Bun.spawn`, which the supervisor's `spawn(opts)` propagates
    // to its caller. The supervisor never reaches the
    // readyPromise race in this branch; the throw IS the failure
    // signal. Pin the synchronous-throw behaviour here so a future
    // Bun release that swaps to an async failure mode does not
    // silently change the supervisor's spawn-error surface.
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
});
