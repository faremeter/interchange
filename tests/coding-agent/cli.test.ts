// End-to-end test for the coding-agent example's CLI. Drives the actual
// `main()` entry through @intx/inference-testing so we exercise
// every real path the binary takes — argument parsing, agent
// construction, send, stream pump, close, exit code — without making a
// network call.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { main } from "@intx/example-coding-agent";
import { setupHarness, type Harness } from "@intx/inference-testing";
import type { InferenceSource } from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-test-cli",
  model: "claude-3-5-sonnet",
};

describe("coding-agent CLI", () => {
  let workDir: string;
  let harness: Harness;
  let stdoutBuf: string;
  let stderrBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "coding-agent-cli-"));
    harness = setupHarness();
    stdoutBuf = "";
    stderrBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("runs end-to-end: parses argv, drives reactor, prints reply, exits 0", async () => {
    harness.scenario.replyOnce("anthropic", { text: "Hello from the model" });

    const contextDir = join(workDir, "ctx");
    const argv = [
      "--cwd",
      workDir,
      "--context-dir",
      contextDir,
      "hello",
      "agent",
    ];

    const runPromise = main(
      argv,
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
        sourceOverride: SOURCE,
        deps: harness.deps,
      },
    );

    await harness.run();
    const code = await runPromise;

    expect(code).toBe(0);
    expect(stdoutBuf).toBe("Hello from the model\n");
    // The stream pump emits one line per reactor event; we should see
    // at least one event (`message.received`) before the cycle completes.
    expect(stderrBuf).toMatch(/\[message\.received\]/);
  });

  test("scopes posix tool writes to --cwd, not --context-dir", async () => {
    // The model issues a write_file with a relative path. The coding agent
    // flows the working directory through env-DI as toolCwd (the --cwd
    // tree), distinct from workdir (the --context-dir isogit store), so a
    // relative write resolves under --cwd and never touches --context-dir.
    const editTree = join(workDir, "src");
    const contextDir = join(workDir, "ctx");
    mkdirSync(editTree, { recursive: true });

    harness.scenario.replyOnce("anthropic", {
      toolCalls: [
        {
          callId: "write-1",
          name: "write_file",
          argsJSON: JSON.stringify({
            path: "sentinel.txt",
            content: "scoped to toolCwd",
          }),
        },
      ],
    });
    harness.scenario.replyOnce("anthropic", { text: "wrote the file" });

    const runPromise = main(
      ["--cwd", editTree, "--context-dir", contextDir, "write", "a", "file"],
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
        sourceOverride: SOURCE,
        deps: harness.deps,
      },
    );

    await harness.run();
    const code = await runPromise;

    expect(code).toBe(0);
    expect(readFileSync(join(editTree, "sentinel.txt"), "utf8")).toBe(
      "scoped to toolCwd",
    );
    expect(existsSync(join(contextDir, "sentinel.txt"))).toBe(false);
  });

  test("missing ANTHROPIC_API_KEY (and no sourceOverride) returns exit code 1", async () => {
    const code = await main(
      ["any prompt"],
      {},
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
      },
    );

    expect(code).toBe(1);
    expect(stderrBuf).toContain("ANTHROPIC_API_KEY is required");
    expect(stdoutBuf).toBe("");
  });

  test("empty prompt returns exit code 1 with a usage message", async () => {
    const code = await main(
      [],
      { ANTHROPIC_API_KEY: "x" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
      },
    );

    expect(code).toBe(1);
    expect(stderrBuf).toContain("usage:");
    expect(stdoutBuf).toBe("");
  });

  test("resume from crash: second run on same contextDir sees prior history", async () => {
    const contextDir = join(workDir, "ctx");
    const argv = ["--cwd", workDir, "--context-dir", contextDir];

    // First run.
    harness.scenario.replyOnce("anthropic", { text: "first reply" });
    const firstRun = main(
      [...argv, "first prompt"],
      { ANTHROPIC_API_KEY: "x" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: () => undefined,
        sourceOverride: SOURCE,
        deps: harness.deps,
      },
    );
    await harness.run();
    expect(await firstRun).toBe(0);
    expect(stdoutBuf).toBe("first reply\n");

    // Reset stdout buffer for the second run.
    stdoutBuf = "";

    // Second run on the same contextDir. A fresh harness for a clean
    // matcher state but the same context dir on disk — proving that
    // the example's persistence really does survive process death.
    harness.dispose();
    harness = setupHarness();
    harness.scenario.replyOnce("anthropic", { text: "second reply" });

    const secondRun = main(
      [...argv, "second prompt"],
      { ANTHROPIC_API_KEY: "x" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: () => undefined,
        sourceOverride: SOURCE,
        deps: harness.deps,
      },
    );
    await harness.run();
    expect(await secondRun).toBe(0);
    expect(stdoutBuf).toBe("second reply\n");

    // Verify the on-disk history projection survived both runs by
    // opening the context store directly — the issue's acceptance gate
    // for resume-from-crash.
    const { createIsogitStore } = await import("@intx/storage-isogit/node");
    const store = await createIsogitStore(contextDir);
    const loaded = await store.load();
    // Each round-trip produces a user turn and an assistant turn, so we
    // expect at least 4 turns total across both runs.
    expect(loaded.turns.length).toBeGreaterThanOrEqual(4);
    const assistantTurns = loaded.turns.filter((t) => t.role === "assistant");
    expect(assistantTurns.length).toBeGreaterThanOrEqual(2);
  });
});
