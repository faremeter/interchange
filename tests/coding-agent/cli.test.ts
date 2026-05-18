// End-to-end test for the coding-agent example's CLI. Drives the actual
// `main()` entry through @interchange/inference-testing so we exercise
// every real path the binary takes — argument parsing, agent
// construction, send, stream pump, close, exit code — without making a
// network call.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { main } from "@interchange/example-coding-agent";
import {
  setupHarness,
  wire,
  type Harness,
} from "@interchange/inference-testing";
import type { ProviderConfig } from "@interchange/types/runtime";

const PROVIDER: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-cli",
  model: "claude-3-5-sonnet",
};

const USAGE_HEAD = {
  input: 10,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

const USAGE_TAIL = {
  input: 0,
  output: 5,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function wireReply(harness: Harness, text: string): void {
  const stream = harness.scenario.createStream();
  const chunks = wire.completeResponse("anthropic", {
    text,
    headUsage: USAGE_HEAD,
    tailUsage: USAGE_TAIL,
  });
  stream.enqueueAll(chunks, { startAt: harness.clock.now() + 10 });
  harness.scenario.whenRequestMatches(() => true, stream);
}

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
    wireReply(harness, "Hello from the model");

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
        providerOverride: PROVIDER,
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

  test("missing ANTHROPIC_API_KEY (and no providerOverride) returns exit code 1", async () => {
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
    wireReply(harness, "first reply");
    const firstRun = main(
      [...argv, "first prompt"],
      { ANTHROPIC_API_KEY: "x" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: () => undefined,
        providerOverride: PROVIDER,
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
    wireReply(harness, "second reply");

    const secondRun = main(
      [...argv, "second prompt"],
      { ANTHROPIC_API_KEY: "x" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: () => undefined,
        providerOverride: PROVIDER,
        deps: harness.deps,
      },
    );
    await harness.run();
    expect(await secondRun).toBe(0);
    expect(stdoutBuf).toBe("second reply\n");

    // Verify the on-disk history projection survived both runs by
    // opening the context store directly — the issue's acceptance gate
    // for resume-from-crash.
    const { createIsogitStore } = await import("@interchange/storage-isogit");
    const store = await createIsogitStore(contextDir);
    const loaded = await store.load();
    // Each round-trip produces a user turn and an assistant turn, so we
    // expect at least 4 turns total across both runs.
    expect(loaded.turns.length).toBeGreaterThanOrEqual(4);
    const assistantTurns = loaded.turns.filter((t) => t.role === "assistant");
    expect(assistantTurns.length).toBeGreaterThanOrEqual(2);
  });
});
