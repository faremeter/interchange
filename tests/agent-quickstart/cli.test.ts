// End-to-end test for agent-quickstart. Drives the example's `main()`
// through @intx/inference-testing so the exact code path a
// user runs is exercised — argument parsing, provider resolution,
// createAgent, send, close — without making a network call.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { main } from "@intx/example-agent-quickstart";
import { setupHarness, type Harness } from "@intx/inference-testing";
import type { InferenceSource } from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-quickstart",
  model: "claude-3-5-sonnet",
};

describe("agent-quickstart CLI", () => {
  let workDir: string;
  let contextDir: string;
  let harness: Harness;
  let stdoutBuf: string;
  let stderrBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-quickstart-"));
    contextDir = join(workDir, "ctx");
    harness = setupHarness();
    stdoutBuf = "";
    stderrBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("sends a prompt, prints the reply, exits 0", async () => {
    harness.scenario.replyOnce("anthropic", { text: "Mercury, Venus, Earth" });

    const run = main(
      ["name", "three", "planets"],
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
        contextDir,
      },
    );

    await harness.run();
    const code = await run;

    expect(code).toBe(0);
    expect(stdoutBuf).toBe("Mercury, Venus, Earth\n");
    expect(stderrBuf).toBe("");
  });

  test("empty prompt returns exit code 1 with a usage message", async () => {
    const code = await main(
      [],
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
        contextDir,
      },
    );

    expect(code).toBe(1);
    expect(stderrBuf).toContain("usage:");
    expect(stdoutBuf).toBe("");
  });

  test("missing ANTHROPIC_API_KEY (and no override) prints help and returns 1", async () => {
    const code = await main(
      ["any", "prompt"],
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
    expect(stderrBuf).toContain("agent-quickstart");
    expect(stderrBuf).toContain("ANTHROPIC_API_KEY");
    expect(stdoutBuf).toBe("");
  });
});
