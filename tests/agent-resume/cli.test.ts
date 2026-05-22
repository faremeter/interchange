// agent-resume integration test. Runs the example's `main()` twice
// against the same contextDir, with a fresh harness for each run to
// prove the persistence really crosses process-shaped boundaries.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { main } from "@intx/example-agent-resume";
import { setupHarness, type Harness } from "@intx/inference-testing";
import type { InferenceSource } from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-resume",
  model: "claude-3-5-sonnet",
};

describe("agent-resume CLI", () => {
  let workDir: string;
  let contextDir: string;
  let harness: Harness;
  let stdoutBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-resume-"));
    contextDir = join(workDir, "ctx");
    harness = setupHarness();
    stdoutBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("first run reports no prior turns; second run sees the first's history", async () => {
    // ---- First run --------------------------------------------------
    harness.scenario.replyOnce("anthropic", {
      text: "Nice to meet you, Alex.",
    });

    const firstRun = main(
      ["my", "name", "is", "alex"],
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: () => undefined,
        sourceOverride: SOURCE,
        deps: harness.deps,
        contextDir,
      },
    );
    await harness.run();
    expect(await firstRun).toBe(0);
    expect(stdoutBuf).toContain("no prior turns");
    expect(stdoutBuf).toContain("Nice to meet you, Alex.");

    // ---- Second run on the same contextDir --------------------------
    stdoutBuf = "";
    harness.dispose();
    harness = setupHarness();
    harness.scenario.replyOnce("anthropic", { text: "Your name is Alex." });

    const secondRun = main(
      ["what", "is", "my", "name?"],
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: () => undefined,
        sourceOverride: SOURCE,
        deps: harness.deps,
        contextDir,
      },
    );
    await harness.run();
    expect(await secondRun).toBe(0);

    // The prior-turns banner must reflect a non-zero count, and the
    // first run's user/assistant pair must be visible in the summary.
    expect(stdoutBuf).toMatch(/\(\d+ prior turns\)/);
    expect(stdoutBuf).toContain("user:");
    expect(stdoutBuf).toContain("assistant:");
    expect(stdoutBuf).toContain("Nice to meet you, Alex.");
    expect(stdoutBuf).toContain("Your name is Alex.");
  });
});
