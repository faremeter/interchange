// agent-audit-log end-to-end test. Drives two prompts through the
// example's main(), then verifies the audit walk discovers the right
// number of commits and the expected files in each.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { main } from "@intx/example-agent-audit-log";
import { setupHarness, type Harness } from "@intx/inference-testing";
import type { InferenceSource } from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-audit-log",
  model: "claude-3-5-sonnet",
};

describe("agent-audit-log CLI", () => {
  let workDir: string;
  let contextDir: string;
  let harness: Harness;
  let stdoutBuf: string;
  let stderrBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-audit-log-"));
    contextDir = join(workDir, "ctx");
    harness = setupHarness();
    stdoutBuf = "";
    stderrBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("sends prompts, then walks the resulting git commits", async () => {
    harness.scenario.replyOnce("anthropic", { text: "100C." });
    harness.scenario.replyOnce("anthropic", { text: "Lower at altitude." });

    const run = main(
      ["boiling point", "and at altitude?"],
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

    expect(stderrBuf).toBe("");
    expect(code).toBe(0);

    // Both replies appeared in the per-prompt block.
    expect(stdoutBuf).toContain("100C.");
    expect(stdoutBuf).toContain("Lower at altitude.");

    // The audit log header reports at least three commits (initial +
    // two send cycles). Be tolerant of additional bookkeeping
    // commits the isogit store may produce.
    const match = /audit log \((\d+) commit\(s\)/.exec(stdoutBuf);
    expect(match).not.toBeNull();
    if (match !== null) {
      expect(Number(match[1])).toBeGreaterThanOrEqual(3);
    }

    // The per-commit listing names every JSONL file produced per
    // cycle.
    expect(stdoutBuf).toContain("turns.jsonl");
    expect(stdoutBuf).toContain("manifest.jsonl");
    expect(stdoutBuf).toContain("prompt.jsonl");
    expect(stdoutBuf).toContain("response.jsonl");
  });

  test("missing prompts return exit code 1 with a usage message", async () => {
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
  });
});
