// agent-blob-spill end-to-end test. Scripts a tool_use → tool_result
// → text-reply cycle through the harness so the example's CLI walks
// the full spill path: oversized handler output, size-cap transform
// rewrite, history walk, BlobReader round-trip.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  DEFAULT_PAYLOAD_CHARS,
  main,
} from "@interchange/example-agent-blob-spill";
import { setupHarness, type Harness } from "@interchange/inference-testing";
import type { ProviderConfig } from "@interchange/types/runtime";

const PROVIDER: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-blob-spill",
  model: "claude-3-5-sonnet",
};

describe("agent-blob-spill CLI", () => {
  let workDir: string;
  let contextDir: string;
  let harness: Harness;
  let stdoutBuf: string;
  let stderrBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-blob-spill-"));
    contextDir = join(workDir, "ctx");
    harness = setupHarness();
    stdoutBuf = "";
    stderrBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("spills oversized tool output and round-trips via BlobReader", async () => {
    // Turn 1: model invokes the tool.
    harness.scenario.replyOnce("anthropic", {
      toolCalls: [
        {
          callId: "fetch_full_logs_1",
          name: "fetch_full_logs",
          argsJSON: "{}",
        },
      ],
    });
    // Turn 2: model returns its final summary text.
    harness.scenario.replyOnce("anthropic", {
      text: "Summary: nothing of consequence in the log.",
    });

    const run = main(
      ["please", "fetch", "the", "logs"],
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
        contextDir,
      },
    );
    await harness.run();
    const code = await run;

    expect(stderrBuf).toBe("");
    expect(code).toBe(0);

    // The assistant's final reply made it through.
    expect(stdoutBuf).toContain("Summary: nothing of consequence");

    // The spill URI is reported in the output.
    expect(stdoutBuf).toMatch(
      /spill URI:\s+tool-output:\/\/\/fetch_full_logs_1/,
    );

    // The resolved blob byte count is at least the default payload size.
    const match = /resolved blob bytes:\s+(\d+)/.exec(stdoutBuf);
    expect(match).not.toBeNull();
    if (match !== null) {
      expect(Number(match[1])).toBeGreaterThanOrEqual(DEFAULT_PAYLOAD_CHARS);
    }

    // The in-history block carries the size-cap transform's
    // truncation notice with the omitted-character count and a
    // reference back to the spill URI.
    expect(stdoutBuf).toContain("Tool output truncated");
    expect(stdoutBuf).toMatch(/omitted \d+ chars/);

    // The spill file exists on disk under tool-output/<callId>.
    const spillPath = join(contextDir, "tool-output", "fetch_full_logs_1.txt");
    expect(existsSync(spillPath)).toBe(true);
    const onDisk = readFileSync(spillPath, "utf-8");
    expect(onDisk.length).toBeGreaterThanOrEqual(DEFAULT_PAYLOAD_CHARS);
    expect(onDisk).toContain("noisy tool emission");
  });
});
