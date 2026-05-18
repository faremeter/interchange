// agent-rich-tool end-to-end test. Drives the full pendingMarker
// dance through @interchange/inference-testing: the model calls
// request_approval, the tool returns a pending marker, the model
// follows up with a "waiting" text, then the CLI delivers a matching
// inbound approval and waits for message.correlated.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { main } from "@interchange/example-agent-rich-tool";
import { setupHarness, type Harness } from "@interchange/inference-testing";
import type { ProviderConfig } from "@interchange/types/runtime";

const PROVIDER: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-rich-tool",
  model: "claude-3-5-sonnet",
};

describe("agent-rich-tool CLI", () => {
  let workDir: string;
  let contextDir: string;
  let harness: Harness;
  let stdoutBuf: string;
  let stderrBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-rich-tool-"));
    contextDir = join(workDir, "ctx");
    harness = setupHarness();
    stdoutBuf = "";
    stderrBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("registers a pending operation and correlates the matching inbound", async () => {
    // Pin the correlation ID so the test can assert against it.
    const CORRELATION_ID = "approval-test-12345";
    const TOOL_CALL_ID = "tool-call-approve-1";

    harness.scenario.replyOnce("anthropic", {
      toolCalls: [
        {
          callId: TOOL_CALL_ID,
          name: "request_approval",
          argsJSON: JSON.stringify({ action: "transfer $1000 to alice" }),
        },
      ],
    });
    harness.scenario.replyOnce("anthropic", {
      text: "I have requested approval; waiting for the approver.",
    });

    const run = main(
      ["transfer", "$1000", "to", "alice"],
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
        correlationIdFor: () => CORRELATION_ID,
      },
    );
    await harness.run();
    const code = await run;

    expect(stderrBuf).toBe("");
    expect(code).toBe(0);

    expect(stdoutBuf).toContain(
      "I have requested approval; waiting for the approver.",
    );
    expect(stdoutBuf).toContain("pending operation registered:");
    expect(stdoutBuf).toContain(`correlationId: ${CORRELATION_ID}`);
    expect(stdoutBuf).toContain("action:        transfer $1000 to alice");
    expect(stdoutBuf).toContain("correlation resolved:");
    expect(stdoutBuf).toContain("event:         message.correlated");
  });
});
