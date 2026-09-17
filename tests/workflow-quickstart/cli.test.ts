// End-to-end test for workflow-quickstart. Drives the example's `main()`
// through @intx/inference-testing so the exact code path a user runs is
// exercised -- definition, runLocal, the loop's while/carry, the step
// invoker's real agent, the tool call inside the loop body, and the
// publish action's effect -- without making a network call.
//
// The load-bearing assertion is the last one. A tool call that the
// authorize seam refuses still comes back to the model as a well-formed
// `tool_result` (carrying the refusal text), the model answers it, and the
// run completes clean -- so terminal status, step outputs, iteration count
// and even the presence of a tool_result all look healthy on a run where
// the tool never executed. Only the tool_result's CONTENT distinguishes
// the two, so the test asserts the word counts the tool actually returned.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { type } from "arktype";

import {
  countWords,
  main,
  MAX_WORDS,
  WORD_COUNT_TOOL,
} from "@intx/example-workflow-quickstart";
import {
  setupHarness,
  type Harness,
  type HarnessRequest,
} from "@intx/inference-testing";
import type { InferenceSource } from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-test-workflow-quickstart",
  model: "claude-3-5-sonnet",
};

/** Twelve words; twice the target. */
const SEED =
  "The only project management tool your growing startup will ever truly need";
/** Exactly `MAX_WORDS` words, so the pass after this one converges. */
const ACCEPTED = "Project management your startup will love";

/**
 * The subset of an Anthropic request body the assertion reads: the
 * `tool_result` blocks the agent sent back after running a tool.
 */
const RequestBody = type({
  messages: type({
    content: type("string | unknown[]"),
    "+": "ignore",
  }).array(),
  "+": "ignore",
});

const ToolResultBlock = type({
  type: "'tool_result'",
  content: type({ type: "'text'", text: "string", "+": "ignore" }).array(),
  "+": "ignore",
});

/** Every `tool_result` text across the requests, in the order sent. */
async function toolResultTexts(
  requests: readonly HarnessRequest[],
): Promise<string[]> {
  const out: string[] = [];
  for (const request of requests) {
    const raw: unknown = await request.json();
    const body = RequestBody.assert(raw);
    for (const message of body.messages) {
      if (typeof message.content === "string") continue;
      for (const block of message.content) {
        const result = ToolResultBlock(block);
        if (result instanceof type.errors) continue;
        for (const part of result.content) out.push(part.text);
      }
    }
  }
  return out;
}

describe("workflow-quickstart CLI", () => {
  let workDir: string;
  let contextDir: string;
  let outputPath: string;
  let harness: Harness;
  let stdoutBuf: string;
  let stderrBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "workflow-quickstart-"));
    contextDir = join(workDir, "ctx");
    outputPath = join(workDir, "tagline.txt");
    harness = setupHarness();
    stdoutBuf = "";
    stderrBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  function runMain(argv: string[]): Promise<number> {
    return main(
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
        contextDir,
        outputPath,
      },
    );
  }

  test("the fixture taglines bracket the target length", () => {
    expect(countWords(SEED)).toBeGreaterThan(MAX_WORDS);
    expect(countWords(ACCEPTED)).toBe(MAX_WORDS);
  });

  test("revises in a loop, then publishes the accepted tagline", async () => {
    // Pass 0: measure the seed, then answer with a shorter tagline.
    harness.scenario.replyOnce("anthropic", {
      toolCalls: [{ name: WORD_COUNT_TOOL, args: { text: SEED } }],
    });
    harness.scenario.replyOnce("anthropic", { text: ACCEPTED });
    // Pass 1: measure again; it already fits, so answer unchanged. `while`
    // then sees a carry state that is short enough and converges.
    harness.scenario.replyOnce("anthropic", {
      toolCalls: [{ name: WORD_COUNT_TOOL, args: { text: ACCEPTED } }],
    });
    harness.scenario.replyOnce("anthropic", { text: ACCEPTED });

    const run = runMain(SEED.split(" "));
    await harness.run();
    const code = await run;

    expect(stderrBuf).toBe("");
    expect(code).toBe(0);

    expect(stdoutBuf).toContain("completed");
    expect(stdoutBuf).toContain('"outcome":"converged"');
    expect(stdoutBuf).toContain('"iterations":2');

    // The publish action's effect ran: it wrote the accepted tagline.
    expect(readFileSync(outputPath, "utf8")).toBe(`${ACCEPTED}\n`);
    expect(stdoutBuf).toContain(`"published":${JSON.stringify(ACCEPTED)}`);
    // The exhausted arm was pruned, so it produced no output.
    expect(stdoutBuf).not.toContain("giveUp");

    // The tool inside the loop body was actually invoked, once per pass,
    // and returned the real counts rather than a refusal.
    const texts = await toolResultTexts(harness.scenario.matchedRequests());
    expect(texts).toEqual([
      String(countWords(SEED)),
      String(countWords(ACCEPTED)),
    ]);
  });

  test("an empty tagline returns exit code 1 with a usage message", async () => {
    const code = await runMain([]);

    expect(code).toBe(1);
    expect(stderrBuf).toContain("usage:");
    expect(stdoutBuf).toBe("");
  });

  test("missing ANTHROPIC_API_KEY (and no override) prints help and returns 1", async () => {
    const code = await main(
      SEED.split(" "),
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
    expect(stderrBuf).toContain("workflow-quickstart");
    expect(stderrBuf).toContain("ANTHROPIC_API_KEY");
    expect(stdoutBuf).toBe("");
  });
});
