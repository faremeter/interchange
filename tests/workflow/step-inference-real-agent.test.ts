// A step's `inference` selector, followed from the workflow definition to the
// provider request body. The unit tests cover each hop on its own (selector
// resolution, the runtime handing the options to the invoker, the invoker
// forwarding them to `agent.send`, the reactor and harness merges, and each
// adapter's mapping); this test composes them: a real agent built by the
// production step invoker, driven by `runLocal`, with deterministic mock
// inference from `@intx/inference-testing`, and asserts on the body the
// provider receives.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { type } from "arktype";

import { createAgent, defineAgent } from "@intx/agent";
import { setupHarness, wire, type Harness } from "@intx/inference-testing";
import type { InferenceSource } from "@intx/types/runtime-core";
import {
  defineWorkflow,
  runLocal,
  step,
  type RunResult,
  type Selector,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";
import { createWorkflowStepInvoker } from "@intx/workflow-host";

import { driveUntilSettled, localStepEnvBase } from "./local-step-env";

const MARKER = "you are the step-inference agent";

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const AnthropicBody = type({
  max_tokens: "number",
  "thinking?": "Record<string, unknown>",
  "output_config?": "Record<string, unknown>",
});

function sourceFor(model: string): InferenceSource {
  return {
    id: `anthropic:${model}`,
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    credentialId: "sk-step-inference",
    model,
  };
}

describe("step inference selector reaches the provider request", () => {
  let harness: Harness;
  let baseDir: string;

  beforeEach(() => {
    harness = setupHarness();
    baseDir = mkdtempSync(join(tmpdir(), "step-inference-real-agent-"));
  });

  afterEach(() => {
    harness.dispose();
    rmSync(baseDir, { recursive: true, force: true });
  });

  // Run a one-step workflow whose agent routes to `model`, serving at most
  // one provider request, and return the run result with the bodies of the
  // requests the provider received.
  async function runStep(
    model: string,
    inference: Selector | undefined,
    triggerPayload: unknown,
  ): Promise<{ result: RunResult; bodies: (typeof AnthropicBody.infer)[] }> {
    const source = sourceFor(model);
    const agent = defineAgent({
      id: "step-inference",
      systemPrompt: MARKER,
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: source.provider, model: source.model }],
      },
    });
    const invokeStep = createWorkflowStepInvoker({
      workflowAuthorize: allowAll,
      buildEnv: () =>
        localStepEnvBase({ baseDir, name: "answer", source, harness }),
      agentFactory: createAgent,
    });

    const reply = harness.scenario.createStream();
    reply.enqueueAll(wire.completeResponse("anthropic", { text: "done" }), {
      startAt: harness.clock.now() + 1,
    });
    harness.scenario.whenRequestBodyMatches(
      (body) => body.includes(MARKER),
      reply,
    );

    const def = defineWorkflow({
      id: "step-inference-real-agent",
      trigger: { type: "manual" },
      steps: {
        answer: step({
          agent,
          input: { from: "trigger.payload" },
          ...(inference !== undefined ? { inference } : {}),
        }),
      },
    });
    const run = runLocal(def, {
      authorize: allowAll,
      triggerPayload,
      hasUpstreamSignalResolver: true,
      invokeStep,
    });

    const result = await driveUntilSettled(harness, run);

    const bodies = await Promise.all(
      harness.scenario
        .matchedRequests()
        .map(async (r) => AnthropicBody.assert(JSON.parse(await r.text()))),
    );
    return { result, bodies };
  }

  test("an effort on an adaptive model reaches output_config.effort", async () => {
    const { result, bodies } = await runStep(
      "claude-opus-5",
      { from: "trigger.payload.tier" },
      { tier: { effort: "medium" } },
    );
    expect(result.terminalStatus).toBe("completed");
    expect(bodies).toHaveLength(1);
    expect(bodies[0]?.thinking).toEqual({ type: "adaptive" });
    expect(bodies[0]?.output_config).toEqual({ effort: "medium" });
  });

  test("an effort on a classic model reaches the thinking budget and cap", async () => {
    const { result, bodies } = await runStep(
      "claude-haiku-4-5-20251001",
      { from: "trigger.payload.tier" },
      { tier: { effort: "medium", maxTokens: 16384 } },
    );
    expect(result.terminalStatus).toBe("completed");
    expect(bodies).toHaveLength(1);
    expect(bodies[0]?.max_tokens).toBe(16384);
    expect(bodies[0]?.thinking).toEqual({
      type: "enabled",
      budget_tokens: 8192,
    });
    expect(bodies[0]).not.toHaveProperty("output_config");
  });

  test("a step with no selector sends the agent's defaults", async () => {
    const { result, bodies } = await runStep("claude-opus-5", undefined, {});
    expect(result.terminalStatus).toBe("completed");
    expect(bodies).toHaveLength(1);
    expect(bodies[0]?.max_tokens).toBe(4096);
    expect(bodies[0]).not.toHaveProperty("thinking");
    expect(bodies[0]).not.toHaveProperty("output_config");
  });

  test("an effort whose budget does not fit the cap is refused unsent", async () => {
    const { result, bodies } = await runStep(
      "claude-haiku-4-5-20251001",
      { from: "trigger.payload.tier" },
      { tier: { effort: "high" } },
    );
    // The default director answers a fatal inference error with an error
    // reply, so the step completes and carries the adapter's message.
    expect(result.terminalStatus).toBe("completed");
    expect(JSON.stringify(result.events)).toContain(
      "needs a thinking budget of 24576 tokens, but maxTokens is 4096",
    );
    expect(bodies).toHaveLength(0);
  });
});
