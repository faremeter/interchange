import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";
import type { PerCallInferenceOptions } from "@intx/types/runtime";
import {
  defineWorkflow,
  runLocal,
  step,
  type Selector,
  type StepInvoker,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const agent = defineAgent({
  id: "a",
  systemPrompt: "a",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "fake", model: "fake" }] },
});

async function runWithInference(
  inference: Selector | undefined,
  triggerPayload: unknown,
) {
  const seen: { inference: PerCallInferenceOptions | undefined }[] = [];
  const invokeStep: StepInvoker = async (req) => {
    seen.push({ inference: req.inference });
    return { output: "done" };
  };
  const def = defineWorkflow({
    id: "step-inference",
    trigger: { type: "manual" },
    steps: {
      s: step({
        agent,
        input: { from: "trigger.payload" },
        ...(inference !== undefined ? { inference } : {}),
      }),
    },
  });
  const result = await runLocal(def, {
    authorize: allowAll,
    triggerPayload,
    hasUpstreamSignalResolver: true,
    invokeStep,
  }).complete;
  return { result, seen };
}

describe("step inference selector", () => {
  test("hands the resolved options to the invoker, not to StepStarted", async () => {
    const { result, seen } = await runWithInference(
      { from: "trigger.payload.tier" },
      { tier: { maxTokens: 8192, effort: "high" } },
    );
    expect(result.terminalStatus).toBe("completed");
    expect(seen).toEqual([{ inference: { maxTokens: 8192, effort: "high" } }]);
    const started = result.events.find((e) => e.kind === "StepStarted");
    expect(started).toBeDefined();
    expect(Object.keys(started ?? {}).sort()).toEqual(
      ["at", "attempt", "input", "kind", "seq", "stepId"].sort(),
    );
  });

  test("an absent selector passes no options", async () => {
    const { result, seen } = await runWithInference(undefined, {});
    expect(result.terminalStatus).toBe("completed");
    expect(seen).toEqual([{ inference: undefined }]);
  });

  test("a null resolution passes no options", async () => {
    const { result, seen } = await runWithInference(
      { from: "trigger.payload.tier" },
      { tier: null },
    );
    expect(result.terminalStatus).toBe("completed");
    expect(seen).toEqual([{ inference: undefined }]);
  });

  test.each([
    ["a non-object", "high"],
    ["a systemPrompt", { systemPrompt: "x" }],
    ["tools", { tools: [] }],
    ["providerOptions", { providerOptions: {} }],
  ])("%s fails the step before the invoker runs", async (_label, tier) => {
    const { result, seen } = await runWithInference(
      { from: "trigger.payload.tier" },
      { tier },
    );
    expect(result.terminalStatus).toBe("failed");
    expect(seen).toEqual([]);
    const failed = result.events.find((e) => e.kind === "StepFailed");
    expect(failed).toBeDefined();
  });
});
