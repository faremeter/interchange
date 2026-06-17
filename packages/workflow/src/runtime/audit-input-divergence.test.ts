// Audit log must agree with the invoker on what a step's input is.
//
// Previously, runStep computed `input = step.input ? evaluate(...) : null`,
// recorded `input ?? null` to the audit blob, but passed the raw
// `input` (possibly `undefined`) to `env.invokeStep`. The audit log
// claimed `inline:null` while the agent received `undefined`. The two
// must observe the same canonicalized value.

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";
import {
  defineWorkflow,
  runLocal,
  step,
  type StepInvoker,
} from "@intx/workflow";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: id,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

describe("audit/invoker input agreement", () => {
  test("a `from` selector that resolves to undefined canonicalizes to null on both sides", async () => {
    // trigger.payload carries an explicit `field: undefined`. The
    // selector's `in` check accepts the key as present and returns
    // `undefined`. The runtime canonicalizes this to `null` before
    // recording the audit blob AND before passing it to the invoker;
    // the two sides must agree.
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "audit-divergence",
      trigger: { type: "manual" },
      steps: {
        s: step({ agent: a, input: { from: "trigger.payload.field" } }),
      },
    });
    let seenByInvoker: unknown = "sentinel";
    const invokeStep: StepInvoker = async ({ input }) => {
      seenByInvoker = input;
      return { output: { received: input } };
    };
    const result = await runLocal(def, {
      triggerPayload: { field: undefined },
      invokeStep,
    }).complete;
    expect(result.terminalStatus).toBe("completed");
    expect(seenByInvoker).toBeNull();
    const started = result.events.find(
      (e) => e.kind === "StepStarted" && e.stepId === "s",
    );
    expect(started).toBeDefined();
    if (started === undefined || started.kind !== "StepStarted") {
      throw new Error("no StepStarted for s");
    }
    expect(started.input.ref).toBe("inline:null");
  });
});
