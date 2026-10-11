// A relayed child gate is not a child failure, so an `onFailure` handler on
// the spawn step is not taken. Delivering the signal completes the child, and
// the success path prunes the handler. Inside a loop body the route is not
// merely unused: `defineWorkflow` rejects it.

import { describe, test, expect } from "bun:test";
import { defineAgent } from "@intx/agent";
import {
  awaitSignal,
  childWorkflow,
  defineWorkflow,
  loop,
  runLocal,
  step,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

function childTerminal(output: unknown): string {
  if (
    typeof output === "object" &&
    output !== null &&
    "terminalStatus" in output &&
    typeof output.terminalStatus === "string"
  ) {
    return output.terminalStatus;
  }
  throw new Error("spawn output missing terminalStatus");
}

const gatedChild = defineWorkflow({
  id: "gated-child",
  trigger: { type: "manual" },
  steps: { hold: awaitSignal({ name: "approve" }) },
});

describe("a relayed child gate under a top-level spawn step", () => {
  test("an onFailure route is not taken, and success prunes the handler", async () => {
    const def = defineWorkflow({
      id: "routed",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({ definition: gatedChild, onFailure: "recover" }),
        recover: step({ agent, after: ["spawn"] }),
      },
    });
    const run = runLocal(def, {
      runId: "run-routed",
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
    });
    await run.signal("approve", { ok: true }, "sig-1");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");

    // The gate was relayed, not refused. The spawn step completed, so the
    // handler branch was pruned and never ran.
    expect(result.events.some((e) => e.kind === "StepFailed")).toBe(false);
    expect(result.outputs).not.toHaveProperty("recover");
    expect(childTerminal(result.outputs.spawn)).toBe("completed");
  });

  test("an unrouted spawn step completes when the signal is delivered", async () => {
    const def = defineWorkflow({
      id: "unrouted",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: gatedChild }) },
    });
    const run = runLocal(def, {
      runId: "run-unrouted",
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
    });
    await run.signal("approve", { ok: true }, "sig-1");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(childTerminal(result.outputs.spawn)).toBe("completed");
    const completed = result.events.find((e) => e.kind === "ChildCompleted");
    expect(
      completed?.kind === "ChildCompleted" ? completed.terminalStatus : "",
    ).toBe("completed");
  });
});

describe("a spawn step inside a loop body", () => {
  test("may not carry an onFailure route at all", () => {
    const body = defineWorkflow({
      id: "body",
      trigger: { type: "manual" },
      steps: {
        spawn: childWorkflow({ definition: gatedChild, onFailure: "recover" }),
        recover: step({ agent, after: ["spawn"] }),
      },
    });
    expect(() =>
      defineWorkflow({
        id: "with-loop",
        trigger: { type: "manual" },
        steps: {
          l: loop({
            body,
            while: "keepGoing",
            carry: "next",
            input: { literal: 0 },
            maxIterations: 2,
            onExhausted: "fin",
          }),
          fin: step({ agent, after: ["l"] }),
        },
      }),
    ).toThrow("may not carry onFailure");
  });
});
