// What an `onFailure` route does with an unanswerable child gate, and where
// such a route may be written at all.
//
// The README tells an author that a handler on the spawn step absorbs this
// refusal like any other child failure, and to leave the step unrouted to see
// it. Both halves hold at a workflow root. Inside a loop body the route is not
// merely ineffective -- `defineWorkflow` rejects it -- so the guidance has a
// boundary worth pinning down.

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

const gatedChild = defineWorkflow({
  id: "gated-child",
  trigger: { type: "manual" },
  steps: { hold: awaitSignal({ name: "approve" }) },
});

describe("a refused child gate under a top-level spawn step", () => {
  test("an onFailure route absorbs it and the run completes", async () => {
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
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");

    // Completing alone would also hold if the child never failed, which is
    // the opposite of what this test is for. Pin the absorption: the spawn
    // step failed, it routed to the handler, and the handler ran. The child's
    // own message naming the gate stays in the child's log, which a terminal
    // child does not share with its parent.
    const failed = result.events.find((e) => e.kind === "StepFailed");
    if (failed?.kind !== "StepFailed") {
      throw new Error("expected the spawn step to fail");
    }
    expect(failed.stepId).toBe("spawn");
    expect(failed.routedTo).toBe("recover");
    expect(failed.error.message).toContain("ended failed");
    expect(result.outputs).toHaveProperty("recover");
  }, 15000);

  test("an unrouted spawn step surfaces it as a failed run", async () => {
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
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");

    // The run must fail AT the spawn step, carrying the child's terminal, so
    // an author reading the parent log is pointed at the child that could not
    // hold its gate. The child was spawned and reported a failed terminal --
    // the refusal is not a spawn that never happened.
    const failed = result.events.find((e) => e.kind === "StepFailed");
    if (failed?.kind !== "StepFailed") {
      throw new Error("expected the spawn step to fail");
    }
    expect(failed.stepId).toBe("spawn");
    expect(failed.error.message).toContain("ended failed");
    const completed = result.events.find((e) => e.kind === "ChildCompleted");
    expect(
      completed?.kind === "ChildCompleted" ? completed.terminalStatus : "",
    ).toBe("failed");
  }, 15000);
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
