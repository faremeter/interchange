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
} from "@intx/workflow";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
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
    const run = runLocal(def, { runId: "run-routed" });
    expect((await run.complete).terminalStatus).toBe("completed");
  }, 15000);

  test("an unrouted spawn step surfaces it as a failed run", async () => {
    const def = defineWorkflow({
      id: "unrouted",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: gatedChild }) },
    });
    const run = runLocal(def, { runId: "run-unrouted" });
    expect((await run.complete).terminalStatus).toBe("failed");
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
