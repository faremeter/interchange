// runLoop driver: convergence vs exhaustion routing, throwing predicate,
// and per-iteration effect exactly-once.

import { describe, test, expect } from "bun:test";

import {
  action,
  defineWorkflow,
  loop,
  runLocal,
  type ActionHandler,
  type LoopFn,
  type RunResult,
} from "@intx/workflow";

// A loop body: one action that echoes its numeric input. The iteration
// input arrives as the child run's trigger payload, so the action reads
// it explicitly (actions do not take the default-input convention).
const body = defineWorkflow({
  id: "body",
  trigger: { type: "manual" },
  steps: {
    count: action({ handler: "echo", input: { from: "trigger.payload" } }),
  },
});

function countOf(childOutput: unknown): number {
  if (
    typeof childOutput === "object" &&
    childOutput !== null &&
    "count" in childOutput
  ) {
    const count = childOutput.count;
    if (typeof count === "number") return count;
  }
  throw new Error("iteration output missing numeric count");
}

// Continue while the echoed count is below 2.
const cont: LoopFn = (childOutput) => countOf(childOutput) < 2;
// Thread the next input as the current numeric input plus one.
const next: LoopFn = (_childOutput, currentInput) =>
  typeof currentInput === "number" ? currentInput + 1 : 0;

const actionResolver = (ref: string): ActionHandler => {
  if (ref === "echo") return async (input) => input;
  if (ref === "consolidate") return async () => "consolidated";
  if (ref === "escalate") return async () => "escalated";
  throw new Error(`unknown handler ${ref}`);
};

const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return cont;
  if (ref === "next") return next;
  throw new Error(`unknown loop fn ${ref}`);
};

function dispatchWorkflow(maxIterations: number) {
  return defineWorkflow({
    id: "loop-parent",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body,
        while: "cont",
        carry: "next",
        input: { literal: 0 },
        maxIterations,
        onExhausted: "escalate",
      }),
      consolidate: action({ handler: "consolidate", after: ["rework"] }),
      escalate: action({ handler: "escalate", after: ["rework"] }),
    },
  });
}

function loopOutcome(result: RunResult): unknown {
  const rework = result.outputs.rework;
  if (typeof rework === "object" && rework !== null && "outcome" in rework) {
    return rework.outcome;
  }
  throw new Error("loop output missing outcome");
}

describe("runLoop", () => {
  test("converges before the cap and routes to the normal dependents", async () => {
    // input 0 -> 1 -> 2; `cont` (count < 2) goes false at count 2, so it
    // converges after 3 iterations, well under the cap of 5.
    const result = await runLocal(dispatchWorkflow(5), {
      actionResolver,
      loopFns,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(loopOutcome(result)).toBe("converged");
    // consolidate ran; escalate was pruned (skipped, not scheduled).
    expect(result.outputs.consolidate).toBe("consolidated");
    expect("escalate" in result.outputs).toBe(false);
  });

  test("hits the cap and routes to onExhausted", async () => {
    // With a cap of 2 the loop exhausts (cont still true at count 1).
    const result = await runLocal(dispatchWorkflow(2), {
      actionResolver,
      loopFns,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(loopOutcome(result)).toBe("exhausted");
    // escalate ran; consolidate was pruned.
    expect(result.outputs.escalate).toBe("escalated");
    expect("consolidate" in result.outputs).toBe(false);
  });

  test("a throwing while predicate fails the loop", async () => {
    const throwingFns = (ref: string): LoopFn => {
      if (ref === "cont")
        return () => {
          throw new Error("predicate boom");
        };
      if (ref === "next") return next;
      throw new Error(`unknown loop fn ${ref}`);
    };
    const result = await runLocal(dispatchWorkflow(5), {
      actionResolver,
      loopFns: throwingFns,
    }).complete;

    expect(result.terminalStatus).toBe("failed");
  });

  test("each iteration's body effect runs exactly once", async () => {
    let effectRuns = 0;
    const effectResolver = (ref: string): ActionHandler => {
      if (ref === "echo")
        return async (input, ctx) => {
          await ctx.perform({
            effectId: "touch",
            capability: "fs:write",
            run: async () => {
              effectRuns += 1;
              return null;
            },
          });
          return input;
        };
      if (ref === "consolidate") return async () => "consolidated";
      if (ref === "escalate") return async () => "escalated";
      throw new Error(`unknown handler ${ref}`);
    };
    const effectBody = defineWorkflow({
      id: "effect-body",
      trigger: { type: "manual" },
      steps: {
        count: action({
          handler: "echo",
          input: { from: "trigger.payload" },
          effect: { requires: ["fs:write"] },
        }),
      },
    });
    const workflow = defineWorkflow({
      id: "loop-effect-parent",
      trigger: { type: "manual" },
      steps: {
        rework: loop({
          body: effectBody,
          while: "cont",
          carry: "next",
          input: { literal: 0 },
          maxIterations: 5,
          onExhausted: "escalate",
        }),
        consolidate: action({ handler: "consolidate", after: ["rework"] }),
        escalate: action({ handler: "escalate", after: ["rework"] }),
      },
    });

    const result = await runLocal(workflow, {
      actionResolver: effectResolver,
      loopFns,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    // Converges after 3 iterations; each iteration is a distinct child
    // run, so its effect key differs and each fires exactly once.
    expect(effectRuns).toBe(3);
  });
});
