// Probes the cap/convergence off-by-one boundary and iteration count.

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

// Continue while count < 2: input 0->1->2, goes false at count 2 (the
// 3rd iteration).
const cont: LoopFn = (childOutput) => countOf(childOutput) < 2;
const next: LoopFn = (_childOutput, currentInput) =>
  typeof currentInput === "number" ? currentInput + 1 : 0;

const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return cont;
  if (ref === "next") return next;
  throw new Error(`unknown loop fn ${ref}`);
};

const actionResolver = (ref: string): ActionHandler => {
  if (ref === "echo") return async (input) => input;
  return async () => `ran:${ref}`;
};

function loopOut(result: RunResult): {
  outcome: unknown;
  iterations: unknown;
  carry: unknown;
} {
  const rework = result.outputs.rework;
  if (
    typeof rework === "object" &&
    rework !== null &&
    "outcome" in rework &&
    "iterations" in rework &&
    "carry" in rework
  ) {
    return {
      outcome: rework.outcome,
      iterations: rework.iterations,
      carry: rework.carry,
    };
  }
  throw new Error("loop output missing fields");
}

function build(maxIterations: number) {
  return defineWorkflow({
    id: "loop-boundary",
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
      escalate: action({ handler: "escalate", after: ["rework"] }),
    },
  });
}

describe("loop boundary", () => {
  test("cap exactly at convergence point counts as converged (cap=3)", async () => {
    // Converges at the 3rd iteration (count reaches 2). cap=3 means the
    // convergence check must win over the exhaustion check on that
    // iteration.
    const result = await runLocal(build(3), { actionResolver, loopFns })
      .complete;
    const out = loopOut(result);
    expect(out.outcome).toBe("converged");
    expect(out.iterations).toBe(3);
    // carry threaded to the input that produced the converging output.
    expect(out.carry).toBe(2);
  });

  test("cap one below convergence exhausts (cap=2)", async () => {
    const result = await runLocal(build(2), { actionResolver, loopFns })
      .complete;
    const out = loopOut(result);
    expect(out.outcome).toBe("exhausted");
    expect(out.iterations).toBe(2);
  });

  test("cap=1 runs exactly one iteration then exhausts", async () => {
    const result = await runLocal(build(1), { actionResolver, loopFns })
      .complete;
    const out = loopOut(result);
    expect(out.outcome).toBe("exhausted");
    expect(out.iterations).toBe(1);
  });
});
