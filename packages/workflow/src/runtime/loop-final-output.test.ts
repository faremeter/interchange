// The loop container's output exposes the FINAL iteration's output.
//
// `runLoop` settles the moment `while` goes false, before `carry` runs, so
// `carry` on the loop's own output is the converging iteration's INPUT. The
// converging iteration's OUTPUT -- the value `while` judged -- is reachable
// only through `final`; the scoped iteration step id (`rework[2]`) is not a
// selector path the grammar admits.

import { describe, test, expect } from "bun:test";

import {
  action,
  defineWorkflow,
  loop,
  runLocal,
  type ActionHandler,
  type LoopFn,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

// One action that returns its numeric input plus one, so an iteration's
// output is always distinguishable from the input it ran on.
const body = defineWorkflow({
  id: "bump-body",
  trigger: { type: "manual" },
  steps: {
    bump: action({ handler: "bump", input: { from: "trigger.payload" } }),
  },
});

function bumpOf(childOutput: unknown): number {
  if (
    typeof childOutput === "object" &&
    childOutput !== null &&
    "bump" in childOutput &&
    typeof childOutput.bump === "number"
  ) {
    return childOutput.bump;
  }
  throw new Error("iteration output missing numeric bump");
}

// `while` judges the iteration OUTPUT, the surface's natural reading: keep
// going while the bump is still under three.
const cont: LoopFn = (childOutput) => bumpOf(childOutput) < 3;
// `carry` threads the iteration's own output forward as the next input.
const next: LoopFn = (childOutput) => bumpOf(childOutput);

const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return cont;
  if (ref === "next") return next;
  throw new Error(`unknown loop fn ${ref}`);
};

const seen: Record<string, unknown> = {};

const actionResolver = (ref: string): ActionHandler => {
  if (ref === "bump") {
    return async (input) => (typeof input === "number" ? input + 1 : 0);
  }
  if (ref === "downstream") {
    return async (input) => {
      seen.downstream = input;
      return input;
    };
  }
  if (ref === "escalate") return async () => "escalated";
  throw new Error(`unknown handler ${ref}`);
};

function build(maxIterations: number, downstreamPath: string) {
  return defineWorkflow({
    id: "loop-final-output",
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
      downstream: action({
        handler: "downstream",
        after: ["rework"],
        input: { from: downstreamPath },
      }),
      escalate: action({ handler: "escalate", after: ["rework"] }),
    },
  });
}

function runWith(maxIterations: number, downstreamPath: string) {
  return runLocal(build(maxIterations, downstreamPath), {
    authorize: allowAll,
    actionResolver,
    loopFns,
  }).complete;
}

describe("loop final-iteration output", () => {
  test("a converged loop exposes the converging iteration's output", async () => {
    // input 0 -> {bump:1} -> carry 1 -> {bump:2} -> carry 2 -> {bump:3};
    // `cont` goes false at 3, so the loop settles before `carry` runs on
    // that third iteration. `carry` is therefore 2 (its input) and `final`
    // is {bump:3} (its output).
    const result = await runWith(10, "steps.rework.output");

    expect(result.terminalStatus).toBe("completed");
    expect(result.outputs.rework).toEqual({
      outcome: "converged",
      iterations: 3,
      carry: 2,
      final: { bump: 3 },
    });
  });

  test("a downstream step selects into the final iteration's output", async () => {
    seen.downstream = undefined;
    const result = await runWith(10, "steps.rework.output.final.bump");

    expect(result.terminalStatus).toBe("completed");
    expect(seen.downstream).toBe(3);
  });

  test("an exhausted loop exposes the last iteration's output", async () => {
    // Capped at two, `cont` is still true at {bump:2}: the loop exhausts and
    // routes to `escalate`, pruning `downstream`. `final` is the last
    // iteration's output either way -- the key does not depend on outcome.
    const result = await runWith(2, "steps.rework.output");

    expect(result.terminalStatus).toBe("completed");
    expect(result.outputs.rework).toEqual({
      outcome: "exhausted",
      iterations: 2,
      carry: 1,
      final: { bump: 2 },
    });
    expect(result.outputs.escalate).toBe("escalated");
    expect("downstream" in result.outputs).toBe(false);
  });
});
