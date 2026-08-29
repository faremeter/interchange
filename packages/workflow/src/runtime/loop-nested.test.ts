// A loop body may contain a nested loop. The inner loop resolves its body ref
// from the SAME top-level bodies map the outer loop uses (a loop iteration
// inherits the parent env), so `enumerateInlineLoopBodies` recursing into loop
// bodies is what makes the inner ref resolvable. This drives a two-level loop
// end-to-end through `runLocal` and asserts the inner loop actually iterates
// under each outer iteration.
//
// The nested definition is assembled by swapping loop bodies rather than through
// `defineWorkflow`, which rejects a nested loop at authoring time. The runtime
// is a pure structural executor over a `WorkflowDefinition`, so a hand-built
// nested definition exercises the resolution seam directly.

import { describe, test, expect } from "bun:test";

import {
  action,
  defineWorkflow,
  loop,
  runLocal,
  type ActionHandler,
  type LoopFn,
  type WorkflowDefinition,
} from "@intx/workflow";

// A leaf body: one action that counts its invocations.
const leaf = defineWorkflow({
  id: "leaf",
  steps: { tick: action({ handler: "tick" }) },
});

// Both loops run three iterations: the pure `while` continues while the threaded
// carry is below 2 (carry 0,1,2 -> stop at 2), independent of the body output.
const cont: LoopFn = (_output, currentInput) =>
  (typeof currentInput === "number" ? currentInput : 0) < 2;
const next: LoopFn = (_output, currentInput) =>
  (typeof currentInput === "number" ? currentInput : 0) + 1;
const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return cont;
  if (ref === "next") return next;
  throw new Error(`unknown loop fn ${ref}`);
};

function oneLoop(id: string, loopStepId: string): WorkflowDefinition {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: {
      [loopStepId]: loop({
        body: leaf,
        while: "cont",
        carry: "next",
        input: { literal: 0 },
        maxIterations: 5,
        onExhausted: "esc",
      }),
      esc: action({ handler: "esc", after: [loopStepId] }),
    },
  });
}

function withLoopBody(
  wf: WorkflowDefinition,
  loopStepId: string,
  body: WorkflowDefinition,
): WorkflowDefinition {
  const primitive = wf.steps[loopStepId];
  if (primitive?.kind !== "loop") {
    throw new Error(`fixture: ${loopStepId} is not a loop`);
  }
  return {
    ...wf,
    steps: { ...wf.steps, [loopStepId]: { ...primitive, body } },
  };
}

function outerLoopOutcome(output: unknown): {
  outcome: unknown;
  iterations: unknown;
} {
  if (
    typeof output === "object" &&
    output !== null &&
    "outcome" in output &&
    "iterations" in output
  ) {
    return { outcome: output.outcome, iterations: output.iterations };
  }
  throw new Error("outer loop output missing outcome/iterations");
}

describe("nested loop", () => {
  test("the inner loop iterates under every outer iteration", async () => {
    let ticks = 0;
    const actionResolver = (ref: string): ActionHandler => {
      if (ref === "tick")
        return async () => {
          ticks += 1;
          return null;
        };
      if (ref === "esc") return async () => "escalated";
      throw new Error(`unknown handler ${ref}`);
    };

    // Outer loop body IS a single-level loop workflow, so nesting is two loops
    // deep: outer -> inner -> leaf.
    const innerLoopBody = oneLoop("inner-wf", "inner");
    const nested = withLoopBody(
      oneLoop("nested-wf", "outer"),
      "outer",
      innerLoopBody,
    );

    const result = await runLocal(nested, { actionResolver, loopFns }).complete;

    expect(result.terminalStatus).toBe("completed");
    // Outer converges after 3 iterations; each runs the inner loop, which itself
    // converges after 3 iterations -- so the leaf action runs 3 * 3 = 9 times.
    expect(ticks).toBe(9);
    const { outcome, iterations } = outerLoopOutcome(result.outputs.outer);
    expect(outcome).toBe("converged");
    expect(iterations).toBe(3);
    // The inner loop converged (its onExhausted branch was pruned), so no
    // escalation ran.
    expect("esc" in result.outputs).toBe(false);
  });
});
