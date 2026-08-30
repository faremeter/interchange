// A loop body may contain a nested loop. The inner loop resolves its body ref
// from the SAME top-level bodies map the outer loop uses (a loop iteration
// inherits the parent env), so `enumerateInlineLoopBodies` recursing into loop
// bodies is what makes the inner ref resolvable. This drives a two-level loop
// end-to-end through `runLocal` and asserts the inner loop actually iterates
// under each outer iteration.
//
// The fixtures assemble a nested definition by swapping a loop's body for a
// loop-containing one (`withLoopBody`) -- a terse fixture helper. The runtime is
// a pure structural executor over a `WorkflowDefinition`, so it runs a nested
// definition however it was assembled.

import { describe, test, expect } from "bun:test";

import {
  action,
  awaitSignal,
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

describe("nested loop awaitSignal (in-process park)", () => {
  test("an inner-loop body signal park relays up through both containers", async () => {
    // outer loop -> inner loop -> body `awaitSignal("go")`. A single delivery on
    // the run's real channel must cascade DOWN two container relays to reach the
    // body's gate: the outer container relays into the inner container's owned
    // channel, which relays into the body's gate. The inner container surfacing
    // its relay await to the outer (env.onSignalPark) is what makes that work.
    const innerBody = defineWorkflow({
      id: "inner-await-body",
      trigger: { type: "manual" },
      steps: { hold: awaitSignal({ name: "go" }) },
    });
    const innerLoopWf = defineWorkflow({
      id: "inner-await-wf",
      trigger: { type: "manual" },
      steps: {
        inner: loop({
          body: innerBody,
          while: "stop",
          carry: "thread",
          maxIterations: 3,
          onExhausted: "iesc",
        }),
        iesc: action({ handler: "esc", after: ["inner"] }),
      },
    });
    const outer = withLoopBody(
      defineWorkflow({
        id: "nested-await-wf",
        trigger: { type: "manual" },
        steps: {
          outer: loop({
            body: leaf,
            while: "stop",
            carry: "thread",
            maxIterations: 3,
            onExhausted: "oesc",
          }),
          oesc: action({ handler: "esc", after: ["outer"] }),
        },
      }),
      "outer",
      innerLoopWf,
    );

    const loopFns = (ref: string): LoopFn => {
      if (ref === "stop") return () => false;
      if (ref === "thread") return () => null;
      throw new Error(`unknown loop fn ${ref}`);
    };
    const actionResolver = (ref: string): ActionHandler => {
      if (ref === "esc") return async () => "escalated";
      throw new Error(`unknown handler ${ref}`);
    };

    const run = runLocal(outer, { actionResolver, loopFns });
    // The in-memory channel queues the delivery until the outermost relay
    // subscribes, so delivering before the chain parks is fine.
    await run.signal("go", { done: true }, "sig-1");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    const { outcome, iterations } = outerLoopOutcome(result.outputs.outer);
    expect(outcome).toBe("converged");
    expect(iterations).toBe(1);
    expect("oesc" in result.outputs).toBe(false);
  });
});

describe("nested loop routing", () => {
  test("an exhausting outer loop routes to onExhausted around converging inner loops", async () => {
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
    const fns = (ref: string): LoopFn => {
      if (ref === "always") return () => true;
      if (ref === "cont") return cont;
      if (ref === "next") return next;
      throw new Error(`unknown loop fn ${ref}`);
    };
    // Inner loop converges after 3 (while `cont`); outer never converges
    // (`always`) and caps at 2, so it exhausts and routes to its onExhausted.
    const innerLoopBody = oneLoop("inner-wf", "inner");
    const nested = withLoopBody(
      defineWorkflow({
        id: "exhaust-nested-wf",
        trigger: { type: "manual" },
        steps: {
          outer: loop({
            body: leaf,
            while: "always",
            carry: "next",
            input: { literal: 0 },
            maxIterations: 2,
            onExhausted: "oesc",
          }),
          oesc: action({ handler: "esc", after: ["outer"] }),
        },
      }),
      "outer",
      innerLoopBody,
    );

    const result = await runLocal(nested, { actionResolver, loopFns: fns })
      .complete;

    expect(result.terminalStatus).toBe("completed");
    const { outcome, iterations } = outerLoopOutcome(result.outputs.outer);
    expect(outcome).toBe("exhausted");
    expect(iterations).toBe(2);
    expect(result.outputs.oesc).toBe("escalated");
    // 2 outer iterations x 3 inner iterations = 6 leaf ticks.
    expect(ticks).toBe(6);
  });

  test("the outer loop threads its carry across nested iterations", async () => {
    const actionResolver = (ref: string): ActionHandler => {
      if (ref === "tick") return async () => null;
      if (ref === "esc") return async () => "escalated";
      throw new Error(`unknown handler ${ref}`);
    };
    // Both loops thread the counter carry; the outer converges when its carry
    // reaches 2 (0 -> 1 -> 2), so its output carry is the value it converged on.
    const innerLoopBody = oneLoop("inner-wf", "inner");
    const nested = withLoopBody(
      oneLoop("carry-nested-wf", "outer"),
      "outer",
      innerLoopBody,
    );

    const result = await runLocal(nested, { actionResolver, loopFns }).complete;

    expect(result.terminalStatus).toBe("completed");
    const outer = result.outputs.outer;
    if (typeof outer !== "object" || outer === null || !("carry" in outer)) {
      throw new Error("outer loop output missing carry");
    }
    expect(outer.carry).toBe(2);
  });
});
