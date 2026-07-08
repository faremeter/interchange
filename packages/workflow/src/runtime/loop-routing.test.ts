// Probes the set-generalized branch closure + diamond guard for runLoop
// routing, plus onExhausted/normal-dependent edge cases.

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

function has(result: RunResult, id: string): boolean {
  return id in result.outputs;
}

describe("loop diamond routing", () => {
  // Diamond: `join` lists BOTH a normal dependent (consolidate) and the
  // onExhausted downstream (afterEscalate) in its after. On converge,
  // consolidate is selected -> join is reachable from a selected root and
  // must stay LIVE even though it is also reachable from the pruned
  // onExhausted side.
  function diamondWorkflow(maxIterations: number) {
    return defineWorkflow({
      id: "loop-diamond",
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
        afterEscalate: action({
          handler: "afterEscalate",
          after: ["escalate"],
        }),
        join: action({
          handler: "join",
          after: ["consolidate", "afterEscalate"],
        }),
      },
    });
  }

  test("converge: join reachable from selected AND not-selected stays live", async () => {
    const result = await runLocal(diamondWorkflow(5), {
      actionResolver,
      loopFns,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(has(result, "consolidate")).toBe(true);
    // escalate + afterEscalate pruned; join stays live because it is
    // reachable from consolidate (selected).
    expect(has(result, "escalate")).toBe(false);
    expect(has(result, "afterEscalate")).toBe(false);
    expect(result.outputs.join).toBe("ran:join");
  });

  test("exhaust: join stays live via afterEscalate side", async () => {
    const result = await runLocal(diamondWorkflow(2), {
      actionResolver,
      loopFns,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(has(result, "escalate")).toBe(true);
    expect(has(result, "afterEscalate")).toBe(true);
    expect(has(result, "consolidate")).toBe(false);
    expect(result.outputs.join).toBe("ran:join");
  });
});

describe("loop no-normal-dependent routing", () => {
  // The loop has an onExhausted but NO normal after-dependents. On
  // converge, `selected` (normal dependents) is EMPTY. The not-taken side
  // (onExhausted) must still be pruned; the run must complete.
  function noDepsWorkflow(maxIterations: number) {
    return defineWorkflow({
      id: "loop-no-deps",
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

  test("converge with empty selected set prunes onExhausted and completes", async () => {
    const result = await runLocal(noDepsWorkflow(5), {
      actionResolver,
      loopFns,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(has(result, "escalate")).toBe(false);
  });

  test("exhaust with empty normal-dependent set runs onExhausted", async () => {
    const result = await runLocal(noDepsWorkflow(2), {
      actionResolver,
      loopFns,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(result.outputs.escalate).toBe("ran:escalate");
  });
});
