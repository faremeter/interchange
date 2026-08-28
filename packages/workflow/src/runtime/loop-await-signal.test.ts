// A loop body may now park on an `awaitSignal` and resume. This drives the
// in-process park path end to end: the body parks on `awaitSignal`, the loop
// container proxies it up as a signal-relay await (driveContainerSignalRelay),
// the delivered signal relays back into the body, and the iteration completes.
// No crash is involved -- in-process park/deliver needs no planLoopResume.

import { describe, test, expect } from "bun:test";

import {
  action,
  awaitSignal,
  defineWorkflow,
  loop,
  runLocal,
  type ActionHandler,
  type LoopFn,
} from "@intx/workflow";

function loopAwaitWorkflow(maxIterations: number) {
  return defineWorkflow({
    id: "await-loop-parent",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body: defineWorkflow({
          id: "await-body",
          trigger: { type: "manual" },
          steps: { hold: awaitSignal({ name: "go" }) },
        }),
        while: "cont",
        carry: "thread",
        maxIterations,
        onExhausted: "escalate",
      }),
      escalate: action({ handler: "escalate", after: ["rework"] }),
    },
  });
}

const actionResolver = (ref: string): ActionHandler => {
  if (ref === "escalate") return async () => "escalated";
  throw new Error(`unknown handler ${ref}`);
};

function holdOf(childOutput: unknown): unknown {
  if (
    typeof childOutput === "object" &&
    childOutput !== null &&
    "hold" in childOutput
  ) {
    return (childOutput as { hold: unknown }).hold;
  }
  throw new Error("iteration output missing awaitSignal step output");
}

function loopOutput(result: { outputs: Record<string, unknown> }): {
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
  throw new Error("loop output missing outcome/iterations/carry");
}

describe("loop body awaitSignal (in-process park)", () => {
  test("parks on the body signal, resumes on delivery, and completes", async () => {
    // `cont` converges after one iteration; the body still parks first.
    const loopFns = (ref: string): LoopFn => {
      if (ref === "cont") return () => false;
      if (ref === "thread") return () => null;
      throw new Error(`unknown loop fn ${ref}`);
    };
    const run = runLocal(loopAwaitWorkflow(3), { actionResolver, loopFns });
    // Deliver on the parent channel where the container's relay awaits it; the
    // in-memory channel queues the delivery until the relay subscribes.
    await run.signal("go", { done: true }, "sig-1");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    const out = loopOutput(result);
    expect(out.outcome).toBe("converged");
    expect(out.iterations).toBe(1);
  });

  test("threads carry forward through a parked-then-resumed iteration", async () => {
    // The body's awaitSignal output becomes the iteration output; `cont`
    // continues while the delivered value is < 2 and `thread` carries it. This
    // is FORWARD carry through an in-process park (not durable crash-carry).
    const loopFns = (ref: string): LoopFn => {
      if (ref === "cont") {
        return (childOutput) => Number(holdOf(childOutput)) < 2;
      }
      if (ref === "thread") return (childOutput) => holdOf(childOutput);
      throw new Error(`unknown loop fn ${ref}`);
    };
    const run = runLocal(loopAwaitWorkflow(5), { actionResolver, loopFns });
    // Iteration 0 consumes the first "go" (1); iteration 1 consumes the second
    // (2). FIFO on the parent channel's "go" queue routes each to its relay.
    await run.signal("go", 1, "sig-1");
    await run.signal("go", 2, "sig-2");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    const out = loopOutput(result);
    expect(out.outcome).toBe("converged");
    // iter0 parked, resumed on 1 (1 < 2 -> continue); carry threaded 1 into
    // iter1, which parked, resumed on 2 (2 < 2 -> converge). The loop's carry
    // is the input carried into the converging iteration -- 1.
    expect(out.iterations).toBe(2);
    expect(out.carry).toBe(1);
  });
});
