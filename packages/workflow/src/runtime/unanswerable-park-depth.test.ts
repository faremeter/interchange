// A gate held two inheritance hops below the run that declared itself
// unanswerable.
//
// The single-hop case is covered alongside the guard itself. This one exists
// because the defect it guards is a missing env spread, and every added
// nesting layer is another place to reintroduce one.
//
// The production chain is: terminal child env (false) -> loop iteration env
// (inherits) -> nested loop iteration env (inherits). Each hop is a spread of
// the container's env, so a single missing spread anywhere reopens the hole.

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";
import {
  awaitSignal,
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

const loopFns = (ref: string) =>
  ref === "keepGoing"
    ? () => false
    : (_o: unknown, carried: unknown) =>
        (typeof carried === "number" ? carried : 0) + 1;

function nestedLoopHoldingGate(timeout?: number) {
  const inner = defineWorkflow({
    id: "inner-body",
    trigger: { type: "manual" },
    steps: {
      hold:
        timeout === undefined
          ? awaitSignal({ name: "approve" })
          : awaitSignal({ name: "approve", timeout, onTimeout: "done" }),
      ...(timeout === undefined
        ? {}
        : { done: step({ agent, after: ["hold"] }) }),
    },
  });
  const outer = defineWorkflow({
    id: "outer-body",
    trigger: { type: "manual" },
    steps: {
      innerLoop: loop({
        body: inner,
        while: "keepGoing",
        carry: "nextCount",
        input: { literal: 0 },
        maxIterations: 2,
        onExhausted: "escalate",
      }),
      escalate: step({ agent, after: ["innerLoop"] }),
    },
  });
  return defineWorkflow({
    id: "top",
    trigger: { type: "manual" },
    steps: {
      outerLoop: loop({
        body: outer,
        while: "keepGoing",
        carry: "nextCount",
        input: { literal: 0 },
        maxIterations: 2,
        onExhausted: "finish",
      }),
      finish: step({ agent, after: ["outerLoop"] }),
    },
  });
}

async function race(p: Promise<unknown>, ms: number) {
  return Promise.race([
    p.then(() => "settled" as const),
    new Promise<"pending">((r) => setTimeout(() => r("pending"), ms)),
  ]);
}

describe("two hops below an unanswerable run", () => {
  test("an untimed gate is refused", async () => {
    const run = runLocal(nestedLoopHoldingGate(), {
      runId: "run-nested-no-resolver",
      hasUpstreamSignalResolver: false,
      loopFns,
    });
    expect(await race(run.complete, 3000)).toBe("settled");
    expect((await run.complete).terminalStatus).toBe("failed");
  }, 10000);

  test("an untimed gate still parks when the tree can answer", async () => {
    const run = runLocal(nestedLoopHoldingGate(), {
      runId: "run-nested-with-resolver",
      hasUpstreamSignalResolver: true,
      loopFns,
    });
    expect(await race(run.complete, 1500)).toBe("pending");
    await run.cancel("supervisor-operator", "teardown");
  }, 10000);

  test("a timed gate two hops down is NOT refused", async () => {
    const run = runLocal(nestedLoopHoldingGate(200), {
      runId: "run-nested-timed",
      hasUpstreamSignalResolver: false,
      loopFns,
    });
    expect(await race(run.complete, 5000)).toBe("settled");
    const events = await run.complete;
    expect(events.terminalStatus).toBe("completed");
  }, 10000);
});
