// A loop body may spawn a `childWorkflow` grandchild. runLocal lifts the loop
// body's inline child to a ref and registers the grandchild in the map the
// iteration's inherited `spawnChild` resolves from, so the grandchild runs as
// its own child run.
//
// The grandchild's single step is a `map` over an empty list: it runs zero
// iterations and completes with no inner step, so the grandchild completes
// WITHOUT a per-child handler resolver (runLocal does not thread the parent's
// resolvers into a child run, mirroring production, where each child resolves
// its own closure). A completed run proves the whole lift/resolve/spawn path,
// since a loop that converges required the body's childWorkflow to complete.

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";

import {
  childWorkflow,
  defineWorkflow,
  loop,
  map,
  runLocal,
  step,
  type LoopFn,
} from "@intx/workflow";

const leaf = defineWorkflow({
  id: "leaf",
  trigger: { type: "manual" },
  steps: {
    fan: map({
      over: { literal: [] },
      step: step({
        agent: defineAgent({
          id: "leaf-agent",
          systemPrompt: "s",
          tools: [],
          capabilities: [],
          inference: { sources: [{ provider: "fake", model: "fake" }] },
        }),
      }),
    }),
  },
});

const cwLoopParent = defineWorkflow({
  id: "cw-loop-parent",
  trigger: { type: "manual" },
  steps: {
    rework: loop({
      body: defineWorkflow({
        id: "cw-loop-body",
        trigger: { type: "manual" },
        steps: { spawn: childWorkflow({ definition: leaf }) },
      }),
      while: "cont",
      carry: "next",
      input: { literal: 0 },
      maxIterations: 2,
      onExhausted: "escalate",
    }),
    escalate: step({ agent: makeAgent("escalate"), after: ["rework"] }),
  },
});

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return () => false;
  if (ref === "next") return () => null;
  throw new Error(`unknown loop fn ${ref}`);
};

describe("childWorkflow inside a loop body", () => {
  test("a loop body spawns a grandchild that runs to completion", async () => {
    const result = await runLocal(cwLoopParent, { loopFns }).complete;

    // The loop converged after one iteration, which required the body's
    // childWorkflow grandchild to spawn (lifted + resolved from the inherited
    // env) and run to completion.
    expect(result.terminalStatus).toBe("completed");
    expect(result.outputs.rework).toMatchObject({
      outcome: "converged",
      iterations: 1,
    });
  });
});
