// Runtime depth guard for childWorkflow spawn recursion via the real runLocal
// recursive spawner. A chain deeper than the ceiling fails the run; the same
// chain completes under the default ceiling, proving the ceiling -- not another
// failure -- is what stops it. The depth-naming message is asserted in the fast
// child-depth unit test and the shared-repo spawn-seam test
// (`workflow-substrate-factory-child-depth.test.ts`).

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";

import {
  childWorkflow,
  defineWorkflow,
  runLocal,
  step,
  type WorkflowAuthorizeFn,
  type WorkflowDefinition,
} from "@intx/workflow";

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

// parent -> outer -> mid -> leaf, each spawning the next inline child; the leaf
// runs one step. Depth: parent 0, outer 1, mid 2, leaf 3.
function nestedChain(): WorkflowDefinition {
  const leaf = defineWorkflow({
    id: "leaf-w",
    trigger: { type: "manual" },
    steps: { work: step({ agent: makeAgent("leaf") }) },
  });
  const mid = defineWorkflow({
    id: "mid-w",
    trigger: { type: "manual" },
    steps: { spawn: childWorkflow({ definition: leaf }) },
  });
  const outer = defineWorkflow({
    id: "outer-w",
    trigger: { type: "manual" },
    steps: { spawn: childWorkflow({ definition: mid }) },
  });
  return defineWorkflow({
    id: "parent-w",
    trigger: { type: "manual" },
    steps: { spawn: childWorkflow({ definition: outer }) },
  });
}

describe("childWorkflow runtime depth guard (runLocal)", () => {
  test("a chain deeper than the ceiling fails the run", async () => {
    // ceiling 2: mid (depth 2) spawning leaf (depth 3) trips the guard.
    const result = await runLocal(nestedChain(), {
      authorize: allowAll,
      maxChildSpawnDepth: 2,
      hasUpstreamSignalResolver: true,
    }).complete;
    expect(result.terminalStatus).toBe("failed");
  });

  test("the same chain completes under the default ceiling", async () => {
    const result = await runLocal(nestedChain(), {
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
    }).complete;
    expect(result.terminalStatus).toBe("completed");
  });

  test("a huge injected ceiling does not spuriously fail a shallow run", async () => {
    // A large override is clamped to the constant, so the depth-4 tree still
    // runs well under the bound and completes. (The clamp arithmetic -- an
    // override can only tighten, never loosen -- is proven in the child-depth
    // unit test; a chain past 32 is impractical to author here.)
    const result = await runLocal(nestedChain(), {
      authorize: allowAll,
      maxChildSpawnDepth: 1_000_000,
      hasUpstreamSignalResolver: true,
    }).complete;
    expect(result.terminalStatus).toBe("completed");
  });
});
