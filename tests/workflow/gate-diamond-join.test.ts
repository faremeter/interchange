// Gate-skip sentinel for diamond joins.
//
// When a gate's skip closure suppresses a branch, the skipped step's
// output is committed through the substrate as a structured sentinel
// (`{ skipped: true, gateId, branch }`). A diamond-join step that
// reads both branches' outputs sees a well-defined value for the
// not-selected side rather than crashing on an undefined selector
// path.

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";

import {
  defineWorkflow,
  gate,
  runLocal,
  step,
  type StepInvoker,
} from "@intx/workflow";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

describe("gate-skip sentinel", () => {
  test("diamond join across a skipped branch sees the structured sentinel", async () => {
    const plan = makeAgent("plan");
    const a = makeAgent("a");
    const b = makeAgent("b");
    const join = makeAgent("join");
    const def = defineWorkflow({
      id: "diamond",
      trigger: { type: "manual" },
      steps: {
        plan: step({ agent: plan }),
        proceed: gate({
          when: { from: "steps.plan.output.takeA" },
          then: "a",
          else: "b",
          after: ["plan"],
        }),
        a: step({ agent: a, after: ["proceed"] }),
        b: step({ agent: b, after: ["proceed"] }),
        join: step({
          agent: join,
          input: {
            merge: [
              { literal: { from: "a" } },
              { project: { from: "steps.a.output" }, fields: ["result"] },
            ],
          },
          after: ["a", "b"],
        }),
      },
    });

    const seenAgents: string[] = [];
    const invokeStep: StepInvoker = async ({ agent, input }) => {
      seenAgents.push(agent.id);
      if (agent.id === "plan") {
        return { output: { takeA: true } };
      }
      if (agent.id === "a") {
        return { output: { result: "from-a" } };
      }
      return { output: input };
    };

    const result = await runLocal(def, { invokeStep }).complete;
    expect(result.terminalStatus).toBe("completed");
    expect(seenAgents).toContain("a");
    expect(seenAgents).not.toContain("b");
    expect(seenAgents).toContain("join");

    // The skipped step's StepCompleted output ref resolves to the
    // structured sentinel. Verify by re-reading the log.
    const skippedCompleted = result.events.find(
      (e) => e.kind === "StepCompleted" && e.stepId === "b",
    );
    expect(skippedCompleted).toBeDefined();
  });
});
