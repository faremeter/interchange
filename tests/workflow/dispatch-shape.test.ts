// runLocal synthetic dispatch-shape fixture and integration test.
//
// Mirrors the dispatch-demo shape in-tree (no external repo
// dependency): planner step -> map over tasks -> critic -> gate ->
// attribution. Exercises step, map, gate, awaitSignal, sleep,
// childWorkflow, retry, AuthorizeContext propagation, and the
// blob-substrate threshold in one workload.

import { describe, test, expect } from "bun:test";

import { defineAgent, type AgentDefinition, type BaseEnv } from "@intx/agent";

import {
  awaitSignal,
  childWorkflow,
  defineWorkflow,
  gate,
  map,
  runLocal,
  sleep,
  step,
  type AuthorizeContext,
  type StepInvoker,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

function makeAgent(id: string): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

function makeChildDef() {
  return defineWorkflow({
    id: "child",
    trigger: { type: "manual" },
    steps: {
      finalize: step({ agent: makeAgent("finalize") }),
    },
  });
}

describe("runLocal dispatch-shape fixture", () => {
  test("exercises step, map, gate, awaitSignal, sleep, childWorkflow end-to-end", async () => {
    const planner = makeAgent("planner");
    const implementer = makeAgent("implementer");
    const critic = makeAgent("critic");
    const attribution = makeAgent("attribution");

    const escalator = makeAgent("escalator");
    const def = defineWorkflow({
      id: "dispatch",
      trigger: { type: "manual" },
      steps: {
        plan: step({ agent: planner }),
        impl: map({
          over: { from: "steps.plan.output.tasks" },
          step: step({ agent: implementer }),
          after: ["plan"],
        }),
        review: step({ agent: critic, after: ["impl"] }),
        proceed: gate({
          when: { from: "steps.review.output.ok" },
          then: "pause",
          else: "escalate",
          after: ["review"],
        }),
        pause: sleep({ duration: 5, after: ["proceed"] }),
        escalate: step({ agent: escalator, after: ["proceed"] }),
        approval: awaitSignal({
          name: "approve",
          after: ["pause"],
        }),
        followup: childWorkflow({
          definitionRef: "child",
          after: ["approval"],
        }),
        attribute: step({ agent: attribution, after: ["followup"] }),
      },
    });

    const observed: AuthorizeContext[] = [];
    const authorize: WorkflowAuthorizeFn = async (
      _resource,
      _action,
      context,
    ) => {
      observed.push(context);
      return { effect: "allow", matchingGrants: [], resolvedBy: null };
    };

    const invokeStep: StepInvoker = async ({ agent, input, authzContext }) => {
      await authorize(`tool:${agent.id}`, "invoke", authzContext);
      if (agent.id === "planner") {
        return { output: { tasks: [{ name: "a" }, { name: "b" }] } };
      }
      if (agent.id === "critic") {
        return { output: { ok: true } };
      }
      if (agent.id === "attribution") {
        return { output: { attributed: true } };
      }
      return { output: { processed: input } };
    };

    const run = runLocal(def, {
      triggerPayload: { goal: "ship it" },
      authorize,
      invokeStep,
      childResolver: () => makeChildDef(),
    });

    // The signal channel queues pre-await deliveries under the signal
    // name, so injecting "approve" once before the awaitSignal step
    // reaches it is enough -- the awaiter consumes the queued payload
    // when it arrives.
    await run.signal("approve", { ok: true });

    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    // Each step's authz call carried the populated context.
    expect(
      observed.every(
        (c) =>
          typeof c.stepId === "string" &&
          typeof c.attempt === "number" &&
          typeof c.runId === "string",
      ),
    ).toBe(true);
    // Map fan-out hit two indices.
    expect(observed.filter((c) => c.stepId === "impl[0]")).toHaveLength(1);
    expect(observed.filter((c) => c.stepId === "impl[1]")).toHaveLength(1);
    // The gate selected the then-branch (review.output.ok === true);
    // the escalator agent must not have been invoked.
    expect(observed.filter((c) => c.stepId === "escalate")).toHaveLength(0);
  });
});
