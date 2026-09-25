import { describe, test, expect } from "bun:test";

import { defineAgent, type AgentDefinition, type BaseEnv } from "@intx/agent";

import {
  action,
  awaitSignal,
  childWorkflow,
  defineWorkflow,
  escalation,
  extractAgent,
  gate,
  loop,
  map,
  onTrigger,
  sleep,
  step,
  type Primitive,
} from "./index";

function makeAgent(id: string): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt: "you are " + id,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

function nestedBody() {
  return defineWorkflow({
    id: "body",
    trigger: { type: "manual" },
    steps: { wait: sleep({ duration: 1 }) },
  });
}

describe("extractAgent", () => {
  test("returns the agent for step and map", () => {
    const stepAgent = makeAgent("step-agent");
    const mapAgent = makeAgent("map-agent");

    expect(extractAgent(step({ agent: stepAgent }))).toBe(stepAgent);
    expect(
      extractAgent(
        map({
          over: { from: "trigger.payload" },
          step: step({ agent: mapAgent }),
        }),
      ),
    ).toBe(mapAgent);
  });

  test("returns null for every non-agent kind", () => {
    const body = nestedBody();
    // Keyed by kind so a newly-added primitive fails this assignment until
    // the author decides whether it carries an agent.
    const nonAgent: Record<
      Exclude<Primitive["kind"], "step" | "map">,
      Primitive
    > = {
      action: action({ handler: "h" }),
      loop: loop({
        body,
        while: "w",
        carry: "c",
        maxIterations: 2,
        onExhausted: "esc",
      }),
      onTrigger: onTrigger({
        on: { type: "mail", to: "desk@example.com" },
        body,
      }),
      gate: gate({ when: { from: "trigger.payload" }, then: "a", else: "b" }),
      awaitSignal: awaitSignal({ name: "go" }),
      sleep: sleep({ duration: 1 }),
      childWorkflow: childWorkflow({ definition: body }),
      escalation: escalation({ to: "ops@example.com" }),
    };

    for (const primitive of Object.values(nonAgent)) {
      expect(extractAgent(primitive)).toBeNull();
    }
  });
});
