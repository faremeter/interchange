import { describe, test, expect } from "bun:test";

import type { AgentDefinition, BaseEnv } from "@intx/agent";
import type { WorkflowDefinition } from "@intx/workflow/definition";

import {
  synthesizeFoldedWorkflow,
  type FoldedWorkflowInput,
} from "./fold-synthesis";

const BASE: FoldedWorkflowInput = {
  workflowId: "wf_agt_1",
  mailAddress: "ins_agt_1@tenant.interchange",
  systemPrompt: "You are the folded agent.",
  description: "A folded agent.",
  inferencePreferences: [{ provider: "anthropic", model: "opus" }],
  toolPackagePins: [{ name: "@intx/tools-mail", version: "0.1.2" }],
};

function stepAgent(wf: WorkflowDefinition): AgentDefinition<BaseEnv> {
  const stepId = wf.stepOrder[0];
  if (stepId === undefined) throw new Error("expected one step");
  const primitive = wf.steps[stepId];
  if (primitive?.kind !== "step") throw new Error("expected a step primitive");
  return primitive.agent;
}

describe("synthesizeFoldedWorkflow", () => {
  test("produces a single-step workflow carrying the mail trigger", () => {
    const wf = synthesizeFoldedWorkflow(BASE);
    expect(wf.id).toBe("wf_agt_1");
    expect(wf.stepOrder).toHaveLength(1);
    expect(wf.triggers).toEqual([
      { type: "mail", to: "ins_agt_1@tenant.interchange" },
    ]);
  });

  test("maps the agent body onto the step agent", () => {
    const agent = stepAgent(synthesizeFoldedWorkflow(BASE));
    expect(agent.systemPrompt).toBe("You are the folded agent.");
    expect(agent.description).toBe("A folded agent.");
    expect(agent.capabilities).toEqual([]);
    expect(agent.toolPackagePins).toEqual([
      { name: "@intx/tools-mail", version: "0.1.2" },
    ]);
    expect(agent.inference.sources).toEqual([
      { provider: "anthropic", model: "opus" },
    ]);
  });

  test("emits toolFactories as an explicit empty array", () => {
    const agent = stepAgent(synthesizeFoldedWorkflow(BASE));
    expect(agent.toolFactories).toEqual([]);
    // Must be present, not absent: a missing key hydrates to undefined and the
    // capability walk's factory loop throws on a folded definition.
    expect("toolFactories" in agent).toBe(true);
  });

  test("omits description when the agent has none", () => {
    const agent = stepAgent(
      synthesizeFoldedWorkflow({ ...BASE, description: null }),
    );
    expect("description" in agent).toBe(false);
  });

  test("carries grantRequirements onto the workflow envelope", () => {
    const grantRequirements = [
      { resource: "secret:vault", action: "use", source: "creator" as const },
    ];
    const wf = synthesizeFoldedWorkflow({ ...BASE, grantRequirements });
    expect(wf.grantRequirements).toEqual(grantRequirements);
  });

  test("raises when the agent has no system prompt", () => {
    expect(() =>
      synthesizeFoldedWorkflow({ ...BASE, systemPrompt: null }),
    ).toThrow(/no system prompt/);
  });
});
