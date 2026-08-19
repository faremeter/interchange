// Source-entry builder for the plain two-step workflow fixture (F3):
// `step1 -> step2` with no gate between them, each step's agent carrying no
// tools. The returned string is a `@intx/*`-importing entry module;
// `bundleWorkflowEntry` inlines it to a self-contained `.mjs` the sidecar
// evaluates in-child.
//
// Parameterised by the mail trigger address, the step ids, and the per-step
// system prompts so a caller pins each agent's inference request to the
// deployment it exercises. The second step declares `after: [step1Id]`, so the
// runtime serialises the two steps and the FIFO mail-dispatch invariant the
// multi-step path pins is exercised end to end.

export type TwoStepFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The first step's key in the workflow's `steps` map. Defaults to `step1`. */
  step1Id?: string;
  /** The second step's key in the workflow's `steps` map. Defaults to `step2`. */
  step2Id?: string;
  /** The first step agent's system prompt. */
  systemPrompt1: string;
  /** The second step agent's system prompt. */
  systemPrompt2: string;
  /** The first agent's `defineAgent` id. Defaults to a stable fixture-local id. */
  agentId1?: string;
  /** The second agent's `defineAgent` id. Defaults to a stable fixture-local id. */
  agentId2?: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function twoStepEntry(params: TwoStepFixtureParams): string {
  const step1Id = params.step1Id ?? "step1";
  const step2Id = params.step2Id ?? "step2";
  const agentId1 = params.agentId1 ?? "two-step-agent1";
  const agentId2 = params.agentId2 ?? "two-step-agent2";
  const workflowId = params.workflowId ?? "wf_two_step";

  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent1 = defineAgent({
  id: ${JSON.stringify(agentId1)},
  systemPrompt: ${JSON.stringify(params.systemPrompt1)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

const agent2 = defineAgent({
  id: ${JSON.stringify(agentId2)},
  systemPrompt: ${JSON.stringify(params.systemPrompt2)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(step1Id)}]: step({ agent: agent1 }),
    [${JSON.stringify(step2Id)}]: step({ agent: agent2, after: [${JSON.stringify(step1Id)}] }),
  },
});
`;
}
