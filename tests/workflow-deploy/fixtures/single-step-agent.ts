// Source-entry builder for the tool-less single-step workflow fixture (F1):
// one mail-triggered `step({ agent })` whose agent carries no tools. The
// returned string is a `@intx/*`-importing entry module; `bundleWorkflowEntry`
// inlines it to a self-contained `.mjs` the sidecar evaluates in-child.
//
// Parameterised by step id, system prompt, and the mail trigger address so a
// caller pins the run's address to the deployment it is exercising. The agent
// and workflow ids default to stable fixture-local values; a caller that
// deploys more than one instance in a single sidecar overrides them to keep the
// definitions distinct.

export type SingleStepAgentFixtureParams = {
  /** The step's key in the workflow's `steps` map. */
  stepId: string;
  /** The agent's system prompt. */
  systemPrompt: string;
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineAgent` id. Defaults to a stable fixture-local id. */
  agentId?: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function singleStepAgentEntry(
  params: SingleStepAgentFixtureParams,
): string {
  const agentId = params.agentId ?? "single-step-agent";
  const workflowId = params.workflowId ?? "wf_single_step_agent";
  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: ${JSON.stringify(agentId)},
  systemPrompt: ${JSON.stringify(params.systemPrompt)},
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
    [${JSON.stringify(params.stepId)}]: step({ agent }),
  },
});
`;
}
