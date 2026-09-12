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

import type { InboundMailPolicy } from "@intx/types/runtime";

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
  /**
   * The agent's single inference source provider. Defaults to `anthropic`
   * (the built-in the mock inference server speaks). A caller that exercises
   * an operator-supplied custom adapter overrides it with the manifest's
   * provider id so the deployed definition pins that provider.
   */
  provider?: string;
  /**
   * The workflow's authored inbound-mail admission policy. Omitted by default,
   * so the deployed definition carries no `inboundMailPolicy` and the recipient
   * resolves to the secure default (reject every non-`clean` outcome). A caller
   * that exercises the enforcement seam sets it to relax a specific outcome
   * (for example `{ missing: "admit" }`) so an otherwise-rejected message is
   * admitted for that deployment.
   */
  inboundMailPolicy?: InboundMailPolicy;
};

export function singleStepAgentEntry(
  params: SingleStepAgentFixtureParams,
): string {
  const agentId = params.agentId ?? "single-step-agent";
  const workflowId = params.workflowId ?? "wf_single_step_agent";
  const provider = params.provider ?? "anthropic";
  const inboundMailPolicyField =
    params.inboundMailPolicy !== undefined
      ? `\n  inboundMailPolicy: ${JSON.stringify(params.inboundMailPolicy)},`
      : "";
  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: ${JSON.stringify(agentId)},
  systemPrompt: ${JSON.stringify(params.systemPrompt)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: ${JSON.stringify(provider)}, model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },${inboundMailPolicyField}
  steps: {
    [${JSON.stringify(params.stepId)}]: step({ agent }),
  },
});
`;
}
