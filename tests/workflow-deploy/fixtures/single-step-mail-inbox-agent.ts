// Source-entry builder for a single-step workflow whose agent PINS the real
// `@intx/tools-mail` bundle, so it carries the full mail toolset --
// `mail_send`, `mail_reply`, `mail_search`, `mail_read`, `mail_wait`. Unlike
// the `single-step-mail-tool` fixture (whose inline module ships only a
// `mail_send` variant), this imports the production sidecar bundle so the
// deployed agent exercises the real inbound read/search/wait handlers against
// the supervisor-backed transport.
//
// The bundle resolves `mail.transport` from `env.capabilities` (the host
// assembles that bag in the sidecar step-env builder), so the agent's inbound
// mail tools read the deployment's committed substrate INBOX once the sidecar
// wires the transport's inbound surface. `bundleWorkflowEntry` inlines the
// bundle into the self-contained `.mjs` the sidecar evaluates in-child.
//
// Parameterised by the step id, the agent's system prompt, the mail trigger
// address, and the agent/workflow ids so a caller pins the run's address and
// keeps multiple deployments in one sidecar distinct.

export type SingleStepMailInboxFixtureParams = {
  /** The step's key in the workflow's `steps` map. Defaults to `step1`. */
  stepId?: string;
  /** The agent's system prompt. */
  systemPrompt: string;
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineAgent` id. Defaults to a stable fixture-local id. */
  agentId?: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
  /**
   * The step's trigger budget (see `StepPrimitive.triggers`). Omitted leaves
   * the step at its `step()` default of `1` (batch: single run, terminal after
   * the first turn). `"unbounded"` makes the step re-arm after every turn so a
   * second inbound mail is dispatched as the next turn on the SAME stable run
   * -- the interactive conversational path.
   */
  triggers?: number | "unbounded";
};

export function singleStepMailInboxEntry(
  params: SingleStepMailInboxFixtureParams,
): string {
  const stepId = params.stepId ?? "step1";
  const agentId = params.agentId ?? "single-step-mail-inbox-agent";
  const workflowId = params.workflowId ?? "wf_single_step_mail_inbox";
  const triggersClause =
    params.triggers !== undefined
      ? `, triggers: ${JSON.stringify(params.triggers)}`
      : "";
  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { mail } from "@intx/tools-mail/sidecar-bundle";

const agent = defineAgent({
  id: ${JSON.stringify(agentId)},
  systemPrompt: ${JSON.stringify(params.systemPrompt)},
  tools: [mail],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(stepId)}]: step({ agent${triggersClause} }),
  },
});
`;
}
