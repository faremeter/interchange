// Source-entry builder for the mail-tool single-step workflow fixture (F4):
// one mail-triggered `step({ agent })` whose agent carries the inline
// `mail_send` tool from the sibling `mail-tool.ts` module. The returned string
// is a `@intx/*`-importing entry module that also imports the tool by absolute
// path; `bundleWorkflowEntry` inlines both into a self-contained `.mjs` the
// sidecar evaluates in-child, so the tool runs with the same filesystem /
// transport side effect the ex-synthetic bundle produced.
//
// Parameterised by the tool variant (fs | transport | ask), the step's system
// prompt, the mail trigger address, and the agent/workflow ids so a caller
// pins the run's address and selects the tool behaviour it exercises.

import path from "node:path";

import type { MailToolVariant } from "./mail-tool";

// Absolute path to the sibling tool module, so the bundled entry resolves it
// through `Bun.build` (which inlines the module) rather than a bare specifier
// the sidecar closure could not resolve.
const MAIL_TOOL_MODULE = path.join(import.meta.dir, "mail-tool.ts");

export type SingleStepMailToolFixtureParams = {
  /** The `mail_send` tool variant the step agent carries. */
  variant: MailToolVariant;
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
};

export function singleStepMailToolEntry(
  params: SingleStepMailToolFixtureParams,
): string {
  const stepId = params.stepId ?? "step1";
  const agentId = params.agentId ?? "single-step-mail-tool-agent";
  const workflowId = params.workflowId ?? "wf_single_step_mail_tool";
  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { mailSendTool } from ${JSON.stringify(MAIL_TOOL_MODULE)};

const agent = defineAgent({
  id: ${JSON.stringify(agentId)},
  systemPrompt: ${JSON.stringify(params.systemPrompt)},
  tools: [mailSendTool(${JSON.stringify(params.variant)})],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(stepId)}]: step({ agent }),
  },
});
`;
}
