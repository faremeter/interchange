// Source-entry builder for the mail-tool two-step workflow fixture (F5):
// `send(mail-tool) -> tail`. The first step's agent carries the inline
// `mail_send` tool from the sibling `mail-tool.ts` module; the second (tail)
// step runs `after: [sendId]` with a plain, tool-less agent. The returned
// string is a `@intx/*`-importing entry module that also imports the tool by
// absolute path; `bundleWorkflowEntry` inlines both into a self-contained
// `.mjs` the sidecar evaluates in-child, so the tool runs with the same
// filesystem / transport side effect the ex-synthetic bundle produced.
//
// Parameterised by the tool variant (fs | transport | ask), the mail trigger
// address, the step ids, and the per-step system prompts so a caller pins each
// agent's inference request to the deployment it exercises and selects the tool
// behaviour it exercises.

import path from "node:path";

import type { MailToolVariant } from "./mail-tool";

// Absolute path to the sibling tool module, so the bundled entry resolves it
// through `Bun.build` (which inlines the module) rather than a bare specifier
// the sidecar closure could not resolve.
const MAIL_TOOL_MODULE = path.join(import.meta.dir, "mail-tool.ts");

export type TwoStepMailToolFixtureParams = {
  /** The `mail_send` tool variant the send step's agent carries. */
  variant: MailToolVariant;
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The send step's key in the workflow's `steps` map. Defaults to `send`. */
  sendId?: string;
  /** The tail step's key in the workflow's `steps` map. Defaults to `tail`. */
  tailId?: string;
  /** The send step agent's system prompt. */
  sendSystemPrompt: string;
  /** The tail step agent's system prompt. */
  tailSystemPrompt: string;
  /** The send agent's `defineAgent` id. Defaults to a stable fixture-local id. */
  sendAgentId?: string;
  /** The tail agent's `defineAgent` id. Defaults to a stable fixture-local id. */
  tailAgentId?: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function twoStepMailToolEntry(
  params: TwoStepMailToolFixtureParams,
): string {
  const sendId = params.sendId ?? "send";
  const tailId = params.tailId ?? "tail";
  const sendAgentId = params.sendAgentId ?? "two-step-mail-tool-send-agent";
  const tailAgentId = params.tailAgentId ?? "two-step-mail-tool-tail-agent";
  const workflowId = params.workflowId ?? "wf_two_step_mail_tool";

  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { mailSendTool } from ${JSON.stringify(MAIL_TOOL_MODULE)};

const sendAgent = defineAgent({
  id: ${JSON.stringify(sendAgentId)},
  systemPrompt: ${JSON.stringify(params.sendSystemPrompt)},
  tools: [mailSendTool(${JSON.stringify(params.variant)})],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

const tailAgent = defineAgent({
  id: ${JSON.stringify(tailAgentId)},
  systemPrompt: ${JSON.stringify(params.tailSystemPrompt)},
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
    [${JSON.stringify(sendId)}]: step({ agent: sendAgent }),
    [${JSON.stringify(tailId)}]: step({ agent: tailAgent, after: [${JSON.stringify(sendId)}] }),
  },
});
`;
}
