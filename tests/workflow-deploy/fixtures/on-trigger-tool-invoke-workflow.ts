// Source-entry builder for the TOOL-BEARING onTrigger body fixture: a single
// top-level `onTrigger` section subscribed to the deployment mail address whose
// inline body is one agent step that carries the inline `mail_send` tool.
//
// The sibling `on-trigger-body` builder renders a TOOL-LESS body agent, which
// proves only that a body agent runs. This builder renders the tool-bearing
// shape, so a deployed run drives the real `tool_use` -> execute ->
// `tool_result` round-trip inside an onTrigger body -- the property an operator
// gets when they approve the body's `tool:<name>` grant at deploy.
//
// The returned string is a `@intx/*`-importing entry module;
// `bundleWorkflowEntry` inlines it (and the sibling tool module it imports by
// absolute path) to a self-contained `.mjs` the sidecar evaluates in-child.

import path from "node:path";

import type { MailToolVariant } from "./mail-tool";

// Absolute path to the sibling tool module, so a bundled entry resolves it
// through `Bun.build` (which inlines the module) rather than a bare specifier
// the sidecar closure could not resolve.
const MAIL_TOOL_MODULE = path.join(import.meta.dir, "mail-tool.ts");

export type OnTriggerToolInvokeFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The section step's key in the outer workflow's `steps` map. */
  sectionId: string;
  /** The body agent step's key in the body workflow's `steps` map. */
  stepId: string;
  /** The body agent's `defineAgent` id. */
  agentId: string;
  /** The body agent's system prompt. */
  systemPrompt: string;
  /** The inline `mail_send` variant the body agent carries. */
  tool: MailToolVariant;
  /** The outer `defineWorkflow` id. */
  workflowId: string;
  /** The body `defineWorkflow` id. */
  bodyWorkflowId: string;
};

export function onTriggerToolInvokeEntry(
  params: OnTriggerToolInvokeFixtureParams,
): string {
  return `
import { defineWorkflow, onTrigger, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { mailSendTool } from ${JSON.stringify(MAIL_TOOL_MODULE)};

const bodyAgent = defineAgent({
  id: ${JSON.stringify(params.agentId)},
  systemPrompt: ${JSON.stringify(params.systemPrompt)},
  tools: [mailSendTool(${JSON.stringify(params.tool)})],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

const body = defineWorkflow({
  id: ${JSON.stringify(params.bodyWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(params.stepId)}]: step({ agent: bodyAgent }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(params.sectionId)}]: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body,
    }),
  },
});
`;
}
