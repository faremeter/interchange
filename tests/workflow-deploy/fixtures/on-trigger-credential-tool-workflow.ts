// Source-entry builder for a credential-consuming onTrigger body. The outer
// workflow is one mail-subscribed section. Its inline body is one agent step
// carrying the credential probe from the sibling bundle. The binding sits on
// the outer definition: deploy reads `credentialBindings` off the root
// workflow, so a binding that exists only on the inner workflow is not
// delivered.

import path from "node:path";

import { CREDENTIAL_BINDING_PACKAGE } from "./credential-tool-workflow";

const CREDENTIAL_TOOL_MODULE = path.join(
  import.meta.dir,
  "credential-tool-bundle.ts",
);

export type OnTriggerCredentialToolBinding = {
  handle: string;
  provider: string;
  name: string;
};

export type OnTriggerCredentialToolFixtureParams = {
  address: string;
  sectionId: string;
  stepId: string;
  agentId: string;
  systemPrompt: string;
  workflowId: string;
  bodyWorkflowId: string;
  binding: OnTriggerCredentialToolBinding;
};

export function onTriggerCredentialToolEntry(
  params: OnTriggerCredentialToolFixtureParams,
): string {
  const binding = params.binding;
  return `
import { defineWorkflow, onTrigger, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { credentialProbe } from ${JSON.stringify(CREDENTIAL_TOOL_MODULE)};

const bodyAgent = defineAgent({
  id: ${JSON.stringify(params.agentId)},
  systemPrompt: ${JSON.stringify(params.systemPrompt)},
  tools: [credentialProbe],
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
  credentialBindings: [
    {
      package: ${JSON.stringify(CREDENTIAL_BINDING_PACKAGE)},
      handle: ${JSON.stringify(binding.handle)},
      provider: ${JSON.stringify(binding.provider)},
      name: ${JSON.stringify(binding.name)},
      locator: "tenant",
    },
  ],
  steps: {
    [${JSON.stringify(params.sectionId)}]: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body,
    }),
  },
});
`;
}
