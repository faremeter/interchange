// Source-entry builder for the credential-consuming single-step workflow: one
// mail-triggered `step({ agent })` whose agent carries the inline credential
// probe tool from the sibling `credential-tool-bundle.ts` module, plus a
// `credentialBindings` entry that binds the probe's declared handle to a
// tenant-owned credential. The returned string is a `@intx/*`-importing entry
// module that also imports the tool by absolute path; `bundleWorkflowEntry`
// inlines both into a self-contained `.mjs` the sidecar evaluates in-child, so
// the probe runs from the workflow's own source closure rather than a pinned
// tarball.
//
// The binding's `package` is the probe factory's `id` (`BUNDLE_ID`). The
// source-ref arm keys a source tool's synthetic `StepToolFactory.packageName`
// to the factory `id`, and a credential is keyed to its consuming tool by
// `toolConsumer(packageName)` on both the delivery descriptor and the Gate-2
// grant. So the author names `credentialBindings[].package` = the factory id,
// and the delivered credential's consumer matches the source tool at run time.

import path from "node:path";

import { BUNDLE_ID } from "./credential-tool-bundle";

// Absolute path to the sibling tool module, so the bundled entry resolves it
// through `Bun.build` (which inlines the module) rather than a bare specifier
// the sidecar closure could not resolve.
const CREDENTIAL_TOOL_MODULE = path.join(
  import.meta.dir,
  "credential-tool-bundle.ts",
);

/**
 * The credential binding's `package`, which MUST equal the probe factory's
 * `id`: the source-ref arm sets the synthetic tool's `packageName` to the
 * factory id, and a credential's consumer is `toolConsumer(packageName)`. A
 * mismatch would deliver the credential to a consumer the source tool does not
 * key on, so the probe's `resolve` would find no binding.
 */
export const CREDENTIAL_BINDING_PACKAGE = BUNDLE_ID;

/**
 * The credential binding the workflow declares, when it declares one. Omitting
 * it produces a workflow whose probe tool takes its credential ONLY over the
 * live `credentials.update` channel (no deploy-frame delivery), so a test can
 * exercise the channel-only delivery path against a source tool.
 */
export type CredentialToolWorkflowBinding = {
  /** The credential handle the binding binds (the handle the probe resolves). */
  handle: string;
  /** The provider name the binding resolves the credential against. */
  provider: string;
  /** The credential name the binding resolves. */
  name: string;
};

export type CredentialToolWorkflowFixtureParams = {
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
   * The credential binding to declare. When omitted, the workflow declares no
   * `credentialBindings`: the deploy carries no credential material, and the
   * probe's credential must arrive over the live `credentials.update` channel.
   */
  binding?: CredentialToolWorkflowBinding;
};

export function credentialToolWorkflowEntry(
  params: CredentialToolWorkflowFixtureParams,
): string {
  const stepId = params.stepId ?? "step1";
  const agentId = params.agentId ?? "credential-tool-workflow-agent";
  const workflowId = params.workflowId ?? "wf_credential_tool_workflow";
  const binding = params.binding;
  const credentialBindingsField =
    binding === undefined
      ? ""
      : `
  credentialBindings: [
    {
      package: ${JSON.stringify(CREDENTIAL_BINDING_PACKAGE)},
      handle: ${JSON.stringify(binding.handle)},
      provider: ${JSON.stringify(binding.provider)},
      name: ${JSON.stringify(binding.name)},
      locator: "tenant",
    },
  ],`;
  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { credentialProbe } from ${JSON.stringify(CREDENTIAL_TOOL_MODULE)};

const agent = defineAgent({
  id: ${JSON.stringify(agentId)},
  systemPrompt: ${JSON.stringify(params.systemPrompt)},
  tools: [credentialProbe],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },${credentialBindingsField}
  steps: {
    [${JSON.stringify(stepId)}]: step({ agent }),
  },
});
`;
}
