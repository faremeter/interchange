// Source-entry builder for the unresolvable-director workflow fixture (F11):
// one mail-triggered `step({ agent })` whose agent declares a `director` ref
// that neither the sidecar's built-in registry nor any closure package can
// resolve. The returned string is a `@intx/*`-importing entry module;
// `bundleWorkflowEntry` inlines it to a self-contained `.mjs` the sidecar
// evaluates in-child.
//
// The install/approve probe walks the definition's capabilities in the sidecar
// and fails closed on the unresolved director, so a deploy of this fixture is
// expected to REJECT with an `unresolvable director: <id>` error before any
// definition is frozen or deployed.
//
// Parameterised by the mail trigger address, the step id, and the bogus
// director ref so a caller pins the run's address and the ref it asserts on.

export type UnresolvableDirectorFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The step's key in the workflow's `steps` map. Defaults to `step1`. */
  stepId?: string;
  /** The bogus `director` ref the registry cannot resolve. */
  directorId: string;
  /** The agent's system prompt. Defaults to a stable fixture-local prompt. */
  systemPrompt?: string;
  /** The `defineAgent` id. Defaults to a stable fixture-local id. */
  agentId?: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function unresolvableDirectorEntry(
  params: UnresolvableDirectorFixtureParams,
): string {
  const stepId = params.stepId ?? "step1";
  const agentId = params.agentId ?? "unresolvable-director-agent";
  const workflowId = params.workflowId ?? "wf_unresolvable_director";
  const systemPrompt =
    params.systemPrompt ?? "You are an integration test agent.";

  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: ${JSON.stringify(agentId)},
  systemPrompt: ${JSON.stringify(systemPrompt)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
  director: { id: ${JSON.stringify(params.directorId)}, config: {} },
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
