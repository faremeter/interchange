// Source-entry builder for the onTrigger-body -> childWorkflow fixture:
// a single top-level `onTrigger` section subscribed to the deployment mail
// address, whose inline body is a `defineWorkflow` whose only step is a
// `childWorkflow` spawn of a trivial one-agent child.
//
// This is the runtime counterpart to the capability-walk grant coverage for the
// same nesting (walk test "collects a childWorkflow's grants nested inside an
// onTrigger body"). At run time the section spawns its body as a suspendable
// child per event; the body's `childWorkflow` step spawns a nested child whose
// per-step agent runs through the sidecar's `childInvokeStep` -- deliberately
// unbuilt (INTR-310) -- so the nested child fails loud rather than fabricating a
// completed run.

export type OnTriggerChildWorkflowFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The section step's key in the workflow's `steps` map. Defaults to `section`. */
  sectionId?: string;
  /** The childWorkflow spawn step's key in the body's `steps` map. Defaults to `spawn`. */
  spawnStepId?: string;
  /** The outer `defineWorkflow` id. */
  workflowId: string;
  /** The body `defineWorkflow` id. Defaults to a stable fixture-local id. */
  bodyWorkflowId?: string;
  /** The nested child `defineWorkflow` id. */
  childWorkflowId: string;
  /** The nested child's single agent step key. */
  childStepId: string;
  /** The nested child agent's `defineAgent` id. */
  childAgentId: string;
  /** The nested child agent's system prompt. */
  childSystemPrompt: string;
};

export function onTriggerChildWorkflowEntry(
  params: OnTriggerChildWorkflowFixtureParams,
): string {
  const sectionId = params.sectionId ?? "section";
  const spawnStepId = params.spawnStepId ?? "spawn";
  const bodyWorkflowId =
    params.bodyWorkflowId ?? "authored-on-trigger-childworkflow-body";

  return `
import {
  childWorkflow,
  defineWorkflow,
  onTrigger,
  step,
} from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const child = defineWorkflow({
  id: ${JSON.stringify(params.childWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(params.childStepId)}]: step({ agent: defineAgent({
      id: ${JSON.stringify(params.childAgentId)},
      systemPrompt: ${JSON.stringify(params.childSystemPrompt)},
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    }) }),
  },
});

const body = defineWorkflow({
  id: ${JSON.stringify(bodyWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(spawnStepId)}]: childWorkflow({ definition: child }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(sectionId)}]: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body,
    }),
  },
});
`;
}
