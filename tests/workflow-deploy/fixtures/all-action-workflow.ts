// Source-entry builder for a workflow made only of deterministic primitives:
// a mail trigger and two dependent `action` steps. It carries no agent
// anywhere, which is the point.
//
// Every other deploy fixture in this suite includes at least one agent step,
// several of them explicitly so the deploy acquires an operator-approved
// inference source for its non-agent steps to pin. A definition with no agent
// advertises no `inference.source:` grant at all, so under `approve-probed`
// there is nothing that could approve the default source. That is the shape
// this fixture exists to deploy.

export type AllActionWorkflowFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function allActionWorkflowEntry(
  params: AllActionWorkflowFixtureParams,
): string {
  const workflowId = params.workflowId ?? "wf_all_action_fixture";
  return `
import { defineWorkflow, action } from "@intx/workflow/definition";

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    gather: action({ handler: "gather" }),
    persist: action({ handler: "persist", after: ["gather"] }),
  },
});

export async function gather(input, _ctx, _signal) {
  return { ok: true, input };
}

export async function persist(input, _ctx, _signal) {
  return { persisted: true, input };
}
`;
}
