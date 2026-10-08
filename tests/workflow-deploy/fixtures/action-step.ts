// Source-entry builder for the single action-step workflow fixture (F9):
// one mail-triggered `action({ handler, effect: { requires: [...] } })` step.
// The deploy-time capability walk lifts each `requires` entry into a runtime
// `effect:<name>` grant. The entry exports the handler (named by the handler
// ref) so the deployment can point `interchange.actions` at it; the handler is
// a no-op that declares the effect but does not perform it.

export type ActionStepFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The action step's key in the workflow's `steps` map. Defaults to `act`. */
  stepId?: string;
  /** The action's handler ref. Defaults to `writer`. */
  handler?: string;
  /** The effect names the action requires. Defaults to `["fs:write"]`. */
  requires?: readonly string[];
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function actionStepEntry(params: ActionStepFixtureParams): string {
  const stepId = params.stepId ?? "act";
  const handler = params.handler ?? "writer";
  const requires = params.requires ?? ["fs:write"];
  const workflowId = params.workflowId ?? "wf_action_step";

  return `
import { action, defineWorkflow } from "@intx/workflow/definition";

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(stepId)}]: action({
      handler: ${JSON.stringify(handler)},
      effect: { requires: ${JSON.stringify(requires)} },
    }),
  },
});

// The action handler, resolved by export name via interchange.actions. It
// declares the effect requirement above but does not perform it, so it needs no
// effect grant of its own; it exercises the resolve + invoke path.
export async function ${handler}(input, _ctx, _signal) {
  return { handled: ${JSON.stringify(handler)}, input };
}
`;
}
