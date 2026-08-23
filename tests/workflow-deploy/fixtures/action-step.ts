// Source-entry builder for the single action-step workflow fixture (F9):
// one mail-triggered `action({ handler, effect: { requires: [...] } })` step.
// The returned string is a `@intx/*`-importing entry module;
// `bundleWorkflowEntry` inlines it to a self-contained `.mjs` the sidecar
// evaluates in-child.
//
// The action step declares an effect requirement; the deploy-time capability
// walk lifts each `requires` entry into a runtime `effect:<name>` grant, which
// the trigger route materializes onto the run principal. The entry ALSO exports
// the handler (named by the handler ref) so the deployment can point
// `interchange.actions` at it: the child host resolves every action handler at
// establish, so a declared-but-unresolvable handler fails closed. The handler
// itself is a no-op that returns a value -- it declares the effect but does not
// perform it, which is enough to exercise deploy-time grant derivation.
//
// Parameterised by the mail trigger address, the step id, the handler ref, and
// the effect requirements so a caller pins the run's address and the effect
// grant it exercises.

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
