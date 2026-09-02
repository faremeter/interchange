// Source-entry builder for an onFailure routing workflow: a mail-triggered
// `action` whose handler throws, carrying `onFailure` to a handler action. The
// deployed child resolves each handler from the closure's interchange.actions
// module; the throwing handler makes the unit fail permanently, so the runtime
// routes to the onFailure handler instead of failing the run. The normal
// dependent is pruned on the failure path.
//
// No handler declares an `effect`, so the deploy needs no effect grant -- it
// exercises the resolve + invoke + route path, not grant derivation.

export type OnFailureActionFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function onFailureActionWorkflowEntry(
  params: OnFailureActionFixtureParams,
): string {
  const workflowId = params.workflowId ?? "wf_onfailure_action";

  return `
import { action, defineWorkflow } from "@intx/workflow/definition";

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    unit: action({ handler: "boom", onFailure: "rescue" }),
    rescue: action({ handler: "noop", after: ["unit"] }),
    normal: action({ handler: "noop", after: ["unit"] }),
  },
});

// The failing unit's handler: it throws, so the action fails permanently and
// routes to the onFailure handler rather than failing the run.
export async function boom() {
  throw new Error("action boom");
}

// The onFailure handler and the (pruned) normal dependent: no-op successes.
export async function noop(input) {
  return { ok: true, input };
}
`;
}
