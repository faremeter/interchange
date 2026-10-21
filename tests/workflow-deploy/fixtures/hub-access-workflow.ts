// Source-entry builder for a workflow whose one action calls the hub REST API
// as its run. The action declares the reserved hub handle, so deploy binds it
// to the run's token without a `credentialBindings` entry. Inside `perform`
// the handler resolves the handle and fetches each path on the hub's pinned
// origin, one effect per path. It returns the status of each response, the
// `id` field of each JSON body, and the message of each error body, so the
// test can tell which check refused a call.
//
// The entry module exports the handler next to `workflow`. The deployment
// points `interchange.actions` at this same module.

import type { GrantRequirement } from "@intx/types";

/** The handler, so the test can read its step output. */
export const HUB_CALLER_HANDLER = "callHub";

/** The effect capability the handler performs each call under. */
export const CALL_CAPABILITY = "call";

/** The outcome of one hub call, as the handler returns it. */
export type HubCallOutcome = {
  status: number;
  id: string | null;
  message: string | null;
};

export type HubAccessWorkflowFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id. */
  workflowId: string;
  /** The hub paths the handler calls, by the name its output reports them. */
  calls: Readonly<Record<string, string>>;
  /** The workflow's grant requirements; none when omitted. */
  grantRequirements?: readonly GrantRequirement[];
};

export function hubAccessWorkflowEntry(
  params: HubAccessWorkflowFixtureParams,
): string {
  const requirementsField =
    params.grantRequirements === undefined
      ? ""
      : `
  grantRequirements: ${JSON.stringify(params.grantRequirements)},`;
  return `
import { action, defineWorkflow } from "@intx/workflow/definition";

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },${requirementsField}
  steps: {
    [${JSON.stringify(HUB_CALLER_HANDLER)}]: action({
      handler: ${JSON.stringify(HUB_CALLER_HANDLER)},
      credentials: ["interchange.hub"],
      effect: { requires: [${JSON.stringify(CALL_CAPABILITY)}] },
    }),
  },
});

const CALLS = ${JSON.stringify(params.calls)};

export async function ${HUB_CALLER_HANDLER}(_input, ctx, _signal) {
  const outcomes = {};
  for (const [name, path] of Object.entries(CALLS)) {
    outcomes[name] = await ctx.perform({
      effectId: "hub-" + name,
      capability: ${JSON.stringify(CALL_CAPABILITY)},
      run: async ({ credentials }) => {
        const hub = await credentials.resolve("interchange.hub");
        if (hub.kind !== "http") {
          throw new Error("expected an http mediated credential, got " + hub.kind);
        }
        const response = await hub.fetch(path);
        const body = await response.json().catch(() => null);
        const record = body !== null && typeof body === "object" ? body : {};
        const error =
          record.error !== null && typeof record.error === "object" ? record.error : {};
        return {
          status: response.status,
          id: typeof record.id === "string" ? record.id : null,
          message: typeof error.message === "string" ? error.message : null,
        };
      },
    });
  }
  return outcomes;
}
`;
}
