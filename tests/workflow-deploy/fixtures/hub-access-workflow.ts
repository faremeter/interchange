// Source-entry builders for workflows whose actions call the hub REST API as
// their run: one with a top-level action, one with the actions inside a
// `childWorkflow` body and a loop body. Each action declares the reserved hub
// handle, so deploy binds it to the run's token without a
// `credentialBindings` entry. Inside `perform` the handler resolves the handle
// and fetches each path on the hub's pinned origin, one effect per path. It
// returns the status of each response, the `id` field of each JSON body, and
// the message of each error body, so the test can tell which check refused a
// call.
//
// The entry module exports the handlers next to `workflow`. The deployment
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
  /** The duration (ms) of a `nap` sleep the hub caller runs after. */
  napMs?: number;
};

export function hubAccessWorkflowEntry(
  params: HubAccessWorkflowFixtureParams,
): string {
  const requirementsField =
    params.grantRequirements === undefined
      ? ""
      : `
  grantRequirements: ${JSON.stringify(params.grantRequirements)},`;
  const napStep =
    params.napMs === undefined
      ? ""
      : `
    nap: sleep({ duration: ${JSON.stringify(params.napMs)} }),`;
  const afterNap =
    params.napMs === undefined
      ? ""
      : `
      after: ["nap"],`;
  return `
import { action, defineWorkflow, sleep } from "@intx/workflow/definition";

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },${requirementsField}
  steps: {${napStep}
    [${JSON.stringify(HUB_CALLER_HANDLER)}]: action({${afterNap}
      handler: ${JSON.stringify(HUB_CALLER_HANDLER)},
      credentials: ["interchange.hub"],
      effect: { requires: [${JSON.stringify(CALL_CAPABILITY)}] },
    }),
  },
});

${hubCallerSource(HUB_CALLER_HANDLER, params.calls)}`;
}

// The source of an action handler that calls each path on the hub through the
// reserved handle and returns the outcome of each call.
function hubCallerSource(
  handler: string,
  calls: Readonly<Record<string, string>>,
): string {
  return `
export async function ${handler}(_input, ctx, _signal) {
  const outcomes = {};
  for (const [name, path] of Object.entries(${JSON.stringify(calls)})) {
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

/** The handler of the action in the `childWorkflow` body. */
export const CHILD_HUB_HANDLER = "callHubFromChild";

/** The handler of the action in the loop body. */
export const LOOP_HUB_HANDLER = "callHubFromLoop";

/** The loop step, so the test can derive its body run id. */
export const HUB_LOOP_STEP_ID = "repeat";

/**
 * Source entry for a workflow whose hub callers run only inside bodies: one in
 * an inline `childWorkflow` body, one in a loop body that runs once. Both
 * bodies act as the top-level run's principal.
 */
export function hubAccessBodiesWorkflowEntry(
  params: Omit<HubAccessWorkflowFixtureParams, "calls" | "napMs"> & {
    /** The hub paths the `childWorkflow` body action calls. */
    childCalls: Readonly<Record<string, string>>;
    /** The hub paths the loop body action calls. */
    loopCalls: Readonly<Record<string, string>>;
  },
): string {
  const requirementsField =
    params.grantRequirements === undefined
      ? ""
      : `
  grantRequirements: ${JSON.stringify(params.grantRequirements)},`;
  const caller = (handler: string) => `action({
      handler: ${JSON.stringify(handler)},
      credentials: ["interchange.hub"],
      effect: { requires: [${JSON.stringify(CALL_CAPABILITY)}] },
    })`;
  return `
import { action, childWorkflow, defineWorkflow, loop } from "@intx/workflow/definition";

const child = defineWorkflow({
  id: ${JSON.stringify(`${params.workflowId}_child`)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(CHILD_HUB_HANDLER)}]: ${caller(CHILD_HUB_HANDLER)},
  },
});

const loopBody = defineWorkflow({
  id: ${JSON.stringify(`${params.workflowId}_loop_body`)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(LOOP_HUB_HANDLER)}]: ${caller(LOOP_HUB_HANDLER)},
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },${requirementsField}
  steps: {
    spawn: childWorkflow({ definition: child }),
    [${JSON.stringify(HUB_LOOP_STEP_ID)}]: loop({
      body: loopBody,
      while: "stopAfterOne",
      carry: "keepInput",
      maxIterations: 2,
      onExhausted: "loopDone",
    }),
    loopDone: action({ handler: "loopDone", after: [${JSON.stringify(HUB_LOOP_STEP_ID)}] }),
  },
});
${hubCallerSource(CHILD_HUB_HANDLER, params.childCalls)}${hubCallerSource(LOOP_HUB_HANDLER, params.loopCalls)}
export function stopAfterOne() {
  return false;
}

export function keepInput(_childOutput, currentInput) {
  return currentInput;
}

export async function loopDone() {
  return null;
}
`;
}
