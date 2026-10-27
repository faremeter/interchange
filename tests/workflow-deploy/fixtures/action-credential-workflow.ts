// Source-entry builder for a workflow whose action handlers use mediated
// credentials. The workflow runs three steps on one mail trigger:
//
//   - `push`: a top-level action that declares the credential handle, resolves
//     it through the credentials `perform` gives its `run`, and fetches a path
//     on the credential's pinned origin.
//   - `spawn`: a childWorkflow whose body runs `pushChild`, an action that does
//     the same from inside the child run.
//   - `rework`: a top-level loop whose body runs `pushLoop` once. A loop body
//     runs as a spawned child, so it takes its grants through the child cap.
//   - `release`: a top-level action that declares no credentials and tries to
//     resolve the same handle inside `perform`. No binding names its handler,
//     so the resolve fails. The handler returns the refusal message as its
//     output, so the run completes and the test can read the message.
//
// With a `gate`, a top-level `sleep` runs first and every other root step runs
// after it, so no handler resolves the credential until the timer fires.
//
// The entry module exports the handlers next to `workflow`. The deployment
// points `interchange.actions` and `interchange.loops` at this same module.

/** The credential handle every declaring action resolves. */
export const ACTION_CREDENTIAL_HANDLE = "origin-token";

/** The handlers that declare the handle, so the test can name their consumers. */
export const TOP_HANDLER = "push";
export const CHILD_HANDLER = "pushChild";
export const LOOP_HANDLER = "pushLoop";
export const SCOPED_OUT_HANDLER = "release";

/** The origin path each handler fetches. */
export const TOP_PATH = "/push";
export const CHILD_PATH = "/child";
export const LOOP_PATH = "/loop";
export const SCOPED_OUT_PATH = "/release";

/** The top-level loop step id, so the test can name its body run. */
export const LOOP_STEP_ID = "rework";

/** The effect capability the handlers perform their fetch under. */
export const CALL_CAPABILITY = "call";

export type ActionCredentialBinding = {
  /** The provider name the binding resolves the credential against. */
  provider: string;
  /** The credential name the binding resolves. */
  name: string;
};

export type ActionCredentialWorkflowFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The outer `defineWorkflow` id. */
  workflowId: string;
  /** The childWorkflow body `defineWorkflow` id. */
  childWorkflowId: string;
  /**
   * The credential each declaring handler is bound to. When omitted, the
   * workflow declares no `credentialBindings`, so its declared handles are
   * unbound and deploy refuses it.
   */
  binding?: ActionCredentialBinding;
  /** A leading `sleep` that every other root step runs after. */
  gate?: { stepId: string; durationMs: number };
};

export function actionCredentialWorkflowEntry(
  params: ActionCredentialWorkflowFixtureParams,
): string {
  const binding = params.binding;
  const bindingFor = (handler: string) =>
    binding === undefined
      ? ""
      : `
    {
      action: ${JSON.stringify(handler)},
      handle: ${JSON.stringify(ACTION_CREDENTIAL_HANDLE)},
      provider: ${JSON.stringify(binding.provider)},
      name: ${JSON.stringify(binding.name)},
      locator: "tenant",
    },`;
  const credentialBindingsField =
    binding === undefined
      ? ""
      : `
  credentialBindings: [${bindingFor(TOP_HANDLER)}${bindingFor(CHILD_HANDLER)}${bindingFor(LOOP_HANDLER)}
  ],`;
  const gate = params.gate;
  // Only the root steps carry `after`; a body's single step has no step to
  // wait on.
  const afterGate =
    gate === undefined ? "" : `after: [${JSON.stringify(gate.stepId)}], `;
  const gateStep =
    gate === undefined
      ? ""
      : `
    [${JSON.stringify(gate.stepId)}]: sleep({ duration: ${JSON.stringify(gate.durationMs)} }),`;
  const declaring = (handler: string, after = "") => `action({
      ${after}handler: ${JSON.stringify(handler)},
      credentials: [${JSON.stringify(ACTION_CREDENTIAL_HANDLE)}],
      effect: { requires: [${JSON.stringify(CALL_CAPABILITY)}] },
    })`;
  return `
import { action, childWorkflow, defineWorkflow, loop, sleep } from "@intx/workflow/definition";

const child = defineWorkflow({
  id: ${JSON.stringify(params.childWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(CHILD_HANDLER)}]: ${declaring(CHILD_HANDLER)},
  },
});

const loopBody = defineWorkflow({
  id: ${JSON.stringify(`${params.workflowId}_loop_body`)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(LOOP_HANDLER)}]: ${declaring(LOOP_HANDLER)},
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },${credentialBindingsField}
  steps: {${gateStep}
    [${JSON.stringify(TOP_HANDLER)}]: ${declaring(TOP_HANDLER, afterGate)},
    spawn: childWorkflow({ ${afterGate}definition: child }),
    [${JSON.stringify(LOOP_STEP_ID)}]: loop({
      ${afterGate}body: loopBody,
      while: "stopAfterOne",
      carry: "keepInput",
      maxIterations: 2,
      onExhausted: "loopDone",
    }),
    loopDone: action({ handler: "loopDone", after: [${JSON.stringify(LOOP_STEP_ID)}] }),
    [${JSON.stringify(SCOPED_OUT_HANDLER)}]: action({
      ${afterGate}handler: ${JSON.stringify(SCOPED_OUT_HANDLER)},
      effect: { requires: [${JSON.stringify(CALL_CAPABILITY)}] },
    }),
  },
});

async function fetchThroughHandle(ctx, effectId, path) {
  return ctx.perform({
    effectId,
    capability: ${JSON.stringify(CALL_CAPABILITY)},
    run: async ({ credentials }) => {
      const mediated = await credentials.resolve(${JSON.stringify(ACTION_CREDENTIAL_HANDLE)});
      if (mediated.kind !== "http") {
        throw new Error("expected an http mediated credential, got " + mediated.kind);
      }
      const response = await mediated.fetch(path);
      return { status: response.status, body: await response.json() };
    },
  });
}

export async function ${TOP_HANDLER}(_input, ctx, _signal) {
  return fetchThroughHandle(ctx, "push-call", ${JSON.stringify(TOP_PATH)});
}

export async function ${CHILD_HANDLER}(_input, ctx, _signal) {
  return fetchThroughHandle(ctx, "child-call", ${JSON.stringify(CHILD_PATH)});
}

export async function ${LOOP_HANDLER}(_input, ctx, _signal) {
  return fetchThroughHandle(ctx, "loop-call", ${JSON.stringify(LOOP_PATH)});
}

export function stopAfterOne() {
  return false;
}

export function keepInput(_childOutput, currentInput) {
  return currentInput;
}

export async function loopDone() {
  return null;
}

export async function ${SCOPED_OUT_HANDLER}(_input, ctx, _signal) {
  try {
    await ctx.perform({
      effectId: "release-call",
      capability: ${JSON.stringify(CALL_CAPABILITY)},
      run: async ({ credentials }) => {
        const mediated = await credentials.resolve(${JSON.stringify(ACTION_CREDENTIAL_HANDLE)});
        await mediated.fetch(${JSON.stringify(SCOPED_OUT_PATH)});
        return null;
      },
    });
    return { refused: null };
  } catch (err) {
    return { refused: err instanceof Error ? err.message : String(err) };
  }
}
`;
}
