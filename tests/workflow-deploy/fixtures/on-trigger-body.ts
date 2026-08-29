// Source-entry builder for the onTrigger section workflow fixture (F7):
// a single top-level `onTrigger` section subscribed to the deployment mail
// address, whose inline body is one of three shapes. The returned string is a
// `@intx/*`-importing entry module; `bundleWorkflowEntry` inlines it to a
// self-contained `.mjs` the sidecar evaluates in-child.
//
// The section spawns its body as a child run per event; the body variant
// selects what that child does:
//   - "agent":       a single tool-less agent step, exercising the body-agent
//                    invoker (the model reply commits as the step output).
//   - "awaitSignal": a single `awaitSignal` gate, exercising the signal-relay
//                    capability. An optional `timeout` adds an `onTimeout`
//                    route to a completing `sleep` step.
//   - "sleep":       a single `sleep` step. A short duration completes on its
//                    own so the section re-arms for the next event (the
//                    between-events recovery shape); a long duration holds the
//                    body mid-step (the container awaits the body terminal), the
//                    drain-teardown shape.
//
// Parameterised by the mail trigger address, the section id, and the body
// variant so a caller pins the run's address and selects the body it exercises.

export type OnTriggerAgentBody = {
  variant: "agent";
  /** The body agent step's key. Defaults to `work`. */
  stepId?: string;
  /** The body agent's system prompt. */
  systemPrompt: string;
  /** The body agent's `defineAgent` id. Defaults to a stable fixture-local id. */
  agentId?: string;
};

export type OnTriggerAwaitSignalBody = {
  variant: "awaitSignal";
  /** The awaited signal's name. */
  signalName: string;
  /** The gate step's key. Defaults to `gate`. */
  gateId?: string;
  /**
   * When set, the gate carries this timeout (ms) and routes `onTimeout` to a
   * completing `sleep` step rather than parking forever.
   */
  timeout?: number;
  /** The onTimeout recover step's key. Defaults to `recover`. */
  recoverId?: string;
  /** The recover sleep's duration (ms). Defaults to 10. */
  recoverDuration?: number;
};

export type OnTriggerSleepBody = {
  variant: "sleep";
  /** The body sleep step's key. Defaults to `wait`. */
  stepId?: string;
  /** The sleep's duration (ms). */
  duration: number;
};

export type OnTriggerBodyVariant =
  | OnTriggerAgentBody
  | OnTriggerAwaitSignalBody
  | OnTriggerSleepBody;

export type OnTriggerBodyFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The section step's key in the workflow's `steps` map. Defaults to `section`. */
  sectionId?: string;
  /** The section's inline body. */
  body: OnTriggerBodyVariant;
  /** The outer `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
  /** The body `defineWorkflow` id. Defaults to a stable fixture-local id. */
  bodyWorkflowId?: string;
  /**
   * The section's drain behavior. Omitted (the onTrigger default `"wait"`) for
   * most tests; a drain-teardown test sets `"cancel"` so a drain aborts the
   * live section.
   */
  drainBehavior?: "cancel" | "wait";
  /**
   * The section's body-failure policy. Omitted (the default `"end"`) unless a
   * test exercises `"tolerate"`.
   */
  onBodyFailure?: "end" | "tolerate";
};

function renderAgentBody(
  body: OnTriggerAgentBody,
  bodyWorkflowId: string,
): { imports: string; body: string } {
  const stepId = body.stepId ?? "work";
  const agentId = body.agentId ?? "on-trigger-body-agent";
  return {
    imports:
      `import { defineWorkflow, step } from "@intx/workflow/definition";\n` +
      `import { defineAgent } from "@intx/agent";\n`,
    body: `
const bodyAgent = defineAgent({
  id: ${JSON.stringify(agentId)},
  systemPrompt: ${JSON.stringify(body.systemPrompt)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

const body = defineWorkflow({
  id: ${JSON.stringify(bodyWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(stepId)}]: step({ agent: bodyAgent }),
  },
});
`,
  };
}

function renderAwaitSignalBody(
  body: OnTriggerAwaitSignalBody,
  bodyWorkflowId: string,
): { imports: string; body: string } {
  const gateId = body.gateId ?? "gate";
  const hasTimeout = body.timeout !== undefined;
  const recoverId = body.recoverId ?? "recover";
  const recoverDuration = body.recoverDuration ?? 10;

  const gateOpts = hasTimeout
    ? `{
      name: ${JSON.stringify(body.signalName)},
      timeout: ${JSON.stringify(body.timeout)},
      onTimeout: ${JSON.stringify(recoverId)},
    }`
    : `{ name: ${JSON.stringify(body.signalName)} }`;

  const recoverStep = hasTimeout
    ? `    [${JSON.stringify(recoverId)}]: sleep({ duration: ${JSON.stringify(recoverDuration)}, after: [${JSON.stringify(gateId)}] }),\n`
    : "";

  const imports = hasTimeout
    ? `import { awaitSignal, defineWorkflow, sleep } from "@intx/workflow/definition";\n`
    : `import { awaitSignal, defineWorkflow } from "@intx/workflow/definition";\n`;

  return {
    imports,
    body: `
const body = defineWorkflow({
  id: ${JSON.stringify(bodyWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(gateId)}]: awaitSignal(${gateOpts}),
${recoverStep}  },
});
`,
  };
}

function renderSleepBody(
  body: OnTriggerSleepBody,
  bodyWorkflowId: string,
): { imports: string; body: string } {
  const stepId = body.stepId ?? "wait";
  return {
    imports: `import { defineWorkflow, sleep } from "@intx/workflow/definition";\n`,
    body: `
const body = defineWorkflow({
  id: ${JSON.stringify(bodyWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(stepId)}]: sleep({ duration: ${JSON.stringify(body.duration)} }),
  },
});
`,
  };
}

function renderBody(
  body: OnTriggerBodyVariant,
  bodyWorkflowId: string,
): { imports: string; body: string } {
  switch (body.variant) {
    case "agent":
      return renderAgentBody(body, bodyWorkflowId);
    case "awaitSignal":
      return renderAwaitSignalBody(body, bodyWorkflowId);
    case "sleep":
      return renderSleepBody(body, bodyWorkflowId);
  }
}

export function onTriggerBodyEntry(params: OnTriggerBodyFixtureParams): string {
  const sectionId = params.sectionId ?? "section";
  const workflowId = params.workflowId ?? "wf_on_trigger_body";
  const bodyWorkflowId = params.bodyWorkflowId ?? "authored-on-trigger-body";

  const rendered = renderBody(params.body, bodyWorkflowId);
  const drainLine =
    params.drainBehavior !== undefined
      ? `      drainBehavior: ${JSON.stringify(params.drainBehavior)},\n`
      : "";
  const policyLine =
    params.onBodyFailure !== undefined
      ? `      onBodyFailure: ${JSON.stringify(params.onBodyFailure)},\n`
      : "";

  return `
${rendered.imports}import { onTrigger } from "@intx/workflow/definition";
${rendered.body}
export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(sectionId)}]: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body,
${drainLine}${policyLine}    }),
  },
});
`;
}
