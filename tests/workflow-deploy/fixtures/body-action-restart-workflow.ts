// Source-entry builder for a workflow whose effects run only inside bodies: one
// action in an inline `childWorkflow` body, one in a loop body that runs once,
// and one in an `onTrigger` section body. The three containers run in
// parallel. The trigger mail starts the run and is also the section's first
// event. The top-level `loopDone` action is the loop's required `onExhausted`
// target. Without `block`, it never runs: the loop converges after one
// iteration, which prunes it, and the section listens for more mail, so the
// top-level run does not end. With `block`, each body fails after the crash,
// the failed loop runs `loopDone`, and the top-level run fails.
//
// Each body action performs one effect that appends its handler name as a line
// to a marker file, so the test counts handler effects from outside the
// runtime. Without `block`, a body then parks on an `awaitSignal` of its own
// name. With `block` set, each effect blocks after it appends its line and
// never settles, so a crash lands while the action is in flight, and the body
// has no step after its action.
//
// The marker path is baked into the source because the workflow process does
// not inherit the test's environment. The entry module exports the handlers and
// the loop functions next to `workflow`. The deployment points
// `interchange.actions` and `interchange.loops` at this same module.

/** The handler of the action in the `childWorkflow` body. */
export const CHILD_MARK_HANDLER = "markChild";

/** The handler of the action in the loop body. */
export const LOOP_MARK_HANDLER = "markLoop";

/** The handler of the action in the `onTrigger` section body. */
export const SECTION_MARK_HANDLER = "markSection";

/** Every body handler, one marker line each when no effect repeats. */
export const MARK_HANDLERS = [
  CHILD_MARK_HANDLER,
  LOOP_MARK_HANDLER,
  SECTION_MARK_HANDLER,
] as const;

/** The effect capability each body action performs its effect under. */
export const MARK_CAPABILITY = "mark";

/** The `childWorkflow` step, so the test can find its `ChildSpawned`. */
export const CHILD_STEP_ID = "spawn";

/** The loop step, so the test can derive its body run id. */
export const LOOP_STEP_ID = "rework";

/** The `onTrigger` section step, so the test can derive its body run id. */
export const SECTION_STEP_ID = "section";

/** The signal the `childWorkflow` body awaits after its action. */
export const CHILD_SIGNAL = "goChild";

/** The signal the loop body awaits after its action. */
export const LOOP_SIGNAL = "goLoop";

/** The signal the section body awaits after its action. */
export const SECTION_SIGNAL = "goSection";

export type BodyActionRestartFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id. */
  workflowId: string;
  /** The absolute path of the file each effect appends a line to. */
  markerPath: string;
  /** Block each effect after it appends its line, and omit the parks. */
  block: boolean;
};

export function bodyActionRestartEntry(
  params: BodyActionRestartFixtureParams,
): string {
  const body = (suffix: string, handler: string, signal: string) => {
    const hold = params.block
      ? ""
      : `
    hold: awaitSignal({
      name: ${JSON.stringify(signal)},
      after: [${JSON.stringify(handler)}],
    }),`;
    return `defineWorkflow({
  id: ${JSON.stringify(`${params.workflowId}_${suffix}`)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(handler)}]: action({
      handler: ${JSON.stringify(handler)},
      effect: { requires: [${JSON.stringify(MARK_CAPABILITY)}] },
    }),${hold}
  },
})`;
  };
  return `
import fs from "node:fs";

import { action, awaitSignal, childWorkflow, defineWorkflow, loop, onTrigger } from "@intx/workflow/definition";

const child = ${body("child", CHILD_MARK_HANDLER, CHILD_SIGNAL)};

const loopBody = ${body("loop_body", LOOP_MARK_HANDLER, LOOP_SIGNAL)};

const sectionBody = ${body("section_body", SECTION_MARK_HANDLER, SECTION_SIGNAL)};

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(CHILD_STEP_ID)}]: childWorkflow({ definition: child }),
    [${JSON.stringify(LOOP_STEP_ID)}]: loop({
      body: loopBody,
      while: "stopAfterOne",
      carry: "keepInput",
      maxIterations: 2,
      onExhausted: "loopDone",
    }),
    loopDone: action({ handler: "loopDone", after: [${JSON.stringify(LOOP_STEP_ID)}] }),
    [${JSON.stringify(SECTION_STEP_ID)}]: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body: sectionBody,
    }),
  },
});
${MARK_HANDLERS.map((handler) => markSource(handler, params)).join("")}
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

function markSource(
  handler: string,
  params: BodyActionRestartFixtureParams,
): string {
  const blockLine = params.block ? "\n      await new Promise(() => {});" : "";
  return `
export async function ${handler}(_input, ctx, _signal) {
  return ctx.perform({
    effectId: "mark-once",
    capability: ${JSON.stringify(MARK_CAPABILITY)},
    run: async () => {
      fs.appendFileSync(${JSON.stringify(params.markerPath)}, ${JSON.stringify(`${handler}\n`)});${blockLine}
      return { marked: true };
    },
  });
}
`;
}
