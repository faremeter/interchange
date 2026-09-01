// Source-entry builder for an onTrigger section whose inline body contains a
// `loop` with an agent body -- the loop-nested-in-a-spawned-body case. The
// section spawns its body as a child run per event; that body runs a loop whose
// own body is a single agent `step`. The deploy pin must recurse into the loop
// body (nested inside the spawned onTrigger body) to pin the loop-body agent
// step's inference source, and the runtime must wire loop support into the
// spawned-body child env; without either, the body child crashes at the first
// iteration.
//
// The entry exports the loop `while`/`carry` functions so the deployment points
// `interchange.loops` at the same bundled entry and `loadWorkflowLoopFnsFromClosure`
// resolves them by export name -- exactly as the top-level loop fixture does.
// The loop converges after three iterations (`keepGoing` reads only the carry
// state), so the body run terminates through the loop's normal dependent
// (`settle`) regardless of the agent's output.
//
// When `loopBodySpawnsGrandchild` is set, the loop body ALSO spawns a
// `childWorkflow` grandchild each iteration. This exercises the spawned-body env
// wiring that merges a loop body's childWorkflow grandchildren into the body's
// in-memory spawn map and caps the grandchild's grants per iteration.

export type OnTriggerLoopBodyFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The section step's key. Defaults to `section`. */
  sectionId?: string;
  /** The outer `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
  /** The onTrigger body `defineWorkflow` id. Defaults to a stable fixture-local id. */
  bodyWorkflowId?: string;
  /**
   * When true, the loop body spawns a `childWorkflow` grandchild (with its own
   * agent step) each iteration, exercising the loop-body grandchild spawn path.
   */
  loopBodySpawnsGrandchild?: boolean;
};

export function onTriggerLoopBodyEntry(
  params: OnTriggerLoopBodyFixtureParams,
): string {
  const sectionId = params.sectionId ?? "section";
  const workflowId = params.workflowId ?? "wf_on_trigger_loop_body";
  const bodyWorkflowId =
    params.bodyWorkflowId ?? "authored-on-trigger-loop-body";
  const spawnsGrandchild = params.loopBodySpawnsGrandchild === true;
  const agentBlock = (id: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: "on-trigger loop body agent",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
})`;

  const grandchildBlock = spawnsGrandchild
    ? `
const grandchild = defineWorkflow({
  id: "on-trigger-loop-grandchild",
  trigger: { type: "manual" },
  steps: {
    deep: step({ agent: ${agentBlock("on-trigger-loop-grandchild-agent")} }),
  },
});
`
    : "";

  const loopBodySteps = spawnsGrandchild
    ? `    turn: step({ agent: ${agentBlock("on-trigger-loop-body-agent")} }),
    spawn: childWorkflow({ definition: grandchild, after: ["turn"] }),`
    : `    turn: step({ agent: ${agentBlock("on-trigger-loop-body-agent")} }),`;

  const definitionImports = spawnsGrandchild
    ? `import { childWorkflow, defineWorkflow, loop, onTrigger, step } from "@intx/workflow/definition";`
    : `import { defineWorkflow, loop, onTrigger, step } from "@intx/workflow/definition";`;

  return `
${definitionImports}
import { defineAgent } from "@intx/agent";
${grandchildBlock}
const loopBody = defineWorkflow({
  id: "on-trigger-inner-loop-body",
  trigger: { type: "manual" },
  steps: {
${loopBodySteps}
  },
});

const body = defineWorkflow({
  id: ${JSON.stringify(bodyWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    rework: loop({
      body: loopBody,
      while: "keepGoing",
      carry: "nextCount",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "escalate",
    }),
    settle: step({ agent: ${agentBlock("on-trigger-loop-settle-agent")}, after: ["rework"] }),
    escalate: step({ agent: ${agentBlock("on-trigger-loop-escalate-agent")}, after: ["rework"] }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(sectionId)}]: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body,
    }),
  },
});

// The loop while/carry functions, resolved by export name via interchange.loops.
// Pure: they read only the carry state, so convergence is deterministic
// regardless of the body agent's output.
export function keepGoing(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) < 2;
}

export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}
`;
}
