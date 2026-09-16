// Source-entry builder for a `childWorkflow` child that contains a `loop`
// whose BODY step carries a real inline tool -- the loop-nested-one-rung-down
// case.
//
// The parent spawns the child as its own run with a FRESH credentials snapshot
// minted over the child definition. The child then runs a loop whose iteration
// inherits that snapshot (a loop iteration re-enters its enclosing run's env),
// so the body step's tool call authorizes against the child's snapshot under
// the body step's own id. A snapshot minted over the child's `stepOrder` alone
// therefore carries no entry for the body step and the call fails, even where
// the same loop works at the top level.
//
// The entry exports the loop `while`/`carry` functions so the deployment points
// `interchange.loops` at the same bundled entry and the fns resolve by export
// name. The loop converges after exactly two iterations: `input` seeds
// `currentInput = 0`; `keepGoing` stays true at 0 and turns false at 1. Both
// read only the carry state, so convergence is deterministic regardless of the
// body agent's output.
//
// STEP ID DISTINCTNESS. Every step id in this fixture -- the parent's, the
// child's, and the loop body's -- is distinct, and anyone changing them must
// keep them so. The per-step tables a step resolves against are keyed by
// `baseStepId(stepId)`, and the deploy-tree address resolver keys off the
// PARENT deployment's mailbox and step count, so a body step id equal to
// another step's id resolves to that other step's entry and an assertion about
// the body step would pass against the wrong step.

import path from "node:path";

// Absolute path to the sibling tool module, so a bundled entry resolves it
// through `Bun.build` (which inlines the module) rather than a bare specifier
// the sidecar closure could not resolve.
const MAIL_TOOL_MODULE = path.join(import.meta.dir, "mail-tool.ts");

/** The parent's toolless leading step id. */
export const PARENT_STEP_ID = "kickoff";

/** The parent's `childWorkflow` spawn step id. */
export const SPAWN_STEP_ID = "delegate";

/** The child's `loop` container step id. */
export const CHILD_LOOP_STEP_ID = "childRework";

/** The loop body's only step id, the one that carries the tool. */
export const LOOP_BODY_STEP_ID = "nestedIterationTurn";

/** Every step id the fixture declares outside the loop body. */
export const NON_BODY_STEP_IDS: readonly string[] = [
  PARENT_STEP_ID,
  SPAWN_STEP_ID,
  CHILD_LOOP_STEP_ID,
  "childSettle",
  "childEscalate",
];

export type ChildWorkflowLoopToolFixtureParams = {
  /** The parent's mail trigger address, the deployment routes on it. */
  address: string;
  /** The child's own mail trigger address. */
  childAddress: string;
  /** The parent's `defineWorkflow` id. */
  workflowId: string;
  /** The child's `defineWorkflow` id. */
  childWorkflowId: string;
};

export function childWorkflowLoopToolEntry(
  params: ChildWorkflowLoopToolFixtureParams,
): string {
  const toollessAgent = (id: string, systemPrompt: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: ${JSON.stringify(systemPrompt)},
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
})`;
  return `
import { childWorkflow, defineWorkflow, loop, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { mailSendTool } from ${JSON.stringify(MAIL_TOOL_MODULE)};

const loopBody = defineWorkflow({
  id: "child-workflow-loop-tool-body",
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(LOOP_BODY_STEP_ID)}]: step({ agent: defineAgent({
      id: "nested-loop-body-tool-agent",
      systemPrompt: "You are the nested loop body's tool-bearing step agent.",
      tools: [mailSendTool("fs")],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    }) }),
  },
});

const child = defineWorkflow({
  id: ${JSON.stringify(params.childWorkflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.childAddress)} },
  steps: {
    [${JSON.stringify(CHILD_LOOP_STEP_ID)}]: loop({
      body: loopBody,
      while: "keepGoing",
      carry: "nextCount",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "childEscalate",
    }),
    childSettle: step({
      agent: ${toollessAgent("child-workflow-loop-settle-agent", "You are the child's converged-arm step agent.")},
      after: [${JSON.stringify(CHILD_LOOP_STEP_ID)}],
    }),
    childEscalate: step({
      agent: ${toollessAgent("child-workflow-loop-escalate-agent", "You are the child's exhausted-arm step agent.")},
      after: [${JSON.stringify(CHILD_LOOP_STEP_ID)}],
    }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(PARENT_STEP_ID)}]: step({
      agent: ${toollessAgent("child-workflow-loop-parent-agent", "You are the parent's toolless leading step agent.")},
    }),
    [${JSON.stringify(SPAWN_STEP_ID)}]: childWorkflow({
      definition: child,
      after: [${JSON.stringify(PARENT_STEP_ID)}],
    }),
  },
});

// The loop while/carry functions, resolved by export name via
// interchange.loops. Pure: they read only the carry state, so convergence is
// deterministic regardless of the body agent's output.
export function keepGoing(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) < 1;
}

export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}
`;
}
