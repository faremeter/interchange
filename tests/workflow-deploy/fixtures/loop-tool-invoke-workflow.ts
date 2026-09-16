// Source-entry builder for a loop workflow whose BODY step carries a real
// inline tool: one mail-triggered `loop` whose body is a single agent `step`
// declaring the `mail_send` tool, plus the two toolless top-level steps on the
// loop's converged and exhausted arms.
//
// Every other deployed loop fixture declares `tools: []` on its body agent, so
// no deployed loop body has ever reached the tool-invocation authorize seam.
// This fixture is the first that does. The two top-level steps stay toolless on
// purpose: they can produce no `tool_result`, so a `tool_result` anywhere in the
// captured inference traffic can only have originated in the loop body.
//
// The entry module exports BOTH `workflow` and the loop `while`/`carry`
// functions, so the deployment points `interchange.loops` at the same bundled
// entry and the loop fns resolve by export name.
//
// The loop converges after exactly two iterations: `input` seeds
// `currentInput = 0`; `keepGoing(output, currentInput)` stays true at 0 and
// turns false at 1; `nextCount` increments. while/carry read only the carry
// state, so convergence is deterministic regardless of the body agent's output.
//
// STEP ID DISTINCTNESS. `LOOP_BODY_STEP_ID` must differ from every entry in
// `TOP_LEVEL_STEP_IDS`, and anyone changing these ids must keep them disjoint.
// The per-step tables a body step resolves against -- the credentials snapshot,
// the pinned inference-source map, and the deploy-tree address resolver -- are
// keyed by `baseStepId(stepId)` in ONE flat namespace shared between the top
// level and the loop body. A body step id equal to a top-level step id
// therefore resolves to the TOP-LEVEL step's entry, and a test asserting that
// the body step resolves its own grants would pass against the wrong step's
// entry and prove nothing.

import path from "node:path";

// Absolute path to the sibling tool module, so a bundled entry resolves it
// through `Bun.build` (which inlines the module) rather than a bare specifier
// the sidecar closure could not resolve.
const MAIL_TOOL_MODULE = path.join(import.meta.dir, "mail-tool.ts");

/** The `loop` container's top-level step id. */
export const LOOP_STEP_ID = "rework";

/** Every top-level step id the fixture declares. */
export const TOP_LEVEL_STEP_IDS: readonly string[] = [
  LOOP_STEP_ID,
  "settle",
  "escalate",
];

/**
 * The loop body's only step id. Deliberately unlike every entry in
 * `TOP_LEVEL_STEP_IDS`; see the STEP ID DISTINCTNESS note above.
 */
export const LOOP_BODY_STEP_ID = "iterationTurn";

/** The loop body workflow's `defineWorkflow` id. */
export const LOOP_BODY_WORKFLOW_ID = "loop-tool-invoke-body";

export type LoopToolInvokeFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The top-level `defineWorkflow` id. */
  workflowId: string;
};

export function loopToolInvokeWorkflowEntry(
  params: LoopToolInvokeFixtureParams,
): string {
  const toollessAgent = (id: string, systemPrompt: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: ${JSON.stringify(systemPrompt)},
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
})`;
  return `
import { defineWorkflow, loop, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { mailSendTool } from ${JSON.stringify(MAIL_TOOL_MODULE)};

const loopBody = defineWorkflow({
  id: ${JSON.stringify(LOOP_BODY_WORKFLOW_ID)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(LOOP_BODY_STEP_ID)}]: step({ agent: defineAgent({
      id: "loop-body-tool-agent",
      systemPrompt: "You are the loop body's tool-bearing step agent.",
      tools: [mailSendTool("fs")],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    }) }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(LOOP_STEP_ID)}]: loop({
      body: loopBody,
      while: "keepGoing",
      carry: "nextCount",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "escalate",
    }),
    settle: step({
      agent: ${toollessAgent("loop-tool-invoke-settle-agent", "You are the converged-arm step agent.")},
      after: [${JSON.stringify(LOOP_STEP_ID)}],
    }),
    escalate: step({
      agent: ${toollessAgent("loop-tool-invoke-escalate-agent", "You are the exhausted-arm step agent.")},
      after: [${JSON.stringify(LOOP_STEP_ID)}],
    }),
  },
});

// The loop while/carry functions, resolved by export name via
// interchange.loops. Pure: they read only the carry state, so convergence is
// deterministic regardless of the body's output.
export function keepGoing(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) < 1;
}

export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}
`;
}
