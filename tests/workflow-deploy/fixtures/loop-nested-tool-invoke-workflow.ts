// Source-entry builder for a NESTED loop workflow whose INNERMOST body step
// carries a real inline tool: a mail-triggered outer `loop` whose body holds an
// inner `loop`, whose body holds the single agent step that declares
// `mail_send`. Every other step in the fixture -- both top-level arms and the
// inner loop's exhausted arm -- is toolless.
//
// This is the rung two levels below the top. An inner loop resolves its body
// ref from the same bodies map as the outer one and inherits the parent's env
// one further rung down, so the innermost step's tool call authorizes against a
// credentials snapshot that had to reach it through two nested descents. The
// plain loop-body tool fixture proves one rung; the spawned-child variant
// proves a loop inside a lifted body. Loop-inside-loop is neither.
//
// Because only the innermost body step carries a tool, a `tool_result` anywhere
// in the captured inference traffic can only have originated there.
//
// The entry module exports BOTH `workflow` and the loop `while`/`carry`
// functions, so the deployment points `interchange.loops` at the same bundled
// entry and both loops resolve the same pure counter functions by export name.
//
// Each loop converges after exactly two iterations: `input` seeds
// `currentInput = 0`; `keepGoing` stays true at 0 and turns false at 1;
// `nextCount` increments. while/carry read only the carry state, so convergence
// is deterministic regardless of the body's output. So the top-level run spawns
// two outer iteration runs, and each outer iteration run spawns two inner
// iteration runs.
//
// STEP ID DISTINCTNESS. Every id below is distinct from every other id in the
// fixture, across all three nesting levels, and anyone changing them must keep
// them so. The per-step tables a body step resolves against -- the credentials
// snapshot, the pinned inference-source map, and the deploy-tree address
// resolver -- are keyed by `baseStepId(stepId)` in ONE flat namespace shared by
// the top level and both loop bodies. A colliding id resolves to the OTHER
// step's entry, so the round-trip below would load the wrong step's tools and
// pass against a broken system. The ids are deliberately unlike one another for
// that reason; `ALL_TOOLLESS_STEP_IDS` is asserted disjoint from
// `INNER_BODY_STEP_ID` in the test.

import path from "node:path";

// Absolute path to the sibling tool module, so a bundled entry resolves it
// through `Bun.build` (which inlines the module) rather than a bare specifier
// the sidecar closure could not resolve.
const MAIL_TOOL_MODULE = path.join(import.meta.dir, "mail-tool.ts");

/** The outer `loop` container's top-level step id. */
export const OUTER_LOOP_STEP_ID = "outerNestCycle";

/** The inner `loop` container's step id, inside the outer loop's body. */
export const INNER_LOOP_STEP_ID = "innerNestCycle";

/**
 * The inner loop body's only step id -- the agent step that declares the
 * inline tool. Distinct from every entry in `ALL_TOOLLESS_STEP_IDS`; see the
 * STEP ID DISTINCTNESS note above.
 */
export const INNER_BODY_STEP_ID = "innermostToolTurn";

/**
 * Every step id in the fixture whose agent declares no tool, at any nesting
 * level: the two top-level arms, the two loop containers, and the inner loop's
 * exhausted arm.
 */
export const ALL_TOOLLESS_STEP_IDS: readonly string[] = [
  OUTER_LOOP_STEP_ID,
  "nestSettleRung",
  "nestEscalateRung",
  INNER_LOOP_STEP_ID,
  "innerEscalateRung",
];

/** The outer loop body workflow's `defineWorkflow` id. */
export const OUTER_BODY_WORKFLOW_ID = "loop-nested-tool-invoke-outer-body";

/** The inner loop body workflow's `defineWorkflow` id. */
export const INNER_BODY_WORKFLOW_ID = "loop-nested-tool-invoke-inner-body";

/** The number of iterations each loop runs before `keepGoing` turns false. */
export const EXPECTED_ITERATIONS = 2;

export type LoopNestedToolInvokeFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The top-level `defineWorkflow` id. */
  workflowId: string;
};

export function loopNestedToolInvokeWorkflowEntry(
  params: LoopNestedToolInvokeFixtureParams,
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

const innerBody = defineWorkflow({
  id: ${JSON.stringify(INNER_BODY_WORKFLOW_ID)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(INNER_BODY_STEP_ID)}]: step({ agent: defineAgent({
      id: "loop-nested-inner-tool-agent",
      systemPrompt: "You are the innermost loop body's tool-bearing agent.",
      tools: [mailSendTool("fs")],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    }) }),
  },
});

const outerBody = defineWorkflow({
  id: ${JSON.stringify(OUTER_BODY_WORKFLOW_ID)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(INNER_LOOP_STEP_ID)}]: loop({
      body: innerBody,
      while: "keepGoing",
      carry: "nextCount",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "innerEscalateRung",
    }),
    innerEscalateRung: step({
      agent: ${toollessAgent("loop-nested-inner-escalate-agent", "You are the inner exhausted-arm step agent.")},
      after: [${JSON.stringify(INNER_LOOP_STEP_ID)}],
    }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(OUTER_LOOP_STEP_ID)}]: loop({
      body: outerBody,
      while: "keepGoing",
      carry: "nextCount",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "nestEscalateRung",
    }),
    nestSettleRung: step({
      agent: ${toollessAgent("loop-nested-settle-agent", "You are the converged-arm step agent.")},
      after: [${JSON.stringify(OUTER_LOOP_STEP_ID)}],
    }),
    nestEscalateRung: step({
      agent: ${toollessAgent("loop-nested-escalate-agent", "You are the exhausted-arm step agent.")},
      after: [${JSON.stringify(OUTER_LOOP_STEP_ID)}],
    }),
  },
});

// Both loops share these pure while/carry functions, resolved by export name
// via interchange.loops. They read only the carry state, so convergence is
// deterministic regardless of the body's output.
export function keepGoing(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) < 1;
}

export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}
`;
}
