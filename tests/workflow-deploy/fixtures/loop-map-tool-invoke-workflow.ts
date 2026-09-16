// Source-entry builder for a loop workflow whose body is a `map` fan-out and
// whose PER-ITEM agent carries a real inline tool: one mail-triggered `loop`
// whose body definition holds a single `map` step, plus the two toolless
// top-level steps on the loop's converged and exhausted arms.
//
// This is the rung where two id transformations compose. `runMap` runs its
// inner step under a scoped id `<mapStepId>[<index>]`, and every deploy-asset
// lookup recovers the base id with `baseStepId`; the map itself sits inside a
// loop body, whose ids reach the deployment's flat namespace only because the
// executable-step walk descends into loop bodies. A per-item tool call
// therefore has to resolve `<mapStepId>[<index>]` back to a body step id that
// something put in the snapshot. Neither the top-level map fan-out fixture nor
// the plain loop-body tool fixture exercises that composition.
//
// The two top-level steps stay toolless on purpose: they can produce no
// `tool_result`, so a `tool_result` anywhere in the captured inference traffic
// can only have originated in a map iteration inside the loop body.
//
// The entry module exports BOTH `workflow` and the loop `while`/`carry`
// functions, so the deployment points `interchange.loops` at the same bundled
// entry and the loop fns resolve by export name.
//
// The loop converges after exactly two iterations: `input` seeds
// `currentInput = 0`; `keepGoing(output, currentInput)` stays true at 0 and
// turns false at 1; `nextCount` increments. while/carry read only the carry
// state, so convergence is deterministic regardless of the body's output. Each
// iteration fans out over `MAP_ITEM_COUNT` literal items.
//
// STEP ID DISTINCTNESS. `LOOP_BODY_MAP_STEP_ID` must differ from every entry in
// `TOP_LEVEL_STEP_IDS`, and anyone changing these ids must keep them disjoint.
// The per-step tables a body step resolves against -- the credentials snapshot,
// the pinned inference-source map, and the deploy-tree address resolver -- are
// keyed by `baseStepId(stepId)` in ONE flat namespace shared between the top
// level and the loop body. A body step id equal to a top-level step id
// therefore resolves to the TOP-LEVEL step's entry, and the round-trip below
// would pass against the wrong step's tools and prove nothing. The ids here are
// deliberately unlike one another for that reason.

import path from "node:path";

// Absolute path to the sibling tool module, so a bundled entry resolves it
// through `Bun.build` (which inlines the module) rather than a bare specifier
// the sidecar closure could not resolve.
const MAIL_TOOL_MODULE = path.join(import.meta.dir, "mail-tool.ts");

/** The `loop` container's top-level step id. */
export const LOOP_STEP_ID = "outerMapCycle";

/** Every top-level step id the fixture declares. */
export const TOP_LEVEL_STEP_IDS: readonly string[] = [
  LOOP_STEP_ID,
  "mapSettleRung",
  "mapEscalateRung",
];

/**
 * The loop body's only step id: the `map` container the per-item agent fans
 * out under. Deliberately unlike every entry in `TOP_LEVEL_STEP_IDS`; see the
 * STEP ID DISTINCTNESS note above.
 */
export const LOOP_BODY_MAP_STEP_ID = "bodyItemFanOut";

/** The loop body workflow's `defineWorkflow` id. */
export const LOOP_BODY_WORKFLOW_ID = "loop-map-tool-invoke-body";

/** The number of literal items each loop iteration's map fans out over. */
export const MAP_ITEM_COUNT = 2;

/** The number of iterations the loop runs before `keepGoing` turns false. */
export const EXPECTED_ITERATIONS = 2;

export type LoopMapToolInvokeFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The top-level `defineWorkflow` id. */
  workflowId: string;
};

export function loopMapToolInvokeWorkflowEntry(
  params: LoopMapToolInvokeFixtureParams,
): string {
  const items = Array.from({ length: MAP_ITEM_COUNT }, (_unused, i) => ({
    id: String.fromCharCode(97 + i),
  }));
  const toollessAgent = (id: string, systemPrompt: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: ${JSON.stringify(systemPrompt)},
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
})`;
  return `
import { defineWorkflow, loop, map, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { mailSendTool } from ${JSON.stringify(MAIL_TOOL_MODULE)};

const loopBody = defineWorkflow({
  id: ${JSON.stringify(LOOP_BODY_WORKFLOW_ID)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(LOOP_BODY_MAP_STEP_ID)}]: map({
      over: { literal: ${JSON.stringify(items)} },
      step: step({ agent: defineAgent({
        id: "loop-map-body-tool-agent",
        systemPrompt: "You are the map item agent inside the loop body.",
        tools: [mailSendTool("fs")],
        capabilities: [],
        inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
      }) }),
    }),
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
      onExhausted: "mapEscalateRung",
    }),
    mapSettleRung: step({
      agent: ${toollessAgent("loop-map-settle-agent", "You are the converged-arm step agent.")},
      after: [${JSON.stringify(LOOP_STEP_ID)}],
    }),
    mapEscalateRung: step({
      agent: ${toollessAgent("loop-map-escalate-agent", "You are the exhausted-arm step agent.")},
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
