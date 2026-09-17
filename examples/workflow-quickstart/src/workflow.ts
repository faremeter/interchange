// The `interchange.workflow` entry point: the module a host imports and
// evaluates to obtain this package's `WorkflowDefinition`.
//
// The host takes the module's exports and looks for values that validate
// as a `WorkflowDefinition`. It requires EXACTLY ONE. That is why
// `revisionPass` below -- itself a full `defineWorkflow` result, because a
// loop body is a whole workflow -- is a module-private const. Exporting it
// would make this entry ambiguous and the deployment would be refused.
//
// The definition is data, not code. `loop.while`, `loop.carry` and
// `action.handler` are strings; the agent's tools are declarations. That
// is what lets the deploy substrate hash the definition, show an operator
// the grants it implies, and freeze the approved shape.

import { defineAgent } from "@intx/agent";
import { action, defineWorkflow, escalation, loop, step } from "@intx/workflow";

import { WORD_COUNT_TOOL, wordCountTool } from "./word-count-tool";

/** How many revision passes the loop will run before giving up. */
export const MAX_PASSES = 4;

/**
 * The loop body. A loop iteration is a separate child run of this
 * workflow: its own run id, its own event log, its own step outputs. The
 * iteration's input arrives as the body's `trigger.payload`.
 *
 * `shorten` is the body's first step, so the default-input convention
 * already gives it `{ from: "trigger.payload" }`; it is spelled out here
 * because a reader should not have to know the convention to follow the
 * data.
 */
const revisionPass = defineWorkflow({
  id: "tagline-revision-pass",
  trigger: { type: "manual" },
  steps: {
    shorten: step({
      agent: defineAgent({
        id: "tagline-editor",
        systemPrompt: [
          "You shorten marketing taglines.",
          'Your input is JSON: { "tagline", "maxWords" }.',
          `Call the ${WORD_COUNT_TOOL} tool to measure a tagline; do not count by eye.`,
          "If the tagline already fits within maxWords, reply with it unchanged.",
          "Otherwise rewrite it shorter while keeping its meaning.",
          "Reply with the tagline alone -- no quotes, no commentary.",
        ].join("\n"),
        tools: [wordCountTool],
        capabilities: [],
        inference: {
          sources: [{ provider: "anthropic", model: "claude-sonnet-5" }],
        },
      }),
      input: { from: "trigger.payload" },
    }),
  },
});

export const workflow = defineWorkflow({
  id: "tagline-review",
  // Nothing fires a manual workflow on its own; a host invokes it and
  // supplies the launch payload. Here that payload is the `RevisionPass`
  // the loop starts from.
  trigger: { type: "manual" },
  steps: {
    revise: loop({
      body: revisionPass,
      while: "stillTooLong",
      carry: "nextPass",
      input: { from: "trigger.payload" },
      maxIterations: MAX_PASSES,
      // Where the run goes when the cap is hit without `while` going
      // false. Required: a loop must say what happens when it does not
      // converge. The converged arm is every OTHER step that depends on
      // the loop -- here, `publish`. Exactly one arm runs; the runtime
      // prunes the other.
      onExhausted: "giveUp",
    }),
    publish: action({
      handler: "publishTagline",
      // The loop's step output is `{ outcome, iterations, carry, final }`:
      // `final` is the converging pass's output, and `carry` is the state
      // that pass started from. The accepted tagline comes from the first
      // and the destination from the second, so the input is assembled
      // from both.
      input: {
        merge: [
          {
            project: { from: "steps.revise.output.carry" },
            fields: ["outputPath"],
          },
          { from: "steps.revise.output.final.shorten" },
        ],
      },
      // The capability floor for this action's effects. `ctx.perform`
      // refuses any capability not listed here.
      effect: { requires: ["fs:write"] },
      after: ["revise"],
    }),
    giveUp: escalation({
      to: "editor@example.com",
      data: { from: "steps.revise.output" },
      after: ["revise"],
    }),
  },
});
