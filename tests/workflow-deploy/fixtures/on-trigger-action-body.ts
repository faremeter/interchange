// Source-entry builder for an onTrigger body that runs an agent, an action,
// and a gate. The action reads the agent step's output and performs one
// declared effect. The gate branches on that effect's result. Both branch
// targets are real steps; one is taken and the other is skipped.
//
// The entry module exports the action handler next to `workflow`. The
// deployment points `interchange.actions` at this same module.

export type OnTriggerActionBodyFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The outer `defineWorkflow` id. */
  workflowId?: string;
  /** The body `defineWorkflow` id. */
  bodyWorkflowId?: string;
};

export function onTriggerActionBodyEntry(
  params: OnTriggerActionBodyFixtureParams,
): string {
  const workflowId = params.workflowId ?? "wf_on_trigger_action_body";
  const bodyWorkflowId =
    params.bodyWorkflowId ?? "authored-on-trigger-action-body";
  return `
import { action, defineWorkflow, gate, onTrigger, sleep, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const bodyAgent = defineAgent({
  id: "agent-body-work",
  systemPrompt: "You are the onTrigger body agent.",
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
    work: step({ agent: bodyAgent }),
    ship: action({
      handler: "ship",
      input: { from: "steps.work.output" },
      effect: { requires: ["ship"] },
      after: ["work"],
    }),
    choose: gate({
      when: { from: "steps.ship.output.shipped" },
      then: "taken",
      else: "left",
      after: ["ship"],
    }),
    taken: sleep({ duration: 10, after: ["choose"] }),
    left: sleep({ duration: 10, after: ["choose"] }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    section: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body,
    }),
  },
});

export async function ship(input, ctx, _signal) {
  if (input === null || typeof input !== "object" || typeof input.reply !== "string") {
    throw new Error("ship expected the agent step output");
  }
  return ctx.perform({
    effectId: "ship-once",
    capability: "ship",
    run: async () => ({ shipped: true, reply: input.reply }),
  });
}
`;
}
