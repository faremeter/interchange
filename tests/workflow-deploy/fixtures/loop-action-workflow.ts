// Source-entry builder for a loop-with-action-body workflow: a mail-triggered
// `loop` whose body is a single `action`, with agent-step dependents at the top
// level. The action body exercises the action runtime (invokeAction resolving a
// handler from interchange.actions and running it against the effect ledger) on
// the already-proven loop container; the agent dependents give the deploy an
// operator-approved inference source, which the non-agent steps (the loop
// container and the loop-body action) pin as their inert default placeholder.
//
// The entry module exports `workflow`, the loop while/carry functions, and the
// action handler; the deployment points interchange.loops AND interchange.actions
// at the same bundled entry. The loop converges after three iterations, each
// running the action body; on convergence the normal dependent (settle) runs and
// the onExhausted target (escalate) is pruned.

export type LoopActionWorkflowFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
  /** The agent inference provider. Defaults to `anthropic` (the mock server). */
  provider?: string;
};

export function loopActionWorkflowEntry(
  params: LoopActionWorkflowFixtureParams,
): string {
  const workflowId = params.workflowId ?? "wf_loop_action_fixture";
  const provider = params.provider ?? "anthropic";
  const agentBlock = (id: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: "loop-action fixture agent",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: ${JSON.stringify(provider)}, model: "mock-model" }],
  },
})`;
  return `
import { defineWorkflow, action, loop, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const loopBody = defineWorkflow({
  id: "loop-action-body",
  trigger: { type: "manual" },
  steps: {
    touch: action({ handler: "touch" }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    rework: loop({
      body: loopBody,
      while: "keepGoing",
      carry: "nextCount",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "escalate",
    }),
    settle: step({ agent: ${agentBlock("settle-agent")}, after: ["rework"] }),
    escalate: step({ agent: ${agentBlock("escalate-agent")}, after: ["rework"] }),
  },
});

export function keepGoing(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) < 2;
}

export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}

// A deterministic action handler resolved by name from interchange.actions. It
// performs no capability-gated effect, so it needs no effect grant; it exercises
// the resolve + invoke path (the effect ledger's dedup and capability checks are
// covered by runtime unit tests).
export async function touch(input, _ctx, _signal) {
  return { touched: true, input };
}
`;
}
