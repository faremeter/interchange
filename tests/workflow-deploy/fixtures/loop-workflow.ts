// Source-entry builder for a loop workflow fixture: one mail-triggered `loop`
// whose body is a single agent `step` (a loop body cannot suspend, so it cannot
// be a sleep/awaitSignal/childWorkflow; an agent step is the wired,
// non-suspending body). The entry module exports BOTH `workflow` and the loop
// `while`/`carry` functions, so the deployment points `interchange.loops` at the
// same bundled entry and `loadWorkflowLoopFnsFromClosure` resolves the refs by
// export name.
//
// The loop converges after exactly three iterations: `input` seeds
// `currentInput = 0`; `keepGoing(output, currentInput)` stays true for 0 and 1
// and turns false at 2; `nextCount` increments. On convergence the normal
// dependent (`settle`) runs and the `onExhausted` target (`escalate`) is pruned.
// while/carry read only the carry state, so convergence is deterministic
// regardless of the body agent's output.

export type LoopWorkflowFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
  /** The agent inference provider. Defaults to `anthropic` (the mock server). */
  provider?: string;
};

export function loopWorkflowEntry(params: LoopWorkflowFixtureParams): string {
  const workflowId = params.workflowId ?? "wf_loop_fixture";
  const provider = params.provider ?? "anthropic";
  const agentBlock = (id: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: "loop fixture agent",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: ${JSON.stringify(provider)}, model: "mock-model" }],
  },
})`;
  return `
import { defineWorkflow, loop, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const loopBody = defineWorkflow({
  id: "loop-body",
  trigger: { type: "manual" },
  steps: {
    turn: step({ agent: ${agentBlock("loop-body-agent")} }),
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

// The loop while/carry functions, resolved by export name via
// interchange.loops. Pure: they read only the carry state, so convergence is
// deterministic regardless of the body's output.
export function keepGoing(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) < 2;
}

export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}
`;
}
