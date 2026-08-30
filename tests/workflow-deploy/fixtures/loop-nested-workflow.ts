// Source-entry builder for a NESTED loop workflow fixture: a mail-triggered
// outer `loop` whose body is itself a workflow containing an inner `loop` over a
// single non-suspending agent `step`. It proves a loop body may contain a nested
// loop end to end on the deployed path: the child host lifts the outer AND inner
// loop bodies, resolves each loop's body ref from the shared bodies map, and the
// deploy-time source pin recurses through both loop bodies to pin the inner
// agent step's inference source. The entry module exports `workflow` and the
// loop `while`/`carry` functions, resolved by export name via interchange.loops
// (both loops reuse the same pure counter functions).
//
// Each loop converges after exactly three iterations (`input` seeds 0;
// `keepGoing` stays true for 0 and 1 and turns false at 2). So the outer loop
// spawns three iteration child runs, and each outer iteration's body run spawns
// three inner iteration child runs -- the count the roundtrip test asserts.

export type LoopNestedWorkflowFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
  /** The agent inference provider. Defaults to `anthropic` (the mock server). */
  provider?: string;
};

export function loopNestedWorkflowEntry(
  params: LoopNestedWorkflowFixtureParams,
): string {
  const workflowId = params.workflowId ?? "wf_loop_nested_fixture";
  const provider = params.provider ?? "anthropic";
  const agentBlock = (id: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: "nested loop fixture agent",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: ${JSON.stringify(provider)}, model: "mock-model" }],
  },
})`;
  return `
import { defineWorkflow, loop, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const innerBody = defineWorkflow({
  id: "inner-loop-body",
  trigger: { type: "manual" },
  steps: {
    turn: step({ agent: ${agentBlock("inner-body-agent")} }),
  },
});

const outerBody = defineWorkflow({
  id: "outer-loop-body",
  trigger: { type: "manual" },
  steps: {
    inner: loop({
      body: innerBody,
      while: "keepGoing",
      carry: "nextCount",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "iescalate",
    }),
    iescalate: step({ agent: ${agentBlock("inner-escalate-agent")}, after: ["inner"] }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    outer: loop({
      body: outerBody,
      while: "keepGoing",
      carry: "nextCount",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "escalate",
    }),
    settle: step({ agent: ${agentBlock("settle-agent")}, after: ["outer"] }),
    escalate: step({ agent: ${agentBlock("escalate-agent")}, after: ["outer"] }),
  },
});

// Both loops share these pure while/carry functions, resolved by export name via
// interchange.loops. They read only the carry state, so convergence is
// deterministic regardless of the body agent's output.
export function keepGoing(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) < 2;
}

export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}
`;
}
