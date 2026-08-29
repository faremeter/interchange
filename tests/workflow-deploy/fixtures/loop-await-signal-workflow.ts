// Source-entry builder for a loop whose BODY parks on an `awaitSignal` and
// resumes -- the deployed counterpart to the in-process runtime tests
// (loop-await-signal.test.ts, loop-suspend-resume.test.ts). A loop iteration
// runs through the suspendable-child seam, so the body's `awaitSignal` proxies
// up onto the loop CONTAINER as a signal-relay await; delivering the signal
// resolves the parked iteration, and the park survives a crash + restart.
//
// The entry module exports BOTH `workflow` and the loop `while`/`carry`
// functions, so the deployment points `interchange.loops` at the same bundled
// entry and `loadWorkflowLoopFnsFromClosure` resolves the refs by export name.
//
// `keepGoing` converges after the first iteration, so the loop parks exactly
// once; a single signal delivery resumes it to convergence (`settle` runs and
// the `onExhausted` target `escalate` is pruned).

export type LoopAwaitSignalFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
  /** The agent inference provider. Defaults to `anthropic` (the mock server). */
  provider?: string;
  /** The author signal name the loop body awaits. Defaults to `go`. */
  signalName?: string;
};

export function loopAwaitSignalEntry(
  params: LoopAwaitSignalFixtureParams,
): string {
  const workflowId = params.workflowId ?? "wf_loop_await_signal";
  const provider = params.provider ?? "anthropic";
  const signalName = params.signalName ?? "go";
  const agentBlock = (id: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: ${JSON.stringify(`${id} agent`)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: ${JSON.stringify(provider)}, model: "mock-model" }],
  },
})`;
  return `
import { defineWorkflow, loop, awaitSignal, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const loopBody = defineWorkflow({
  id: "await-body",
  trigger: { type: "manual" },
  steps: {
    hold: awaitSignal({ name: ${JSON.stringify(signalName)} }),
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

// The loop while/carry functions, resolved by export name via interchange.loops.
// keepGoing converges after the first iteration, so the loop parks once.
export function keepGoing(_childOutput, _currentInput) {
  return false;
}

export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}
`;
}
