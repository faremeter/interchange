// Source-entry builder for a loop body whose `childWorkflow` grandchild PARKS on
// an `awaitSignal` that is never delivered, so the grandchild is genuinely
// in-flight when a drain lands. This is the childWorkflow-terminal analog of the
// loop/onTrigger body local teardown: on the drain cascade the grandchild --
// running in-process under the workflow-process principal, which cannot sign a
// supervisor `CancelRequested` -- must tear down LOCALLY (its parked step fails
// to `RunFailed`), not wedge on a rejected control-plane cancel. A wedged
// grandchild would leave the loop body's spawn step awaiting a terminal that
// never comes and hang the whole run.
//
// `keepGoing` converges after the first iteration, so the loop spawns the
// grandchild exactly once.

export type LoopChildWorkflowParkedFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id of the outer workflow. */
  workflowId: string;
  /** The grandchild `defineWorkflow` id. */
  childWorkflowId: string;
  /** The author signal name the grandchild parks on (never delivered). */
  signalName?: string;
};

export function loopChildWorkflowParkedEntry(
  params: LoopChildWorkflowParkedFixtureParams,
): string {
  const signalName = params.signalName ?? "grandchild-never-arrives";
  const agentBlock = (id: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: ${JSON.stringify(`${id} agent`)},
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
})`;
  return `
import { awaitSignal, childWorkflow, defineWorkflow, loop, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const child = defineWorkflow({
  id: ${JSON.stringify(params.childWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    hold: awaitSignal({ name: ${JSON.stringify(signalName)} }),
  },
});

const loopBody = defineWorkflow({
  id: "authored-loop-childworkflow-parked-body",
  trigger: { type: "manual" },
  steps: {
    spawn: childWorkflow({ definition: child }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
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

export function keepGoing(_childOutput, _currentInput) {
  return false;
}

export function nextCount(_childOutput, currentInput) {
  return (typeof currentInput === "number" ? currentInput : 0) + 1;
}
`;
}
