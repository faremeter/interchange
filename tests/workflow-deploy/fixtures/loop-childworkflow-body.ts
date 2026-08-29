// Source-entry builder for a loop-body -> childWorkflow fixture: a top-level
// `loop` whose inline body's only step is a `childWorkflow` spawn of a trivial
// one-agent grandchild. The loop-body childWorkflow is lifted transitively to
// `<workflowId>__<loopStepId>__<spawnStepId>` (the deploy enumerator recurses
// into the loop body), and the grandchild's per-step agent runs a REAL agent
// through the sidecar. The entry exports `workflow` plus the loop `while`/`carry`
// functions, so the deployment points `interchange.loops` at the same bundled
// entry.
//
// `keepGoing` converges after the first iteration, so the loop spawns the
// grandchild exactly once and converges (`settle` runs, `escalate` is pruned).

export type LoopChildWorkflowFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The loop step's key in the workflow's `steps` map. Defaults to `rework`. */
  loopStepId?: string;
  /** The childWorkflow spawn step's key in the body's `steps` map. Defaults to `spawn`. */
  spawnStepId?: string;
  /** The outer `defineWorkflow` id. */
  workflowId: string;
  /** The loop body `defineWorkflow` id. Defaults to a stable fixture-local id. */
  bodyWorkflowId?: string;
  /** The grandchild `defineWorkflow` id. */
  childWorkflowId: string;
  /** The grandchild's single agent step key. */
  childStepId: string;
  /** The grandchild agent's `defineAgent` id. */
  childAgentId: string;
  /** The grandchild agent's system prompt. */
  childSystemPrompt: string;
};

export function loopChildWorkflowEntry(
  params: LoopChildWorkflowFixtureParams,
): string {
  const loopStepId = params.loopStepId ?? "rework";
  const spawnStepId = params.spawnStepId ?? "spawn";
  const bodyWorkflowId =
    params.bodyWorkflowId ?? "authored-loop-childworkflow-body";
  const agentBlock = (id: string, prompt: string) => `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: ${JSON.stringify(prompt)},
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
})`;

  return `
import { childWorkflow, defineWorkflow, loop, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const child = defineWorkflow({
  id: ${JSON.stringify(params.childWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(params.childStepId)}]: step({
      agent: ${agentBlock(params.childAgentId, params.childSystemPrompt)},
    }),
  },
});

const loopBody = defineWorkflow({
  id: ${JSON.stringify(bodyWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    [${JSON.stringify(spawnStepId)}]: childWorkflow({ definition: child }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(loopStepId)}]: loop({
      body: loopBody,
      while: "keepGoing",
      carry: "nextCount",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "escalate",
    }),
    settle: step({
      agent: ${agentBlock("settle-agent", "settle-agent agent")},
      after: [${JSON.stringify(loopStepId)}],
    }),
    escalate: step({
      agent: ${agentBlock("escalate-agent", "escalate-agent agent")},
      after: [${JSON.stringify(loopStepId)}],
    }),
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
