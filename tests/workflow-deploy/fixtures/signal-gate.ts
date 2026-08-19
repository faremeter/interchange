// Source-entry builder for the signal-gated multi-step workflow fixture (F2):
// `step1 -> gate = awaitSignal(...) -> step2`, where the second step is
// optional. The returned string is a `@intx/*`-importing entry module;
// `bundleWorkflowEntry` inlines it to a self-contained `.mjs` the sidecar
// evaluates in-child.
//
// Parameterised by the mail trigger address, the awaited signal name, an
// optional `drainBehavior` on the gate, the step and gate ids, and the per-step
// system prompts so a caller pins each agent's inference request to the
// deployment it exercises. Omit `systemPrompt2` to deploy a `step1 -> gate`
// workflow with no tail step (the drain round-trip shape); supply it to deploy
// the full `step1 -> gate -> step2` shape (the signal park/resume shape).

export type SignalGateFixtureParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The awaited signal's name. */
  signalName: string;
  /** The gate's drain behaviour. Omitted leaves the primitive default (`wait`). */
  drainBehavior?: "cancel" | "wait";
  /** The first step's key in the workflow's `steps` map. Defaults to `step1`. */
  step1Id?: string;
  /** The gate's key in the workflow's `steps` map. Defaults to `gate`. */
  gateId?: string;
  /** The second step's key. Defaults to `step2`; only used when `systemPrompt2` is set. */
  step2Id?: string;
  /** The first step agent's system prompt. */
  systemPrompt1: string;
  /**
   * The second step agent's system prompt. When set, the workflow carries a
   * `step2` after the gate; when omitted, the workflow ends at the gate.
   */
  systemPrompt2?: string;
  /** The first agent's `defineAgent` id. Defaults to a stable fixture-local id. */
  agentId1?: string;
  /** The second agent's `defineAgent` id. Defaults to a stable fixture-local id. */
  agentId2?: string;
  /** The `defineWorkflow` id. Defaults to a stable fixture-local id. */
  workflowId?: string;
};

export function signalGateEntry(params: SignalGateFixtureParams): string {
  const step1Id = params.step1Id ?? "step1";
  const gateId = params.gateId ?? "gate";
  const step2Id = params.step2Id ?? "step2";
  const agentId1 = params.agentId1 ?? "signal-gate-agent1";
  const agentId2 = params.agentId2 ?? "signal-gate-agent2";
  const workflowId = params.workflowId ?? "wf_signal_gate";
  const hasStep2 = params.systemPrompt2 !== undefined;

  const drainBehaviorLine =
    params.drainBehavior !== undefined
      ? `    drainBehavior: ${JSON.stringify(params.drainBehavior)},\n`
      : "";

  const agent2Block = hasStep2
    ? `
const agent2 = defineAgent({
  id: ${JSON.stringify(agentId2)},
  systemPrompt: ${JSON.stringify(params.systemPrompt2)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});
`
    : "";

  const step2Entry = hasStep2
    ? `    [${JSON.stringify(step2Id)}]: step({ agent: agent2, after: [${JSON.stringify(gateId)}] }),\n`
    : "";

  return `
import { awaitSignal, defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent1 = defineAgent({
  id: ${JSON.stringify(agentId1)},
  systemPrompt: ${JSON.stringify(params.systemPrompt1)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});
${agent2Block}
export const workflow = defineWorkflow({
  id: ${JSON.stringify(workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    [${JSON.stringify(step1Id)}]: step({ agent: agent1 }),
    [${JSON.stringify(gateId)}]: awaitSignal({
      name: ${JSON.stringify(params.signalName)},
${drainBehaviorLine}      after: [${JSON.stringify(step1Id)}],
    }),
${step2Entry}  },
});
`;
}
