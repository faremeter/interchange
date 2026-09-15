// Source-entry builder for a top-level `childWorkflow` whose child holds a
// gate nothing can answer.
//
// The child is a terminal child: it carries no address of its own, and the
// spawner awaits its terminal rather than driving it across parks. An untimed
// `awaitSignal` there therefore waits on a signal that can never arrive. The
// deployment must fail the run at the gate instead, and no approval may reach
// the tenant.
//
// `timedGate` renders the same shape with a deadline, which is answerable in
// process and must still run: it is the boundary the refusal must not cross.
//
// `askTool` renders the shape the issue actually reports: instead of an author
// gate, the child's step carries a tool marked for approval, so the park is a
// control-plane suspension rather than a named signal. That is the only shape
// that would register a correlation with the hub if it were allowed to park,
// which is what makes "no approval reached an operator" checkable.

import path from "node:path";

// Absolute, so the bundler inlines the sibling module rather than leaving a
// bare specifier the sidecar closure cannot resolve.
const MAIL_TOOL_MODULE = path.join(import.meta.dir, "mail-tool.ts");

export type ChildWorkflowUnanswerableGateParams = {
  /** The mail trigger's `to` address the deployment routes on. */
  address: string;
  /** The `defineWorkflow` id of the outer workflow. */
  workflowId: string;
  /** The child `defineWorkflow` id. */
  childWorkflowId: string;
  /** The author signal name the child waits on, never delivered. */
  signalName: string;
  /**
   * Give the child's gate a deadline, routing to the child's trailing step on
   * expiry. The gate then resolves on its own timer with no upstream
   * involvement, so the run must complete rather than being refused.
   */
  timedGate?: boolean;
  /**
   * Give the child a tool-bearing agent step marked for approval instead of an
   * author gate, so the child parks on the control plane rather than on a name
   * the author chose.
   */
  askTool?: boolean;
};

export function childWorkflowUnanswerableGateEntry(
  params: ChildWorkflowUnanswerableGateParams,
): string {
  if (params.askTool === true) return askToolEntry(params);

  const gate =
    params.timedGate === true
      ? `awaitSignal({ name: ${JSON.stringify(params.signalName)}, timeout: 2000, onTimeout: "settle" })`
      : `awaitSignal({ name: ${JSON.stringify(params.signalName)} })`;
  const childSteps =
    params.timedGate === true
      ? `    hold: ${gate},\n    settle: step({ agent: ${agentBlock("child-settle-agent")}, after: ["hold"] }),`
      : `    hold: ${gate},`;
  return `
import { awaitSignal, childWorkflow, defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const child = defineWorkflow({
  id: ${JSON.stringify(params.childWorkflowId)},
  trigger: { type: "manual" },
  steps: {
${childSteps}
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    spawn: childWorkflow({ definition: child }),
    after: step({ agent: ${agentBlock("after-agent")}, after: ["spawn"] }),
  },
});
`;
}

/**
 * The child's step carries the approval-marked mail tool, so its call suspends
 * on the control plane instead of waiting on an author-chosen name.
 */
function askToolEntry(params: ChildWorkflowUnanswerableGateParams): string {
  return `
import { childWorkflow, defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { mailSendTool } from ${JSON.stringify(MAIL_TOOL_MODULE)};

const child = defineWorkflow({
  id: ${JSON.stringify(params.childWorkflowId)},
  trigger: { type: "manual" },
  steps: {
    hold: step({
      agent: defineAgent({
        id: "child-ask-agent",
        systemPrompt: "Call the tool.",
        tools: [mailSendTool("ask")],
        capabilities: [],
        inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
      }),
    }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
    spawn: childWorkflow({ definition: child }),
    after: step({ agent: ${agentBlock("after-agent")}, after: ["spawn"] }),
  },
});
`;
}

function agentBlock(id: string): string {
  return `defineAgent({
  id: ${JSON.stringify(id)},
  systemPrompt: ${JSON.stringify(`${id} agent`)},
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
})`;
}
