// Source-entry builder for the parent -> child workflow fixture (F8):
// a parent workflow with one or more leading agent steps, one or more inline
// `childWorkflow` spawn steps, and optional trailing agent steps. Each child is
// embedded inline (an owned import) and may itself carry nested `childWorkflow`
// spawns, so the same builder renders parent, child, and grandchild rungs. The
// returned string is a `@intx/*`-importing entry module; `bundleWorkflowEntry`
// inlines it to a self-contained `.mjs` the sidecar evaluates in-child.
//
// Only the parent is deployed; the deploy step lifts each inline child to an
// internal ref and the runtime resolves it from the in-memory closure map at
// spawn time. Parameterised by the workflow ids, mail addresses, per-step
// agent prompts, and the spawn/step topology so a caller reproduces the single,
// nested, and sibling-fanout shapes off one builder.

/** A tool-less agent step in a parent or child workflow. */
export type AgentStepSpec = {
  /** The step's key in the workflow's `steps` map. */
  stepId: string;
  /** The step agent's `defineAgent` id. */
  agentId: string;
  /** The step agent's system prompt. */
  systemPrompt: string;
  /** Steps this one runs after. Omitted leaves the step unordered. */
  after?: readonly string[];
};

/** An inline child (or grandchild) workflow a spawn step embeds. */
export type InlineChildSpec = {
  /** The child's `defineWorkflow` id. */
  workflowId: string;
  /** The child's mail trigger address. */
  address: string;
  /** The child's leading agent steps. */
  steps: readonly AgentStepSpec[];
  /** The child's own inline `childWorkflow` spawns. */
  spawns?: readonly InlineChildSpawnSpec[];
};

/** A `childWorkflow` spawn step embedding an inline child. */
export type InlineChildSpawnSpec = {
  /** The spawn step's key in the enclosing workflow's `steps` map. */
  stepId: string;
  /** Steps the spawn runs after. */
  after: readonly string[];
  /** The inline child definition the spawn embeds. */
  child: InlineChildSpec;
};

export type ChildWorkflowFixtureParams = {
  /** The parent's `defineWorkflow` id. */
  workflowId: string;
  /** The parent's mail trigger address. */
  address: string;
  /** The parent's leading agent steps. */
  steps: readonly AgentStepSpec[];
  /** The parent's inline `childWorkflow` spawns. */
  spawns: readonly InlineChildSpawnSpec[];
};

function renderAgentStep(spec: AgentStepSpec): string {
  const afterClause =
    spec.after !== undefined ? `, after: ${JSON.stringify(spec.after)}` : "";
  return `    [${JSON.stringify(spec.stepId)}]: step({ agent: defineAgent({
      id: ${JSON.stringify(spec.agentId)},
      systemPrompt: ${JSON.stringify(spec.systemPrompt)},
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    })${afterClause} }),\n`;
}

function renderChildWorkflowExpr(child: InlineChildSpec): string {
  const stepEntries = child.steps.map(renderAgentStep).join("");
  const spawnEntries = (child.spawns ?? []).map(renderSpawnStep).join("");
  return `defineWorkflow({
    id: ${JSON.stringify(child.workflowId)},
    trigger: { type: "mail", to: ${JSON.stringify(child.address)} },
    steps: {
${stepEntries}${spawnEntries}    },
  })`;
}

function renderSpawnStep(spec: InlineChildSpawnSpec): string {
  return `    [${JSON.stringify(spec.stepId)}]: childWorkflow({
      definition: ${renderChildWorkflowExpr(spec.child)},
      after: ${JSON.stringify(spec.after)},
    }),\n`;
}

export function childWorkflowEntry(params: ChildWorkflowFixtureParams): string {
  const stepEntries = params.steps.map(renderAgentStep).join("");
  const spawnEntries = params.spawns.map(renderSpawnStep).join("");
  return `
import { childWorkflow, defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  steps: {
${stepEntries}${spawnEntries}  },
});
`;
}
