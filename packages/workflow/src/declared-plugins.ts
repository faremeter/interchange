// Enumerate the plugin-package names a workflow's agents declare.
//
// A plugin package contributes no agent-visible tool factory: its
// `definePlugin` factory reaches an agent only through `env.plugins`, so
// the only record of which plugin packages a workflow uses is the
// per-agent `AgentDefinition.plugins` list. The deploy-time probe needs
// that union up front -- before the capability walk runs -- so it can load
// each declared plugin's static tool `definitions` from the materialized
// closure and surface the plugin-contributed tool grants into the walk.
//
// The traversal mirrors the capability walk's agent extraction (step and
// map carry an agent; loop, onTrigger, and inline childWorkflow bodies are
// nested definitions whose own agents are collected recursively). A
// by-`ref` body is an independent asset with its own approval surface and
// is not descended into here, matching the walk.

import type { Primitive, WorkflowDefinition } from "./definition/index";

/**
 * Collect the deduplicated union of every plugin-package name declared by
 * any agent reachable in the definition, including agents nested in loop,
 * inline onTrigger, and inline childWorkflow bodies. Order is deterministic
 * (first-seen) so a caller building a load plan is reproducible.
 */
export function collectDeclaredPluginNames(
  definition: WorkflowDefinition,
): string[] {
  const names = new Set<string>();
  collectFromDefinition(definition, names);
  return [...names];
}

function collectFromDefinition(
  definition: WorkflowDefinition,
  names: Set<string>,
): void {
  for (const stepId of definition.stepOrder) {
    const primitive = definition.steps[stepId];
    if (primitive === undefined) continue;
    collectFromPrimitive(primitive, names);
  }
}

function collectFromPrimitive(primitive: Primitive, names: Set<string>): void {
  switch (primitive.kind) {
    case "step":
      addAgentPlugins(primitive.agent.plugins, names);
      return;
    case "map":
      addAgentPlugins(primitive.step.agent.plugins, names);
      return;
    case "loop":
      collectFromDefinition(primitive.body, names);
      return;
    case "onTrigger":
      if ("inline" in primitive.body) {
        collectFromDefinition(primitive.body.inline, names);
      }
      return;
    case "childWorkflow":
      if ("inline" in primitive.definition) {
        collectFromDefinition(primitive.definition.inline, names);
      }
      return;
    default:
      // Non-agent, non-nesting primitives (gate, awaitSignal, sleep,
      // escalation, action) declare no plugins.
      return;
  }
}

function addAgentPlugins(
  plugins: readonly string[] | undefined,
  names: Set<string>,
): void {
  for (const name of plugins ?? []) {
    names.add(name);
  }
}
