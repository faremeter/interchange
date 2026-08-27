import type { SidecarCapabilityRule } from "@intx/types";

import type { Primitive, WorkflowDefinition } from "./definition/index";

export function collectSidecarCapabilityRules(
  definition: WorkflowDefinition,
): readonly SidecarCapabilityRule[] {
  const rules: SidecarCapabilityRule[] = [];
  collectDefinitionRules(definition, rules);
  const uniqueRules = new Map<string, SidecarCapabilityRule>();
  for (const rule of rules) {
    const key = JSON.stringify([rule.capability, rule.effect]);
    if (!uniqueRules.has(key)) uniqueRules.set(key, rule);
  }
  return [...uniqueRules.values()];
}

function collectDefinitionRules(
  definition: WorkflowDefinition,
  rules: SidecarCapabilityRule[],
): void {
  const capabilities = definition.sidecarPlacement?.capabilities ?? [];
  rules.push(...capabilities.map((rule) => ({ ...rule })));
  for (const stepId of definition.stepOrder) {
    const primitive = definition.steps[stepId];
    if (primitive !== undefined) collectPrimitiveRules(primitive, rules);
  }
}

function collectPrimitiveRules(
  primitive: Primitive,
  rules: SidecarCapabilityRule[],
): void {
  // onTrigger and childWorkflow refs are internal post-extraction handles with
  // no body to inspect. The approval-time projection currently runs before
  // extraction, while both bodies are inline. If that ordering changes, callers
  // must resolve refs for this walk rather than silently omitting their rules.
  switch (primitive.kind) {
    case "loop":
      collectDefinitionRules(primitive.body, rules);
      return;
    case "onTrigger":
      if ("inline" in primitive.body) {
        collectDefinitionRules(primitive.body.inline, rules);
      }
      return;
    case "childWorkflow":
      if ("inline" in primitive.definition) {
        collectDefinitionRules(primitive.definition.inline, rules);
      }
      return;
    case "step":
    case "map":
    case "action":
    case "gate":
    case "escalation":
    case "awaitSignal":
    case "sleep":
      return;
    default: {
      const exhaustive: never = primitive;
      return exhaustive;
    }
  }
}
