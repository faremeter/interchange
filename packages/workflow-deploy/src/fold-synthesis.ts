// Agent-to-workflow fold: reading a folded definition's launch body.
//
// `extractFoldedBody` reads the launch-relevant fields back out of a folded
// single-step `workflow.json`. The inverse builder, `synthesizeFoldedWorkflow`,
// is test-only and lives in `@intx/workflow-deploy/testing`.

import type { GrantRequirement } from "@intx/types";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { WorkflowDefinition } from "@intx/workflow/definition";

/** The launch-relevant body a single-step definition carries. */
export interface FoldedBody {
  readonly systemPrompt: string;
  readonly toolPackagePins: readonly ToolPackagePin[];
  readonly grantRequirements: readonly GrantRequirement[];
  /**
   * The step agent's declared model, or null when it declares none. A
   * definition with no `modelRequirements` manifest resolves its inference
   * sources against the catalog from this model instead.
   */
  readonly model: string | null;
}

/**
 * Read the launch body back out of a folded workflow definition -- the inverse
 * of `synthesizeFoldedWorkflow`. A folded definition is a single `step`-kind
 * primitive carrying the agent; the system prompt and tool-package pins live on
 * that agent, the grant requirements on the envelope. Raises if the definition
 * is not that single-step shape, so a malformed `workflow.json` surfaces here
 * rather than launching a broken instance.
 */
export function extractFoldedBody(definition: WorkflowDefinition): FoldedBody {
  const [stepId, ...rest] = definition.stepOrder;
  if (stepId === undefined || rest.length > 0) {
    throw new Error(
      `folded definition ${definition.id} is not single-step (${String(
        definition.stepOrder.length,
      )} steps)`,
    );
  }
  const primitive = definition.steps[stepId];
  if (primitive === undefined || primitive.kind !== "step") {
    throw new Error(
      `folded definition ${definition.id} step ${stepId} is not a step primitive`,
    );
  }
  return {
    systemPrompt: primitive.agent.systemPrompt,
    toolPackagePins: primitive.agent.toolPackagePins ?? [],
    grantRequirements: definition.grantRequirements ?? [],
    model: primitive.agent.inference.sources[0]?.model ?? null,
  };
}
