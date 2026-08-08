// Structural extraction of inline onTrigger section bodies.
//
// An onTrigger primitive carries its section body either inline (the authored
// form) or as a `{ ref }` to a separately-deployed body definition. The
// runtime only dispatches a `{ ref }` body -- an inline body reaching the
// runtime is a deploy-step bug (see `runOnTrigger`). This module performs the
// pure structural rewrite: replace each inline body with a `{ ref }` and return
// the extracted body definitions.
//
// It carries NO deploy machinery (no capability walk, no source-pinning, no hub
// write), so the callers that run it over a RE-EVALUATED closure -- the
// source-ref run child and the sidecar deploy router, which have neither a
// director registry nor the operator approval set -- share the exact rewrite
// the live-authored orchestrator (`extractOnTriggerBodies`) layers its
// walk/pin/write onto.

import type { Primitive, WorkflowDefinition } from "./definition/index";

export interface ExtractedOnTriggerBody {
  /** The body's ref -- `<workflowId>__<stepId>` -- and the id of `definition`. */
  readonly ref: string;
  /** The inline body lifted to a standalone definition (its id is `ref`). */
  readonly definition: WorkflowDefinition;
}

export interface OnTriggerBodyRewrite {
  /** The workflow with every inline onTrigger body replaced by a `{ ref }`. */
  readonly workflow: WorkflowDefinition;
  /** The extracted body definitions, one per rewritten inline body. */
  readonly bodies: readonly ExtractedOnTriggerBody[];
}

/**
 * Replace each inline onTrigger body with a `{ ref }` and return the extracted
 * body definitions (each body's id is its ref). Pure and side-effect-free: no
 * walk, no pin, no write. When the workflow has no inline onTrigger body the
 * original object is returned unchanged with an empty `bodies`.
 */
export function rewriteInlineOnTriggerBodies(
  workflow: WorkflowDefinition,
): OnTriggerBodyRewrite {
  const steps: Record<string, Primitive> = { ...workflow.steps };
  const bodies: ExtractedOnTriggerBody[] = [];
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.kind !== "onTrigger") continue;
    if (!("inline" in primitive.body)) continue;
    const ref = `${workflow.id}__${stepId}`;
    bodies.push({ ref, definition: { ...primitive.body.inline, id: ref } });
    steps[stepId] = { ...primitive, body: { ref } };
  }
  if (bodies.length === 0) {
    return { workflow, bodies: [] };
  }
  return { workflow: { ...workflow, steps }, bodies };
}
