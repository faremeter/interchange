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
// director registry nor the operator approval set -- share one exact structural
// rewrite, kept separate from the capability walk and source-pinning the deploy
// layers on elsewhere.

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
 * Derive an onTrigger section body's ref from its owning workflow id and the
 * step id that carries the section. This is the SINGLE owner of the
 * `<workflowId>__<stepId>` scheme: the live rewrite here and the source-ref
 * hub's inert-body enumerator both mint refs through it, and the source-ref run
 * child re-derives the same ref when it rewrites the re-evaluated closure
 * (`run-child`). A hub that stages a body's `sources.json` under this ref and a
 * run child that reads it back must agree byte-for-byte, so the scheme lives in
 * one place rather than being re-spelled at each site.
 */
export function onTriggerBodyRef(workflowId: string, stepId: string): string {
  return `${workflowId}__${stepId}`;
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
    const ref = onTriggerBodyRef(workflow.id, stepId);
    bodies.push({ ref, definition: { ...primitive.body.inline, id: ref } });
    steps[stepId] = { ...primitive, body: { ref } };
  }
  if (bodies.length === 0) {
    return { workflow, bodies: [] };
  }
  return { workflow: { ...workflow, steps }, bodies };
}

export interface ExtractedChildWorkflowBody {
  /** The body's ref -- `<workflowId>__<stepId>` -- and the id of `definition`. */
  readonly ref: string;
  /** The inline child lifted to a standalone definition (its id is `ref`). */
  readonly definition: WorkflowDefinition;
}

export interface ChildWorkflowBodyRewrite {
  /** The workflow with every inline childWorkflow definition replaced by a `{ ref }`. */
  readonly workflow: WorkflowDefinition;
  /** The extracted child definitions, one per rewritten inline child. */
  readonly bodies: readonly ExtractedChildWorkflowBody[];
}

/**
 * Replace each inline `childWorkflow` definition with a `{ ref }` and return the
 * extracted child definitions (each child's id is its ref). The childWorkflow
 * counterpart to {@link rewriteInlineOnTriggerBodies}: pure and
 * side-effect-free (no walk, no pin, no write), and it mints refs through the
 * same {@link onTriggerBodyRef} `<workflowId>__<stepId>` scheme -- a step
 * carries at most one of an onTrigger section or a childWorkflow, so the two
 * rewriters never collide on a ref. The runtime dispatches a `{ ref }` child by
 * resolving the extracted definition from an in-memory map keyed by the ref, so
 * the host lifts these bodies at child boot and never reads a separate on-disk
 * asset. When the workflow has no inline childWorkflow the original object is
 * returned unchanged with an empty `bodies`.
 */
export function rewriteInlineChildWorkflowBodies(
  workflow: WorkflowDefinition,
): ChildWorkflowBodyRewrite {
  const steps: Record<string, Primitive> = { ...workflow.steps };
  const bodies: ExtractedChildWorkflowBody[] = [];
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.kind !== "childWorkflow") continue;
    if (!("inline" in primitive.definition)) continue;
    const ref = onTriggerBodyRef(workflow.id, stepId);
    bodies.push({
      ref,
      definition: { ...primitive.definition.inline, id: ref },
    });
    steps[stepId] = { ...primitive, definition: { ref } };
  }
  if (bodies.length === 0) {
    return { workflow, bodies: [] };
  }
  return { workflow: { ...workflow, steps }, bodies };
}
