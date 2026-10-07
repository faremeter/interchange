// Structural extraction of inline onTrigger section bodies.
//
// An onTrigger primitive carries its section body either inline (the authored
// form) or as a `{ ref }` to a separately-deployed body definition. The
// runtime only dispatches a `{ ref }` body -- an inline body reaching the
// runtime is a deploy-step bug (see `runOnTrigger`). This module performs the
// pure structural rewrite: replace each inline body with a `{ ref }` and
// return the extracted body definitions.
//
// No deploy machinery here (no capability walk, no source-pinning, no hub
// write), so callers that run it over a RE-EVALUATED closure -- the source-ref
// run child and the sidecar deploy router -- share one exact structural
// rewrite.

import type { Primitive, WorkflowDefinition } from "./definition/index";
import type { LoopFnRegistry } from "./runtime/env";

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
 * Derive an inline body's ref from its owning workflow id and the step id that
 * carries it. The SINGLE owner of the `<workflowId>__<stepId>` scheme, shared
 * by every inline-body kind (onTrigger sections, childWorkflow children, loop
 * bodies). Refs never collide because each `__`-joined segment is atomic (a
 * step id may not contain `__`, enforced in `normalize`) and a step id is
 * exactly one primitive kind. Atomicity matters once a ref spans more than two
 * segments -- a loop body's `childWorkflow` grandchild is
 * `<workflowId>__<loopStepId>__<childStepId>` -- so a hub recording sources
 * under this ref and a run child looking them up agree byte-for-byte.
 */
export function inlineBodyRef(workflowId: string, stepId: string): string {
  return `${workflowId}__${stepId}`;
}

/**
 * Replace each inline onTrigger body with a `{ ref }` and return the
 * extracted body definitions (each body's id is its ref). Pure and
 * side-effect-free. With no inline body, returns the workflow unchanged and
 * an empty `bodies`.
 */
export function rewriteInlineOnTriggerBodies(
  workflow: WorkflowDefinition,
): OnTriggerBodyRewrite {
  const steps: Record<string, Primitive> = { ...workflow.steps };
  const bodies: ExtractedOnTriggerBody[] = [];
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.kind !== "onTrigger") continue;
    if (!("inline" in primitive.body)) continue;
    const ref = inlineBodyRef(workflow.id, stepId);
    bodies.push({ ref, definition: { ...primitive.body.inline, id: ref } });
    steps[stepId] = { ...primitive, body: { ref } };
  }
  if (bodies.length === 0) {
    return { workflow, bodies: [] };
  }
  return { workflow: { ...workflow, steps }, bodies };
}

export interface ExtractedLoopBody {
  /** The body's ref -- `<workflowId>__<stepId>` -- and the id of `definition`. */
  readonly ref: string;
  /** The loop body lifted to a standalone definition (its id is `ref`). */
  readonly definition: WorkflowDefinition;
}

/**
 * Collect each `loop` primitive's inline body as a `{ ref, definition }` pair,
 * WITHOUT rewriting the workflow. A loop keeps its body inline -- both hash
 * layers project it inline, so replacing it with a `{ ref }` would change
 * every existing loop's hash. This enumerator mints a fresh copy
 * (`{ ...body, id: ref }`) for the runtime bodies map and leaves
 * `primitive.body` byte-identical. Pure and side-effect-free.
 *
 * The scan RECURSES into each loop body so a nested loop's body is registered
 * too, under `inlineBodyRef(<parentBodyRef>, <innerLoopId>)` -- the exact ref
 * `runLoop` derives at runtime. A loop body inherits the parent env, so an
 * inner loop resolves its ref from this single top-level map; the map must
 * therefore carry every depth. The recursion follows loop bodies only: a
 * `childWorkflow` inside a loop body is a separate child run whose own loops
 * enumerate when its run boots. It also stays a pure body LIFT (grandchildren
 * still inline) -- the host rewrites each returned body's `childWorkflow`
 * children itself.
 */
export function enumerateInlineLoopBodies(
  workflow: WorkflowDefinition,
): readonly ExtractedLoopBody[] {
  const bodies: ExtractedLoopBody[] = [];
  for (const [stepId, primitive] of Object.entries(workflow.steps)) {
    if (primitive.kind !== "loop") continue;
    const ref = inlineBodyRef(workflow.id, stepId);
    const definition: WorkflowDefinition = { ...primitive.body, id: ref };
    bodies.push({ ref, definition });
    bodies.push(...enumerateInlineLoopBodies(definition));
  }
  return bodies;
}

/**
 * Force-resolve every loop `while`/`carry` ref reachable from these
 * definitions, so a missing loop fn surfaces at establish rather than mid-run.
 * Recurses into a loop's inline body. The caller passes lifted
 * onTrigger/childWorkflow bodies separately -- they are `{ ref }` in the
 * enclosing definition and this walk does not descend into them.
 */
export function eagerlyResolveLoopFns(
  definitions: readonly WorkflowDefinition[],
  loopFns: LoopFnRegistry,
): void {
  const visit = (def: WorkflowDefinition): void => {
    for (const step of Object.values(def.steps)) {
      if (step.kind === "loop") {
        // Each call throws (fail closed) if the ref names no export, or an
        // export that is not a function.
        loopFns(step.while);
        loopFns(step.carry);
        visit(step.body);
      }
    }
  };
  for (const def of definitions) visit(def);
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
 * Replace each inline `childWorkflow` definition with a `{ ref }` and return
 * the extracted child definitions (each child's id is its ref). The
 * counterpart to {@link rewriteInlineOnTriggerBodies}: pure and
 * side-effect-free, minting refs through the same {@link inlineBodyRef}
 * scheme -- a step carries at most one of an onTrigger section or a
 * childWorkflow, so the two rewriters never collide. The runtime resolves a
 * `{ ref }` child from an in-memory map keyed by the ref, so the host lifts
 * these bodies at child boot. With no inline child, returns the workflow
 * unchanged and an empty `bodies`.
 */
export function rewriteInlineChildWorkflowBodies(
  workflow: WorkflowDefinition,
): ChildWorkflowBodyRewrite {
  const steps: Record<string, Primitive> = { ...workflow.steps };
  const bodies: ExtractedChildWorkflowBody[] = [];
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.kind !== "childWorkflow") continue;
    if (!("inline" in primitive.definition)) continue;
    const ref = inlineBodyRef(workflow.id, stepId);
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
