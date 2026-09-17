// The canonical walk over the steps a workflow definition can execute.
//
// `stepOrder` lists only the steps of ONE definition record. A workflow's
// executable surface is larger: a `loop` carries an inline body, and an inline
// `onTrigger` section or `childWorkflow` carries a full nested definition. A
// consumer that answers "which steps run here" off `stepOrder` alone therefore
// under-counts every nested body, and a consumer that hand-rolls its own
// recursion drifts from every other one the first time a primitive kind is
// added. This module owns the traversal so neither happens.
//
// Three properties the walk is built around:
//
//   - The set of bodies to descend into is an EXPLICIT argument
//     (`StepWalkDescent`), never an implicit house rule. The two descents in
//     use are named constants with documented meanings, because a consumer's
//     answer changes with the setting and a caller that cannot name its
//     setting cannot be reconciled with another caller's.
//   - `nestedWorkflowBodies` dispatches over an EXHAUSTIVE switch with a
//     `never` assignment. A newly-added primitive kind fails at compile time
//     rather than silently reading as a leaf: the deploy-time capability walk
//     descends through here to collect a nested closure's grants, and a missed
//     container kind there is a silent, fail-open authorization gap. The
//     compiler, not a remembered call site, owns that coverage.
//   - The walk knows every step's ancestor chain, so it HANDS THAT CHAIN OVER
//     (`StepWalkEntry.path`) instead of leaving a consumer to rebuild it from
//     the order the walk calls back in. A rebuilt position would pin the
//     traversal's internal call order as an unwritten contract.
//
// The traversal core (`walkStepTree`) is generic over the step and tree types
// so it also serves the inert wire projection, whose steps are `unknown` and
// are validated by the caller as it descends. Only the live-definition
// descent (`nestedWorkflowBodies`) can be typed, so only it carries the
// exhaustive switch.

import type { Primitive } from "./primitives";
import type { WorkflowDefinition } from "./workflow";

/**
 * The part of a workflow definition the walk reads: the ordered step ids and
 * the record they index. The live `WorkflowDefinition` satisfies it, and so
 * does the inert wire projection (whose step values are `unknown`), so one
 * traversal serves both representations.
 */
export interface StepTree<TStep> {
  readonly stepOrder: readonly string[];
  readonly steps: Readonly<Record<string, TStep>>;
}

/**
 * Which nested bodies the walk descends into. Every field is required: a walk
 * whose answer depends on a setting the caller never stated cannot be
 * reconciled with another caller's answer, so there is no default.
 */
export interface StepWalkDescent {
  /**
   * Descend into a `loop` primitive's inline body. A loop body runs in-process
   * as a child run sharing the parent's env, and its step ids share the
   * enclosing definition's flat step-id namespace.
   */
  readonly loopBodies: boolean;
  /**
   * Descend into an `onTrigger` section's `{ inline }` body. A `{ ref }` body
   * is a separately-deployed asset with nothing to descend into, so it is never
   * reached regardless of this setting. An inline section body is lifted to its
   * own definition at deploy, so its step ids are a namespace of their own and
   * not part of the enclosing definition's flat namespace.
   */
  readonly inlineOnTriggerBodies: boolean;
  /**
   * Descend into a `childWorkflow`'s `{ inline }` definition. As with an
   * onTrigger section, a `{ ref }` child has nothing to descend into, and an
   * inline child is lifted to its own definition with its own step-id
   * namespace.
   */
  readonly inlineChildWorkflowBodies: boolean;
}

/**
 * Descend into every body form a deployment carries: loop bodies, inline
 * onTrigger section bodies, and inline childWorkflow definitions.
 *
 * This is the setting that yields EVERY STEP ID THAT CAN EXECUTE IN THIS
 * DEPLOYMENT. A step reached under this descent runs as part of the deployment
 * the walked definition describes -- in-process for a loop body, as a spawned
 * child run for a lifted section or child body -- so a consumer asking what the
 * deployment can run, rather than what one definition record lists, uses this
 * one. The deploy-time capability walk uses it, because an operator approving a
 * deployment must approve everything it can run.
 *
 * It deliberately crosses the lifted-body boundary, so the ids it reaches span
 * more than one step-id namespace; see {@link executableStepIds}.
 */
export const EXECUTABLE_STEP_DESCENT: StepWalkDescent = Object.freeze({
  loopBodies: true,
  inlineOnTriggerBodies: true,
  inlineChildWorkflowBodies: true,
});

/**
 * Descend into loop bodies and stop at the lifted-body boundary.
 *
 * This is the setting that yields every step id in ONE FLAT STEP-ID NAMESPACE.
 * A loop body's steps resolve against the enclosing definition's flat map (the
 * body shares the parent env), whereas an inline onTrigger section or
 * childWorkflow body is lifted to its own definition and keyed under its own
 * ref, so its ids belong to a different map. A consumer building or reading a
 * per-definition map keyed by plain step id uses this one; the deploy's
 * per-step inference-source pin does.
 */
export const LOOP_BODY_DESCENT: StepWalkDescent = Object.freeze({
  loopBodies: true,
  inlineOnTriggerBodies: false,
  inlineChildWorkflowBodies: false,
});

/**
 * The chain of step ids one walked step was reached through: the top-rung step
 * first, the step itself last, one entry per rung the walk descended.
 *
 * The type is non-empty because a step's own id is always the last entry. A
 * top-rung step's path is `[stepId]` alone, so a consumer reads `path[0]` for
 * the top-rung step an entry descends from with no length check of its own.
 */
export type StepWalkPath = readonly [string, ...string[]];

/** One step the walk reached, with the tree it is an entry of. */
export interface StepWalkEntry<TStep, TTree> {
  readonly stepId: string;
  readonly step: TStep;
  /**
   * The tree `stepId` keys into: the walked definition itself for a top-rung
   * step, or the nested body the walk descended into for a deeper one.
   */
  readonly tree: TTree;
  /**
   * The chain of step ids this step was reached through, ending in `stepId`.
   * Two nested bodies may legitimately carry the same step id, so the path --
   * not `stepId` alone -- is what names a step's position in the walk.
   *
   * The chain is rooted at the tree the walk was GIVEN, not at any enclosing
   * definition the caller happens to hold. {@link walkNestedWorkflowSteps}
   * starts one walk per nested body, so those bodies' steps are rooted at the
   * body and the enclosing primitive's step id is not on the path.
   */
  readonly path: StepWalkPath;
}

export interface StepWalkArgs<TStep, TTree extends StepTree<TStep>> {
  readonly tree: TTree;
  /**
   * The nested bodies to descend into for a given step, already filtered by the
   * caller's `StepWalkDescent`. Supplied by the caller because the descent is
   * the one part that differs per representation: a live definition reads its
   * bodies off the typed primitive ({@link nestedWorkflowBodies}), while an
   * inert wire projection must validate each body as it reaches it.
   */
  readonly nestedTrees: (step: TStep) => readonly TTree[];
  /**
   * Caller label prefixed to the walk's throw, so a malformed definition is
   * traceable to whoever walked it.
   */
  readonly context: string;
  readonly visit: (entry: StepWalkEntry<TStep, TTree>) => void;
}

/**
 * Visit every step of `tree` and of the nested bodies `nestedTrees` selects, in
 * pre-order: a step is visited before the bodies it carries, and a body's steps
 * are visited in its own `stepOrder` before the next sibling step. Consumers
 * that accumulate into an ordered result depend on that order.
 *
 * Each entry carries the {@link StepWalkEntry.path} it was reached through, so
 * a consumer that needs a step's position reads it rather than reconstructing
 * it from the order the walk happens to call `visit` and `nestedTrees` in.
 *
 * A `stepOrder` entry with no matching `steps` record entry throws. The
 * definition validator forecloses it, so reaching the throw means a
 * hand-assembled or tampered definition, which must fail loud rather than walk
 * a surface that silently omits a step.
 */
export function walkStepTree<TStep, TTree extends StepTree<TStep>>(
  args: StepWalkArgs<TStep, TTree>,
): void {
  const walk = (tree: TTree, parentPath: StepWalkPath | readonly []): void => {
    for (const stepId of tree.stepOrder) {
      const step = tree.steps[stepId];
      if (step === undefined) {
        throw new Error(
          `${args.context}step ${stepId} listed in stepOrder is missing from steps`,
        );
      }
      const path: StepWalkPath = [...parentPath, stepId];
      args.visit({ stepId, step, tree, path });
      for (const nested of args.nestedTrees(step)) {
        walk(nested, path);
      }
    }
  };
  walk(args.tree, []);
}

/**
 * The nested body definitions a live primitive carries, filtered by `descent`.
 * A container whose body is a `{ ref }` yields nothing: the referenced asset is
 * deployed and walked on its own, and there is no inline tree to descend into.
 *
 * The switch is EXHAUSTIVE: a newly-added primitive kind fails the `never`
 * assignment below at compile time, forcing the author to decide whether it
 * carries a nested body rather than letting it read as a leaf. That matters
 * most for the deploy-time capability walk, whose per-step approval must cover
 * everything a step can run -- a container kind missed here would drop a nested
 * closure's grants silently, and `director:` grants are not re-gated at
 * runtime, so the gap fails open.
 */
export function nestedWorkflowBodies(
  primitive: Primitive,
  descent: StepWalkDescent,
): readonly WorkflowDefinition[] {
  switch (primitive.kind) {
    case "loop":
      return descent.loopBodies ? [primitive.body] : [];
    case "onTrigger":
      return descent.inlineOnTriggerBodies && "inline" in primitive.body
        ? [primitive.body.inline]
        : [];
    case "childWorkflow":
      return descent.inlineChildWorkflowBodies &&
        "inline" in primitive.definition
        ? [primitive.definition.inline]
        : [];
    case "step":
    case "map":
    case "action":
    case "gate":
    case "escalation":
    case "awaitSignal":
    case "sleep":
      // Leaf primitives: no nested body to descend into.
      return [];
    default: {
      const exhaustive: never = primitive;
      throw new Error(
        `workflow step walk: unhandled primitive kind ${JSON.stringify(
          (exhaustive as { kind: string }).kind,
        )}`,
      );
    }
  }
}

export interface WorkflowStepWalkArgs {
  readonly descent: StepWalkDescent;
  /** See {@link StepWalkArgs.context}. */
  readonly context: string;
  readonly visit: (entry: StepWalkEntry<Primitive, WorkflowDefinition>) => void;
}

/**
 * Visit every step of a live workflow definition, plus every step of the nested
 * bodies `descent` selects, in the pre-order {@link walkStepTree} defines.
 */
export function walkWorkflowSteps(
  args: WorkflowStepWalkArgs & { readonly definition: WorkflowDefinition },
): void {
  walkStepTree<Primitive, WorkflowDefinition>({
    tree: args.definition,
    nestedTrees: (primitive) => nestedWorkflowBodies(primitive, args.descent),
    context: args.context,
    visit: args.visit,
  });
}

/**
 * Visit every step INSIDE a single primitive's nested bodies, transitively, and
 * not the primitive itself. The caller that already holds the primitive handles
 * it directly and uses this for everything it can run underneath.
 */
export function walkNestedWorkflowSteps(
  args: WorkflowStepWalkArgs & { readonly primitive: Primitive },
): void {
  for (const body of nestedWorkflowBodies(args.primitive, args.descent)) {
    walkWorkflowSteps({ ...args, definition: body });
  }
}

/**
 * Every step id that can execute in the deployment this definition describes:
 * the walk under {@link EXECUTABLE_STEP_DESCENT}, deduplicated, in first-reach
 * order.
 *
 * Deduplication is what makes this a set of ids rather than a list of steps.
 * That is exact within one flat namespace (a loop body's ids resolve against
 * the enclosing definition's map, so a repeated id IS the same id), and it is a
 * deliberate flattening across the lifted-body boundary: an inline onTrigger or
 * childWorkflow body keys its steps under its own ref, so two steps in
 * different bodies may legitimately share an id and appear here once. A
 * consumer that must keep those apart walks with an explicit descent and reads
 * each entry's `tree` instead.
 */
export function executableStepIds(
  definition: WorkflowDefinition,
): readonly string[] {
  const ids = new Set<string>();
  walkWorkflowSteps({
    definition,
    descent: EXECUTABLE_STEP_DESCENT,
    context: "executableStepIds: ",
    visit: ({ stepId }) => {
      ids.add(stepId);
    },
  });
  return [...ids];
}
