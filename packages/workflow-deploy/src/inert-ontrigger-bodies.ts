// Enumerate the inline trigger bodies of a FROZEN inert projection -- both
// onTrigger section bodies and childWorkflow child definitions, transitively.
//
// On the source-ref (code-sourced) deploy the hub never holds a live
// `WorkflowDefinition` -- it has only the inert `WorkflowProjectionDefinition`
// the gate froze and hashed. This module walks that frozen projection and lifts
// each inline body so the hub can stage it and pin its per-step inference
// sources. Enumeration itself is purely structural: the deploy pin reads each
// body step's declared `(provider, model)` preference through
// `readInertStepInference` when it resolves sources.
//
// The walk mirrors the runtime's per-rung rewrite exactly, so the ref each body
// is staged under equals the ref the runtime reads it back by:
//   - childWorkflow bodies are lifted at EVERY rung (the runtime re-runs
//     `rewriteInlineChildWorkflowBodies` per child run), so the enumeration
//     recurses into every lifted body;
//   - onTrigger bodies are lifted only at the TOP level (the runtime lifts them
//     once at child boot, never inside a spawned body), so an onTrigger section
//     nested inside a spawned body would reach the runtime inline and hard-fail
//     -- this module rejects it at deploy rather than staging an asset the
//     runtime will never read;
//   - a loop body is NOT lifted as an asset -- it runs in-process sharing the
//     parent env, so it needs no sources.json of its own -- but the enumeration
//     recurses INTO it to lift any childWorkflow grandchild, keyed under
//     `inlineBodyRef(loopBodyRef, childStepId)` to match the runtime's rewrite
//     of the loop body copy.
//
// It reads NOTHING off an unvalidated `unknown`: the wire projection types its
// `steps` as pass-through `unknown`, so every field this module reaches is first
// validated through an arktype. A step that claims to be an agent-bearing
// primitive (`step`/`map`) but carries no well-formed `agent.modelSources` is a
// malformed projection and throws, rather than silently pinning a fallback.
//
// Reading a nested body off an inert step is also the one part of the canonical
// step walk the inert representation has to supply itself, so it lives here as
// `inertNestedBodies` -- the counterpart of the live `nestedWorkflowBodies`.
// Every consumer that walks a frozen projection descends through it, so the
// inert descent rule has one owner the way the live one does.
//
// Both readers are EXHAUSTIVE OVER THE SAME KIND SET, and that is what the
// reader's shape here buys. The live reader switches over the typed `Primitive`
// union and ends in a `never` assignment; an inert step is `unknown` on the
// wire, so no switch over it can be typed that way. Exhaustiveness over a kind
// set is a property of a TABLE'S KEYS though, independent of how its values are
// typed -- so this reader is a `Record<Primitive["kind"], ...>` whose arms still
// take `unknown` and still probe with arktype. Keying it on the same union the
// live `never` guards makes the two readers fail together: a body-bearing
// primitive added to the live reader and forgotten here is a compile error
// rather than a container the deploy-time totality check reads as a leaf.

import { type } from "arktype";
import {
  WorkflowProjectionDefinition,
  WorkflowStep,
} from "@intx/types/sidecar";
import { inlineBodyRef } from "@intx/workflow";
import {
  LOOP_BODY_DESCENT,
  walkStepTree,
  type Primitive,
  type StepWalkDescent,
} from "@intx/workflow/definition";

/**
 * A body step's declared preferred inference-source identity -- the `(provider,
 * model)` of its agent's first `modelSources` entry. All the pin resolver needs.
 */
export interface InertBodyStepPreference {
  readonly provider: string;
  readonly model: string;
}

/**
 * A body step's inference shape as read off the inert projection: whether the
 * step is agent-bearing, and its declared `(provider, model)` preference. An
 * agent whose `modelSources` is empty reads as `{ isAgent: true, preference:
 * null }` -- distinct from a genuine non-agent step (`{ isAgent: false }`),
 * because an agent resolves and reads a source at runtime and so needs an
 * approval-checked pin even when it declares no preference, whereas a non-agent
 * step never resolves a source.
 */
export interface InertStepInference {
  readonly isAgent: boolean;
  readonly preference: InertBodyStepPreference | null;
}

/**
 * An inline trigger body (an onTrigger section or a childWorkflow child) lifted
 * out of a frozen inert projection, ready for the source-ref hub to pin per-step
 * sources and carry as a `referencedDefinitions` entry.
 */
export interface EnumeratedInertBody {
  /**
   * The body's ref -- `inlineBodyRef(projection.id, stepId)`. This is also
   * `definition.id`, the id the sidecar stages the body's `sources.json` under,
   * and the id the source-ref run child re-derives when it rewrites the
   * re-evaluated closure -- so the three agree byte-for-byte.
   */
  readonly ref: string;
  /**
   * The inline body projection, its id overridden to `ref` (matching the live
   * rewrite). Carried verbatim except for the id: the source-ref re-verify
   * recomputes the body's wire hash over the re-evaluated closure body, so this
   * must be the same inert form the closure projects to.
   */
  readonly definition: typeof WorkflowProjectionDefinition.infer;
}

// A single `modelSources` entry, canonicalized to its `(provider, model)`
// identity -- the shape the inert projector emits (`projectModelSource`).
const InertModelSource = type({ provider: "string > 0", model: "string > 0" });

// An onTrigger step carrying an inline body. `body: { inline }` fails to match a
// `{ ref }` body, so an already-ref onTrigger step is skipped (a frozen
// source-ref projection keeps its bodies inline, so in practice all match).
const InlineOnTriggerStep = type({
  kind: "'onTrigger'",
  body: { inline: "unknown" },
});

// An onTrigger step whose body is a `{ ref }` to a separately-deployed asset.
// Paired with `InlineOnTriggerStep` so a reader can tell that form -- nothing to
// descend into -- apart from an onTrigger step carrying no recognizable body
// slot at all, which is a malformed projection. `OnTriggerBody`'s ref arm types
// the ref as a plain `string`, so this matches it exactly rather than narrowing.
const RefOnTriggerStep = type({
  kind: "'onTrigger'",
  body: { ref: "string" },
});

// A childWorkflow step carrying an inline child definition. Mirrors
// `InlineOnTriggerStep`: an already-ref child (`definition: { ref }`) is skipped,
// but a frozen source-ref projection keeps its children inline.
const InlineChildWorkflowStep = type({
  kind: "'childWorkflow'",
  definition: { inline: "unknown" },
});

// A childWorkflow step whose child is a `{ ref }`. Mirrors `RefOnTriggerStep`.
const RefChildWorkflowStep = type({
  kind: "'childWorkflow'",
  definition: { ref: "string" },
});

// The agent surface the resolver reads. Undeclared keys pass through, so this
// matches a full inert agent while typing only `modelSources`.
const AgentWithModelSources = type({ modelSources: InertModelSource.array() });

// The two agent-bearing primitive shapes, mirroring `extractAgent`: a `step`
// carries `agent`, a `map` carries `step.agent`.
const StepWithAgent = type({ kind: "'step'", agent: AgentWithModelSources });
const MapWithAgent = type({
  kind: "'map'",
  step: { agent: AgentWithModelSources },
});

// Just the discriminant, to distinguish a malformed agent-bearing step from a
// legitimate non-agent primitive.
const StepKind = type({ kind: "string" });

/**
 * Reduce a validated `modelSources` list to its first entry's `(provider,
 * model)` identity, or `null` when the list is empty. Mirrors the live path's
 * `stepAgent?.inference.sources[0] ?? null`.
 */
function firstPreference(
  modelSources: readonly (typeof InertModelSource.infer)[],
): InertBodyStepPreference | null {
  const first = modelSources[0];
  return first !== undefined
    ? { provider: first.provider, model: first.model }
    : null;
}

const InertLoopStep = type({ kind: "'loop'", body: "unknown" });

/**
 * Validate one inline body value as a workflow projection. `where` names the
 * reader and the body it was reached through, so a malformed frozen projection
 * is traceable to the exact position that carried it.
 */
function validateInertBodyProjection(
  inlineBody: unknown,
  where: string,
): WorkflowProjectionDefinition {
  const validated = WorkflowProjectionDefinition(inlineBody);
  if (validated instanceof type.errors) {
    throw new Error(
      `${where} is not a valid workflow projection: ${validated.summary}`,
    );
  }
  return validated;
}

/**
 * If an inert projection step is a `loop`, return its body projection (a nested
 * inert workflow definition); otherwise null. A loop body runs in-process as a
 * child run sharing the parent's env, so its agent steps resolve their pinned
 * inference source from the same flat top-level sources map -- the source pin
 * recurses through this into loop bodies. Throws on a `loop` step whose body is
 * not a valid projection (a malformed frozen projection).
 */
export function inertLoopBody(
  stepValue: unknown,
): WorkflowProjectionDefinition | null {
  const asLoop = InertLoopStep(stepValue);
  if (asLoop instanceof type.errors) return null;
  return validateInertBodyProjection(
    asLoop.body,
    "inertLoopBody: loop step body",
  );
}

/**
 * Read the nested inert body projections one step of a single primitive kind
 * carries, filtered by `descent`. The step arrives as `unknown` -- the wire
 * projection types its steps that way -- so each arm validates what it reads.
 */
type InertNestedBodyReader = (
  stepValue: unknown,
  descent: StepWalkDescent,
) => readonly WorkflowProjectionDefinition[];

/** A leaf primitive: no nested body to descend into under any descent. */
const NO_NESTED_BODY: InertNestedBodyReader = () => [];

/**
 * The per-kind inert body readers, keyed by the LIVE `Primitive["kind"]` union
 * -- the same union the live `nestedWorkflowBodies` switch guards with its
 * `never` assignment. The keys are what make this reader exhaustive: a primitive
 * kind added to that union leaves a missing property here and fails to compile,
 * so the live reader and this one fail together rather than the inert half
 * silently reading a new container as a leaf.
 *
 * Each arm separates the three non-defect outcomes a container has -- an inline
 * body the descent wants, an inline body the descent excludes, and a `{ ref }`
 * body whose asset is deployed and walked on its own -- from the one outcome
 * that is a defect: a step that announces a container kind but carries no
 * recognizable body slot. That is a malformed frozen projection and throws,
 * because reading it as a leaf would silently shrink whatever surface the caller
 * is walking.
 *
 * The `{ ref }` check lives inside its own kind's arm, mirroring the live
 * switch's `"inline" in primitive.body`, so the two readers are structurally
 * parallel rather than accidentally in agreement.
 */
const inertNestedBodyReaders: Record<Primitive["kind"], InertNestedBodyReader> =
  {
    loop: (stepValue, descent) => {
      // A loop body is always inline -- `LoopPrimitive.body` has no `{ ref }`
      // form -- so a loop step with no body projection is malformed, not a leaf.
      const body = inertLoopBody(stepValue);
      if (body === null) {
        throw new Error(
          "inertNestedBodies: loop step carries no body; a loop body is always inline",
        );
      }
      return descent.loopBodies ? [body] : [];
    },
    onTrigger: (stepValue, descent) => {
      const asInline = InlineOnTriggerStep(stepValue);
      if (!(asInline instanceof type.errors)) {
        return descent.inlineOnTriggerBodies
          ? [
              validateInertBodyProjection(
                asInline.body.inline,
                "inertNestedBodies: inline onTrigger body",
              ),
            ]
          : [];
      }
      if (RefOnTriggerStep(stepValue) instanceof type.errors) {
        throw new Error(
          "inertNestedBodies: onTrigger step carries neither an inline body nor a { ref } body",
        );
      }
      return [];
    },
    childWorkflow: (stepValue, descent) => {
      const asInline = InlineChildWorkflowStep(stepValue);
      if (!(asInline instanceof type.errors)) {
        return descent.inlineChildWorkflowBodies
          ? [
              validateInertBodyProjection(
                asInline.definition.inline,
                "inertNestedBodies: inline childWorkflow body",
              ),
            ]
          : [];
      }
      if (RefChildWorkflowStep(stepValue) instanceof type.errors) {
        throw new Error(
          "inertNestedBodies: childWorkflow step carries neither an inline definition nor a { ref } definition",
        );
      }
      return [];
    },
    step: NO_NESTED_BODY,
    map: NO_NESTED_BODY,
    action: NO_NESTED_BODY,
    gate: NO_NESTED_BODY,
    escalation: NO_NESTED_BODY,
    awaitSignal: NO_NESTED_BODY,
    sleep: NO_NESTED_BODY,
  };

/**
 * The nested inert body projections one frozen-projection step carries,
 * filtered by `descent`. This is the inert analogue of the live
 * `nestedWorkflowBodies`, and it exists so the inert representation states the
 * descent rule in ONE place the way the live one does.
 *
 * The step's kind is validated against the closed wire enum before dispatch, so
 * an unrecognized kind throws instead of reading as a leaf. Every production
 * caller hands over a step that already passed that same enum through
 * `WorkflowSteps`' narrow, so the throw marks a projection assembled outside the
 * validated path rather than an input the wire admits.
 *
 * Indexing the `Primitive["kind"]`-keyed table with a wire-validated kind is
 * also what ties the two hand-written enumerations of the primitive set
 * together at compile time: a kind the wire admits but the live union lacks
 * fails this dispatch.
 */
export function inertNestedBodies(
  stepValue: unknown,
  descent: StepWalkDescent,
): readonly WorkflowProjectionDefinition[] {
  const parsed = WorkflowStep(stepValue);
  if (parsed instanceof type.errors) {
    throw new Error(
      `inertNestedBodies: step is not a known workflow primitive: ${parsed.summary}`,
    );
  }
  return inertNestedBodyReaders[parsed.kind](stepValue, descent);
}

/**
 * Visit every step of a frozen inert projection in `stepOrder`, recursing into
 * `loop` bodies. This is the walk that stays inside ONE FLAT STEP-ID NAMESPACE:
 * a loop body runs in-process as a child run sharing the parent's env, so its
 * steps resolve against the enclosing definition's per-step tables, while an
 * onTrigger section or childWorkflow body is lifted to its own definition with
 * tables of its own and the walk stops at that boundary.
 *
 * The traversal itself is the canonical `walkStepTree`. Only the per-rung body
 * read is supplied here, because the inert projection types its steps as
 * `unknown` and `inertLoopBody` is what validates one.
 *
 * `context` is the caller label the walk's throw is prefixed with, so a
 * malformed projection is traceable to whoever walked it.
 */
export function forEachInertLoopBodyStep(
  args: { definition: WorkflowProjectionDefinition; context: string },
  visit: (entry: { stepId: string; step: unknown }) => void,
): void {
  walkStepTree<unknown, WorkflowProjectionDefinition>({
    tree: args.definition,
    context: args.context,
    nestedTrees: (stepValue) => inertNestedBodies(stepValue, LOOP_BODY_DESCENT),
    visit: ({ stepId, step }) => {
      visit({ stepId, step });
    },
  });
}

/**
 * Every step id in a frozen inert projection's FLAT STEP-ID NAMESPACE: its own
 * `stepOrder` plus the step ids of every `loop` body it carries, transitively,
 * deduplicated in first-reach order.
 *
 * This is the domain of every per-deployment table keyed by plain step id --
 * the credentials snapshot above all. A loop iteration inherits the parent
 * run's env, so a body step authorizes against the SAME snapshot the enclosing
 * definition's steps do; a snapshot built from `stepOrder` alone therefore has
 * no entry for it and the child's authorize throws on the body's first tool
 * call. Deduplication is exact here: the ids share one namespace, so a repeated
 * id IS the same id.
 */
export function inertFlatNamespaceStepIds(args: {
  definition: WorkflowProjectionDefinition;
  context: string;
}): readonly string[] {
  const ids = new Set<string>();
  forEachInertLoopBodyStep(args, ({ stepId }) => {
    ids.add(stepId);
  });
  return [...ids];
}

/**
 * Read an inert projection step's inference shape: whether it is agent-bearing
 * and its declared `(provider, model)` preference. Mirrors `extractAgent`: a
 * `step` carries the agent directly, a `map` carries it on its inner step, and
 * any other primitive is a non-agent (`{ isAgent: false, preference: null }`).
 * A `step`/`map` that fails the agent shape is a malformed projection and
 * throws. An agent with an empty `modelSources` reads as `{ isAgent: true,
 * preference: null }`, so a caller can tell it apart from a non-agent step and
 * still pin it an approval-checked source.
 *
 * `context` is a caller label the throw prefixes with, so a malformed step is
 * traceable to whoever read it (an inline body enumeration, or the top-level
 * projection step-source pinning). Exported so both the body enumeration here
 * and the source pins in `orchestrator.ts` read a step through one validator.
 */
export function readInertStepInference(
  stepValue: unknown,
  context: string,
  stepId: string,
): InertStepInference {
  const asStep = StepWithAgent(stepValue);
  if (!(asStep instanceof type.errors)) {
    return {
      isAgent: true,
      preference: firstPreference(asStep.agent.modelSources),
    };
  }
  const asMap = MapWithAgent(stepValue);
  if (!(asMap instanceof type.errors)) {
    return {
      isAgent: true,
      preference: firstPreference(asMap.step.agent.modelSources),
    };
  }
  const kind = StepKind(stepValue);
  if (
    !(kind instanceof type.errors) &&
    (kind.kind === "step" || kind.kind === "map")
  ) {
    throw new Error(
      `${context}step ${stepId} is a ${kind.kind} primitive but carries no valid agent.modelSources`,
    );
  }
  return { isAgent: false, preference: null };
}

/**
 * Lift one inline body out of an enclosing projection: validate it, override its
 * id to `inlineBodyRef(enclosingId, stepId)` (the ref the sidecar stages
 * under and the run child re-derives). Every field other than the id rides
 * verbatim so the body's wire hash matches the re-evaluated closure's
 * projection.
 */
function liftInertBody(
  enclosingId: string,
  stepId: string,
  inlineBody: unknown,
  kind: "onTrigger" | "childWorkflow",
): EnumeratedInertBody {
  const ref = inlineBodyRef(enclosingId, stepId);
  const validatedBody = validateInertBodyProjection(
    inlineBody,
    `enumerateInertBodies: inline ${kind} body at step ${stepId}`,
  );
  const definition = { ...validatedBody, id: ref };
  return { ref, definition };
}

/**
 * Enumerate a frozen inert projection's inline trigger bodies -- onTrigger
 * sections and childWorkflow children -- transitively. Pure: it only reads
 * `projection`, validating every field it reaches. Each returned body carries
 * its ref and the inline body projection (id set to the ref). A projection with
 * no inline body yields an empty array.
 *
 * The descent mirrors the runtime's per-rung rewrite (see the module comment):
 * childWorkflow children are lifted at every depth; onTrigger sections are
 * lifted only at the top level, and a nested onTrigger section throws.
 */
export function enumerateInertBodies(
  projection: typeof WorkflowProjectionDefinition.infer,
): readonly EnumeratedInertBody[] {
  return enumerateInertBodiesAtDepth(projection, true);
}

function enumerateInertBodiesAtDepth(
  projection: typeof WorkflowProjectionDefinition.infer,
  isTopLevel: boolean,
): EnumeratedInertBody[] {
  const bodies: EnumeratedInertBody[] = [];
  for (const [stepId, stepValue] of Object.entries(projection.steps)) {
    const asOnTrigger = InlineOnTriggerStep(stepValue);
    if (!(asOnTrigger instanceof type.errors)) {
      if (!isTopLevel) {
        throw new Error(
          `enumerateInertBodies: onTrigger section at step ${stepId} is nested inside a spawned body; the runtime lifts onTrigger sections only at the top level, so a nested section reaches the runtime inline and fails. Move it to the top-level workflow.`,
        );
      }
      const lifted = liftInertBody(
        projection.id,
        stepId,
        asOnTrigger.body.inline,
        "onTrigger",
      );
      bodies.push(lifted);
      // The runtime rewrites childWorkflows inside a running onTrigger body,
      // so recurse -- but that body is no longer top level.
      bodies.push(...enumerateInertBodiesAtDepth(lifted.definition, false));
      continue;
    }
    const asChild = InlineChildWorkflowStep(stepValue);
    if (!(asChild instanceof type.errors)) {
      const lifted = liftInertBody(
        projection.id,
        stepId,
        asChild.definition.inline,
        "childWorkflow",
      );
      bodies.push(lifted);
      bodies.push(...enumerateInertBodiesAtDepth(lifted.definition, false));
      continue;
    }
    // A loop body may carry a `childWorkflow` grandchild. Unlike the two arms
    // above, the loop body itself is NOT pushed to `bodies`: it runs in-process
    // sharing the parent env, so it is never a staged asset and needs no
    // sources.json of its own (its agent steps pin into the flat top-level map).
    // Recurse into it (no longer top level) ONLY to lift its childWorkflow
    // grandchildren, keying each under its ref so the deploy stages the same ref
    // the runtime re-derives -- `inlineBodyRef(loopBodyRef, childStepId)`.
    const loopBody = inertLoopBody(stepValue);
    if (loopBody !== null) {
      const loopBodyRef = inlineBodyRef(projection.id, stepId);
      const loopBodyDefinition = { ...loopBody, id: loopBodyRef };
      bodies.push(...enumerateInertBodiesAtDepth(loopBodyDefinition, false));
    }
  }
  return bodies;
}
