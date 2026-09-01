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

import { type } from "arktype";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { inlineBodyRef } from "@intx/workflow";

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

// A childWorkflow step carrying an inline child definition. Mirrors
// `InlineOnTriggerStep`: an already-ref child (`definition: { ref }`) is skipped,
// but a frozen source-ref projection keeps its children inline.
const InlineChildWorkflowStep = type({
  kind: "'childWorkflow'",
  definition: { inline: "unknown" },
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
 * If an inert projection step is a `loop`, return its body projection (a nested
 * inert workflow definition); otherwise null. A loop body runs in-process as a
 * child run sharing the parent's env, so its agent steps resolve their pinned
 * inference source from the same flat top-level sources map -- the source pin
 * recurses through this into loop bodies. Throws on a `loop` step whose body is
 * not a valid projection (a malformed frozen projection).
 */
export function inertLoopBody(
  stepValue: unknown,
): typeof WorkflowProjectionDefinition.infer | null {
  const asLoop = InertLoopStep(stepValue);
  if (asLoop instanceof type.errors) return null;
  const body = WorkflowProjectionDefinition(asLoop.body);
  if (body instanceof type.errors) {
    throw new Error(
      `inertLoopBody: loop step body is not a valid workflow projection: ${body.summary}`,
    );
  }
  return body;
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
  const validatedBody = WorkflowProjectionDefinition(inlineBody);
  if (validatedBody instanceof type.errors) {
    throw new Error(
      `enumerateInertBodies: inline ${kind} body at step ${stepId} is not a valid workflow projection: ${validatedBody.summary}`,
    );
  }
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
