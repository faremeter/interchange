// Enumerate the inline onTrigger section bodies of a FROZEN inert projection.
//
// The live-authored deploy path holds the live `WorkflowDefinition` and lifts
// its inline onTrigger bodies with `extractOnTriggerBodies` (see
// `orchestrator.ts`), reading each body agent's declared inference preference
// straight off the live `AgentDefinition`. The source-ref (code-sourced) deploy
// path never holds the live definition -- the hub has only the inert
// `WorkflowProjectionDefinition` the gate froze and hashed. This module is the
// source-ref counterpart: it walks that frozen projection, lifts each inline
// onTrigger body, and surfaces each body step's declared `(provider, model)`
// preference from the projection's `modelSources` so the hub can pin per-body
// inference sources through the SAME resolver + approval gate the live path uses
// (`pickStepInferenceSource`).
//
// It reads NOTHING off an unvalidated `unknown`: the wire projection types its
// `steps` as pass-through `unknown`, so every field this module reaches is first
// validated through an arktype. A step that claims to be an agent-bearing
// primitive (`step`/`map`) but carries no well-formed `agent.modelSources` is a
// malformed projection and throws, rather than silently pinning a fallback.

import { type } from "arktype";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { onTriggerBodyRef } from "@intx/workflow";

/**
 * A body step's declared preferred inference-source identity -- the `(provider,
 * model)` of its agent's first `modelSources` entry. All the pin resolver needs.
 */
export interface InertBodyStepPreference {
  readonly provider: string;
  readonly model: string;
}

/**
 * An inline onTrigger body lifted out of a frozen inert projection, ready for
 * the source-ref hub to pin per-step sources and carry as a
 * `referencedDefinitions` entry.
 */
export interface EnumeratedInertOnTriggerBody {
  /**
   * The body's ref -- `onTriggerBodyRef(projection.id, stepId)`. This is also
   * `definition.id`, the id the sidecar stages the body's `workflow.json` and
   * `sources.json` under, and the id the source-ref run child re-derives when it
   * rewrites the re-evaluated closure -- so the three agree byte-for-byte.
   */
  readonly ref: string;
  /**
   * The inline body projection, its id overridden to `ref` (matching the live
   * rewrite). Carried verbatim except for the id: the source-ref re-verify
   * recomputes the body's wire hash over the re-evaluated closure body, so this
   * must be the same inert form the closure projects to.
   */
  readonly definition: typeof WorkflowProjectionDefinition.infer;
  /**
   * Each body step's declared preference, keyed by the body's own step id, one
   * entry per `definition.stepOrder` entry. `null` for a step that declares no
   * preference (a non-agent step, or an agent with an empty `modelSources`); the
   * resolver then pins the deploy's approved `defaultSource`.
   */
  readonly preferredByStep: Readonly<
    Record<string, InertBodyStepPreference | null>
  >;
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

/**
 * Read an inert projection step's declared inference preference. Mirrors
 * `extractAgent`: a `step` carries the agent directly, a `map` carries it on its
 * inner step, and any other primitive declares none (`null`). A `step`/`map`
 * that fails the agent shape is a malformed projection and throws.
 *
 * `context` is a caller label the throw prefixes with, so a malformed step is
 * traceable to whoever read it (an inline onTrigger body enumeration, or the
 * top-level projection step-source pinning). Exported so both the body
 * enumeration here and the top-level source pin in `orchestrator.ts` read a
 * step's preference through one validator.
 */
export function readInertStepPreference(
  stepValue: unknown,
  context: string,
  stepId: string,
): InertBodyStepPreference | null {
  const asStep = StepWithAgent(stepValue);
  if (!(asStep instanceof type.errors)) {
    return firstPreference(asStep.agent.modelSources);
  }
  const asMap = MapWithAgent(stepValue);
  if (!(asMap instanceof type.errors)) {
    return firstPreference(asMap.step.agent.modelSources);
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
  return null;
}

/**
 * Enumerate a frozen inert projection's inline onTrigger bodies. Pure: it only
 * reads `projection`, validating every field it reaches. Each returned body
 * carries its ref, the inline body projection (id set to the ref), and each body
 * step's declared preference. A projection with no inline onTrigger body yields
 * an empty array.
 */
export function enumerateInertOnTriggerBodies(
  projection: typeof WorkflowProjectionDefinition.infer,
): readonly EnumeratedInertOnTriggerBody[] {
  const bodies: EnumeratedInertOnTriggerBody[] = [];
  for (const [stepId, stepValue] of Object.entries(projection.steps)) {
    const asInline = InlineOnTriggerStep(stepValue);
    if (asInline instanceof type.errors) continue;

    const ref = onTriggerBodyRef(projection.id, stepId);
    const validatedBody = WorkflowProjectionDefinition(asInline.body.inline);
    if (validatedBody instanceof type.errors) {
      throw new Error(
        `enumerateInertOnTriggerBodies: inline onTrigger body at step ${stepId} is not a valid workflow projection: ${validatedBody.summary}`,
      );
    }
    // Override only the id, to the ref the sidecar stages under and the run
    // child re-derives; every other field rides verbatim so the body's wire
    // hash matches the re-evaluated closure's projection.
    const definition = { ...validatedBody, id: ref };

    const preferredByStep: Record<string, InertBodyStepPreference | null> = {};
    for (const bodyStepId of definition.stepOrder) {
      preferredByStep[bodyStepId] = readInertStepPreference(
        definition.steps[bodyStepId],
        `enumerateInertOnTriggerBodies: body ${ref} `,
        bodyStepId,
      );
    }
    bodies.push({ ref, definition, preferredByStep });
  }
  return bodies;
}
