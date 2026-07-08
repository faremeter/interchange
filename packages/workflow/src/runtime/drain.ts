// Workflow runtime drain surface.
//
// `DrainController` is the runtime body's read-only view of a host-
// initiated drain. The runtime observes `signal` at exactly four sites
// inside `run.ts` (the main loop entry, the retry-between-attempts
// step in `runStep`, `waitForTimer`, and `runAwaitSignal`). At each
// observation the runtime consults `behaviorFor(stepId)` for the
// in-flight step. A `"cancel"` behavior aborts the step's local
// controller and the existing cancellation cascade tears the step
// down. A `"wait"` behavior leaves the step running; the supervisor's
// `drainTimeout` accumulator (host side) escalates to a signed
// `CancelRequested{origin: "supervisor-drain"}` when the deadline
// runs out against `"cancel"`-behavior work.
//
// `runLocal` wires a no-op DrainController whose `signal` never
// fires. Production wires the real controller from
// `@intx/workflow-host/src/drain-controller.ts`, which flips its
// signal on receipt of the supervisor's `drain` control mail and
// resolves `behaviorFor` against the live `RunState` plus the loaded
// `WorkflowDefinition`.

import type { DrainBehavior, WorkflowDefinition } from "../definition/index";

/**
 * Runtime body's view of the host-initiated drain. The runtime body
 * never holds a private mutator on this object; the host implements
 * the mutating side and exposes the read-only surface here.
 */
export interface DrainController {
  /**
   * Aborts when the supervisor issues `drain` against this workflow-
   * process. The runtime body consults this at the four observation
   * points; a fired signal alone is insufficient to abort a step --
   * the runtime cross-references `behaviorFor(stepId)` first.
   */
  readonly signal: AbortSignal;
  /**
   * Resolve the declared drainBehavior for the in-flight step. The
   * runtime body invokes this with the primitive's id when the drain
   * signal has aborted and the runtime is about to settle a step-
   * level decision (continue waiting vs cascade through cancel).
   */
  behaviorFor(stepId: string): DrainBehavior;
}

/**
 * Compute the drainBehavior for a primitive in a workflow definition.
 * Shared between the production and runLocal controllers so the
 * default-resolution rule lives in exactly one place.
 *
 * The default-by-kind table mirrors the constructors in
 * `definition/primitives.ts`:
 *   - `step`, `sleep`, `childWorkflow` default to `"cancel"`.
 *   - `awaitSignal` defaults to `"wait"` (the human-in-the-loop case).
 *   - `gate`, `escalation`, `map` carry no `drainBehavior` of their
 *     own; the runtime never blocks inside them long enough for
 *     drain to matter, so the function returns `"cancel"` so the
 *     observation-site short-circuit defers to the existing cancel
 *     cascade. `map`'s inner step carries its own behavior; the
 *     runtime queries the inner step id when iterating.
 */
export function resolveDrainBehavior(
  definition: WorkflowDefinition,
  stepId: string,
): DrainBehavior {
  const primitive = lookupPrimitive(definition, stepId);
  if (primitive === null) return "cancel";
  switch (primitive.kind) {
    case "step":
      return primitive.drainBehavior ?? "cancel";
    case "action":
      return primitive.drainBehavior ?? "cancel";
    case "sleep":
      return primitive.drainBehavior ?? "cancel";
    case "childWorkflow":
      return primitive.drainBehavior ?? "cancel";
    case "awaitSignal":
      return primitive.drainBehavior ?? "wait";
    case "map":
    case "gate":
    case "escalation":
      return "cancel";
  }
}

/**
 * Resolve a step id to its primitive. The map-inner step id shape is
 * `<mapId>[<index>]`; the runtime uses the outer map's inner step for
 * behavior resolution because the outer map's iteration carries the
 * inner step's behavior.
 */
function lookupPrimitive(
  definition: WorkflowDefinition,
  stepId: string,
): import("../definition/index").Primitive | null {
  const direct = definition.steps[stepId];
  if (direct !== undefined) return direct;
  const mapMatch = /^([^[]+)\[\d+\]$/.exec(stepId);
  if (mapMatch !== null) {
    const outerId = mapMatch[1];
    if (outerId === undefined) return null;
    const outer = definition.steps[outerId];
    if (outer !== undefined && outer.kind === "map") {
      return outer.step;
    }
  }
  return null;
}

/**
 * No-op DrainController. The `signal` is a fresh AbortSignal that
 * never aborts; `behaviorFor` consults the supplied definition via
 * `resolveDrainBehavior`. runLocal wires this so the runtime body's
 * drain observation points always read `behaviorFor` against the
 * real declared behavior but the signal-aborted branch never fires.
 */
export function createNoopDrainController(
  definition: WorkflowDefinition,
): DrainController {
  const controller = new AbortController();
  return {
    signal: controller.signal,
    behaviorFor(stepId) {
      return resolveDrainBehavior(definition, stepId);
    },
  };
}

/**
 * Resolve the four-observation-point gate: given a drain controller
 * and a step id, returns `true` if the runtime body should abort the
 * step's local controller right now. The single source of truth so
 * the four observation sites in `run.ts` use the same predicate.
 */
export function shouldAbortForDrain(
  drain: DrainController,
  stepId: string,
): boolean {
  if (!drain.signal.aborted) return false;
  return drain.behaviorFor(stepId) === "cancel";
}
