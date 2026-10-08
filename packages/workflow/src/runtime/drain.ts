// Workflow runtime drain surface.
//
// `DrainController` is the runtime body's read-only view of a host-initiated
// drain. The runtime observes `signal` at four sites in `run.ts` and consults
// `behaviorFor(stepId)` for the in-flight step: a `"cancel"` behavior aborts
// the step's local controller (the existing cancellation cascade tears it
// down); a `"wait"` behavior leaves it running, and the supervisor's
// `drainTimeout` escalates to a signed `CancelRequested{origin:
// "supervisor-drain"}`.
//
// `runLocal` wires a no-op controller whose `signal` never fires; production
// wires `@intx/workflow-host/src/drain-controller.ts`.

import {
  stepTriggerBudget,
  type DrainBehavior,
  type WorkflowDefinition,
} from "../definition/index";
import { baseStepId } from "./step-scope";

/**
 * Runtime body's read-only view of the host-initiated drain; the host
 * implements the mutating side.
 */
export interface DrainController {
  /**
   * Aborts when the supervisor issues `drain`. A fired signal alone is
   * insufficient to abort a step -- the runtime cross-references
   * `behaviorFor(stepId)` first.
   */
  readonly signal: AbortSignal;
  /**
   * Resolve the declared drainBehavior for the in-flight step, consulted
   * when the drain signal has aborted.
   */
  behaviorFor(stepId: string): DrainBehavior;
}

/**
 * Compute the drainBehavior for a primitive in a workflow definition.
 * Shared between the production and runLocal controllers so the
 * default-resolution rule lives in one place. Defaults mirror the
 * constructors in `definition/primitives.ts`; `gate`, `escalation`, and
 * `map` (outer) carry no behavior of their own and return `"cancel"`.
 */
export function resolveDrainBehavior(
  definition: WorkflowDefinition,
  stepId: string,
): DrainBehavior {
  const primitive = lookupPrimitive(definition, stepId);
  if (primitive === null) return "cancel";
  switch (primitive.kind) {
    case "step": {
      // A long-lived step (budget != 1) is definitionally interactive;
      // draining means "stop feeding new input", not "abort the paused
      // waiter". The author can still override explicitly.
      const budget = stepTriggerBudget(primitive);
      return primitive.drainBehavior ?? (budget !== 1 ? "wait" : "cancel");
    }
    case "action":
      return primitive.drainBehavior ?? "cancel";
    case "loop":
      return primitive.drainBehavior ?? "cancel";
    case "sleep":
      return primitive.drainBehavior ?? "cancel";
    case "childWorkflow":
      return primitive.drainBehavior ?? "cancel";
    case "onTrigger":
      // A live event-driven section is definitionally interactive;
      // draining means "stop feeding new events", not "abort the paused
      // waiter".
      return primitive.drainBehavior ?? "wait";
    case "awaitSignal":
      return primitive.drainBehavior ?? "wait";
    case "map":
    case "gate":
    case "escalation":
      return "cancel";
  }
}

/**
 * Resolve a step id to its primitive; a map-inner step id `<mapId>[<i>]`
 * resolves to the outer map's inner step, which carries the behavior.
 */
function lookupPrimitive(
  definition: WorkflowDefinition,
  stepId: string,
): import("../definition/index").Primitive | null {
  const direct = definition.steps[stepId];
  if (direct !== undefined) return direct;
  const outerId = baseStepId(stepId);
  if (outerId !== stepId) {
    const outer = definition.steps[outerId];
    if (outer !== undefined && outer.kind === "map") {
      return outer.step;
    }
  }
  return null;
}

/**
 * No-op DrainController: `signal` never aborts; `behaviorFor` consults
 * the supplied definition. runLocal wires this.
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
 * The single source of truth for the four observation sites in
 * `run.ts`: true when the drain signal fired and the step's behavior
 * is `"cancel"`.
 */
export function shouldAbortForDrain(
  drain: DrainController,
  stepId: string,
): boolean {
  if (!drain.signal.aborted) return false;
  return drain.behaviorFor(stepId) === "cancel";
}
