// Production `DrainController` implementation for the workflow-host
// child.
//
// The `@intx/workflow` runtime body observes drain at four sites
// (main loop entry, retry-between-attempts in runStep, waitForTimer,
// runAwaitSignal). Each observation consults `behaviorFor(stepId)`
// to decide whether to abort the step's local controller. This file
// supplies the production controller the workflow-process child wires
// against the runtime env.
//
// The controller owns three pieces of state:
//   1. An `AbortController` whose signal exposes drain status to the
//      runtime body.
//   2. A reference to the live `WorkflowDefinition` the child loaded;
//      `behaviorFor` consults the primitive at the requested stepId
//      and returns its declared drainBehavior.
//   3. An `accumulatedCancelMs` counter the supervisor's drainTimeout
//      accumulator reads. The runtime body increments it; the
//      supervisor consults it without owning the writer.
//
// The supervisor calls `requestDrain()` when its control-loop receives
// the `drain` mail; the controller flips its signal and the runtime
// body's observation points pick up the change on their next tick.

import { resolveDrainBehavior, type DrainController } from "@intx/workflow";
import type { DrainBehavior, WorkflowDefinition } from "@intx/workflow";

export interface WorkflowHostDrainController extends DrainController {
  /**
   * Flip the drain signal. Idempotent; a second call after the
   * signal has already aborted is a no-op so the supervisor's
   * control-loop can re-deliver `drain` without breaking the
   * accumulator's ordering.
   */
  requestDrain(): void;
  /**
   * Whether the drain has been requested. Distinct from
   * `signal.aborted` only across the brief window where the signal
   * has been aborted but a downstream observer has not yet read it.
   */
  readonly drainRequested: boolean;
}

export interface CreateWorkflowHostDrainControllerOpts {
  /**
   * The workflow definition the child loaded at startup. The
   * controller consults this for `behaviorFor` resolution. The
   * definition is immutable across the run lifetime; a redeploy
   * tears the workflow-process down and respawns it.
   */
  definition: WorkflowDefinition;
  /**
   * Optional per-id override resolver. When supplied, the controller
   * consults this before falling back to `resolveDrainBehavior`. The
   * supervisor uses this for map-iteration steps whose runtime id
   * shape (`<mapId>[<index>]`) the workflow definition does not
   * carry directly; `resolveDrainBehavior` already handles the
   * common case, so the override is reserved for host-specific
   * extensions that diverge from the workflow-level conventions.
   */
  behaviorOverride?: (stepId: string) => DrainBehavior | undefined;
}

export function createWorkflowHostDrainController(
  opts: CreateWorkflowHostDrainControllerOpts,
): WorkflowHostDrainController {
  const controller = new AbortController();
  let requested = false;
  return {
    signal: controller.signal,
    behaviorFor(stepId) {
      const override = opts.behaviorOverride?.(stepId);
      if (override !== undefined) return override;
      return resolveDrainBehavior(opts.definition, stepId);
    },
    requestDrain() {
      if (requested) return;
      requested = true;
      controller.abort();
    },
    get drainRequested() {
      return requested;
    },
  };
}
