// DAG scheduling helpers for the workflow runtime.
//
// A step is schedulable once every dependency named in its `after`
// field has reached a terminal phase in the run state. The runtime
// asks `nextSchedulable` for the set of ids it should kick off on
// each tick.

import type { Primitive, WorkflowDefinition } from "../definition/index";
import {
  isTerminalRunPhase,
  isTerminalStepPhase,
  type RunState,
  type StepPhase,
} from "../state-machine/index";

/**
 * A loop container `<loopId>` (or its synthetic iteration step
 * `<loopId>[i]`) left in-flight in a seed log is the one resumable
 * in-flight shape: `runLoop` re-derives its position from the log and
 * continues. Every other in-flight/awaiting-* step stays rejected on
 * resume. The resume guard and `nextSchedulable` both key their loop
 * carve-out on this single predicate so the id-parsing lives in exactly
 * one place. A synthetic iteration id `<loopId>[i]` is not a definition
 * key (brackets are outside STEP_ID_PATTERN), so it is stripped back to
 * its container to resolve the kind.
 */
export function isResumableInFlightLoopStep(
  def: WorkflowDefinition,
  stepId: string,
  phase: StepPhase,
): boolean {
  if (phase !== "in-flight") return false;
  const containerId = stepId.replace(/\[\d+\]$/, "");
  return def.steps[containerId]?.kind === "loop";
}

export function nextSchedulable(
  def: WorkflowDefinition,
  state: RunState,
  inFlight: ReadonlySet<string>,
): readonly Primitive[] {
  // A primitive can only be started inside the `running` phase. The
  // state machine rejects StepStarted in any other phase; mirroring
  // the constraint here keeps the scheduler in step with the
  // transition function's view of what is legal.
  //
  // `state.steps.has(stepId)` skips steps in `awaiting-signal`,
  // `awaiting-timer`, and `in-flight`. The in-process runtime body
  // has no surface for re-arming those primitives on resume; a seed
  // log that lands a step in any of those phases is rejected up
  // front by `runtimeRun` with `RuntimeResumeUnsupportedError`. A
  // durable production runtime resuming the same log either has the
  // supervisor commit signals and timers on the workflow process's
  // behalf, or extends this function and makes the primitive runners
  // idempotent against already-committed `StepStarted` /
  // `SignalAwaited` events. The choice is shaped by the production
  // substrate; the in-process body declines instead of stalling.
  if (state.phase !== "running") {
    return [];
  }
  const out: Primitive[] = [];
  for (const stepId of def.stepOrder) {
    // The in-memory in-flight skip stays ahead of the loop exemption so a
    // loop already running this process is not double-scheduled.
    if (inFlight.has(stepId)) continue;
    const existing = state.steps.get(stepId);
    // Skip any step already in state.steps EXCEPT a resumable in-flight
    // loop container, which is re-scheduled so runLoop can re-derive its
    // cursor from the log and continue.
    if (
      existing !== undefined &&
      !isResumableInFlightLoopStep(def, stepId, existing.phase)
    ) {
      continue;
    }
    const primitive = def.steps[stepId];
    if (!primitive) continue;
    if (!areDepsResolved(primitive, state)) continue;
    out.push(primitive);
  }
  return out;
}

function areDepsResolved(primitive: Primitive, state: RunState): boolean {
  const after = primitive.after;
  if (after === undefined || after.length === 0) return true;
  for (const dep of after) {
    const depStep = state.steps.get(dep);
    if (!depStep) return false;
    if (!isTerminalStepPhase(depStep.phase)) return false;
  }
  return true;
}

export function isRunDone(def: WorkflowDefinition, state: RunState): boolean {
  if (isTerminalRunPhase(state.phase)) return true;
  for (const stepId of def.stepOrder) {
    const stepState = state.steps.get(stepId);
    if (!stepState) return false;
    if (!isTerminalStepPhase(stepState.phase)) return false;
  }
  return true;
}

export function hasFailedStep(state: RunState): boolean {
  for (const step of state.steps.values()) {
    if (step.phase === "failed") return true;
  }
  return false;
}
