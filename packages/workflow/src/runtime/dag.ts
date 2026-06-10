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
} from "../state-machine/index";

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
  // `state.steps.has(stepId)` also skips steps in `awaiting-signal`
  // and `awaiting-timer`. That is intentional for the in-process
  // runner: resuming a mid-await primitive needs the signal channel
  // rehydrated and the timer re-armed, which a single-shot
  // in-process runner does not do. A durable production runtime
  // resuming the same log either has the supervisor commit signals
  // and timers on the workflow process's behalf, or extends this
  // function and makes the primitive runners idempotent against
  // already-committed `StepStarted` / `SignalAwaited` events. The
  // choice is shaped by the production substrate.
  if (state.phase !== "running") {
    return [];
  }
  const out: Primitive[] = [];
  for (const stepId of def.stepOrder) {
    if (state.steps.has(stepId)) continue;
    if (inFlight.has(stepId)) continue;
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
