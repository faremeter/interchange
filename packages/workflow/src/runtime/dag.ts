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
import { baseStepId } from "./step-scope";

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
  const containerId = baseStepId(stepId);
  return def.steps[containerId]?.kind === "loop";
}

/**
 * An `awaitSignal` step left `awaiting-signal` in a seed log is resumable:
 * a run re-driving the durable log re-offers the gate so `runAwaitSignal`
 * skips its already-emitted `StepStarted`/`SignalAwaited` and RE-PARKS on
 * the signal channel, holding a live awaiter for a signal delivered later
 * (the human-in-the-loop case: the operator signals AFTER the restart, so
 * the signal is not yet on the log at recovery time). Without this the
 * re-driving run would stall the instant it reaches the still-awaiting
 * gate, and a post-recovery signal would find no awaiter. The resume guard
 * and `nextSchedulable` both key on this predicate so the carve-out lives
 * in exactly one place, mirroring `isResumableInFlightLoopStep`.
 */
export function isResumableAwaitingSignalStep(
  def: WorkflowDefinition,
  stepId: string,
  phase: StepPhase,
): boolean {
  if (phase !== "awaiting-signal") return false;
  return def.steps[stepId]?.kind === "awaitSignal";
}

/**
 * An `awaitSignal` step left `in-flight` in a seed log is resumable ONLY
 * when the step declares no timeout. Without a timeout, an awaitSignal
 * step reaches `in-flight` exactly one way: a `SignalReceived` (or a
 * pre-await queued signal consumed by `SignalAwaited`) moved it off
 * `awaiting-signal` -- the signal is logically received and the step just
 * needs its `StepCompleted`. This is the crash-after-`SignalReceived`-
 * before-`StepCompleted` window: a run re-driving the durable log finds
 * the gate `in-flight`, and without this carve-out `nextSchedulable` skips
 * it, its dependents are blocked on the non-terminal gate, and the run
 * stalls. Re-offering the gate lets `runAwaitSignal` recover the payload
 * from the logged `SignalReceived` and short-circuit to completion without
 * parking (distinct from `isResumableAwaitingSignalStep`, which re-parks a
 * gate whose signal has NOT yet arrived).
 *
 * The `timeout === undefined` clause is load-bearing for correctness, not
 * an optimization. A timeout-bearing awaitSignal also reaches `in-flight`
 * when its `TimerFired` lands (`handleTimerFired` moves an awaiting-signal
 * step to `in-flight`), and the reduced state carries no field that
 * distinguishes "signal received" from "timeout fired" -- both leave the
 * step `in-flight` with an empty `pendingTimers`. Admitting the
 * timeout-bearing case would risk completing a timed-out run with a
 * signal payload it never received, so it stays rejected byte-for-byte.
 */
export function isResumableReceivedAwaitSignalStep(
  def: WorkflowDefinition,
  stepId: string,
  phase: StepPhase,
): boolean {
  if (phase !== "in-flight") return false;
  const primitive = def.steps[stepId];
  if (primitive?.kind !== "awaitSignal") return false;
  return primitive.timeout === undefined;
}

/**
 * An `in-flight` step whose primitive is an invocation boundary -- an
 * agent `step` or a deterministic `action` -- is a crash
 * mid-invocation, not a resumable coordination primitive: a durable
 * `StepStarted` with no `StepCompleted`, whose invoked primitive was
 * dispatched once and has no runtime-body re-arm surface, so it cannot
 * be re-invoked safely. Both kinds flush their `StepStarted` durably
 * before invoking (the `commitDurable` barrier in `runStep` and
 * `runAction`), so a lone crash of either always leaves this residual.
 * The resume guard settles such a step as a terminal `StepFailed`
 * (at-most-once refusal) rather than throwing. For an `action` the
 * per-effect ledger is a deeper exactly-once line of defense for effects
 * routed through the EffectContext; the barrier is what makes the action
 * non-re-invocable at this layer.
 *
 * Container/coordination primitives left `in-flight` -- a `map` outer
 * step, a timeout-bearing `awaitSignal` reduced to `in-flight`, a
 * `childWorkflow`, etc. -- are deliberately excluded: they have a
 * re-arm surface the in-process runtime body lacks (rebuilding the map
 * iteration state, distinguishing a fired timeout from a received
 * signal), so they stay `RuntimeResumeUnsupportedError` and the host
 * owns recovery. A synthetic map/loop inner id (`<id>[i]`) is not a
 * definition key, so it resolves to `undefined` and is excluded here;
 * resumable loop iterations are handled by
 * `isResumableInFlightLoopStep`, and a mid-`map` inner step stays
 * unsupported with its container.
 */
export function isCrashedInvocationStep(
  def: WorkflowDefinition,
  stepId: string,
  phase: StepPhase,
): boolean {
  if (phase !== "in-flight") return false;
  const kind = def.steps[stepId]?.kind;
  return kind === "step" || kind === "action";
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
  // `awaiting-timer`, and `in-flight`. The in-process runtime body has no
  // surface for re-arming an `awaiting-timer` or a generic in-flight
  // primitive on resume; a seed log that lands a step in those phases is
  // rejected up front by `runtimeRun` with `RuntimeResumeUnsupportedError`.
  // The exceptions re-offered below are the resumable carve-outs: a
  // mid-loop container, an `awaitSignal` step still `awaiting-signal`
  // (`isResumableAwaitingSignalStep`, re-parked so a later signal
  // resolves it), and an `awaitSignal` step left `in-flight` by an
  // already-logged `SignalReceived` (`isResumableReceivedAwaitSignalStep`,
  // the crash-after-signal-before-StepCompleted window). The resume guard
  // keys on the SAME predicates so the two views agree.
  if (state.phase !== "running") {
    return [];
  }
  const out: Primitive[] = [];
  for (const stepId of def.stepOrder) {
    // The in-memory in-flight skip stays ahead of the exemptions so a
    // step already running this process is not double-scheduled.
    if (inFlight.has(stepId)) continue;
    const existing = state.steps.get(stepId);
    // Skip any step already in state.steps EXCEPT a resumable carve-out,
    // which is re-scheduled so its runner can re-derive its position from
    // the log and continue (runLoop re-derives its cursor; runAwaitSignal
    // re-parks a still-awaiting gate or short-circuits an already-received
    // one to completion).
    if (
      existing !== undefined &&
      !isResumableInFlightLoopStep(def, stepId, existing.phase) &&
      !isResumableAwaitingSignalStep(def, stepId, existing.phase) &&
      !isResumableReceivedAwaitSignalStep(def, stepId, existing.phase)
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
