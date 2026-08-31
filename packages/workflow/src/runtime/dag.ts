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
 * `<loopId>[i]`) left non-terminal in a seed log is resumable in both of
 * its live phases: `in-flight` while an iteration body is mid-flight
 * (`runLoop` re-derives its cursor from the log and continues) and
 * `awaiting-signal` while the container is proxy-parked on an iteration
 * body's approval or author-`awaitSignal` gate (`runLoop`'s resume planner
 * re-links the park and drives it), mirroring `isResumableOnTriggerStep`.
 * Every other in-flight/awaiting-* step stays rejected on resume. The
 * resume guard and `nextSchedulable` both key their loop carve-out on this
 * single predicate so the id-parsing lives in exactly one place. A
 * synthetic iteration id `<loopId>[i]` is not a definition key (brackets
 * are outside STEP_ID_PATTERN), so it is stripped back to its container to
 * resolve the kind.
 */
export function isResumableLoopStep(
  def: WorkflowDefinition,
  stepId: string,
  phase: StepPhase,
): boolean {
  if (phase !== "in-flight" && phase !== "awaiting-signal") return false;
  const containerId = baseStepId(stepId);
  return def.steps[containerId]?.kind === "loop";
}

/**
 * A step left `awaiting-signal` in a seed log is resumable, for both
 * primitive kinds that park a durable awaiter:
 *
 *   - an `awaitSignal` gate: a run re-driving the durable log re-offers it
 *     so `runAwaitSignal` skips its already-emitted
 *     `StepStarted`/`SignalAwaited` and RE-PARKS on the signal channel,
 *     holding a live awaiter for a signal delivered later (the
 *     human-in-the-loop case: the operator signals AFTER the restart, so
 *     the signal is not yet on the log at recovery time);
 *   - an agent `step` suspended on a reactor gate: it parked under the
 *     reserved `signalName(correlationId)` channel exactly like a gate, so
 *     the same re-offer lets `runStep` recover that channel from the
 *     durable `SignalAwaited`, RE-PARK, and re-invoke the agent with the
 *     delivered decision once the signal arrives -- without re-invoking the
 *     agent against the original input (which would restart the suspended
 *     turn from scratch).
 *
 * Without this the re-driving run would stall the instant it reaches the
 * still-awaiting step, and a post-recovery signal would find no awaiter, so
 * a durable suspension would not actually survive a crash. The resume guard
 * and `nextSchedulable` both key on this predicate so the carve-out lives
 * in exactly one place, mirroring `isResumableLoopStep`.
 */
export function isResumableAwaitingSignalStep(
  def: WorkflowDefinition,
  stepId: string,
  phase: StepPhase,
): boolean {
  if (phase !== "awaiting-signal") return false;
  const kind = def.steps[stepId]?.kind;
  return kind === "awaitSignal" || kind === "step";
}

/**
 * An `awaitSignal` step left `in-flight` in a seed log is resumable: some
 * mover took it off `awaiting-signal` -- a `SignalReceived` (or a pre-await
 * queued signal consumed by `SignalAwaited`), or, for a timed gate, a
 * `TimerFired` -- so the step only needs its `StepCompleted` (or, on timeout,
 * its routing/failure). This is the crash-after-move-before-`StepCompleted`
 * window: a run re-driving the durable log finds the gate `in-flight`, and
 * without this carve-out `nextSchedulable` skips it, its dependents are blocked
 * on the non-terminal gate, and the run stalls. Re-offering the gate lets
 * `runAwaitSignal` reconstruct the outcome from the log and short-circuit to
 * completion without parking (distinct from `isResumableAwaitingSignalStep`,
 * which re-parks a gate whose signal has NOT yet arrived).
 *
 * The reduced state cannot itself distinguish "signal received" from "timeout
 * fired" -- both leave the step `in-flight` with an empty `pendingTimers`. The
 * durable log CAN: `runAwaitSignal` replays it, folding the gate's own
 * `TimerFired` as a competing mover, to recover which mover won (and, for a
 * signal, its payload). That replay is what makes the timed case safe to admit
 * here, so a fired timeout resolves to a timeout outcome rather than to a
 * signal payload it never received.
 */
export function isResumableReceivedAwaitSignalStep(
  def: WorkflowDefinition,
  stepId: string,
  phase: StepPhase,
): boolean {
  if (phase !== "in-flight") return false;
  return def.steps[stepId]?.kind === "awaitSignal";
}

/**
 * An onTrigger section container left non-terminal in a seed log is
 * resumable in both of its live phases: `in-flight` while a body run is
 * mid-flight (like a loop container, `runOnTrigger` re-derives its cursor
 * from the settled body runs and continues) and `awaiting-signal` while
 * parked between events (like an `awaitSignal` gate, it re-parks on the
 * input channel so the next event resolves it). The section never
 * self-completes, so -- unlike an agent step -- neither phase is a crash
 * mid-invocation to settle as failed. A synthetic per-event id strips to
 * its container to resolve the kind, mirroring the loop carve-out.
 */
export function isResumableOnTriggerStep(
  def: WorkflowDefinition,
  stepId: string,
  phase: StepPhase,
): boolean {
  if (phase !== "in-flight" && phase !== "awaiting-signal") return false;
  return def.steps[baseStepId(stepId)]?.kind === "onTrigger";
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
 * step, a `childWorkflow`, etc. -- are deliberately excluded: they have a
 * re-arm surface the in-process runtime body lacks (rebuilding the map
 * iteration state), so they stay `RuntimeResumeUnsupportedError` and the host
 * owns recovery. (An `awaitSignal` gate left `in-flight`, timed or not, is
 * instead admitted by `isResumableReceivedAwaitSignalStep`, which reconstructs
 * its outcome from the log.) A synthetic map/loop inner id (`<id>[i]`) is not a
 * definition key, so it resolves to `undefined` and is excluded here;
 * resumable loop iterations are handled by
 * `isResumableLoopStep`, and a mid-`map` inner step stays
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

/**
 * A `sleep` step left non-terminal in a seed log is resumable in both of its
 * live phases: `awaiting-timer` while its `TimerSet` is unfired (`runSleep`
 * re-adopts the durable timer and re-parks on `waitForTimer`, honouring the
 * persisted `fireAt` rather than restarting the clock) and `in-flight` after
 * its `TimerFired` landed but before its `StepCompleted` (`runSleep` completes
 * it without re-parking -- the crash-after-`TimerFired`-before-`StepCompleted`
 * window, mirroring `isResumableReceivedAwaitSignalStep`). `TimerFired` is the
 * only mover off `awaiting-timer` for a sleep, so the in-flight window carries
 * no competing outcome to reconstruct.
 *
 * The kind guard is exact for a reason: a retrying `step`/`action` also parks
 * in `awaiting-timer` during backoff (`handleAttemptScheduled`), and that
 * residual has no sleep-resume path -- it must stay `RuntimeResumeUnsupportedError`
 * and let the host own recovery. Only a `sleep` primitive is admitted here. The
 * resume guard and `nextSchedulable` both key on this single predicate so the
 * carve-out lives in exactly one place, mirroring the other predicates.
 */
export function isResumableSleepStep(
  def: WorkflowDefinition,
  stepId: string,
  phase: StepPhase,
): boolean {
  if (phase !== "awaiting-timer" && phase !== "in-flight") return false;
  return def.steps[stepId]?.kind === "sleep";
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
  // surface for re-arming a generic in-flight primitive on resume; a seed log
  // that lands a step in those phases is rejected up front by `runtimeRun`
  // with `RuntimeResumeUnsupportedError`. The exceptions re-offered below are
  // the resumable carve-outs: a loop container in-flight or awaiting-signal
  // (`isResumableLoopStep`, re-derived by `runLoop`), an `awaitSignal` step
  // still `awaiting-signal` (`isResumableAwaitingSignalStep`, re-parked so a
  // later signal resolves it), an `awaitSignal` step left `in-flight` by an
  // already-logged `SignalReceived` (`isResumableReceivedAwaitSignalStep`,
  // the crash-after-signal-before-StepCompleted window), an onTrigger
  // section container in-flight or awaiting-signal
  // (`isResumableOnTriggerStep`, re-derived by `runOnTrigger`), and a `sleep`
  // step awaiting-timer or in-flight (`isResumableSleepStep`, re-adopted by
  // `runSleep`). The resume guard keys on the SAME predicates so the two
  // views agree.
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
      !isResumableLoopStep(def, stepId, existing.phase) &&
      !isResumableAwaitingSignalStep(def, stepId, existing.phase) &&
      !isResumableReceivedAwaitSignalStep(def, stepId, existing.phase) &&
      !isResumableOnTriggerStep(def, stepId, existing.phase) &&
      !isResumableSleepStep(def, stepId, existing.phase)
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
