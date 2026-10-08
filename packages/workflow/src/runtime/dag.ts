// DAG scheduling helpers for the workflow runtime.
//
// A step is schedulable once every dependency named in its `after` field has
// reached a terminal phase in the run state. The runtime asks
// `nextSchedulable` for the ids it should kick off on each tick.

import type { Primitive, WorkflowDefinition } from "../definition/index";
import {
  isTerminalRunPhase,
  isTerminalStepPhase,
  type RunState,
  type StepPhase,
} from "../state-machine/index";
import { baseStepId } from "./step-scope";

/**
 * A loop container `<loopId>` (or its synthetic iteration step `<loopId>[i]`)
 * left non-terminal in a seed log is resumable in both live phases: `in-flight`
 * while an iteration body is mid-flight (`runLoop` re-derives its cursor from
 * the log) and `awaiting-signal` while the container is proxy-parked on an
 * iteration body's approval or author-`awaitSignal` gate (`runLoop`'s resume
 * planner re-links the park and drives it). A synthetic iteration id is
 * stripped back to its container -- not a definition key -- to resolve the kind.
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
 * A step left `awaiting-signal` in a seed log is resumable for both
 * primitive kinds that park a durable awaiter:
 *
 *   - an `awaitSignal` gate: `runAwaitSignal` skips its already-emitted
 *     `StepStarted`/`SignalAwaited` and re-parks on the signal channel;
 *   - an agent `step` suspended on a reactor gate: `runStep` recovers the
 *     reserved `signalName(correlationId)` channel from the durable
 *     `SignalAwaited`, re-parks, and re-invokes the agent with the delivered
 *     decision once the signal arrives -- without re-invoking it against the
 *     original input.
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
 * An `awaitSignal` step left `in-flight` in a seed log is resumable: a mover
 * (`SignalReceived`, a pre-await queued signal, or for a timed gate
 * `TimerFired`) already took it off `awaiting-signal`, so it only needs its
 * `StepCompleted` -- the crash-after-move-before-`StepCompleted` window;
 * `runAwaitSignal` reconstructs the outcome from the log and short-circuits
 * to completion without parking (distinct from `isResumableAwaitingSignalStep`,
 * which re-parks a gate whose signal has NOT yet arrived). The reduced state
 * cannot distinguish "signal received" from "timeout fired"; the durable log
 * can, so a fired timeout resolves to a timeout outcome rather than a signal
 * payload it never received.
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
 * resumable in both live phases: `in-flight` while a body run is mid-flight
 * (`runOnTrigger` re-derives its cursor from the settled body runs) and
 * `awaiting-signal` while parked between events (it re-parks on the input
 * channel so the next event resolves it). The section never self-completes,
 * so neither phase is a crash mid-invocation to settle as failed.
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
 * An `in-flight` `step`/`action` is a crash mid-invocation: a durable
 * `StepStarted` with no `StepCompleted`, dispatched once, with no
 * runtime-body re-arm surface to re-invoke it safely. Both kinds flush
 * `StepStarted` durably before invoking, so a lone crash always leaves this
 * residual. The resume guard settles it as a terminal `StepFailed`
 * (at-most-once refusal) rather than throwing.
 *
 * Container/coordination primitives left `in-flight` (a `map` outer step, a
 * `childWorkflow`) are deliberately excluded -- they have a re-arm surface
 * the in-process body lacks -- and stay `RuntimeResumeUnsupportedError`. An
 * `awaitSignal` gate left `in-flight` is admitted by
 * `isResumableReceivedAwaitSignalStep`; resumable loop iterations by
 * `isResumableLoopStep`.
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
 * A `sleep` step left non-terminal in a seed log is resumable in both live
 * phases: `awaiting-timer` while its `TimerSet` is unfired (`runSleep`
 * re-adopts the durable timer, honouring the persisted `fireAt`) and
 * `in-flight` after `TimerFired` landed but before `StepCompleted` (`runSleep`
 * completes it without re-parking).
 *
 * The kind guard is exact: a retrying `step`/`action` also parks in
 * `awaiting-timer` during backoff, and that residual has no sleep-resume path.
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
  // A primitive can only be started inside the `running` phase; the state
  // machine rejects StepStarted in any other phase.
  //
  // `state.steps.has(stepId)` skips steps in `awaiting-signal`,
  // `awaiting-timer`, and `in-flight` -- the in-process body has no surface
  // for re-arming a generic in-flight primitive on resume. The predicates
  // above are the resumable carve-outs, and the resume guard keys on the same
  // predicates so the two views agree.
  if (state.phase !== "running") {
    return [];
  }
  const out: Primitive[] = [];
  for (const stepId of def.stepOrder) {
    // Keep the in-memory in-flight skip ahead of the exemptions so a step
    // already running this process is not double-scheduled.
    if (inFlight.has(stepId)) continue;
    const existing = state.steps.get(stepId);
    // Skip any step already in state.steps except a resumable carve-out,
    // which is re-scheduled so its runner can re-derive its position from
    // the log.
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
