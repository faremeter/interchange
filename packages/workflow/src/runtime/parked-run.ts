// Whether a workflow child can be started fresh on a deployment's committed
// log and pick up every live run where it stopped. The answer is the resume
// guard (`resumeResidualOf`) plus the same loop and onTrigger planners the
// runtime drives, applied to the top-level run and to every live run it
// spawned. This module does not list park shapes of its own.

import type { BodyFailurePolicy } from "../definition/index";
import {
  applyEvent,
  emptyState,
  type RunState,
  type WorkflowEvent,
} from "../state-machine/index";
import { resumeResidualOf, type StepKindLookup } from "./dag";
import {
  loopIterationCursor,
  loopOccurrenceReattach,
  onTriggerOccurrenceReattach,
  type OccurrenceReattach,
} from "./run";
import { baseStepId, loopBodyRunId } from "./step-scope";

export type ParkedRunVerdict = "parked" | "not-parked" | "unknown";

export type ParkedRunClassification = {
  readonly verdict: ParkedRunVerdict;
  readonly reason: string;
  /** Earliest pending timer of a live run, when the verdict is `parked`. */
  readonly wakeAt?: string;
  /**
   * In `park` mode, a run took an input -- its trigger, a signal, or a timer
   * -- that it has not acted on yet. A stopped child would leave that input
   * with nothing to wake it, so the park must not go ahead.
   */
  readonly pendingInput?: true;
};

/**
 * `replace` judges a worker that was lost wherever it was: a step it was
 * invoking settles as failed on resume, and the log is parked only when the
 * resume guard would re-attach at least one park. `park` judges a worker the
 * host is about to stop on purpose: nothing may be mid-invocation, and no
 * run may hold input it has not acted on (`pendingInput`). A run held between
 * the steps of work it already began is parked too.
 */
export type ParkedRunMode = "replace" | "park";

export async function classifyParkedRun(args: {
  readonly anchorRunId: string;
  /** The top-level definition's `steps`: a live definition or its projection. */
  readonly steps: Readonly<Record<string, unknown>>;
  readonly readRun: (runId: string) => Promise<readonly WorkflowEvent[]>;
  readonly mode: ParkedRunMode;
}): Promise<ParkedRunClassification> {
  const log = await args.readRun(args.anchorRunId);
  if (log.length === 0) {
    return args.mode === "park"
      ? { verdict: "parked", reason: "no run has started" }
      : { verdict: "not-parked", reason: "committed log is empty" };
  }
  const anchor = foldRun(args.anchorRunId, log, args.steps, true);
  const state = anchor.state;
  if (state.phase !== "running") {
    return { verdict: "not-parked", reason: `anchor phase is ${state.phase}` };
  }
  const judged = await judgeRun(
    { readRun: args.readRun, mode: args.mode },
    { steps: args.steps, ...anchor },
  );
  if (judged.verdict === "stop") return judged.classification;
  if (args.mode === "replace" && !judged.park) {
    return {
      verdict: "not-parked",
      reason: "resume guard found no re-attachable park",
    };
  }
  return {
    verdict: "parked",
    reason:
      args.mode === "park"
        ? "no run is mid-invocation"
        : "resume guard would re-attach a park",
    ...(judged.wakeAt !== undefined ? { wakeAt: judged.wakeAt } : {}),
  };
}

type Judge =
  | {
      readonly verdict: "stop";
      readonly classification: ParkedRunClassification;
    }
  | {
      readonly verdict?: undefined;
      readonly park: boolean;
      readonly wakeAt?: string;
    };

type JudgeContext = {
  readonly readRun: (runId: string) => Promise<readonly WorkflowEvent[]>;
  readonly mode: ParkedRunMode;
};

// A run's log reduced once, with whether an input still waits for the run
// to act on it.
type FoldedRun = {
  readonly state: RunState;
  readonly log: readonly WorkflowEvent[];
  readonly awaitsWork: boolean;
};

type JudgedRun = FoldedRun & {
  readonly steps: Readonly<Record<string, unknown>>;
};

async function judgeRun(ctx: JudgeContext, run: JudgedRun): Promise<Judge> {
  if (ctx.mode === "park" && run.awaitsWork) {
    return {
      verdict: "stop",
      classification: {
        verdict: "not-parked",
        reason: `run ${run.state.runId} has input it has not acted on`,
        pendingInput: true,
      },
    };
  }
  const lookup = kindLookup(run.steps);
  let park = false;
  let wakeAt = earliestTimer(run.state);
  for (const [stepId, step] of run.state.steps) {
    const residual = resumeResidualOf(lookup, stepId, step.phase);
    if (residual === "unsupported") {
      return stop("not-parked", `step ${stepId} is ${step.phase}`);
    }
    if (residual === "crashed-invocation" && ctx.mode === "park") {
      return stop("not-parked", `step ${stepId} is mid-invocation`);
    }
    const kind = stepKind(run.steps[stepId]);
    if (
      residual === "continue" &&
      step.phase === "awaiting-signal" &&
      kind !== "loop" &&
      kind !== "onTrigger" &&
      kind !== "childWorkflow"
    ) {
      park = true;
    }
  }
  for (const [stepId, step] of run.state.steps) {
    if (step.phase !== "in-flight" && step.phase !== "awaiting-signal") {
      continue;
    }
    const raw = run.steps[stepId];
    const kind = stepKind(raw);
    let judged: Judge;
    if (kind === "loop") {
      judged = await judgeLoop(ctx, run, stepId, raw);
    } else if (kind === "onTrigger") {
      judged = await judgeOnTrigger(ctx, run, stepId, raw);
    } else if (kind === "childWorkflow") {
      judged = await judgeChildWorkflow(ctx, run, stepId, raw);
    } else {
      continue;
    }
    if (judged.verdict === "stop") return judged;
    if (judged.park) park = true;
    wakeAt = earlier(wakeAt, judged.wakeAt);
  }
  return { park, ...(wakeAt !== undefined ? { wakeAt } : {}) };
}

async function judgeLoop(
  ctx: JudgeContext,
  run: JudgedRun,
  stepId: string,
  raw: unknown,
): Promise<Judge> {
  const maxIterations = maxIterationsOf(raw);
  if (maxIterations === undefined) {
    return stop("unknown", `loop ${stepId} has no maxIterations`);
  }
  const recorded = failurePolicyOf(raw, "onIterationFailure");
  if (recorded === "invalid") {
    return stop("unknown", `loop ${stepId} has an invalid onIterationFailure`);
  }
  // Absent is the same default `loopIterationCursor` fills in at the read.
  const policy = recorded === "tolerate" ? "tolerate" : "end";
  let frontier: number | undefined;
  for (let iteration = 0; iteration < maxIterations; iteration += 1) {
    const cursor = loopIterationCursor(
      run.state,
      run.state.runId,
      stepId,
      iteration,
      policy,
    );
    if (cursor.kind === "completed" || cursor.kind === "tolerated") continue;
    if (cursor.kind === "frontier") frontier = iteration;
    break;
  }
  if (frontier === undefined) return { park: false };
  const childRunId = loopBodyRunId(run.state.runId, stepId, frontier);
  const body = loopBodySteps(raw);
  const child = foldRun(childRunId, await ctx.readRun(childRunId), body, false);
  const reattach = await loopOccurrenceReattach({
    loopId: stepId,
    runId: run.state.runId,
    iteration: frontier,
    state: run.state,
    log: run.log,
    childState: child.state,
  });
  return finishReattach(ctx, reattach, child, body, stepId);
}

async function judgeOnTrigger(
  ctx: JudgeContext,
  run: JudgedRun,
  stepId: string,
  raw: unknown,
): Promise<Judge> {
  const policy = failurePolicyOf(raw, "onBodyFailure");
  if (policy === "invalid") {
    return stop("unknown", `onTrigger ${stepId} has an invalid onBodyFailure`);
  }
  const onBodyFailure =
    policy === "end" || policy === "tolerate" ? { onBodyFailure: policy } : {};
  const reattach = await onTriggerOccurrenceReattach({
    sectionId: stepId,
    ...onBodyFailure,
    state: run.state,
    log: run.log,
    childState: emptyState(stepId),
  });
  // The planner reads the child only when the container is not already
  // parked. Load that child when the decision names one, and re-judge so
  // the unrelayed-body throw sees the committed child log.
  if (reattach.kind !== "parked" && reattach.kind !== "reenter") {
    return finishReattach(ctx, reattach, undefined, undefined, stepId);
  }
  if (reattach.childRunId === undefined) {
    return finishReattach(ctx, reattach, undefined, undefined, stepId);
  }
  const body = onTriggerBodySteps(raw);
  const child = foldRun(
    reattach.childRunId,
    await ctx.readRun(reattach.childRunId),
    body,
    false,
  );
  const again = await onTriggerOccurrenceReattach({
    sectionId: stepId,
    ...onBodyFailure,
    state: run.state,
    log: run.log,
    childState: child.state,
  });
  return finishReattach(ctx, again, child, body, stepId);
}

// A spawn step's child resumes through the parent: the parent reattaches the
// child it recorded and relays the child's parks. Judge that child as a run of
// its own definition, so a child mid-invocation or holding an unsupported step
// is caught at any depth.
async function judgeChildWorkflow(
  ctx: JudgeContext,
  run: JudgedRun,
  stepId: string,
  raw: unknown,
): Promise<Judge> {
  let childRunId: string | undefined;
  for (const child of run.state.children.values()) {
    if (child.spawnedBy === stepId && child.terminalStatus === undefined) {
      childRunId = child.childRunId;
    }
  }
  // No live child: the spawn has not committed one yet, or the child ended
  // and the parent still has to record it. Resume spawns or settles it.
  if (childRunId === undefined) return { park: false };
  const steps = childWorkflowSteps(raw);
  if (steps === "ref" || steps === undefined) {
    return stop("unknown", `step ${stepId} child definition is not inline`);
  }
  const child = foldRun(
    childRunId,
    await ctx.readRun(childRunId),
    steps,
    false,
  );
  if (child.state.phase !== "running") return { park: false };
  return judgeRun(ctx, { steps, ...child });
}

async function finishReattach(
  ctx: JudgeContext,
  reattach: OccurrenceReattach,
  child: FoldedRun | undefined,
  body: Readonly<Record<string, unknown>> | "ref" | undefined,
  stepId: string,
): Promise<Judge> {
  if (reattach.kind === "rejected") {
    return ctx.mode === "park"
      ? stop("not-parked", `step ${stepId} cannot be re-attached`)
      : { park: false };
  }
  if (reattach.kind === "open") return { park: false };
  if (reattach.kind === "parked" && reattach.childRunId === undefined) {
    return { park: true };
  }
  if (body === "ref" || body === undefined) {
    return stop("unknown", `step ${stepId} body is not inline`);
  }
  if (child === undefined) {
    return stop("unknown", `step ${stepId} body run was not read`);
  }
  const judged = await judgeRun(ctx, { steps: body, ...child });
  if (judged.verdict === "stop") return judged;
  return {
    park: reattach.kind === "parked",
    ...(judged.wakeAt !== undefined ? { wakeAt: judged.wakeAt } : {}),
  };
}

// Reduce a run's log, judging each event against the state it was applied
// to, as the runtime saw it, for whether an input still waits for the run to
// act on it. A body that is only a ref is not judged here.
function foldRun(
  runId: string,
  log: readonly WorkflowEvent[],
  steps: Readonly<Record<string, unknown>> | "ref" | undefined,
  topLevel: boolean,
): FoldedRun {
  let state = emptyState(runId);
  let pending: PendingInput | null = null;
  for (const event of log) {
    const before = state;
    state = applyEvent(before, event);
    if (typeof steps !== "object") continue;
    pending = trackInput(steps, topLevel, before, state, event, pending);
  }
  if (typeof steps !== "object" || pending === null) {
    return { state, log, awaitsWork: false };
  }
  if (pending.trigger) return { state, log, awaitsWork: true };
  for (const stepId of pending.released.keys()) {
    // A released step still running is judged by its own progress: a
    // container by its body, an agent step as mid-invocation.
    const running =
      state.steps.get(stepId)?.phase === "in-flight" &&
      !isLanding(steps, stepId);
    if (!running) return { state, log, awaitsWork: true };
  }
  return { state, log, awaitsWork: false };
}

// Input the run has not acted on: its trigger, and the steps signals or
// timers set running, each with whether it has landed.
type PendingInput = {
  readonly trigger: boolean;
  readonly released: ReadonlyMap<string, boolean>;
};

// Input starts the top-level run or sets waiting steps running; a nested
// run's start is its parent step's progress. A gate or sleep an input
// released completing is only that input landing. A released step doing
// anything else acts on its input. A step starting acts on the trigger and
// on every input that has landed: a hold blocks every start but a skip, and
// the scheduler starts every ready step together.
function trackInput(
  steps: Readonly<Record<string, unknown>>,
  topLevel: boolean,
  before: RunState,
  after: RunState,
  event: WorkflowEvent,
  pending: PendingInput | null,
): PendingInput | null {
  switch (event.kind) {
    case "RunStarted":
      return topLevel ? { trigger: true, released: new Map() } : pending;
    case "SignalReceived":
    case "TimerFired": {
      const resumed = resumedWaitingSteps(before, after);
      return resumed.length === 0 ? pending : release(pending, resumed);
    }
    case "SignalAwaited":
      // An await that took a signal queued before it goes straight on.
      if (after.steps.get(event.stepId)?.phase === "in-flight") {
        return release(pending, [event.stepId]);
      }
      break;
    // A hold does not stop a cancelling run.
    case "CancelRequested":
    case "RunCompleted":
    case "RunFailed":
    case "RunCancelled":
      return null;
    default:
      break;
  }
  if (pending === null) return null;
  const stepId = "stepId" in event ? event.stepId : undefined;
  if (stepId !== undefined && pending.released.has(stepId)) {
    const released = new Map(pending.released);
    const lands =
      (event.kind === "StepCompleted" || event.kind === "StepFailed") &&
      isLanding(steps, stepId);
    if (lands) released.set(stepId, true);
    else released.delete(stepId);
    return pendingOf(pending.trigger, released);
  }
  if (event.kind === "StepStarted" && event.skip !== true) {
    const unlanded = [...pending.released].filter(([, landed]) => !landed);
    return pendingOf(false, new Map(unlanded));
  }
  return pending;
}

function release(
  pending: PendingInput | null,
  stepIds: readonly string[],
): PendingInput {
  const released = new Map(pending?.released ?? []);
  for (const stepId of stepIds) released.set(stepId, false);
  return { trigger: pending?.trigger ?? false, released };
}

function pendingOf(
  trigger: boolean,
  released: ReadonlyMap<string, boolean>,
): PendingInput | null {
  return trigger || released.size > 0 ? { trigger, released } : null;
}

function resumedWaitingSteps(before: RunState, after: RunState): string[] {
  const resumed: string[] = [];
  for (const [stepId, step] of after.steps) {
    const was = before.steps.get(stepId)?.phase;
    if (
      step.phase === "in-flight" &&
      (was === "awaiting-signal" || was === "awaiting-timer")
    ) {
      resumed.push(stepId);
    }
  }
  return resumed;
}

function isLanding(
  steps: Readonly<Record<string, unknown>>,
  stepId: string,
): boolean {
  const kind = stepKind(steps[baseStepId(stepId)]);
  return kind === "awaitSignal" || kind === "sleep";
}

function stop(verdict: ParkedRunVerdict, reason: string): Judge {
  return { verdict: "stop", classification: { verdict, reason } };
}

function earliestTimer(state: RunState): string | undefined {
  let earliest: string | undefined;
  for (const timer of state.pendingTimers.values()) {
    earliest = earlier(earliest, timer.fireAt);
  }
  return earliest;
}

function earlier(
  a: string | undefined,
  b: string | undefined,
): string | undefined {
  if (a === undefined) return b;
  if (b === undefined) return a;
  return Date.parse(b) < Date.parse(a) ? b : a;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stepKind(step: unknown): string | undefined {
  if (!isRecord(step)) return undefined;
  const kind = step["kind"];
  return typeof kind === "string" ? kind : undefined;
}

function kindLookup(steps: Readonly<Record<string, unknown>>): StepKindLookup {
  const out: Record<string, { readonly kind: string }> = {};
  for (const [id, step] of Object.entries(steps)) {
    const kind = stepKind(step);
    if (kind !== undefined) out[id] = { kind };
  }
  return { steps: out };
}

function maxIterationsOf(step: unknown): number | undefined {
  if (!isRecord(step)) return undefined;
  const value = step["maxIterations"];
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    return undefined;
  }
  return value;
}

function failurePolicyOf(
  step: unknown,
  field: "onIterationFailure" | "onBodyFailure",
): BodyFailurePolicy | "absent" | "invalid" {
  if (!isRecord(step) || !Object.prototype.hasOwnProperty.call(step, field)) {
    return "absent";
  }
  const value = step[field];
  if (value === "end" || value === "tolerate") return value;
  return "invalid";
}

function nestedSteps(
  value: unknown,
): Readonly<Record<string, unknown>> | undefined {
  if (!isRecord(value)) return undefined;
  const steps = value["steps"];
  if (!isRecord(steps)) return undefined;
  return steps;
}

function loopBodySteps(
  step: unknown,
): Readonly<Record<string, unknown>> | undefined {
  if (!isRecord(step)) return undefined;
  return nestedSteps(step["body"]);
}

function onTriggerBodySteps(
  step: unknown,
): Readonly<Record<string, unknown>> | "ref" | undefined {
  if (!isRecord(step) || !isRecord(step["body"])) return undefined;
  const body = step["body"];
  if (typeof body["ref"] === "string") return "ref";
  return nestedSteps(body["inline"]);
}

function childWorkflowSteps(
  step: unknown,
): Readonly<Record<string, unknown>> | "ref" | undefined {
  if (!isRecord(step) || !isRecord(step["definition"])) return undefined;
  const definition = step["definition"];
  if (typeof definition["ref"] === "string") return "ref";
  return nestedSteps(definition["inline"]);
}
