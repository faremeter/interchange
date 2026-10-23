// Whether a workflow child started fresh on a committed log would pick a
// lost worker's runs back up. The answer is the resume guard
// (`resumeResidualOf`) plus the same loop and onTrigger planners the runtime
// drives. This module does not list park shapes of its own.

import type { BodyFailurePolicy } from "../definition/index";
import { resumeFromLog, type WorkflowEvent } from "../state-machine/index";
import { resumeResidualOf, type StepKindLookup } from "./dag";
import {
  loopIterationCursor,
  loopOccurrenceReattach,
  onTriggerOccurrenceReattach,
  type OccurrenceReattach,
} from "./run";
import { loopBodyRunId } from "./step-scope";

export type ParkedRunVerdict = "parked" | "not-parked" | "unknown";

export type ParkedRunClassification = {
  readonly verdict: ParkedRunVerdict;
  readonly reason: string;
};

export async function classifyParkedRun(args: {
  readonly anchorRunId: string;
  /** The top-level definition's `steps`: a live definition or its projection. */
  readonly steps: Readonly<Record<string, unknown>>;
  readonly readRun: (runId: string) => Promise<readonly WorkflowEvent[]>;
}): Promise<ParkedRunClassification> {
  const events = await args.readRun(args.anchorRunId);
  if (events.length === 0) {
    return { verdict: "not-parked", reason: "committed log is empty" };
  }
  const state = resumeFromLog(args.anchorRunId, events);
  if (state.phase !== "running") {
    return { verdict: "not-parked", reason: `anchor phase is ${state.phase}` };
  }
  const lookup = kindLookup(args.steps);
  let hasPark = false;
  for (const [stepId, step] of state.steps) {
    const residual = resumeResidualOf(lookup, stepId, step.phase);
    if (residual === "unsupported") {
      return {
        verdict: "not-parked",
        reason: `step ${stepId} is ${step.phase}`,
      };
    }
    const kind = stepKind(args.steps[stepId]);
    if (
      residual === "continue" &&
      step.phase === "awaiting-signal" &&
      kind !== "loop" &&
      kind !== "onTrigger"
    ) {
      hasPark = true;
    }
  }
  for (const [stepId, step] of state.steps) {
    if (step.phase !== "in-flight" && step.phase !== "awaiting-signal") {
      continue;
    }
    const raw = args.steps[stepId];
    const kind = stepKind(raw);
    if (kind === "loop") {
      const judged = await judgeLoop({
        readRun: args.readRun,
        anchorRunId: args.anchorRunId,
        stepId,
        raw,
        state,
        log: events,
      });
      if (judged.verdict !== undefined) return judged.classification;
      if (judged.park) hasPark = true;
    } else if (kind === "onTrigger") {
      const judged = await judgeOnTrigger({
        readRun: args.readRun,
        stepId,
        raw,
        state,
        log: events,
      });
      if (judged.verdict !== undefined) return judged.classification;
      if (judged.park) hasPark = true;
    }
  }
  return hasPark
    ? { verdict: "parked", reason: "resume guard would re-attach a park" }
    : {
        verdict: "not-parked",
        reason: "resume guard found no re-attachable park",
      };
}

type Judge =
  | {
      readonly verdict: "stop";
      readonly classification: ParkedRunClassification;
      readonly park?: false;
    }
  | { readonly verdict?: undefined; readonly park: boolean };

async function judgeLoop(args: {
  readRun: (runId: string) => Promise<readonly WorkflowEvent[]>;
  anchorRunId: string;
  stepId: string;
  raw: unknown;
  state: ReturnType<typeof resumeFromLog>;
  log: readonly WorkflowEvent[];
}): Promise<Judge> {
  const maxIterations = maxIterationsOf(args.raw);
  if (maxIterations === undefined) {
    return stop("unknown", `loop ${args.stepId} has no maxIterations`);
  }
  const recorded = failurePolicyOf(args.raw, "onIterationFailure");
  if (recorded === "invalid") {
    return stop(
      "unknown",
      `loop ${args.stepId} has an invalid onIterationFailure`,
    );
  }
  // Absent is the same default `loopIterationCursor` fills in at the read.
  const policy = recorded === "tolerate" ? "tolerate" : "end";
  let frontier: number | undefined;
  for (let iteration = 0; iteration < maxIterations; iteration += 1) {
    const cursor = loopIterationCursor(
      args.state,
      args.anchorRunId,
      args.stepId,
      iteration,
      policy,
    );
    if (cursor.kind === "completed" || cursor.kind === "tolerated") continue;
    if (cursor.kind === "frontier") frontier = iteration;
    break;
  }
  if (frontier === undefined) return { park: false };
  const childRunId = loopBodyRunId(args.anchorRunId, args.stepId, frontier);
  const childState = resumeFromLog(childRunId, await args.readRun(childRunId));
  const reattach = await loopOccurrenceReattach({
    loopId: args.stepId,
    runId: args.anchorRunId,
    iteration: frontier,
    state: args.state,
    log: args.log,
    childState,
  });
  return finishReattach(
    reattach,
    childState,
    loopBodyLookup(args.raw),
    args.stepId,
  );
}

async function judgeOnTrigger(args: {
  readRun: (runId: string) => Promise<readonly WorkflowEvent[]>;
  stepId: string;
  raw: unknown;
  state: ReturnType<typeof resumeFromLog>;
  log: readonly WorkflowEvent[];
}): Promise<Judge> {
  const policy = failurePolicyOf(args.raw, "onBodyFailure");
  if (policy === "invalid") {
    return stop(
      "unknown",
      `onTrigger ${args.stepId} has an invalid onBodyFailure`,
    );
  }
  const childState = resumeFromLog(args.stepId, []);
  const reattach = await onTriggerOccurrenceReattach({
    sectionId: args.stepId,
    ...(policy === "end" || policy === "tolerate"
      ? { onBodyFailure: policy }
      : {}),
    state: args.state,
    log: args.log,
    childState,
  });
  // The planner reads the child only when the container is not already
  // parked. Load that child when the decision names one, and re-judge so
  // the unrelayed-body throw sees the committed child log.
  if (reattach.kind !== "parked" && reattach.kind !== "reenter") {
    return { park: false };
  }
  if (reattach.childRunId === undefined)
    return { park: reattach.kind === "parked" };
  const loaded = resumeFromLog(
    reattach.childRunId,
    await args.readRun(reattach.childRunId),
  );
  const again = await onTriggerOccurrenceReattach({
    sectionId: args.stepId,
    ...(policy === "end" || policy === "tolerate"
      ? { onBodyFailure: policy }
      : {}),
    state: args.state,
    log: args.log,
    childState: loaded,
  });
  return finishReattach(
    again,
    loaded,
    onTriggerBodyLookup(args.raw),
    args.stepId,
  );
}

function finishReattach(
  reattach: OccurrenceReattach,
  childState: ReturnType<typeof resumeFromLog>,
  body: StepKindLookup | "ref" | undefined,
  stepId: string,
): Judge {
  if (reattach.kind === "rejected" || reattach.kind === "open") {
    return { park: false };
  }
  if (reattach.kind === "parked" && reattach.childRunId === undefined) {
    return { park: true };
  }
  if (body === "ref" || body === undefined) {
    return stop("unknown", `step ${stepId} body is not inline`);
  }
  for (const [id, step] of childState.steps) {
    if (resumeResidualOf(body, id, step.phase) === "unsupported") {
      return stop("not-parked", `child step ${id} is ${step.phase}`);
    }
  }
  return { park: reattach.kind === "parked" };
}

function stop(verdict: ParkedRunVerdict, reason: string): Judge {
  return { verdict: "stop", classification: { verdict, reason } };
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

function nestedSteps(value: unknown): StepKindLookup | undefined {
  if (!isRecord(value)) return undefined;
  const steps = value["steps"];
  if (!isRecord(steps)) return undefined;
  return kindLookup(steps);
}

function loopBodyLookup(step: unknown): StepKindLookup | "ref" | undefined {
  if (!isRecord(step)) return undefined;
  return nestedSteps(step["body"]);
}

function onTriggerBodyLookup(
  step: unknown,
): StepKindLookup | "ref" | undefined {
  if (!isRecord(step) || !isRecord(step["body"])) return undefined;
  const body = step["body"];
  if (typeof body["ref"] === "string") return "ref";
  return nestedSteps(body["inline"]);
}
