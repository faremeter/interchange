import type { StepPlace } from "@intx/types";
import type { ApprovalSnapshot } from "@intx/types/runtime";
import { signalName } from "@intx/types/signals";

import {
  resumeFromLog,
  type RunState,
  type WorkflowEvent,
} from "../state-machine/index";
import type { LoadParkedApproval } from "./env";
import { baseStepId, scopedStepIndex } from "./step-scope";

/**
 * Reads a body run's log. `stepPathPrefix` is that body run's step path
 * prefix.
 */
type ReadBodyLog = (
  runId: string,
  stepPathPrefix: readonly string[],
) => Promise<readonly WorkflowEvent[]>;

/**
 * The place of a step that can hold a parked approval, from its run's step
 * path prefix and the step id its run's log records. Only an agent step parks
 * on an approval, and the only scoped agent step id is a map item's, so a
 * scoped id decodes to the item's map index.
 */
export function parkedStepPlace(
  stepPathPrefix: readonly string[],
  stepId: string,
): StepPlace {
  const mapIndex = scopedStepIndex(stepId);
  return {
    stepPath: [...stepPathPrefix, baseStepId(stepId)],
    ...(mapIndex !== undefined ? { mapIndex } : {}),
  };
}

/**
 * The approval snapshot for a step parked on `correlationId`. A step that
 * suspended stores it with its pending operation. A container that relays a
 * body's park stores none, so the body run still awaiting the same
 * correlation is followed instead, through nested relays too.
 * `stepPathPrefix` is the step path prefix of `runId`.
 */
export async function findParkedApprovalSnapshot(args: {
  readLog: ReadBodyLog;
  load: LoadParkedApproval;
  runId: string;
  stepPathPrefix: readonly string[];
  state: RunState;
  stepId: string;
  attempt: number;
  correlationId: string;
}): Promise<ApprovalSnapshot | undefined> {
  const direct = await args.load({
    runId: args.runId,
    stepId: args.stepId,
    attempt: args.attempt,
    place: parkedStepPlace(args.stepPathPrefix, args.stepId),
    correlationId: args.correlationId,
  });
  if (direct !== undefined) return direct;
  return relayedSnapshot(
    args,
    args.state,
    args.stepPathPrefix,
    args.stepId,
    new Set(),
  );
}

// A run is read once.
async function relayedSnapshot(
  args: {
    readLog: ReadBodyLog;
    load: LoadParkedApproval;
    correlationId: string;
  },
  state: RunState,
  stepPathPrefix: readonly string[],
  stepId: string,
  seen: Set<string>,
): Promise<ApprovalSnapshot | undefined> {
  const name = signalName(args.correlationId);
  for (const child of state.children.values()) {
    if (child.spawnedBy !== stepId || child.terminalStatus !== undefined) {
      continue;
    }
    if (seen.has(child.childRunId)) continue;
    seen.add(child.childRunId);
    // A run's step path prefix is the chain of `ChildSpawned.stepId` values
    // from its top-level run down to it. The runtime extends a run's prefix by
    // the same container step id when it spawns the child, so a caller that
    // walks the logs gets the prefix the runtime gave the child.
    const childPrefix = [...stepPathPrefix, child.spawnedBy];
    const events = [...(await args.readLog(child.childRunId, childPrefix))];
    if (events.length === 0) continue;
    const childState = resumeFromLog(child.childRunId, events);
    for (const childStep of childState.steps.values()) {
      if (childStep.phase !== "awaiting-signal") continue;
      if (childStep.awaitingSignal?.name !== name) continue;
      const direct = await args.load({
        runId: child.childRunId,
        stepId: childStep.stepId,
        attempt: childStep.currentAttempt,
        place: parkedStepPlace(childPrefix, childStep.stepId),
        correlationId: args.correlationId,
      });
      if (direct !== undefined) return direct;
      const nested = await relayedSnapshot(
        args,
        childState,
        childPrefix,
        childStep.stepId,
        seen,
      );
      if (nested !== undefined) return nested;
    }
  }
  return undefined;
}
