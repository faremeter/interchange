import type { ApprovalSnapshot } from "@intx/types/runtime";
import { signalName } from "@intx/types/signals";

import { resumeFromLog, type RunState } from "../state-machine/index";
import type { LoadParkedApproval, RepoStore } from "./env";

/**
 * The approval snapshot for a step parked on `correlationId`. A step that
 * suspended stores it with its pending operation. A container that relays a
 * body's park stores none, so the body run still awaiting the same
 * correlation is followed instead, through nested relays too.
 */
export async function findParkedApprovalSnapshot(args: {
  repoStore: Pick<RepoStore, "read">;
  load: LoadParkedApproval;
  runId: string;
  state: RunState;
  stepId: string;
  attempt: number;
  correlationId: string;
}): Promise<ApprovalSnapshot | undefined> {
  const direct = await args.load({
    runId: args.runId,
    stepId: args.stepId,
    attempt: args.attempt,
    correlationId: args.correlationId,
  });
  if (direct !== undefined) return direct;
  return relayedSnapshot(args, args.state, args.stepId, new Set());
}

// A run is read once.
async function relayedSnapshot(
  args: {
    repoStore: Pick<RepoStore, "read">;
    load: LoadParkedApproval;
    correlationId: string;
  },
  state: RunState,
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
    const events = [...(await args.repoStore.read(child.childRunId))];
    if (events.length === 0) continue;
    const childState = resumeFromLog(child.childRunId, events);
    for (const childStep of childState.steps.values()) {
      if (childStep.phase !== "awaiting-signal") continue;
      if (childStep.awaitingSignal?.name !== name) continue;
      const direct = await args.load({
        runId: child.childRunId,
        stepId: childStep.stepId,
        attempt: childStep.currentAttempt,
        correlationId: args.correlationId,
      });
      if (direct !== undefined) return direct;
      const nested = await relayedSnapshot(
        args,
        childState,
        childStep.stepId,
        seen,
      );
      if (nested !== undefined) return nested;
    }
  }
  return undefined;
}
