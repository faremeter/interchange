// A workflow event on disk: its flat shape, its path, and the append that
// writes it. The supervisor writes events too, and the sidecar that loads it
// must not load the workflow runtime, so these stay apart from the runtime's
// store adapter.

import {
  parseEventSeq,
  WORKFLOW_RUN_EVENTS_DIR,
  WORKFLOW_RUN_RUNS_PREFIX,
  type Principal,
  type RepoId,
  type RepoStore as SubstrateRepoStore,
  type WriteResult,
} from "@intx/hub-sessions/substrate";
import type { WorkflowEvent } from "@intx/workflow";

/**
 * Translate a state-machine `WorkflowEvent` (using `kind` as the
 * discriminator) into the on-disk envelope shape (`{seq, type,
 * ...rest}`) the workflow-run kind handler validates and the
 * substrate's `subscribeKind` helper filters on. Exported so the
 * supervisor's terminal-commit path encodes a supervisor-authored
 * `RunFailed` through the same single source of the on-disk shape.
 */
export function workflowEventToOnDisk(
  event: WorkflowEvent,
  seq: number,
): Record<string, unknown> {
  const { kind, seq: _eventSeq, ...rest } = event;
  return { seq, type: kind, ...rest };
}

/**
 * A per-event file of a run's log in a parent tree. `oid` reads the
 * file's bytes through the delta's `readBlobByOid`.
 */
export type PriorRunEvent = { seq: number; oid: string };

export type SubstrateDeltaReads = Parameters<
  Parameters<SubstrateRepoStore["writeTreeDelta"]>[3]["computeDelta"]
>[1];

/**
 * Append to a run's per-event log in one commit that writes only the
 * new event files. `compute` receives the run's events in the parent
 * tree, ascending by seq, and returns the files to add, keyed by path;
 * an empty result leaves the log unchanged. Reading the parent's file
 * names instead of its bytes keeps the blobs an append reads and writes
 * independent of how many events the run already holds; the events tree
 * is still listed and rewritten whole, so that part grows with the run.
 */
export async function appendRunEvents(args: {
  substrate: SubstrateRepoStore;
  principal: Principal;
  repoId: RepoId;
  ref: string;
  runId: string;
  message: string;
  compute: (
    events: readonly PriorRunEvent[],
    prior: SubstrateDeltaReads,
  ) => Promise<Record<string, string | Uint8Array>>;
}): Promise<WriteResult> {
  const dir = `${WORKFLOW_RUN_RUNS_PREFIX}/${args.runId}/${WORKFLOW_RUN_EVENTS_DIR}`;
  return args.substrate.writeTreeDelta(args.principal, args.repoId, args.ref, {
    changedPathPrefixes: new Set([`${dir}/`]),
    message: args.message,
    computeDelta: async (_parentCommitSha, prior) => {
      const events: PriorRunEvent[] = [];
      for (const entry of await prior.listDirOids(dir)) {
        const seq = parseEventSeq(entry.name);
        if (seq !== null) events.push({ seq, oid: entry.oid });
      }
      events.sort((a, b) => a.seq - b.seq);
      return { puts: await args.compute(events, prior), deletes: [] };
    },
  });
}

/** The path of the per-event file at `seq` in a run's log. */
export function runEventPath(runId: string, seq: number): string {
  return `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/${WORKFLOW_RUN_EVENTS_DIR}/${String(seq)}.json`;
}
