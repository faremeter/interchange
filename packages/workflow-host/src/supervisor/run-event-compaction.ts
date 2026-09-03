// Run-event compaction path for the per-deployment supervisor.
//
// Once a workflow run terminates, the supervisor folds that run's
// per-event `events/<seq>.json` blobs into one combined `events.jsonl`
// and drops the per-event files. This shrinks the workflow-run repo's
// file count -- and every per-commit cost that scales with it --
// without losing any event. The fold writes under the substrate's
// per-repo lock as the `supervisor` principal, whose `anchorRunId`
// the workflow-run kind handler checks against `repoId.id`.

import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
  WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions/substrate";
import {
  classifyTerminalEvent,
  parseEventSeq,
  WORKFLOW_RUN_EVENTS_FILE,
  encodeCombinedEventLog,
} from "@intx/hub-sessions/substrate";

import { SUPERVISOR_PRINCIPAL_KIND } from "./cancel-signing";

const RUNS_PREFIX = "runs";
const EVENTS_DIR = "events";

export type CompactRunEventsOpts = {
  /** Substrate handle the supervisor writes through. */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo for this deployment. */
  repoId: RepoId;
  /** Events ref the workflow-run repo writes to. */
  ref: string;
  /** Anchor run id used to construct the supervisor principal. */
  anchorRunId: string;
  /** Run to seal. */
  runId: string;
};

/**
 * Fold a terminated run's per-event `events/<seq>.json` blobs into one
 * combined `events.jsonl`, dropping the per-event files. This shrinks the
 * repo's file count -- and so every per-commit cost that scales with it --
 * without losing any event.
 *
 * Idempotent and terminal-only: a run already sealed (no `events/` subtree)
 * or one whose latest event is not terminal is left untouched, so the call
 * is safe to repeat. The live caller invokes it once per run, right after the
 * run terminates; `recoverInterruptedCompactions` re-runs it for a run whose
 * fold a crash interrupted before it could seal.
 *
 * The combined file is the verbatim byte concatenation of the per-event
 * blobs in seq order (`encodeCombinedEventLog`), the exact shape the
 * workflow-run kind handler's compaction validation requires. It is written
 * as a sibling of `events/`, so returning it from the merge while omitting
 * the per-event files lets the substrate's prefix clear drop them.
 */
export async function compactRunEvents(
  opts: CompactRunEventsOpts,
): Promise<{ compacted: boolean }> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = opts.substrate.getRepoDir(opts.repoId);
  const eventsDir = path.join(dir, RUNS_PREFIX, opts.runId, EVENTS_DIR);

  // Cheap pre-check off the working tree to skip an empty commit when there
  // is nothing to seal (already combined, or not yet terminal). The merge
  // re-reads the prefix under the per-repo lock, so the seal stays
  // consistent if another writer raced in between.
  let filenames: string[];
  try {
    filenames = await fs.readdir(eventsDir);
  } catch (cause) {
    if (isErrnoNotFound(cause)) return { compacted: false };
    throw cause;
  }
  const seqs: number[] = [];
  for (const name of filenames) {
    const seq = parseEventSeq(name);
    if (seq === null) continue;
    seqs.push(seq);
  }
  if (seqs.length === 0) return { compacted: false };
  const lastRaw = await fs.readFile(
    path.join(eventsDir, `${String(Math.max(...seqs))}.json`),
    "utf8",
  );
  let parsed: unknown;
  try {
    parsed = JSON.parse(lastRaw);
  } catch {
    return { compacted: false };
  }
  if (typeof parsed !== "object" || parsed === null || !("type" in parsed)) {
    return { compacted: false };
  }
  const lastType = (parsed as { type?: unknown }).type;
  if (
    typeof lastType !== "string" ||
    !classifyTerminalEvent(lastType).terminal
  ) {
    return { compacted: false };
  }

  const prefix = `${RUNS_PREFIX}/${opts.runId}/${EVENTS_DIR}/`;
  const combinedPath = `${RUNS_PREFIX}/${opts.runId}/${WORKFLOW_RUN_EVENTS_FILE}`;
  const principal: WorkflowRunSupervisorPrincipal = {
    kind: SUPERVISOR_PRINCIPAL_KIND,
    anchorRunId: opts.anchorRunId,
  };
  let sealed = false;
  await opts.substrate.writeTreePreservingPrefix(
    principal,
    opts.repoId,
    opts.ref,
    {
      preservePrefix: prefix,
      merge: async (existing) => {
        const entries: { seq: number; bytes: Uint8Array }[] = [];
        for (const [filepath, bytes] of existing) {
          const name = filepath.slice(prefix.length);
          const seq = parseEventSeq(name);
          if (seq === null) {
            throw new Error(
              `supervisor run-event-compaction: unexpected non-event file ${filepath} under run ${opts.runId}; refusing to compact`,
            );
          }
          entries.push({ seq, bytes });
        }
        if (entries.length === 0) return {};
        entries.sort((a, b) => a.seq - b.seq);
        sealed = true;
        return {
          [combinedPath]: encodeCombinedEventLog(entries.map((e) => e.bytes)),
        };
      },
      message: `compact run ${opts.runId} events`,
    },
  );
  return { compacted: sealed };
}

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  return (cause as { code?: unknown }).code === "ENOENT";
}
