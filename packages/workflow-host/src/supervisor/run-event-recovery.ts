// Bounded recovery for run-event compaction folds a crash interrupted.
//
// When a run terminates, the supervisor fires `compactRunEvents` in the
// background. A crash between the terminal commit and that fold leaves the
// run terminal but still in per-event form, and the terminal signal never
// fires again for it. At the next spawn the boot scan proposes those runs;
// this sweep re-runs the idempotent fold for each so the leaked per-event
// file count is reclaimed.

import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";

import { compactRunEvents } from "./run-event-compaction";

export type RecoverInterruptedCompactionsOpts = {
  /** Substrate handle the supervisor writes through. */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo for this deployment. */
  repoId: RepoId;
  /** Events ref the workflow-run repo writes to. */
  ref: string;
  /** Anchor run id used to construct the supervisor principal. */
  anchorRunId: string;
  /** Runs the boot scan proposes as terminal-but-per-event. */
  pendingSealRunIds: readonly string[];
};

/** A run whose recovery fold threw, paired with the failure cause. */
export type RecoveryFoldFailure = { runId: string; message: string };

/**
 * Re-seal runs a crash left terminal but still in per-event form, by re-running
 * the idempotent `compactRunEvents` for each proposed run. `compactRunEvents`
 * is authoritative: it no-ops a run that is already sealed or whose latest
 * event is not terminal, so a stale or mistaken proposal is a harmless no-op.
 *
 * Folds run serially. Every fold contends the same per-repo write lock that
 * live dispatch also takes, so folding one run at a time drains the backlog
 * without a thundering herd on that lock. One run's failure is caught so it
 * cannot abort the rest; the failed run id and its cause are returned -- not
 * logged here -- so the caller owns how to surface the aggregate.
 */
export async function recoverInterruptedCompactions(
  opts: RecoverInterruptedCompactionsOpts,
): Promise<{ sealed: number; failed: RecoveryFoldFailure[] }> {
  let sealed = 0;
  const failed: RecoveryFoldFailure[] = [];
  for (const runId of opts.pendingSealRunIds) {
    try {
      const { compacted } = await compactRunEvents({
        substrate: opts.substrate,
        repoId: opts.repoId,
        ref: opts.ref,
        anchorRunId: opts.anchorRunId,
        runId,
      });
      if (compacted) sealed += 1;
    } catch (cause) {
      failed.push({
        runId,
        message: cause instanceof Error ? cause.message : String(cause),
      });
    }
  }
  return { sealed, failed };
}
