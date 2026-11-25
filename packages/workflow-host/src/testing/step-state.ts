import {
  serializeStepStateWalEntry,
  stepStateWalBucket,
  stepStateWalBucketPrefix,
  stepStateWalEntryPath,
  workflowRunStepStatePrefix,
  type Principal,
  type RepoId,
  type RepoStore,
  type StepStateMetadata,
} from "@intx/hub-sessions";

/**
 * Commit step state with no invocation stamp, as an earlier version of the
 * conversation store wrote it: one WAL entry at boundary 0, under the state
 * directory of `stepId` in `runId`, that holds every turn. The state
 * directory must hold no state yet.
 */
export async function commitUnstampedStepState(args: {
  substrate: RepoStore;
  workflowRunRepoId: RepoId;
  workflowRunRef: string;
  principal: Principal;
  runId: string;
  stepId: string;
  turns: readonly unknown[];
  metadata: Omit<StepStateMetadata, "invocation" | "parkedTurnStart">;
}): Promise<void> {
  const stateDir = workflowRunStepStatePrefix(args.runId, args.stepId);
  const entryPath = stepStateWalEntryPath(stateDir, 0);
  await args.substrate.writeTreePreservingPrefix(
    args.principal,
    args.workflowRunRepoId,
    args.workflowRunRef,
    {
      preservePrefix: stepStateWalBucketPrefix(stateDir, stepStateWalBucket(0)),
      merge: async () => ({
        [entryPath]: serializeStepStateWalEntry(0, args.turns, args.metadata),
      }),
      message: `commit unstamped state for ${args.stepId}`,
    },
  );
}
