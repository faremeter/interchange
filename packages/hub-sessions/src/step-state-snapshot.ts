import { type } from "arktype";

import { StepStateSnapshot } from "@intx/types";

import type { RepoId, RepoStore } from "./repo-store/types";
import {
  createCommittedStepStateReader,
  reconstructStepState,
} from "./step-state";
import {
  workflowRunLegacyAgentStatePrefix,
  workflowRunStepStatePrefix,
} from "./workflow-run-kind";

const HUB_PRINCIPAL = { kind: "hub" } as const;
const WORKFLOW_RUN_REF = "refs/heads/main";

/**
 * Export an agent step's state as last committed to the Hub's copy of a
 * deployment's workflow-run history. `runId` is the deployment's top-level
 * run: a single-step deployment that predates the steps subtree, and whose
 * agent has not restored since, still keeps its conversation at the legacy
 * `agent-state/<stepId>/`, which is read in its place. Pending operations are
 * left out, since their correlation ids only mean something inside the run
 * that registered them. Returns `null` when the step has no committed state.
 */
export async function readStepStateSnapshot(args: {
  repoStore: Pick<RepoStore, "openCommittedReads">;
  repoId: RepoId;
  runId: string;
  stepId: string;
}): Promise<StepStateSnapshot | null> {
  const reads = await args.repoStore.openCommittedReads(
    HUB_PRINCIPAL,
    args.repoId,
    WORKFLOW_RUN_REF,
  );
  if (reads === null) return null;
  const label = `${args.repoId.id} run ${args.runId} step ${args.stepId}`;
  const state =
    (await reconstructStepState(
      createCommittedStepStateReader(
        reads,
        workflowRunStepStatePrefix(args.runId, args.stepId),
      ),
      label,
    )) ??
    (await reconstructStepState(
      createCommittedStepStateReader(
        reads,
        workflowRunLegacyAgentStatePrefix(args.stepId),
      ),
      label,
    ));
  if (state === null) return null;
  const snapshot = StepStateSnapshot({
    version: 1,
    turns: state.turns,
    tokenUsage: state.tokenUsage,
    connectorState: state.connectorState,
  });
  if (snapshot instanceof type.errors) {
    throw new Error(
      `step state for ${label} is not a valid snapshot: ${snapshot.summary}`,
    );
  }
  return snapshot;
}
