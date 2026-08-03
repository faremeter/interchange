import {
  readCommittedWorkflowRunLifecycle,
  type RepoId,
  type RepoStore,
  type WorkflowRunLifecycle,
} from "@intx/hub-sessions";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

// Workflow-run events commit on the substrate's default branch; the
// supervisor wires the workflow-process child against this ref.
export const WORKFLOW_RUN_REF = "refs/heads/main";

export function workflowRunRepoIdForAddress(agentAddress: string): RepoId {
  return {
    kind: "workflow-run",
    id: deriveWorkflowRunRepoId(agentAddress),
  };
}

/** Read one or more run lifecycles from the same committed workflow-run tip. */
export async function readDurableWorkflowRunLifecycles(
  repoStore: RepoStore,
  agentAddress: string,
  runIds: readonly string[],
): Promise<ReadonlyMap<string, WorkflowRunLifecycle>> {
  const reads = await repoStore.openCommittedReads(
    { kind: "hub" },
    workflowRunRepoIdForAddress(agentAddress),
    WORKFLOW_RUN_REF,
  );
  const lifecycles = new Map<string, WorkflowRunLifecycle>();
  for (const runId of new Set(runIds)) {
    lifecycles.set(
      runId,
      await readCommittedWorkflowRunLifecycle(reads, runId),
    );
  }
  return lifecycles;
}

export async function readDurableWorkflowRunLifecycle(
  repoStore: RepoStore,
  agentAddress: string,
  runId: string,
): Promise<WorkflowRunLifecycle> {
  const lifecycles = await readDurableWorkflowRunLifecycles(
    repoStore,
    agentAddress,
    [runId],
  );
  return lifecycles.get(runId) ?? "absent";
}
