import {
  readCommittedWorkflowRunLifecycle,
  type RepoId,
  type RepoStore,
  type WorkflowRunLifecycle,
} from "@intx/hub-sessions";
import {
  deriveRunAddress,
  deriveWorkflowRunRepoId,
} from "@intx/workflow-deploy";

// Workflow-run events commit on the substrate's default branch; the
// supervisor wires the workflow-process child against this ref.
export const WORKFLOW_RUN_REF = "refs/heads/main";

export function workflowRunRepoIdForAddress(agentAddress: string): RepoId {
  return {
    kind: "workflow-run",
    id: deriveWorkflowRunRepoId(agentAddress),
  };
}

// The workflow-run repo that holds a top-level run's committed event log,
// addressed by the run's own id and the tenant's domain. The sidecar's deploy
// router keys this repo by `deriveWorkflowRunRepoId(deploymentAddress)`, where
// the deployment address is `deriveRunAddress({ runId, domain })` (see
// `deployWorkflowDefinition`, which passes `deploymentDomain: tenant.domain`).
// Every read side must reconstruct the identical address and apply the same
// sanitization, or it opens a different on-disk repo than the one events
// committed to -- so both the deploy/observe routes and the run-observe routes
// derive it through this one function.
export function workflowRunRepoId(runId: string, tenantDomain: string): RepoId {
  const deploymentAddress = deriveRunAddress({
    runId,
    domain: tenantDomain,
  });
  return workflowRunRepoIdForAddress(deploymentAddress);
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
