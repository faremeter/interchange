import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import type { AgentRepoStore } from "./agent-repo";
import type {
  AllocatedSidecarTarget,
  SidecarAllocationRouter,
} from "./ws/sidecar-handler";

export const WORKFLOW_RUN_RESTORE_REFS = [
  "refs/heads/main",
  "refs/heads/events",
] as const;

/**
 * Replay every authoritative workflow-run ref the runtime understands onto an
 * exact replacement allocation. Refs are sent sequentially and the function
 * resolves only after the worker acknowledges each one, making it a barrier
 * the deploy path can place before supervisor spawn.
 */
export async function restoreWorkflowRunToAllocation(args: {
  agentRepoStore: AgentRepoStore;
  allocationRouter: Pick<
    SidecarAllocationRouter,
    "sendWorkflowRunPackToAllocation"
  >;
  allocationTarget: AllocatedSidecarTarget;
  agentAddress: string;
}): Promise<void> {
  const { agentRepoStore, allocationRouter, allocationTarget, agentAddress } =
    args;
  const principal = { kind: "hub" } as const;
  const repoId = {
    kind: "workflow-run" as const,
    id: deriveWorkflowRunRepoId(agentAddress),
  };

  for (const ref of WORKFLOW_RUN_RESTORE_REFS) {
    const tip = await agentRepoStore.repoStore.resolveRef(
      principal,
      repoId,
      ref,
    );
    if (tip === null) continue;

    const pack = await agentRepoStore.repoStore.createPack(
      principal,
      repoId,
      ref,
    );
    await allocationRouter.sendWorkflowRunPackToAllocation(
      allocationTarget,
      agentAddress,
      pack.pack,
      pack.ref,
      pack.commitSha,
    );
  }
}
