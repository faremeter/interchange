import type { AgentRepoStore } from "./agent-repo";
import {
  workflowRunRepoIdForAddress,
  WORKFLOW_RUN_REF,
} from "./workflow-run-kind";
import type {
  AllocatedSidecarTarget,
  SidecarAllocationRouter,
} from "./ws/sidecar-handler";

export const WORKFLOW_RUN_RESTORE_REFS = [
  WORKFLOW_RUN_REF,
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
  signal?: AbortSignal;
}): Promise<void> {
  const { agentRepoStore, allocationRouter, allocationTarget, agentAddress } =
    args;
  const principal = { kind: "hub" } as const;
  const repoId = workflowRunRepoIdForAddress(agentAddress);

  for (const ref of WORKFLOW_RUN_RESTORE_REFS) {
    args.signal?.throwIfAborted();
    const tip = await agentRepoStore.repoStore.resolveRef(
      principal,
      repoId,
      ref,
    );
    args.signal?.throwIfAborted();
    if (tip === null) continue;

    const pack = await agentRepoStore.repoStore.createPack(
      principal,
      repoId,
      ref,
    );
    args.signal?.throwIfAborted();
    await allocationRouter.sendWorkflowRunPackToAllocation(
      allocationTarget,
      agentAddress,
      pack.pack,
      pack.ref,
      pack.commitSha,
      args.signal,
    );
  }
  args.signal?.throwIfAborted();
}
