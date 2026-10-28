// Helpers the standby tests share: put a deployment in standby over the link.

import { WorkflowControlHistoryPendingError } from "@intx/hub-sessions";

import { waitFor, type DeployFlowEnv } from "../hub-agent/lib/deploy-flow-env";

// Longer than any test's budget, so the worker's acknowledgement, not the
// clock, settles a control request.
export const CONTROL_TIMEOUT_MS = 300_000;

type AllocationTarget = { allocationId: string; generation: number };

// The standby attempt can overtake the child's last pushes; a repeated one
// answers again once that history has landed.
export async function putInStandby(
  env: DeployFlowEnv,
  target: AllocationTarget,
): Promise<void> {
  await waitFor(
    async () => {
      try {
        await env.hub.router.putAllocationInStandby(target, CONTROL_TIMEOUT_MS);
        return true;
      } catch (error) {
        if (error instanceof WorkflowControlHistoryPendingError) return false;
        throw error;
      }
    },
    { diagnostics: env.sidecarDiagnostics },
  );
}
