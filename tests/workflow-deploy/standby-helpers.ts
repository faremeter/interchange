// Helpers the standby tests share: put a deployment in standby over the link,
// or drive the lifecycle service's sweep to it as the Hub does.

import { sidecar as sidecarTable, sidecarAllocation } from "@intx/db/schema";
import {
  WorkflowControlHistoryPendingError,
  createWorkflowHistoryReceiveTracker,
  createWorkflowLifecycleService,
  createWorkflowRunReader,
} from "@intx/hub-sessions";
import type { TestDb } from "@intx/test-harness/db-harness";

import {
  SIDECAR_ID,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

// Longer than any test's budget, so the worker's acknowledgement, not the
// clock, settles a control request.
export const CONTROL_TIMEOUT_MS = 300_000;

const MANIFEST_DIGEST = "1".repeat(64);

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

async function seedAllocation(
  db: TestDb["db"],
  ids: { tenantId: string; deploymentId: string },
  target: AllocationTarget,
): Promise<void> {
  const provisioner = {
    provisionerId: "test",
    provisionerApiVersion: 2 as const,
    provisionerBindingFingerprint: "test:1",
    adapterManifestDigest: MANIFEST_DIGEST,
  };
  await db.insert(sidecarTable).values({
    id: SIDECAR_ID,
    tokenHashSha256: new Uint8Array(32),
    ...provisioner,
  });
  await db.insert(sidecarAllocation).values({
    id: target.allocationId,
    tenantId: ids.tenantId,
    anchorRunId: ids.deploymentId,
    ...provisioner,
    maxDisconnectedMs: 900_000,
    sidecarId: SIDECAR_ID,
    status: "allocated",
    generation: target.generation,
    ensureAcceptedGeneration: target.generation,
  });
}

// A lifecycle service for one deployment, whose allocation this records, and
// the sweep that carries out its standby and wake requests.
export async function startStandbySweep(
  env: DeployFlowEnv,
  db: TestDb["db"],
  opts: {
    tenantId: string;
    deploymentId: string;
    target: AllocationTarget;
  },
) {
  const { tenantId, deploymentId, target } = opts;
  await seedAllocation(db, { tenantId, deploymentId }, target);
  let commandSettled = Promise.withResolvers<undefined>();
  // The sweep pauses between passes on this clock, which the sweep moves.
  let nowMs = Date.now();
  const lifecycle = createWorkflowLifecycleService({
    db,
    retentionRouter: env.hub.router,
    runReader: createWorkflowRunReader(env.hub.agentRepoStore.repoStore),
    historyReceives: createWorkflowHistoryReceiveTracker(),
    standbyRouter: env.hub.router,
    onStandbyCommandSettled: (runId) => {
      if (runId === deploymentId) commandSettled.resolve(undefined);
    },
    now: () => new Date(nowMs),
  });
  const standby = async () =>
    (await lifecycle.getStatus(tenantId, deploymentId))?.standby;
  // The state once the sweep has carried out the pending request. This run is
  // the only one the sweep has work for, so a pass that selects a run sends its
  // command, and sends it again once that settles unanswered: the sidecar's
  // first answer can wait on history the child is still pushing.
  const settled = async () => {
    for (;;) {
      const current = await standby();
      if (current?.state !== "entering" && current?.state !== "waking")
        return current;
      commandSettled = Promise.withResolvers<undefined>();
      if (await lifecycle.reconcileNext()) await commandSettled.promise;
      // The sweep selects nothing while it pauses between passes.
      else nowMs += 1_000;
    }
  };
  const untilInStandby = async (): Promise<void> => {
    const current = await settled();
    if (current === null || current === undefined) {
      throw new Error("the run ended before standby");
    }
    if (current.state !== "standby") {
      throw new Error(`standby was refused: ${String(current.reason)}`);
    }
  };
  return { lifecycle, standby, settled, untilInStandby };
}
