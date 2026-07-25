import { eq, inArray } from "drizzle-orm";

import { workflowDeployment, workflowRun } from "@intx/db/schema";
import type { DB } from "@intx/db";

/**
 * Resolve display names for `workflow`-kind principals, keyed by their refId.
 *
 * A workflow run-principal's refId is the run id. Its human-facing label is the
 * run's address: an externally-triggered or native run reaches it by joining
 * the runId through `workflow_run` to `workflow_deployment`, while a folded
 * launch's run carries its address directly on `workflow_run` and has no
 * deployment. Left-join and prefer the deployment address, falling back to the
 * run's own. Returns a map from runId to `Workflow (<address>)`; a runId with
 * no run row, or a run with neither address, is simply absent (the caller falls
 * back to the raw refId).
 */
export async function resolveWorkflowPrincipalNames(
  db: DB["db"],
  runIds: string[],
): Promise<Map<string, string>> {
  const names = new Map<string, string>();
  if (runIds.length === 0) return names;

  const runs = await db
    .select({
      runId: workflowRun.id,
      runAddress: workflowRun.address,
      deploymentAddress: workflowDeployment.address,
    })
    .from(workflowRun)
    .leftJoin(
      workflowDeployment,
      eq(workflowRun.deploymentId, workflowDeployment.id),
    )
    .where(inArray(workflowRun.id, runIds));

  for (const r of runs) {
    const address = r.deploymentAddress ?? r.runAddress;
    if (address !== null) {
      names.set(r.runId, `Workflow (${address})`);
    }
  }
  return names;
}
