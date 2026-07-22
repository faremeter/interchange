import { eq, inArray } from "drizzle-orm";

import { workflowDeployment, workflowRun } from "@intx/db/schema";
import type { DB } from "@intx/db";

/**
 * Resolve display names for `workflow`-kind principals, keyed by their refId.
 *
 * A workflow run-principal's refId is the run id (not the deployment id): the
 * externally-triggered run mints the principal keyed on its runId. The only
 * human-facing label is the run's deployment address, reached by joining the
 * runId through `workflow_run` to `workflow_deployment`. Returns a map from
 * runId to `Workflow (<address>)`; a runId with no run row, or whose
 * deployment has vanished, is simply absent (the caller falls back to the raw
 * refId). Only externally-triggered runs mint these principals, so no
 * null-principal / internal-run branch is needed.
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
      address: workflowDeployment.address,
    })
    .from(workflowRun)
    .innerJoin(
      workflowDeployment,
      eq(workflowRun.deploymentId, workflowDeployment.id),
    )
    .where(inArray(workflowRun.id, runIds));

  for (const r of runs) {
    names.set(r.runId, `Workflow (${r.address})`);
  }
  return names;
}
