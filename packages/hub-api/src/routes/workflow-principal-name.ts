import { eq, inArray } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";

import { workflowDefinition, workflowRun } from "@intx/db/schema";
import type { DB } from "@intx/db";

/**
 * Resolve display names for `workflow`-kind principals, keyed by their refId.
 *
 * A workflow run-principal's refId is the run id, and its human-facing label is
 * the run's routing address. A folded launch's run carries that address
 * directly. A native run -- the deployment's anchor run, or a child run of it
 * -- reaches the address on the anchor run, the workflow_run whose id is the
 * deployment id. So this self-joins the run to its anchor on the deployment id
 * and prefers the anchor's address, falling back to the run's own (the folded
 * case, which has no deployment). Returns a map from runId to
 * `Workflow (<address>)`; a runId with no run row, or a run with neither
 * address, is absent (the caller falls back to the raw refId).
 */
export async function resolveWorkflowPrincipalNames(
  db: DB["db"],
  runIds: string[],
): Promise<Map<string, string>> {
  const names = new Map<string, string>();
  if (runIds.length === 0) return names;

  const anchor = alias(workflowRun, "anchor");
  const runs = await db
    .select({
      runId: workflowRun.id,
      runAddress: workflowRun.address,
      anchorAddress: anchor.address,
    })
    .from(workflowRun)
    .leftJoin(anchor, eq(workflowRun.deploymentId, anchor.id))
    .where(inArray(workflowRun.id, runIds));

  for (const r of runs) {
    const address = r.anchorAddress ?? r.runAddress;
    if (address !== null) {
      names.set(r.runId, `Workflow (${address})`);
    }
  }
  return names;
}

/**
 * Resolve display names for `workflow`-kind principals whose refId is a
 * workflow_definition id -- the folded agent's stable actor identity, re-keyed
 * off the legacy agent principal. Unlike a run-principal, a definition-principal
 * carries the definition's plain name, not a routing address. Returns a map from
 * definitionId to name; a definitionId with no definition row is absent (the
 * caller falls back to the raw refId). A refId that is actually a run id with no
 * address falls through to here and matches no definition, so it stays absent.
 */
export async function resolveDefinitionPrincipalNames(
  db: DB["db"],
  definitionIds: string[],
): Promise<Map<string, string>> {
  const names = new Map<string, string>();
  if (definitionIds.length === 0) return names;

  const defs = await db
    .select({ id: workflowDefinition.id, name: workflowDefinition.name })
    .from(workflowDefinition)
    .where(inArray(workflowDefinition.id, definitionIds));

  for (const d of defs) {
    names.set(d.id, d.name);
  }
  return names;
}
