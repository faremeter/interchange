import { eq, inArray } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";

import { workflowDefinition, workflowRun } from "@intx/db/schema";
import type { DB } from "@intx/db";

/**
 * Resolve display names for `workflow`-kind principals, keyed by their refId.
 *
 * A run-principal's refId is the run id and its label is the run's routing
 * address. A native run (the deployment's anchor run, or a child of it)
 * reaches the address on the anchor run, so this self-joins the run to its
 * anchor on the deployment id and prefers the anchor's address, falling back
 * to the run's own (the folded case, which has no deployment). Returns a map
 * from runId to `Workflow (<address>)`; a runId with no run row, or a run
 * with neither address, is absent (the caller falls back to the raw refId).
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
    .leftJoin(anchor, eq(workflowRun.anchorRunId, anchor.id))
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
 * off the legacy agent principal. A definition-principal carries the
 * definition's plain name, not a routing address. Returns a map from
 * definitionId to name; a definitionId with no definition row is absent.
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

/**
 * Resolve display names for `workflow`-kind principals, keyed by refId,
 * applying the run-then-definition fallthrough: a refId that names a run
 * resolves to its address label; one that does not (a re-keyed definition
 * principal) falls through to the definition resolver. A refId neither
 * resolves is absent, so the caller can fall back to the raw refId.
 */
export async function resolveWorkflowPrincipalLabels(
  db: DB["db"],
  refIds: string[],
): Promise<Map<string, string>> {
  const labels = await resolveWorkflowPrincipalNames(db, refIds);
  const unresolved = refIds.filter((id) => !labels.has(id));
  if (unresolved.length > 0) {
    const defNames = await resolveDefinitionPrincipalNames(db, unresolved);
    for (const [defId, name] of defNames) {
      labels.set(defId, name);
    }
  }
  return labels;
}
