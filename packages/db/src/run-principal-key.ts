import {
  and,
  eq,
  getTableName,
  sql,
  type AnyColumn,
  type SQL,
} from "drizzle-orm";

import type { DBExecutor } from "./client";
import { recordPublicKey, retireActiveKey } from "./principal-key-store";
import { principal } from "./schema/principals";
import { principalKey } from "./schema/principal-keys";
import { workflowRun } from "./schema/workflow-run";

// A column of the enclosing query, named with its table. Drizzle renders a
// bare column reference unqualified in a single-table statement, and inside
// these subqueries an unqualified `tenant_id` or `id` would bind to the
// principal tables instead of the enclosing row. The qualifier is the real
// table name, so the columns must come from an unaliased table.
function enclosing(column: AnyColumn): SQL {
  return sql`${sql.identifier(getTableName(column.table))}.${sql.identifier(column.name)}`;
}

// The run principal and its active key, joined under aliases of their own so
// that no name in the subquery collides with the enclosing query.
function runPrincipalKeyJoin(tenantId: AnyColumn, runId: AnyColumn): SQL {
  return sql`from "principal" "run_principal" inner join "principal_key" "run_principal_key" on "run_principal_key"."principal_id" = "run_principal"."id" and "run_principal_key"."status" = 'active' where "run_principal"."tenant_id" = ${enclosing(tenantId)} and "run_principal"."kind" = 'workflow' and "run_principal"."ref_id" = ${enclosing(runId)}`;
}

/**
 * A condition that holds when the run's principal has an active key: the
 * key the sidecar running the deployment recorded when its initialization
 * completed. It is the durable proof that the deployment's current allocation
 * generation finished initializing. `tenantId` and `runId` are columns of the
 * enclosing query.
 */
export function runPrincipalHasActiveKey(
  tenantId: AnyColumn,
  runId: AnyColumn,
): SQL {
  return sql`exists (select 1 ${runPrincipalKeyJoin(tenantId, runId)})`;
}

/**
 * The active key of the run's principal as a scalar subquery, for a query
 * that reads many runs. Null when the run's principal holds no active key.
 * `tenantId` and `runId` are columns of the enclosing query.
 */
export function runPrincipalActiveKeySql(
  tenantId: AnyColumn,
  runId: AnyColumn,
): SQL<string | null> {
  return sql<
    string | null
  >`(select "run_principal_key"."public_key" ${runPrincipalKeyJoin(tenantId, runId)} limit 1)`;
}

/**
 * The active key of the run's principal, or null when the run has none: its
 * deployment has not finished initializing, or its sidecar was replaced and
 * the replacement has not recorded a key yet.
 */
export async function loadRunPrincipalActiveKey(
  executor: DBExecutor,
  tenantId: string,
  runId: string,
): Promise<string | null> {
  const [row] = await executor
    .select({ publicKey: principalKey.publicKey })
    .from(principal)
    .innerJoin(
      principalKey,
      and(
        eq(principalKey.principalId, principal.id),
        eq(principalKey.status, "active"),
      ),
    )
    .where(
      and(
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "workflow"),
        eq(principal.refId, runId),
      ),
    )
    .limit(1);
  return row?.publicKey ?? null;
}

/**
 * The run principal of a run: the `kind = 'workflow'` principal whose `refId`
 * is the run id. A deployment's run principal exists from deploy. Returns
 * null when the run has none.
 */
async function loadRunPrincipalId(
  executor: DBExecutor,
  tenantId: string,
  runId: string,
): Promise<string | null> {
  const [row] = await executor
    .select({ id: principal.id })
    .from(principal)
    .where(
      and(
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "workflow"),
        eq(principal.refId, runId),
      ),
    )
    .limit(1);
  return row?.id ?? null;
}

/**
 * Make `publicKey`, which the sidecar running the deployment minted, the
 * active key of the deployment's run principal. The sidecar holds the private
 * key. Locks the deployment's anchor row first: every writer of a run's key
 * and every reader that must not race one holds that lock. Returns false when
 * `runId` names no deployment anchor in the tenant, and throws when the
 * anchor has no run principal. Call it in a transaction.
 */
export async function recordRunPrincipalKey(
  tx: DBExecutor,
  tenantId: string,
  runId: string,
  publicKey: string,
): Promise<boolean> {
  // FOR NO KEY UPDATE, as an update of a non-key column would take: it
  // conflicts with the other anchor locks but not with the key-share locks
  // that inserts referencing the anchor take.
  const [anchor] = await tx
    .select({ id: workflowRun.id })
    .from(workflowRun)
    .where(
      and(
        eq(workflowRun.id, runId),
        eq(workflowRun.anchorRunId, runId),
        eq(workflowRun.tenantId, tenantId),
      ),
    )
    .for("no key update");
  if (anchor === undefined) return false;
  const principalId = await loadRunPrincipalId(tx, tenantId, runId);
  if (principalId === null) {
    throw new Error(`Run ${runId} has no run principal to hold its key`);
  }
  await recordPublicKey(tx, principalId, publicKey);
  return true;
}

/**
 * Retire the active key of the run's principal, so the key a replaced sidecar
 * held no longer answers for the run. A run with no run principal, or with no
 * active key, is left as it is. The caller holds the deployment's anchor row
 * lock.
 */
export async function retireRunPrincipalKey(
  tx: DBExecutor,
  tenantId: string,
  runId: string,
): Promise<void> {
  const principalId = await loadRunPrincipalId(tx, tenantId, runId);
  if (principalId === null) return;
  await retireActiveKey(tx, principalId);
}
