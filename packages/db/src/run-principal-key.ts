import { and, eq } from "drizzle-orm";

import type { DBExecutor } from "./client";
import { recordPublicKey, retireActiveKey } from "./principal-key-store";
import { principal } from "./schema/principals";

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
 * Make `publicKey`, which the sidecar running the run minted, the active key
 * of the run's principal. The sidecar holds the private key. The caller holds
 * the run's anchor row lock, which serializes every writer of a run's key.
 * Throws when the run has no run principal.
 */
export async function recordRunPrincipalKey(
  tx: DBExecutor,
  tenantId: string,
  runId: string,
  publicKey: string,
): Promise<void> {
  const principalId = await loadRunPrincipalId(tx, tenantId, runId);
  if (principalId === null) {
    throw new Error(`Run ${runId} has no run principal to hold its key`);
  }
  await recordPublicKey(tx, principalId, publicKey);
}

/**
 * Retire the active key of the run's principal, so the key a replaced sidecar
 * held no longer answers for the run. A run with no run principal, or with no
 * active key, is left as it is.
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
