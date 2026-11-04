import { and, eq, inArray, type SQL } from "drizzle-orm";

import type { DB, DBExecutor } from "./client";
import { principal } from "./schema/principals";
import { parsePrincipalRow } from "./parse-row";
import type { PrincipalKeyStore } from "./principal-key-store";
import { deleteRunTokensForPrincipals } from "./run-token-store";

type DBHandle = DB["db"];

type PrincipalInsert = typeof principal.$inferInsert;
type ParsedPrincipal = ReturnType<typeof parsePrincipalRow>;

/**
 * Store for the `principal` table. This module is the single owner of
 * principal creation: every principal insert in the system routes through
 * this factory or `insertRunPrincipal`. Every principal the factory creates
 * is minted an active signing key in the same transaction, so a user
 * principal never exists without a key. A run principal gets no hub key.
 *
 * Each method accepts an optional transaction handle so the principal row and
 * its key join the transaction that co-writes the principal's roles and grants.
 * When no handle is passed the store opens its own transaction, because the
 * principal insert and the key mint are two writes that must commit together.
 */
export function createPrincipalStore(
  db: DBHandle,
  principalKeyStore: PrincipalKeyStore,
) {
  return {
    /**
     * Insert a new principal and mint its signing key, failing loudly on a
     * natural-key conflict. Callers that have already established the principal
     * is new (a fresh tenant owner, an invite past its existence pre-check) use
     * this so a concurrent duplicate surfaces as a unique violation rather than
     * a silent success.
     */
    async create(
      row: PrincipalInsert,
      tx?: DBExecutor,
    ): Promise<ParsedPrincipal> {
      const run = async (executor: DBExecutor): Promise<ParsedPrincipal> => {
        const [inserted] = await executor
          .insert(principal)
          .values(row)
          .returning();
        if (inserted === undefined) {
          throw new Error(
            `principalStore.create: insert returned no row for ${row.id}`,
          );
        }
        await principalKeyStore.generate(inserted.id, executor);
        return parsePrincipalRow(inserted);
      };
      return tx === undefined ? db.transaction(run) : run(tx);
    },
  };
}

export type PrincipalStore = ReturnType<typeof createPrincipalStore>;

/**
 * Insert the run principal of a run, in the caller's transaction, unless the
 * run already has one. A run principal is the `kind = 'workflow'` principal
 * whose `refId` is the run id. It gets no hub signing key: the party that
 * runs the workflow holds its private key.
 *
 * The conflict target is the natural key `(tenantId, kind, refId)`, so a
 * second insert for the same run inserts nothing. Returns whether this call
 * inserted the row; a caller that expects a fresh run decides what an
 * existing principal means.
 */
export async function insertRunPrincipal(
  tx: DBExecutor,
  row: { id: string; tenantId: string; runId: string; createdAt: Date },
): Promise<boolean> {
  const inserted = await tx
    .insert(principal)
    .values({
      id: row.id,
      tenantId: row.tenantId,
      kind: "workflow",
      refId: row.runId,
      status: "active",
      createdAt: row.createdAt,
      updatedAt: row.createdAt,
    })
    .onConflictDoNothing({
      target: [principal.tenantId, principal.kind, principal.refId],
    })
    .returning({ id: principal.id });
  return inserted.length > 0;
}

/**
 * Deactivate the run principals of the given runs and delete their run
 * tokens, in the caller's transaction. A run's principal is the
 * `kind = 'workflow'` principal whose `refId` is the run id; a run with none
 * (an internal, workflow-spawned run) is skipped.
 */
export async function deactivateRunPrincipals(
  tx: DBExecutor,
  tenantId: string,
  runIds: readonly string[],
  now: Date | SQL,
): Promise<void> {
  if (runIds.length === 0) return;
  const deactivated = await tx
    .update(principal)
    .set({ status: "deactivated", updatedAt: now })
    .where(
      and(
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "workflow"),
        inArray(principal.refId, [...runIds]),
      ),
    )
    .returning({ id: principal.id });
  await deleteRunTokensForPrincipals(
    tx,
    deactivated.map((row) => row.id),
  );
}
