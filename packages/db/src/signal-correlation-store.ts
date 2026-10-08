import { and, eq, isNull } from "drizzle-orm";

import type { DB, DBExecutor } from "./client";
import { signalCorrelation } from "./schema/signal-correlations";
import { parseSignalCorrelationRow } from "./parse-row";

type DBHandle = DB["db"];

type SignalCorrelationInsert = typeof signalCorrelation.$inferInsert;
type ParsedSignalCorrelation = ReturnType<typeof parseSignalCorrelationRow>;

/**
 * Store for the `signal_correlation` table: maps an in-flight signal's
 * correlation id to the run and address to resume when it resolves. Methods
 * accept an optional transaction handle so a claim and its approval flip are
 * atomic.
 */
export function createSignalCorrelationStore(db: DBHandle) {
  return {
    async register(
      row: SignalCorrelationInsert,
      tx?: DBExecutor,
    ): Promise<ParsedSignalCorrelation> {
      const [inserted] = await (tx ?? db)
        .insert(signalCorrelation)
        .values(row)
        .returning();
      if (inserted === undefined) {
        throw new Error(
          `signalCorrelationStore.register: insert returned no row for ${row.correlationId}`,
        );
      }
      return parseSignalCorrelationRow(inserted);
    },

    /**
     * Idempotent `register`: on a `correlationId` primary-key conflict the
     * insert is a no-op and this returns `null`, so a redelivered register
     * frame (sidecar reconnect, log replay, supervisor restart) does not fail
     * the co-write. Returns the parsed row only when this call inserted.
     */
    async registerIfAbsent(
      row: SignalCorrelationInsert,
      tx?: DBExecutor,
    ): Promise<ParsedSignalCorrelation | null> {
      const [inserted] = await (tx ?? db)
        .insert(signalCorrelation)
        .values(row)
        .onConflictDoNothing({ target: signalCorrelation.correlationId })
        .returning();
      return inserted === undefined
        ? null
        : parseSignalCorrelationRow(inserted);
    },

    async resolveRoute(
      correlationId: string,
      tx?: DBExecutor,
    ): Promise<ParsedSignalCorrelation | null> {
      const row = await (tx ?? db).query.signalCorrelation.findFirst({
        where: eq(signalCorrelation.correlationId, correlationId),
      });
      return row === undefined ? null : parseSignalCorrelationRow(row);
    },

    /**
     * Atomically claim a correlation for terminal delivery. The
     * `resolved_at IS NULL` guard makes the claim single-shot, so a
     * redelivered signal is not delivered twice. `signalId`, when provided,
     * records which signal instance won the claim.
     */
    async claimTerminal(
      correlationId: string,
      resolvedAt: Date,
      signalId: string | null,
      tx?: DBExecutor,
    ): Promise<ParsedSignalCorrelation | null> {
      const [claimed] = await (tx ?? db)
        .update(signalCorrelation)
        .set({ resolvedAt, signalId })
        .where(
          and(
            eq(signalCorrelation.correlationId, correlationId),
            isNull(signalCorrelation.resolvedAt),
          ),
        )
        .returning();
      return claimed === undefined ? null : parseSignalCorrelationRow(claimed);
    },
  };
}

export type SignalCorrelationStore = ReturnType<
  typeof createSignalCorrelationStore
>;
