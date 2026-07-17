import { and, eq, isNull } from "drizzle-orm";

import type { DB } from "./client";
import { signalCorrelation } from "./schema/signal-correlations";
import { parseSignalCorrelationRow } from "./parse-row";

type DBHandle = DB["db"];

type SignalCorrelationInsert = typeof signalCorrelation.$inferInsert;
type ParsedSignalCorrelation = ReturnType<typeof parseSignalCorrelationRow>;

/**
 * Store for the `signal_correlation` table, which maps an in-flight signal
 * (correlation id) to the run and address that must be resumed when the signal
 * resolves. Each method accepts an optional transaction handle so the resolver
 * can claim the correlation and flip its approval atomically.
 */
export function createSignalCorrelationStore(db: DBHandle) {
  return {
    async register(
      row: SignalCorrelationInsert,
      tx?: DBHandle,
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

    async resolveRoute(
      correlationId: string,
      tx?: DBHandle,
    ): Promise<ParsedSignalCorrelation | null> {
      const row = await (tx ?? db).query.signalCorrelation.findFirst({
        where: eq(signalCorrelation.correlationId, correlationId),
      });
      return row === undefined ? null : parseSignalCorrelationRow(row);
    },

    /**
     * Atomically claim a correlation for terminal delivery. The
     * `resolved_at IS NULL` guard makes the claim single-shot: the first caller
     * stamps `resolvedAt` and gets the row back; any second caller matches no
     * row and receives null, so a redelivered signal is not delivered twice.
     * When `signalId` is provided it is persisted on the same update, recording
     * which signal instance won the claim for redelivery idempotency.
     */
    async claimTerminal(
      correlationId: string,
      resolvedAt: Date,
      signalId: string | null,
      tx?: DBHandle,
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
