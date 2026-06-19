import type { modelPricing } from "./schema/catalog";

export type ModelPricingRow = typeof modelPricing.$inferSelect;

/**
 * Selects the active pricing row per currency from an offering's append-only
 * price history. For each currency, the active row is the one with the
 * greatest `effectiveFrom` that is at or before `asOf`; rows dated after
 * `asOf` are not yet in effect. The `(offeringId, currency, effectiveFrom)`
 * uniqueness on the table guarantees no ties, so the selection is
 * unambiguous.
 *
 * Pure: `asOf` is a parameter, never read from the clock, so the same
 * function serves discovery (asOf = now) and historical cost attribution
 * (asOf = the call's billing timestamp).
 */
export function resolveActivePrice(
  rows: ModelPricingRow[],
  asOf: Date,
): ModelPricingRow[] {
  const byCurrency = new Map<string, ModelPricingRow>();
  for (const row of rows) {
    if (row.effectiveFrom > asOf) continue;
    const current = byCurrency.get(row.currency);
    if (current === undefined || row.effectiveFrom > current.effectiveFrom) {
      byCurrency.set(row.currency, row);
    }
  }
  return Array.from(byCurrency.values());
}
