import { describe, expect, test } from "bun:test";

import { resolveActivePrice, type ModelPricingRow } from "./pricing";

function row(
  overrides: Partial<ModelPricingRow> &
    Pick<ModelPricingRow, "currency" | "effectiveFrom">,
): ModelPricingRow {
  return {
    id: `${overrides.currency}-${overrides.effectiveFrom.toISOString()}`,
    tenantId: "t1",
    offeringId: "off1",
    inputTokenPrice: null,
    outputTokenPrice: null,
    cacheReadTokenPrice: null,
    cacheWriteTokenPrice: null,
    thinkingTokenPrice: null,
    perRequestFee: null,
    perImageFee: null,
    perAudioFee: null,
    createdAt: new Date("2020-01-01T00:00:00Z"),
    ...overrides,
  };
}

const JAN = new Date("2024-01-01T00:00:00Z");
const JUN = new Date("2024-06-01T00:00:00Z");
const DEC = new Date("2024-12-01T00:00:00Z");

describe("resolveActivePrice", () => {
  test("returns an empty list when there are no rows", () => {
    expect(resolveActivePrice([], JUN)).toEqual([]);
  });

  test("picks the latest row at or before asOf for a currency", () => {
    const rows = [
      row({ currency: "USD", effectiveFrom: JAN, inputTokenPrice: "1" }),
      row({ currency: "USD", effectiveFrom: JUN, inputTokenPrice: "2" }),
    ];
    const active = resolveActivePrice(rows, DEC);
    expect(active).toHaveLength(1);
    expect(active[0]?.inputTokenPrice).toBe("2");
  });

  test("ignores rows whose effectiveFrom is after asOf", () => {
    const rows = [
      row({ currency: "USD", effectiveFrom: JAN, inputTokenPrice: "1" }),
      row({ currency: "USD", effectiveFrom: DEC, inputTokenPrice: "2" }),
    ];
    const active = resolveActivePrice(rows, JUN);
    expect(active).toHaveLength(1);
    expect(active[0]?.inputTokenPrice).toBe("1");
  });

  test("treats a row effective exactly at asOf as in effect", () => {
    const rows = [row({ currency: "USD", effectiveFrom: JUN })];
    expect(resolveActivePrice(rows, JUN)).toHaveLength(1);
  });

  test("resolves each currency independently", () => {
    const rows = [
      row({ currency: "USD", effectiveFrom: JAN, inputTokenPrice: "usd-old" }),
      row({ currency: "USD", effectiveFrom: JUN, inputTokenPrice: "usd-new" }),
      row({ currency: "CREDIT", effectiveFrom: JAN, inputTokenPrice: "cr" }),
    ];
    const active = resolveActivePrice(rows, DEC);
    const byCurrency = new Map(active.map((r) => [r.currency, r]));
    expect(byCurrency.get("USD")?.inputTokenPrice).toBe("usd-new");
    expect(byCurrency.get("CREDIT")?.inputTokenPrice).toBe("cr");
  });

  test("excludes a currency entirely when all its rows are future-dated", () => {
    const rows = [
      row({ currency: "USD", effectiveFrom: JAN }),
      row({ currency: "CREDIT", effectiveFrom: DEC }),
    ];
    const active = resolveActivePrice(rows, JUN);
    expect(active.map((r) => r.currency)).toEqual(["USD"]);
  });
});
