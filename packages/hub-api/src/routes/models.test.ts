import { describe, test, expect } from "bun:test";

import type {
  ModelPricingRow,
  ModelOfferingRow,
  ModelProviderRow,
  ModelRow,
  ResolvedOffering,
} from "@intx/db";

import { composeDiscoveredModels } from "./models";

const D = new Date("2025-01-01T00:00:00Z");

function model(id: string, canonicalName: string): ModelRow {
  return {
    id,
    tenantId: "t1",
    canonicalName,
    displayName: null,
    description: null,
    disabled: false,
    createdAt: D,
    updatedAt: D,
  };
}

function provider(id: string, name: string): ModelProviderRow {
  return {
    id,
    tenantId: "t1",
    name,
    plugin: "anthropic",
    baseURL: "https://example.com",
    credentialId: "crd_x",
    walletId: null,
    disabled: false,
    createdAt: D,
    updatedAt: D,
  };
}

function offering(
  id: string,
  modelId: string,
  providerId: string,
  priority: number,
): ModelOfferingRow {
  return {
    id,
    tenantId: "t1",
    modelId,
    providerId,
    priority,
    deploymentTags: [],
    capabilities: [],
    quirks: null,
    disabled: false,
    createdAt: D,
    updatedAt: D,
  };
}

function ro(
  m: ModelRow,
  p: ModelProviderRow,
  o: ModelOfferingRow,
): ResolvedOffering {
  return {
    offering: o,
    model: m,
    provider: p,
    origin: { tenantId: "t1", direct: true },
  };
}

function priceRow(
  offeringId: string,
  currency: string,
  effectiveFrom: Date,
  inputTokenPrice: string,
): ModelPricingRow {
  return {
    id: `${offeringId}-${currency}-${effectiveFrom.toISOString()}`,
    tenantId: "t1",
    offeringId,
    currency,
    inputTokenPrice,
    outputTokenPrice: null,
    cacheReadTokenPrice: null,
    cacheWriteTokenPrice: null,
    thinkingTokenPrice: null,
    perRequestFee: null,
    perImageFee: null,
    perAudioFee: null,
    effectiveFrom,
    createdAt: D,
  };
}

describe("composeDiscoveredModels", () => {
  test("returns an empty list for no offerings", () => {
    expect(composeDiscoveredModels([], [], new Date())).toEqual([]);
  });

  test("groups providers under one model, ordered by priority", () => {
    const m = model("mdl_1", "claude");
    const fast = ro(
      m,
      provider("mpv_fast", "fast"),
      offering("mof_a", "mdl_1", "mpv_fast", 0),
    );
    const slow = ro(
      m,
      provider("mpv_slow", "slow"),
      offering("mof_b", "mdl_1", "mpv_slow", 10),
    );

    // Pass them out of priority order to prove the sort.
    const result = composeDiscoveredModels([slow, fast], [], new Date());
    expect(result).toHaveLength(1);
    expect(result[0]?.id).toBe("mdl_1");
    expect(result[0]?.offerings.map((o) => o.providerName)).toEqual([
      "fast",
      "slow",
    ]);
  });

  test("breaks a priority tie by provider name", () => {
    const m = model("mdl_1", "claude");
    const bravo = ro(
      m,
      provider("mpv_b", "bravo"),
      offering("mof_b", "mdl_1", "mpv_b", 5),
    );
    const alpha = ro(
      m,
      provider("mpv_a", "alpha"),
      offering("mof_a", "mdl_1", "mpv_a", 5),
    );

    const result = composeDiscoveredModels([bravo, alpha], [], new Date());
    expect(result[0]?.offerings.map((o) => o.providerName)).toEqual([
      "alpha",
      "bravo",
    ]);
  });

  test("emits separate entries for distinct models", () => {
    const a = ro(
      model("mdl_a", "claude"),
      provider("mpv_1", "p1"),
      offering("mof_a", "mdl_a", "mpv_1", 0),
    );
    const b = ro(
      model("mdl_b", "gpt"),
      provider("mpv_1", "p1"),
      offering("mof_b", "mdl_b", "mpv_1", 0),
    );
    const result = composeDiscoveredModels([a, b], [], new Date());
    expect(new Set(result.map((m) => m.canonicalName))).toEqual(
      new Set(["claude", "gpt"]),
    );
  });

  test("attaches the active price per currency at asOf", () => {
    const m = model("mdl_1", "claude");
    const o = ro(
      m,
      provider("mpv_1", "p1"),
      offering("mof_a", "mdl_1", "mpv_1", 0),
    );
    const priceRows = [
      priceRow("mof_a", "USD", new Date("2024-01-01T00:00:00Z"), "old"),
      priceRow("mof_a", "USD", new Date("2024-06-01T00:00:00Z"), "new"),
      priceRow("mof_a", "USD", new Date("2025-06-01T00:00:00Z"), "future"),
    ];

    const result = composeDiscoveredModels(
      [o],
      priceRows,
      new Date("2024-12-01T00:00:00Z"),
    );
    const prices = result[0]?.offerings[0]?.pricing ?? [];
    expect(prices).toHaveLength(1);
    expect(prices[0]?.inputTokenPrice).toBe("new");
    expect(prices[0]?.currency).toBe("USD");
  });

  test("groups flat pricing rows by offering and ignores foreign rows", () => {
    const m = model("mdl_1", "claude");
    const a = ro(
      m,
      provider("mpv_a", "a"),
      offering("mof_a", "mdl_1", "mpv_a", 0),
    );
    const b = ro(
      m,
      provider("mpv_b", "b"),
      offering("mof_b", "mdl_1", "mpv_b", 1),
    );

    const priceRows = [
      priceRow("mof_a", "USD", new Date("2024-01-01T00:00:00Z"), "a-price"),
      priceRow("mof_b", "USD", new Date("2024-01-01T00:00:00Z"), "b-price"),
      // Belongs to no offering in the input; must not attach anywhere.
      priceRow("mof_zzz", "USD", new Date("2024-01-01T00:00:00Z"), "orphan"),
    ];

    const result = composeDiscoveredModels([a, b], priceRows, new Date());
    const offerings = result[0]?.offerings ?? [];
    const priceFor = (id: string) =>
      offerings.find((o) => o.offeringId === id)?.pricing[0]?.inputTokenPrice;
    expect(priceFor("mof_a")).toBe("a-price");
    expect(priceFor("mof_b")).toBe("b-price");
  });

  test("an offering with no pricing yields an empty pricing array", () => {
    const m = model("mdl_1", "claude");
    const o = ro(
      m,
      provider("mpv_1", "p1"),
      offering("mof_a", "mdl_1", "mpv_1", 0),
    );
    const result = composeDiscoveredModels([o], [], new Date());
    expect(result[0]?.offerings[0]?.pricing).toEqual([]);
  });
});
