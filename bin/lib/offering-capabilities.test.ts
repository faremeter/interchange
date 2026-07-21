import { describe, expect, test } from "bun:test";

import { catalogCapabilitiesFor } from "@intx/inference-discovery/catalog";

import type { CatalogOfferingSpec } from "./catalog-seed-data";
import { offeringCapabilities } from "./offering-capabilities";

function offering(
  overrides: Partial<CatalogOfferingSpec>,
): CatalogOfferingSpec {
  return {
    model: "test-model",
    priority: 0,
    discoverySource: null,
    curatedCapabilities: [],
    quirks: {},
    price: { input: "0", output: "0" },
    ...overrides,
  };
}

// A (provider, model) with real fixture-bearing rows in the support matrix.
const PROBED_SOURCE = { provider: "opencode-zen", model: "kimi-k2.6" };

describe("offeringCapabilities", () => {
  test("an unprobed offering advertises only its curated capabilities", () => {
    const result = offeringCapabilities(
      offering({
        discoverySource: null,
        curatedCapabilities: ["long-context"],
      }),
    );
    expect(result).toEqual(["long-context"]);
  });

  test("an unprobed offering with no curated tags advertises nothing", () => {
    expect(offeringCapabilities(offering({}))).toEqual([]);
  });

  test("a probed offering advertises the matrix wire set then its curated tags", () => {
    const result = offeringCapabilities(
      offering({
        discoverySource: PROBED_SOURCE,
        curatedCapabilities: ["long-context"],
      }),
    );
    // Structural, not a snapshot: the wire portion is whatever the helper
    // returns for this tuple, in wire-then-curated order.
    expect(result).toEqual([
      ...catalogCapabilitiesFor(PROBED_SOURCE.provider, PROBED_SOURCE.model),
      "long-context",
    ]);
  });

  test("a probed offering with no curated tags advertises exactly the wire set", () => {
    const result = offeringCapabilities(
      offering({ discoverySource: PROBED_SOURCE }),
    );
    expect(result).toEqual(
      catalogCapabilitiesFor(PROBED_SOURCE.provider, PROBED_SOURCE.model),
    );
  });
});
