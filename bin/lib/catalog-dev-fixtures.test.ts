import { describe, expect, test } from "bun:test";

import { catalogProviders } from "@intx/inference-catalog";

import {
  credentialFixtures,
  priceFixtures,
  unpricedOfferings,
} from "./catalog-dev-fixtures";

// Assert the seed's coverage pre-check statically: every provider needs a
// credential fixture, and every offering is priced or explicitly listed
// unpriced (never neither, never both), so a catalog addition that leaves a
// gap fails in CI rather than only when the seed runs against a live DB.
describe("catalog dev fixtures cover the catalog", () => {
  for (const provider of catalogProviders) {
    test(`${provider.name} has a credential fixture`, () => {
      expect(credentialFixtures[provider.name]).toBeDefined();
    });

    for (const offering of provider.offerings) {
      test(`${provider.name}/${offering.model} is priced xor unpriced`, () => {
        const priced =
          priceFixtures[provider.name]?.[offering.model] !== undefined;
        const unpriced = (unpricedOfferings[provider.name] ?? []).includes(
          offering.model,
        );
        expect(priced).not.toBe(unpriced);
      });
    }
  }
});
