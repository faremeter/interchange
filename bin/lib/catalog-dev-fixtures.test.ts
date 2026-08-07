import { describe, expect, test } from "bun:test";

import { catalogProviders } from "@intx/inference-catalog";

import {
  credentialFixtures,
  priceFixtures,
  unpricedOfferings,
} from "./catalog-dev-fixtures";

// The seed joins the published catalog to seed-local credential and price
// fixtures by (provider, model). A gap is otherwise only caught at seed runtime
// (against a live DB) by the coverage pre-check in bin/seed.ts. This asserts the
// same coverage statically: every provider needs a credential fixture, and
// every offering must be priced or explicitly listed unpriced — never neither,
// never both — so adding a provider or offering to @intx/inference-catalog
// without resolving its fixtures fails in CI rather than only when the seed runs.
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
