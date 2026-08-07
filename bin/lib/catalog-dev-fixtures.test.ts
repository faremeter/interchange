import { describe, expect, test } from "bun:test";

import { catalogProviders } from "@intx/inference-catalog";

import { credentialFixtures, priceFixtures } from "./catalog-dev-fixtures";

// The seed joins the published catalog to seed-local credential and price
// fixtures by (provider, model). A missing fixture is otherwise only caught at
// seed runtime (against a live DB) by the coverage pre-check in bin/seed.ts.
// This asserts the same coverage statically, so adding a provider or offering
// to @intx/inference-catalog without a matching fixture fails in CI rather than
// only when someone runs the seed.
describe("catalog dev fixtures cover the catalog", () => {
  for (const provider of catalogProviders) {
    test(`${provider.name} has a credential fixture`, () => {
      expect(credentialFixtures[provider.name]).toBeDefined();
    });

    test(`${provider.name} has a price fixture map`, () => {
      expect(priceFixtures[provider.name]).toBeDefined();
    });

    for (const offering of provider.offerings) {
      test(`${provider.name}/${offering.model} has a price fixture`, () => {
        expect(priceFixtures[provider.name]?.[offering.model]).toBeDefined();
      });
    }
  }
});
