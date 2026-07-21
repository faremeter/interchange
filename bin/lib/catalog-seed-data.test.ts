import { describe, test, expect } from "bun:test";
import { type, type Type } from "arktype";

import {
  AnthropicQuirks,
  GoogleGenAIQuirks,
  OpenAIQuirks,
} from "@intx/inference/providers";
import { catalogCapabilitiesFor } from "@intx/inference-discovery/catalog";
import { CURATED_CAPABILITIES } from "@intx/types";

import { catalogProviders, type CatalogPlugin } from "./catalog-seed-data";

// Each catalog plugin maps to the adapter quirk validator that governs the
// shape its offerings may carry. `openai` and `openai-compatible` share the
// OpenAI adapter, so they share its validator.
const quirkValidatorByPlugin: Record<CatalogPlugin, Type> = {
  anthropic: AnthropicQuirks,
  openai: OpenAIQuirks,
  "openai-compatible": OpenAIQuirks,
  "google-genai": GoogleGenAIQuirks,
};

function isPlainObject(value: unknown): boolean {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

describe("catalog seed offering quirks", () => {
  for (const provider of catalogProviders) {
    for (const offering of provider.offerings) {
      const label = `${provider.name} / ${offering.model}`;

      test(`${label} carries an explicit plain-object quirks bag`, () => {
        expect(isPlainObject(offering.quirks)).toBe(true);
      });

      test(`${label} quirks validate against the ${provider.plugin} adapter`, () => {
        const validator = quirkValidatorByPlugin[provider.plugin];
        expect(validator(offering.quirks) instanceof type.errors).toBe(false);
      });
    }
  }
});

describe("catalog seed offering capabilities", () => {
  for (const provider of catalogProviders) {
    for (const offering of provider.offerings) {
      const label = `${provider.name} / ${offering.model}`;

      // A wire capability hand-authored into curatedCapabilities would let a row
      // claim it without matrix proof, bypassing the discovery seeding. Every
      // curated entry must be a genuinely non-probeable capability. (Currently
      // satisfied on every offering; this is a regression guard for a future
      // edit that smuggles a wire capability into the curated list.)
      test(`${label} curated capabilities are all non-probeable`, () => {
        for (const capability of offering.curatedCapabilities) {
          expect(
            (CURATED_CAPABILITIES as readonly string[]).includes(capability),
          ).toBe(true);
        }
      });

      const source = offering.discoverySource;
      if (source !== null) {
        // A declared discovery source that expands to nothing is a typo'd or
        // stale tuple that should be null instead. Non-emptiness is a liveness
        // floor only — it does not prove the tuple names the intended model.
        test(`${label} discovery source is live in the matrix`, () => {
          expect(
            catalogCapabilitiesFor(source.provider, source.model).length,
          ).toBeGreaterThan(0);
        });
      }
    }
  }
});
