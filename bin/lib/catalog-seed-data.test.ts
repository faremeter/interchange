import { describe, test, expect } from "bun:test";
import { type, type Type } from "arktype";

import {
  AnthropicQuirks,
  GoogleGenAIQuirks,
  OpenAIQuirks,
} from "@intx/inference/providers";

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
