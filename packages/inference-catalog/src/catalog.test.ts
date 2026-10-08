import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, test } from "bun:test";
import { type, type Type } from "arktype";

import {
  AnthropicQuirks,
  GoogleGenAIQuirks,
  OpenAIQuirks,
} from "@intx/inference/providers";
import {
  baseURLForCatalogProvider,
  catalogCapabilitiesFor,
} from "@intx/inference-discovery/catalog";
import { CAPABILITIES, CURATED_CAPABILITIES } from "@intx/types";

import { CATALOG_CAPABILITIES } from "./capability";
import { catalogModels } from "./models";
import { catalogProviders, type CatalogPlugin } from "./providers";

// Plugin -> adapter quirk validator; `openai` and `openai-compatible` share
// the OpenAI adapter.
const quirkValidatorByPlugin: Record<CatalogPlugin, Type> = {
  anthropic: AnthropicQuirks,
  openai: OpenAIQuirks,
  "openai-compatible": OpenAIQuirks,
  "google-genai": GoogleGenAIQuirks,
};

function isPlainObject(value: unknown): boolean {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const curatedCapabilityNames = new Set<string>(CURATED_CAPABILITIES);

describe("catalog offering quirks", () => {
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

describe("catalog offering capabilities", () => {
  for (const provider of catalogProviders) {
    for (const offering of provider.offerings) {
      const label = `${provider.name} / ${offering.model}`;

      // A hand-authored curated entry must be genuinely non-probeable; else a
      // row could claim a capability without matrix proof.
      test(`${label} curated capabilities are all non-probeable`, () => {
        for (const capability of offering.curatedCapabilities) {
          expect(curatedCapabilityNames.has(capability)).toBe(true);
        }
      });

      const source = offering.discoverySource;
      if (source !== null) {
        // A source that expands to nothing is a stale tuple; it should be
        // null instead.
        test(`${label} discovery source is live in the matrix`, () => {
          expect(
            catalogCapabilitiesFor(source.provider, source.model).length,
          ).toBeGreaterThan(0);
        });
      }

      // The baked literal must equal the matrix wire set for the provenance
      // tuple plus curated tags. On matrix drift this fails and the message
      // prints the value to bake in.
      test(`${label} baked capabilities match the discovery matrix`, () => {
        const wire = source
          ? catalogCapabilitiesFor(source.provider, source.model)
          : [];
        const expected = [...wire, ...offering.curatedCapabilities];
        if (
          JSON.stringify(offering.capabilities) !== JSON.stringify(expected)
        ) {
          throw new Error(
            `Baked capabilities for ${label} are stale: the discovery matrix ` +
              `moved and the catalog needs re-baking.\n` +
              `  expected: ${JSON.stringify(expected)}\n` +
              `  baked:    ${JSON.stringify(offering.capabilities)}`,
          );
        }
      });
    }
  }
});

describe("catalog capability vocabulary", () => {
  // The local vocabulary must track @intx/types; on drift, re-sync
  // CATALOG_CAPABILITIES.
  test("CATALOG_CAPABILITIES matches @intx/types CAPABILITIES", () => {
    expect(CATALOG_CAPABILITIES.length).toBe(CAPABILITIES.length);
    expect(new Set<string>(CATALOG_CAPABILITIES)).toEqual(
      new Set<string>(CAPABILITIES),
    );
  });
});

describe("catalog provider base URLs", () => {
  // Every brand the catalog draws from must resolve in the discovery
  // brand→base-URL map to a URL a catalog provider serves. The two maps
  // duplicate deliberately; this catches them drifting apart.
  const brands = new Set<string>(
    catalogProviders
      .flatMap((p) => p.offerings)
      .map((o) => o.discoverySource?.provider)
      .filter((provider): provider is string => provider !== undefined),
  );

  for (const brand of brands) {
    test(`${brand} base URL agrees with a catalog provider`, () => {
      const base = baseURLForCatalogProvider(brand);
      expect(base).toBeDefined();
      const match = catalogProviders.find(
        (p) =>
          p.baseURL === base &&
          p.offerings.some((o) => o.discoverySource?.provider === brand),
      );
      expect(match).toBeDefined();
    });
  }
});

describe("catalog public surface", () => {
  // The published `.d.ts` must not reference @intx/types (a build-only
  // devDependency). Since `dependencies` is empty, no non-test source file
  // importing it proves the declarations cannot; comments are stripped before
  // the quoted-specifier match so prose references do not trip it.
  const importSpecifier = /["']@intx\/types["']/;
  const stripComments = (source: string): string =>
    source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
  test("no non-test source file imports @intx/types", () => {
    const srcDir = import.meta.dir;
    const offenders: string[] = [];
    for (const entry of readdirSync(srcDir)) {
      if (!entry.endsWith(".ts") || entry.endsWith(".test.ts")) continue;
      const contents = stripComments(readFileSync(join(srcDir, entry), "utf8"));
      if (importSpecifier.test(contents)) offenders.push(entry);
    }
    expect(offenders).toEqual([]);
  });
});

describe("catalog offering models", () => {
  // Cross-check between models.ts and providers.ts: every offering must name a
  // model the catalogModels list carries, or the seed drops the offering and
  // the `./models` subpath omits it.
  const modelNames = new Set(catalogModels.map((model) => model.canonicalName));
  for (const provider of catalogProviders) {
    for (const offering of provider.offerings) {
      test(`${provider.name} / ${offering.model} is a known catalog model`, () => {
        expect(modelNames.has(offering.model)).toBe(true);
      });
    }
  }
});
