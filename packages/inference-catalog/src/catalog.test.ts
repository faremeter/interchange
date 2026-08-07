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
import { catalogProviders, type CatalogPlugin } from "./providers";

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

      // A wire capability hand-authored into curatedCapabilities would let a row
      // claim it without matrix proof. Every curated entry must be a genuinely
      // non-probeable capability.
      test(`${label} curated capabilities are all non-probeable`, () => {
        for (const capability of offering.curatedCapabilities) {
          expect(curatedCapabilityNames.has(capability)).toBe(true);
        }
      });

      const source = offering.discoverySource;
      if (source !== null) {
        // A declared discovery source that expands to nothing is a typo'd or
        // stale tuple that should be null instead. Non-emptiness is a liveness
        // floor only.
        test(`${label} discovery source is live in the matrix`, () => {
          expect(
            catalogCapabilitiesFor(source.provider, source.model).length,
          ).toBeGreaterThan(0);
        });
      }

      // The baked literal must equal the wire set the matrix proves for the
      // provenance tuple, followed by the curated tags. When discovery captures
      // a new session that changes the matrix, this fails and the catalog needs
      // re-baking — the message prints the value to bake in.
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
  // The locally-owned vocabulary must stay identical to the canonical one. If
  // @intx/types adds or removes a capability, this fails and CATALOG_CAPABILITIES
  // must be re-synced — the drift guard that lets the catalog own the type
  // without a runtime dependency on @intx/types.
  test("CATALOG_CAPABILITIES matches @intx/types CAPABILITIES", () => {
    expect(CATALOG_CAPABILITIES.length).toBe(CAPABILITIES.length);
    expect(new Set<string>(CATALOG_CAPABILITIES)).toEqual(
      new Set<string>(CAPABILITIES),
    );
  });
});

describe("catalog provider base URLs", () => {
  // Every discovery brand the catalog draws from must resolve, in
  // @intx/inference-discovery's brand→base-URL map, to a base URL that some
  // catalog provider carrying that brand actually serves. The two sources
  // duplicate the base URLs deliberately (the discovery map cannot import the
  // catalog); this catches them drifting apart.
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
  // The published `.d.ts` must not reference @intx/types (a build/test-only
  // devDependency), or an external consumer inherits a dangling reference.
  // Because `dependencies` is empty, no non-test source file importing
  // @intx/types is a sufficient proof that the emitted declarations cannot
  // reference it. Comments are stripped before matching — they carry no
  // reference into the emitted output, and capability.ts legitimately discusses
  // @intx/types in prose — then the module specifier is matched in a quoted
  // import position (`from "@intx/types"`, `import("@intx/types")`), which is
  // the form any real static reference takes.
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
