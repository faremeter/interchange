import { describe, test, expect } from "bun:test";
import path from "node:path";
import type { LastCycleSource } from "@intx/types/runtime";
import type { AdapterFactory, ProviderAdapter } from "../adapter";
import type { ModuleImporter } from "../manifest";
import type { AdapterManifest } from "@intx/types";
import { createBuiltinRegistry, loadAdapterRegistry } from "./index";

const builtinProviders = [
  "anthropic",
  "openai",
  "openai-compatible",
  "google-genai",
];

function createSource(provider: string): LastCycleSource {
  return { sourceId: "src-1", provider, model: "test-model" };
}

describe("createBuiltinRegistry", () => {
  test("resolves every shipped provider to a usable adapter", () => {
    const registry = createBuiltinRegistry();

    for (const provider of builtinProviders) {
      const adapter = registry.resolve(createSource(provider));
      expect(typeof adapter.buildRequest).toBe("function");
      expect(typeof adapter.parseResponse).toBe("function");
    }
  });

  test("does not report an unregistered provider as a member", () => {
    expect(createBuiltinRegistry().has("custom")).toBe(false);
  });

  test("does not include corbits-system-one", () => {
    expect(createBuiltinRegistry().has("corbits-system-one")).toBe(false);
  });
});

describe("loadAdapterRegistry", () => {
  test("merges a custom adapter on top of the built-ins", async () => {
    const customAdapter: ProviderAdapter = {
      buildRequest: () => ({ url: "custom", headers: {}, body: "" }),
      parseResponse: () => [],
      parseJSONResponse: () => [],
    };
    const customFactory: AdapterFactory = () => customAdapter;
    const importer: ModuleImporter = (specifier) => {
      expect(specifier).toBe("custom-pkg");
      return Promise.resolve({ makeAdapter: customFactory });
    };
    const manifest: AdapterManifest = [
      { provider: "custom", specifier: "custom-pkg", export: "makeAdapter" },
    ];

    const registry = await loadAdapterRegistry(manifest, { import: importer });

    expect(registry.resolve(createSource("custom"))).toBe(customAdapter);
    for (const provider of builtinProviders) {
      const adapter = registry.resolve(createSource(provider));
      expect(typeof adapter.buildRequest).toBe("function");
    }
  });

  test("returns just the built-ins for an empty manifest", async () => {
    const registry = await loadAdapterRegistry([]);

    expect(registry.has("anthropic")).toBe(true);
    expect(registry.has("custom")).toBe(false);
  });

  test("loads a fixture factory for corbits-system-one without a network call", async () => {
    const specifier = path.resolve(
      import.meta.dir,
      "fixtures/system-one-adapter.ts",
    );
    const manifest: AdapterManifest = [
      {
        provider: "corbits-system-one",
        specifier,
        export: "makeAdapter",
      },
    ];

    const registry = await loadAdapterRegistry(manifest);
    const adapter = registry.resolve(createSource("corbits-system-one"));

    expect(registry.has("corbits-system-one")).toBe(true);
    expect(adapter.buildRequest([], "jev-latest", {})).toEqual({
      url: "fixture://system-one",
      headers: {},
      body: "",
    });
  });
});
