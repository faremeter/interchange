import { describe, test, expect } from "bun:test";
import type { LastCycleSource } from "@intx/types/runtime";
import type { AdapterFactory, ProviderAdapter } from "../adapter";
import type { AdapterManifest, ModuleImporter } from "../manifest";
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
});

describe("loadAdapterRegistry", () => {
  test("merges a custom adapter on top of the built-ins", async () => {
    const customAdapter: ProviderAdapter = {
      buildRequest: () => ({ url: "custom", headers: {}, body: "" }),
      parseResponse: () => [],
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
});
