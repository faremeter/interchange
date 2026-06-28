import { describe, test, expect } from "bun:test";
import type { LastCycleSource } from "@intx/types/runtime";
import type { AdapterFactory, ProviderAdapter } from "@intx/inference";
import { loadAdapterRegistry } from "@intx/inference/providers";

import { parseAdapterManifest } from "./workflow-substrate-factory";

// Failure-isolation hedge for the cross-process custom-adapter path:
// proves the child-side load path (parse the serialized manifest, then
// `loadAdapterRegistry` with an injected importer, then resolve) in
// isolation, so a red forked-child integration test points at the
// spawn/IPC plumbing rather than at adapter loading. No fixture module
// is loaded from disk -- the importer is injected.

function createStubAdapter(url: string): ProviderAdapter {
  return {
    buildRequest: () => ({ url, headers: {}, body: "" }),
    parseResponse: () => [],
  };
}

function createSource(provider: string): LastCycleSource {
  return { sourceId: "src-1", provider, model: "test-model" };
}

describe("child-side adapter manifest load path", () => {
  test("parses a serialized manifest and resolves the custom adapter", async () => {
    const customAdapter = createStubAdapter("https://custom.invalid");
    const make: AdapterFactory = () => customAdapter;

    const manifest = parseAdapterManifest(
      JSON.stringify([
        { provider: "custom", specifier: "custom-pkg", export: "make" },
      ]),
    );
    const registry = await loadAdapterRegistry(manifest, {
      import: (specifier) => {
        expect(specifier).toBe("custom-pkg");
        return Promise.resolve({ make });
      },
    });

    expect(registry.resolve(createSource("custom"))).toBe(customAdapter);
    // Built-ins remain resolvable alongside the custom provider.
    expect(
      typeof registry.resolve(createSource("anthropic")).buildRequest,
    ).toBe("function");
  });

  test("an empty serialized manifest yields just the built-ins", async () => {
    const registry = await loadAdapterRegistry(parseAdapterManifest("[]"));

    expect(registry.has("anthropic")).toBe(true);
    expect(registry.has("custom")).toBe(false);
  });

  test("a malformed manifest fails loud naming the env key", () => {
    expect(() => parseAdapterManifest("{not json")).toThrow(
      /SIDECAR_ADAPTER_MANIFEST is not valid JSON/,
    );
    expect(() =>
      parseAdapterManifest(JSON.stringify([{ provider: 1 }])),
    ).toThrow(/SIDECAR_ADAPTER_MANIFEST failed validation/);
  });
});
