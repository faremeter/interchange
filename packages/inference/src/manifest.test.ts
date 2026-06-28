import { describe, test, expect } from "bun:test";
import type { AdapterFactory, ProviderAdapter } from "./adapter";
import {
  loadAdapterFactories,
  type AdapterManifest,
  type ModuleImporter,
} from "./manifest";

function createStubAdapter(): ProviderAdapter {
  return {
    buildRequest: () => ({ url: "", headers: {}, body: "" }),
    parseResponse: () => [],
  };
}

function createImporter(modules: Record<string, unknown>): ModuleImporter {
  return (specifier) => {
    if (!(specifier in modules)) {
      throw new Error(`unexpected specifier: ${specifier}`);
    }
    return Promise.resolve(modules[specifier]);
  };
}

describe("loadAdapterFactories", () => {
  test("loads a factory from a module's named export", async () => {
    const factory: AdapterFactory = () => createStubAdapter();
    const importer = createImporter({ "custom-pkg": { makeAdapter: factory } });
    const manifest: AdapterManifest = [
      { provider: "custom", specifier: "custom-pkg", export: "makeAdapter" },
    ];

    const factories = await loadAdapterFactories(manifest, {
      import: importer,
    });

    expect(factories.custom).toBe(factory);
  });

  test("throws naming the specifier and export when the export is missing", async () => {
    const importer = createImporter({
      "custom-pkg": { other: createStubAdapter },
    });
    const manifest: AdapterManifest = [
      { provider: "custom", specifier: "custom-pkg", export: "makeAdapter" },
    ];

    await expect(
      loadAdapterFactories(manifest, { import: importer }),
    ).rejects.toThrow(/makeAdapter from custom-pkg/);
  });

  test("throws when the named export is not a function", async () => {
    const importer = createImporter({ "custom-pkg": { makeAdapter: 42 } });
    const manifest: AdapterManifest = [
      { provider: "custom", specifier: "custom-pkg", export: "makeAdapter" },
    ];

    await expect(
      loadAdapterFactories(manifest, { import: importer }),
    ).rejects.toThrow(/makeAdapter from custom-pkg/);
  });

  test("throws when the module does not resolve to an object", async () => {
    const numberImporter = createImporter({ "custom-pkg": 42 });
    const nullImporter = createImporter({ "custom-pkg": null });
    const manifest: AdapterManifest = [
      { provider: "custom", specifier: "custom-pkg", export: "makeAdapter" },
    ];

    await expect(
      loadAdapterFactories(manifest, { import: numberImporter }),
    ).rejects.toThrow(/did not resolve to an object: custom-pkg/);
    await expect(
      loadAdapterFactories(manifest, { import: nullImporter }),
    ).rejects.toThrow(/did not resolve to an object: custom-pkg/);
  });

  test("lets a later entry override an earlier one by provider key", async () => {
    const first: AdapterFactory = () => createStubAdapter();
    const second: AdapterFactory = () => createStubAdapter();
    const importer = createImporter({
      "first-pkg": { make: first },
      "second-pkg": { make: second },
    });
    const manifest: AdapterManifest = [
      { provider: "custom", specifier: "first-pkg", export: "make" },
      { provider: "custom", specifier: "second-pkg", export: "make" },
    ];

    const factories = await loadAdapterFactories(manifest, {
      import: importer,
    });

    expect(factories.custom).toBe(second);
  });
});
