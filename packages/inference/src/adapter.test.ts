import { describe, test, expect } from "bun:test";
import type { LastCycleSource } from "@intx/types/runtime";
import {
  createAdapterRegistry,
  type AdapterFactory,
  type ProviderAdapter,
} from "./adapter";

function createStubAdapter(): ProviderAdapter {
  return {
    buildRequest: () => ({ url: "", headers: {}, body: "" }),
    parseResponse: () => [],
  };
}

function createSource(provider: string): LastCycleSource {
  return { sourceId: "src-1", provider, model: "test-model" };
}

describe("createAdapterRegistry", () => {
  test("resolves a source to the adapter from its provider factory", () => {
    const adapter = createStubAdapter();
    const registry = createAdapterRegistry({ test: () => adapter });

    expect(registry.resolve(createSource("test"))).toBe(adapter);
  });

  test("passes the source to the factory", () => {
    const seen: LastCycleSource[] = [];
    const registry = createAdapterRegistry({
      test: (source) => {
        seen.push(source);
        return createStubAdapter();
      },
    });

    const source = createSource("test");
    registry.resolve(source);

    expect(seen).toEqual([source]);
  });

  test("forwards the quirks bag to the factory untouched", () => {
    const seen: unknown[] = [];
    const registry = createAdapterRegistry({
      test: (_source, quirks) => {
        seen.push(quirks);
        return createStubAdapter();
      },
    });

    const quirks = { some: "bag" };
    registry.resolve(createSource("test"), quirks);

    expect(seen).toHaveLength(1);
    expect(seen[0]).toBe(quirks);
  });

  test("passes undefined to the factory when the quirks bag is omitted", () => {
    const seen = { called: false, quirks: "sentinel" as unknown };
    const registry = createAdapterRegistry({
      test: (_source, quirks) => {
        seen.called = true;
        seen.quirks = quirks;
        return createStubAdapter();
      },
    });

    registry.resolve(createSource("test"));

    expect(seen.called).toBe(true);
    expect(seen.quirks).toBeUndefined();
  });

  test("invokes the factory fresh on every resolve", () => {
    let calls = 0;
    const registry = createAdapterRegistry({
      test: () => {
        calls += 1;
        return createStubAdapter();
      },
    });

    const first = registry.resolve(createSource("test"));
    const second = registry.resolve(createSource("test"));

    expect(calls).toBe(2);
    expect(first).not.toBe(second);
  });

  test("throws a loud error for an unknown provider", () => {
    const registry = createAdapterRegistry({ test: createStubAdapter });

    expect(() => registry.resolve(createSource("missing"))).toThrow(
      "Unknown inference provider: missing",
    );
  });

  test("reports membership through has", () => {
    const registry = createAdapterRegistry({ test: createStubAdapter });

    expect(registry.has("test")).toBe(true);
    expect(registry.has("missing")).toBe(false);
  });

  test("does not treat Object.prototype members as registered providers", () => {
    const registry = createAdapterRegistry({ test: createStubAdapter });

    expect(registry.has("toString")).toBe(false);
    expect(registry.has("constructor")).toBe(false);
    expect(() => registry.resolve(createSource("toString"))).toThrow(
      "Unknown inference provider: toString",
    );
  });

  test("lets later keys override earlier ones when composing", () => {
    const base = createStubAdapter();
    const override = createStubAdapter();
    const builtins: Record<string, AdapterFactory> = { test: () => base };
    const registry = createAdapterRegistry({
      ...builtins,
      test: () => override,
    });

    expect(registry.resolve(createSource("test"))).toBe(override);
  });

  test("closes over a copy so later mutation of the input is ignored", () => {
    const factories: Record<string, AdapterFactory> = {
      test: createStubAdapter,
    };
    const registry = createAdapterRegistry(factories);

    delete factories.test;
    factories.added = createStubAdapter;

    expect(registry.has("test")).toBe(true);
    expect(registry.has("added")).toBe(false);
  });
});
