import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import type { ReactorDirector } from "@intx/types/runtime";

import { defineDirector } from "./director";
import {
  createDefaultDirectorRegistry,
  createDirectorRegistry,
} from "./director-registry";
import { defaultDirectorFactory } from "./default-director";

function stubDirector(): ReactorDirector {
  return {
    async decide() {
      return { type: "wait" };
    },
  };
}

const emptySchema = type({});

const factoryA = defineDirector({
  id: "@vendor/a/one",
  configSchema: emptySchema,
  factory: () => stubDirector(),
}).factory;

const factoryB = defineDirector({
  id: "@vendor/b/two",
  configSchema: emptySchema,
  factory: () => stubDirector(),
}).factory;

const dupOfA = defineDirector({
  id: "@vendor/a/one",
  configSchema: emptySchema,
  factory: () => stubDirector(),
}).factory;

describe("createDirectorRegistry", () => {
  test("indexes factories by id and resolves them", () => {
    const registry = createDirectorRegistry({
      factories: [factoryA, factoryB],
      defaultId: factoryA.id,
    });

    expect(registry.resolve({ id: factoryA.id, config: {} })).toBe(factoryA);
    expect(registry.resolve({ id: factoryB.id, config: {} })).toBe(factoryB);
  });

  test("throws on id collision", () => {
    expect(() =>
      createDirectorRegistry({
        factories: [factoryA, dupOfA],
        defaultId: factoryA.id,
      }),
    ).toThrow(/id collision/);
  });

  test("throws when the defaultId is not in factories", () => {
    expect(() =>
      createDirectorRegistry({
        factories: [factoryA],
        defaultId: "@nope/missing/one",
      }),
    ).toThrow(/not in registry factories/);
  });

  test("throws on resolve of an unknown id", () => {
    const registry = createDirectorRegistry({
      factories: [factoryA],
      defaultId: factoryA.id,
    });

    expect(() =>
      registry.resolve({ id: "@nope/missing/one", config: {} }),
    ).toThrow(/unknown director in registry/);
  });

  test("defaultFactory returns the factory registered under defaultId", () => {
    const registry = createDirectorRegistry({
      factories: [factoryA, factoryB],
      defaultId: factoryB.id,
    });

    expect(registry.defaultFactory()).toBe(factoryB);
  });

  test("buildDefaultRef returns a fresh ref with empty config", () => {
    const registry = createDirectorRegistry({
      factories: [factoryA],
      defaultId: factoryA.id,
    });

    const ref1 = registry.buildDefaultRef();
    const ref2 = registry.buildDefaultRef();

    expect(ref1.id).toBe(factoryA.id);
    expect(ref1.config).toEqual({});
    expect(ref1).not.toBe(ref2);
    expect(ref1.config).not.toBe(ref2.config);
  });
});

describe("createDefaultDirectorRegistry", () => {
  test("registers the canonical default factory under its id", () => {
    const registry = createDefaultDirectorRegistry();

    expect(registry.defaultFactory()).toBe(defaultDirectorFactory);
    expect(
      registry.resolve({ id: defaultDirectorFactory.id, config: {} }),
    ).toBe(defaultDirectorFactory);
  });

  test("the default id is @intx/agent/default", () => {
    expect(defaultDirectorFactory.id).toBe("@intx/agent/default");
  });

  test("buildDefaultRef references @intx/agent/default with empty config", () => {
    const registry = createDefaultDirectorRegistry();
    const ref = registry.buildDefaultRef();

    expect(ref.id).toBe("@intx/agent/default");
    expect(ref.config).toEqual({});
  });
});
