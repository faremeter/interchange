import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import type { ReactorDirector } from "@intx/types/runtime";

import { defineDirector } from "./director";
import type { BaseEnv } from "./env";

const emptySchema = type({});

function stubDirector(): ReactorDirector {
  return {
    async decide() {
      return { type: "wait" };
    },
  };
}

describe("defineDirector", () => {
  test("attaches id, requires, and configSchema to the factory", () => {
    const defined = defineDirector({
      id: "@vendor/pkg/name",
      configSchema: emptySchema,
      requires: ["transport"],
      factory: () => stubDirector(),
    });

    expect(defined.factory.id).toBe("@vendor/pkg/name");
    expect(defined.factory.requires).toEqual(["transport"]);
    expect(defined.factory.configSchema).toBe(emptySchema);
    expect(Object.isFrozen(defined.factory.requires)).toBe(true);
  });

  test("defaults requires to an empty frozen array", () => {
    const defined = defineDirector({
      id: "@vendor/pkg/name",
      configSchema: emptySchema,
      factory: () => stubDirector(),
    });

    expect(defined.factory.requires).toEqual([]);
    expect(Object.isFrozen(defined.factory.requires)).toBe(true);
  });

  test("rejects bare ids at definition time", () => {
    expect(() =>
      defineDirector({
        id: "default",
        configSchema: emptySchema,
        factory: () => stubDirector(),
      }),
    ).toThrow(/must be package-namespaced/);
  });

  test("build returns a DirectorRef with the id and supplied config", () => {
    interface Config {
      maxTurns?: number;
    }
    const defined = defineDirector<Config>({
      id: "pkg/budget",
      configSchema: type({ "maxTurns?": "number" }),
      factory: () => stubDirector(),
    });

    const ref = defined.build({ maxTurns: 12 });
    expect(ref.id).toBe("pkg/budget");
    expect(ref.config).toEqual({ maxTurns: 12 });
  });

  test("build rejects config that fails schema validation", () => {
    interface Config {
      maxTurns: number;
    }
    const defined = defineDirector<Config>({
      id: "pkg/strict",
      configSchema: type({ maxTurns: "number" }),
      factory: () => stubDirector(),
    });

    // Bypass the type system to exercise the runtime check.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentionally invalid config to test the runtime schema-rejection path
    const bad = { maxTurns: "lots" } as unknown as Config;
    expect(() => defined.build(bad)).toThrow(/validation failed/);
  });

  test("the factory is invoked with config, env, and agent context", () => {
    interface Config {
      label: string;
    }
    let seenConfig: Config | undefined;
    let seenAgent:
      | {
          systemPrompt: string;
          toolDefinitions: readonly unknown[];
          compactorNames: readonly string[];
        }
      | undefined;

    const defined = defineDirector<Config>({
      id: "pkg/probe",
      configSchema: type({ label: "string" }),
      factory: (config, _env, agent) => {
        seenConfig = config;
        seenAgent = {
          systemPrompt: agent.systemPrompt,
          toolDefinitions: agent.toolDefinitions,
          compactorNames: agent.compactorNames,
        };
        return stubDirector();
      },
    });

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub, never indexed beyond identity comparison
    const env = {} as BaseEnv;
    const agentContext = {
      systemPrompt: "you are a probe",
      toolDefinitions: [] as const,
      compactorNames: [] as const,
    };

    defined.factory({ label: "x" }, env, agentContext);
    expect(seenConfig).toEqual({ label: "x" });
    expect(seenAgent?.systemPrompt).toBe("you are a probe");
    expect(seenAgent?.toolDefinitions).toEqual([]);
    expect(seenAgent?.compactorNames).toEqual([]);
  });

  test("rejects a non-callable configSchema at build time", () => {
    const defined = defineDirector({
      id: "pkg/bogus",
      // configSchema is typed as unknown; passing a plain object here
      // is structurally allowed at the type level. The runtime guard in
      // build() rejects it.
      configSchema: { not: "a validator" },
      factory: () => stubDirector(),
    });

    expect(() => defined.build({})).toThrow(
      /configSchema must be an arktype validator/,
    );
  });

  test("does not mutate the factory function when reused across calls", () => {
    // A caller that shares a single factory function across two
    // `defineDirector` registrations needs each annotated factory to
    // be a distinct identity with its own metadata. Mutating
    // `opts.factory` would let the second call silently overwrite the
    // first's annotations and return the same identity twice.
    const sharedFactory = () => stubDirector();
    const a = defineDirector({
      id: "pkg/director-a",
      configSchema: emptySchema,
      factory: sharedFactory,
    });
    const b = defineDirector({
      id: "pkg/director-b",
      configSchema: emptySchema,
      requires: ["transport"],
      factory: sharedFactory,
    });

    expect(a.factory.id).toBe("pkg/director-a");
    expect(b.factory.id).toBe("pkg/director-b");
    expect(a.factory.requires).toEqual([]);
    expect(b.factory.requires).toEqual(["transport"]);
    expect(a.factory).not.toBe(b.factory);
    // The shared underlying factory is itself not annotated; only the
    // wrappers carry metadata. Reflect off the function value directly
    // so we can probe for accidental annotations without a cast.
    expect(Reflect.get(sharedFactory, "id")).toBeUndefined();
    expect(Reflect.get(sharedFactory, "requires")).toBeUndefined();
    expect(Reflect.get(sharedFactory, "configSchema")).toBeUndefined();
  });
});
