import { describe, test, expect } from "bun:test";

import type { ToolDefinition } from "@intx/types/runtime";

import {
  type ToolBundle,
  type ToolFactory,
  createToolRunner,
  defineTool,
  DuplicateToolError,
  fromToolRunner,
  stringTool,
  tool,
} from "./tool";
import type { BaseEnv } from "./env";

const DEF_A: ToolDefinition = {
  name: "a",
  description: "tool a",
  inputSchema: { type: "object" },
};

const DEF_B: ToolDefinition = {
  name: "b",
  description: "tool b",
  inputSchema: { type: "object" },
};

describe("createToolRunner", () => {
  test("dispatches to a full handler by tool name", async () => {
    const runner = createToolRunner([
      tool({
        definition: DEF_A,
        handler: async (call) => ({
          callId: call.id,
          content: `got ${String(call.arguments.x)}`,
        }),
      }),
    ]);

    const result = await runner.run(
      { id: "c1", name: "a", arguments: { x: 5 } },
      new AbortController().signal,
    );

    expect(result).toEqual({ callId: "c1", content: "got 5" });
  });

  test("lifts the string handler return into a ToolResult", async () => {
    const runner = createToolRunner([
      stringTool({
        definition: DEF_A,
        handler: async (args) => `hello ${String(args.name)}`,
      }),
    ]);

    const result = await runner.run(
      { id: "c2", name: "a", arguments: { name: "world" } },
      new AbortController().signal,
    );

    expect(result).toEqual({ callId: "c2", content: "hello world" });
  });

  test("returns an error ToolResult for an unknown tool name", async () => {
    const runner = createToolRunner([
      tool({
        definition: DEF_A,
        handler: async (call) => ({ callId: call.id, content: "" }),
      }),
    ]);

    const result = await runner.run(
      { id: "c3", name: "nope", arguments: {} },
      new AbortController().signal,
    );

    expect(result.isError).toBe(true);
    expect(result.callId).toBe("c3");
    expect(result.content).toBe("unknown tool: nope");
  });

  test("wraps a thrown error from a full handler into an error ToolResult", async () => {
    const runner = createToolRunner([
      tool({
        definition: DEF_A,
        handler: async () => {
          throw new Error("boom");
        },
      }),
    ]);

    const result = await runner.run(
      { id: "c4", name: "a", arguments: {} },
      new AbortController().signal,
    );

    expect(result).toEqual({ callId: "c4", content: "boom", isError: true });
  });

  test("wraps a thrown error from a string handler into an error ToolResult", async () => {
    const runner = createToolRunner([
      stringTool({
        definition: DEF_A,
        handler: async () => {
          throw new Error("bad input");
        },
      }),
    ]);

    const result = await runner.run(
      { id: "c5", name: "a", arguments: {} },
      new AbortController().signal,
    );

    expect(result).toEqual({
      callId: "c5",
      content: "bad input",
      isError: true,
    });
  });

  test("exposes definitions in registration order", () => {
    const runner = createToolRunner([
      tool({
        definition: DEF_A,
        handler: async (call) => ({ callId: call.id, content: "" }),
      }),
      stringTool({ definition: DEF_B, handler: async () => "x" }),
    ]);

    expect(runner.definitions.map((d) => d.name)).toEqual(["a", "b"]);
  });

  test("throws DuplicateToolError at construction on duplicate names", () => {
    expect(() =>
      createToolRunner([
        tool({
          definition: DEF_A,
          handler: async (call) => ({ callId: call.id, content: "" }),
        }),
        stringTool({ definition: DEF_A, handler: async () => "x" }),
      ]),
    ).toThrow(DuplicateToolError);
  });

  test("propagates the AbortSignal to the handler", async () => {
    let received: AbortSignal | undefined;
    const runner = createToolRunner([
      tool({
        definition: DEF_A,
        handler: async (call, signal) => {
          received = signal;
          return { callId: call.id, content: "ok" };
        },
      }),
    ]);

    const ctl = new AbortController();
    await runner.run({ id: "c6", name: "a", arguments: {} }, ctl.signal);

    expect(received).toBe(ctl.signal);
  });

  test("fromToolRunner wraps each definition as a full-handler AgentTool", async () => {
    const calls: string[] = [];
    const stubRunner = {
      definitions: [DEF_A, DEF_B] as const,
      run: async (call: {
        id: string;
        name: string;
        arguments: Record<string, unknown>;
      }) => {
        calls.push(call.name);
        return Promise.resolve({
          callId: call.id,
          content: `ran ${call.name}`,
        });
      },
    };

    const tools = fromToolRunner(stubRunner);
    const runner = createToolRunner(tools);

    expect(runner.definitions.map((d) => d.name)).toEqual(["a", "b"]);

    const ra = await runner.run(
      { id: "c8", name: "a", arguments: {} },
      new AbortController().signal,
    );
    expect(ra).toEqual({ callId: "c8", content: "ran a" });

    const rb = await runner.run(
      { id: "c9", name: "b", arguments: {} },
      new AbortController().signal,
    );
    expect(rb).toEqual({ callId: "c9", content: "ran b" });

    expect(calls).toEqual(["a", "b"]);
  });

  test("string handler receives the parsed arguments object", async () => {
    let received: Record<string, unknown> | undefined;
    const runner = createToolRunner([
      stringTool({
        definition: DEF_A,
        handler: async (args) => {
          received = args;
          return "ok";
        },
      }),
    ]);

    await runner.run(
      { id: "c7", name: "a", arguments: { foo: 1, bar: "x" } },
      new AbortController().signal,
    );

    expect(received).toEqual({ foo: 1, bar: "x" });
  });
});

describe("defineTool", () => {
  function emptyBundle(): ToolBundle {
    return {
      definitions: [],
      async run(call) {
        return { callId: call.id, content: "" };
      },
    };
  }

  test("attaches id and frozen requires to the factory", () => {
    const factory = defineTool({
      id: "@vendor/pkg/name",
      requires: ["transport", "address"],
      factory: () => emptyBundle(),
    });

    expect(factory.id).toBe("@vendor/pkg/name");
    expect(factory.requires).toEqual(["transport", "address"]);
    expect(Object.isFrozen(factory.requires)).toBe(true);
  });

  test("defaults requires to an empty frozen array when omitted", () => {
    const factory = defineTool({
      id: "@vendor/pkg/name",
      factory: () => emptyBundle(),
    });

    expect(factory.requires).toEqual([]);
    expect(Object.isFrozen(factory.requires)).toBe(true);
  });

  test("rejects bare ids", () => {
    expect(() =>
      defineTool({
        id: "bareName",
        factory: () => emptyBundle(),
      }),
    ).toThrow(/must be package-namespaced/);
  });

  test("the factory's behaviour is preserved as the callable shape", () => {
    let envSeen: BaseEnv | undefined;
    const factory = defineTool({
      id: "pkg/probe",
      factory: (env) => {
        envSeen = env;
        return {
          definitions: [
            {
              name: "probe",
              description: "probe",
              inputSchema: { type: "object" },
            },
          ],
          async run(call) {
            return { callId: call.id, content: "probed" };
          },
        };
      },
    });

    // Tool factories declare which env keys they touch via `requires`;
    // their factory body should only read those keys. The probe tool
    // here declares nothing, so the env it receives is only checked for
    // reference equality. Constructing a fully-structured `BaseEnv`
    // would force importing several unrelated types; the partial object
    // exercise is intentional.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub, never indexed beyond identity comparison
    const fakeEnv = { workdir: "/tmp/x" } as unknown as BaseEnv;
    const bundle = factory(fakeEnv);
    expect(envSeen).toBe(fakeEnv);
    expect(bundle.definitions).toHaveLength(1);
    expect(bundle.definitions[0]?.name).toBe("probe");
  });

  test("does not mutate the requires array supplied by the caller", () => {
    const requires: readonly string[] = ["transport"];
    const factory = defineTool({
      id: "pkg/name",
      requires,
      factory: () => emptyBundle(),
    });

    expect(factory.requires).toEqual(["transport"]);
    expect(factory.requires).not.toBe(requires);
  });

  test("does not mutate the factory function when reused across calls", () => {
    // A caller that shares a single factory function across two
    // `defineTool` registrations needs each annotated factory to be a
    // distinct identity with its own metadata. Mutating `opts.factory`
    // would let the second call silently overwrite the first's
    // annotations and return the same identity twice.
    const sharedFactory: ToolFactory = () => emptyBundle();
    const a = defineTool({
      id: "pkg/tool-a",
      factory: sharedFactory,
    });
    const b = defineTool({
      id: "pkg/tool-b",
      requires: ["transport"],
      factory: sharedFactory,
    });

    expect(a.id).toBe("pkg/tool-a");
    expect(b.id).toBe("pkg/tool-b");
    expect(a.requires).toEqual([]);
    expect(b.requires).toEqual(["transport"]);
    expect(a).not.toBe(b);
    // The shared underlying factory is itself not annotated; only the
    // wrappers carry metadata. Reflect off the function value directly
    // so we can probe for accidental annotations without a cast.
    expect(Reflect.get(sharedFactory, "id")).toBeUndefined();
    expect(Reflect.get(sharedFactory, "requires")).toBeUndefined();
  });
});
