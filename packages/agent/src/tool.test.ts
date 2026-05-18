import { describe, test, expect } from "bun:test";

import type { ToolDefinition } from "@interchange/types/runtime";

import { createToolRunner, DuplicateToolError, stringTool, tool } from "./tool";

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
