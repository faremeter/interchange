import { describe, test, expect } from "bun:test";
import type {
  ToolCall,
  ToolDefinition,
  ToolResult,
  ToolRunner,
} from "@intx/types/runtime";

import { mergeToolRunners } from "./merge-tool-runners";

function makeRunner(
  label: string,
  definitions: ToolDefinition[],
): ToolRunner & { definitions: ToolDefinition[] } {
  return {
    definitions,
    async run(call: ToolCall): Promise<ToolResult> {
      return { callId: call.id, content: `${label}:${call.name}` };
    },
  };
}

const TOOL_DEF = (name: string): ToolDefinition => ({
  name,
  description: `Tool ${name}`,
  inputSchema: { type: "object", properties: {} },
});

const signal = AbortSignal.timeout(5000);

describe("mergeToolRunners dispatch", () => {
  test("routes each call to the runner whose definitions declare it", async () => {
    const a = makeRunner("a", [TOOL_DEF("read_file")]);
    const b = makeRunner("b", [TOOL_DEF("mail_send")]);

    const merged = mergeToolRunners([a, b]);

    const r1 = await merged.run(
      { id: "c1", name: "read_file", arguments: {} },
      signal,
    );
    const r2 = await merged.run(
      { id: "c2", name: "mail_send", arguments: {} },
      signal,
    );

    expect(r1.content).toBe("a:read_file");
    expect(r2.content).toBe("b:mail_send");
  });

  test("returns Unknown tool error for a name not declared by any runner", async () => {
    const a = makeRunner("a", [TOOL_DEF("read_file")]);

    const merged = mergeToolRunners([a]);
    const result = await merged.run(
      { id: "c1", name: "nonexistent", arguments: {} },
      signal,
    );

    expect(result.callId).toBe("c1");
    expect(result.isError).toBe(true);
    expect(result.content).toEqual({ error: `Unknown tool: "nonexistent"` });
  });

  test("three-way merge dispatches to each runner", async () => {
    const a = makeRunner("a", [TOOL_DEF("alpha")]);
    const b = makeRunner("b", [TOOL_DEF("beta")]);
    const c = makeRunner("c", [TOOL_DEF("gamma")]);

    const merged = mergeToolRunners([a, b, c]);

    const r1 = await merged.run(
      { id: "1", name: "alpha", arguments: {} },
      signal,
    );
    const r2 = await merged.run(
      { id: "2", name: "beta", arguments: {} },
      signal,
    );
    const r3 = await merged.run(
      { id: "3", name: "gamma", arguments: {} },
      signal,
    );

    expect(r1.content).toBe("a:alpha");
    expect(r2.content).toBe("b:beta");
    expect(r3.content).toBe("c:gamma");
  });

  test("combined definitions preserve input order and within-runner order", () => {
    const a = makeRunner("a", [TOOL_DEF("a1"), TOOL_DEF("a2")]);
    const b = makeRunner("b", [TOOL_DEF("b1"), TOOL_DEF("b2")]);

    const merged = mergeToolRunners([a, b]);

    expect(merged.definitions.map((d) => d.name)).toEqual([
      "a1",
      "a2",
      "b1",
      "b2",
    ]);
  });

  test("forwards the caller's AbortSignal to the underlying runner", async () => {
    let receivedSignal: AbortSignal | undefined;
    const captureRunner: ToolRunner & { definitions: ToolDefinition[] } = {
      definitions: [TOOL_DEF("capture")],
      async run(call: ToolCall, sig: AbortSignal): Promise<ToolResult> {
        receivedSignal = sig;
        return { callId: call.id, content: "ok" };
      },
    };

    const merged = mergeToolRunners([captureRunner]);
    const ctl = new AbortController();
    await merged.run({ id: "1", name: "capture", arguments: {} }, ctl.signal);

    expect(receivedSignal).toBe(ctl.signal);
  });
});

describe("mergeToolRunners collision detection", () => {
  test("throws on duplicate name across two runners, naming both indices", () => {
    const a = makeRunner("a", [TOOL_DEF("shared")]);
    const b = makeRunner("b", [TOOL_DEF("shared")]);

    expect(() => mergeToolRunners([a, b])).toThrow(
      new Error(
        'Tool name collision on "shared": registered by both runners[0] and runners[1]',
      ),
    );
  });

  test("throws when a single runner declares the same name twice", () => {
    const a = makeRunner("a", [TOOL_DEF("dup"), TOOL_DEF("dup")]);

    expect(() => mergeToolRunners([a])).toThrow(
      new Error('Tool name collision on "dup": runners[0] declares it twice'),
    );
  });
});

describe("mergeToolRunners empty input", () => {
  test("throws on an empty runners list", () => {
    expect(() => mergeToolRunners([])).toThrow(
      new Error("mergeToolRunners called with no runners"),
    );
  });
});
