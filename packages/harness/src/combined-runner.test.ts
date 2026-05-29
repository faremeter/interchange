import { describe, test, expect } from "bun:test";
import type {
  ToolCall,
  ToolDefinition,
  ToolResult,
  ToolRunner,
} from "@intx/types/runtime";

import type { DeployToolInfo } from "./deploy-tree";
import { buildCombinedRunner } from "./tools";

type ToolHandler = (call: ToolCall, signal: AbortSignal) => Promise<ToolResult>;

// The combined runner's collision check inspects the mail-handler map's
// keys; its dispatch path only invokes a mail handler when the call name
// matches a mail tool. The tests below never dispatch to mail, so the
// handlers themselves are throwing stubs — the keyset is what matters.
function makeMailHandlerKeys(): Map<string, ToolHandler> {
  const unreachable: ToolHandler = async () => {
    throw new Error("mail handler unreachable in dispatch tests");
  };
  return new Map<string, ToolHandler>([
    ["mail_send", unreachable],
    ["mail_reply", unreachable],
    ["mail_search", unreachable],
    ["mail_read", unreachable],
    ["mail_wait", unreachable],
  ]);
}

function makePosixRunner(definitions: ToolDefinition[]): ToolRunner & {
  definitions: ToolDefinition[];
} {
  return {
    definitions,
    async run(call) {
      return { callId: call.id, content: `posix:${call.name}` };
    },
  };
}

function makeDeployTool(name: string, hasHandler: boolean): DeployToolInfo {
  return {
    definition: {
      name,
      description: `Tool ${name}`,
      inputSchema: { type: "object", properties: {} },
    },
    hasHandler,
  };
}

const TOOL_DEF = (name: string): ToolDefinition => ({
  name,
  description: `Tool ${name}`,
  inputSchema: { type: "object", properties: {} },
});

const signal = AbortSignal.timeout(5000);

describe("buildCombinedRunner dispatch", () => {
  test("routes caller-declared tool to the caller's runner", async () => {
    const posix = makePosixRunner([TOOL_DEF("read_file")]);
    const runner = buildCombinedRunner(
      makeMailHandlerKeys(),
      posix,
      posix.definitions,
      [],
    );

    const result = await runner.run(
      { id: "c1", name: "read_file", arguments: {} },
      signal,
    );

    expect(result.callId).toBe("c1");
    expect(result.isError).not.toBe(true);
    expect(result.content).toBe("posix:read_file");
  });

  test("returns 'handler not implemented' for deploy tool with handler", async () => {
    const posix = makePosixRunner([]);
    const runner = buildCombinedRunner(
      makeMailHandlerKeys(),
      posix,
      [],
      [makeDeployTool("custom", true)],
    );

    const result = await runner.run(
      { id: "c2", name: "custom", arguments: {} },
      signal,
    );

    expect(result.callId).toBe("c2");
    expect(result.isError).toBe(true);
    expect(result.content).toContain("handler.ts");
    expect(result.content).toContain("not yet implemented");
  });

  test("returns 'declared but no handler' for a deploy tool with no handler and no caller match", async () => {
    const posix = makePosixRunner([]);
    const runner = buildCombinedRunner(
      makeMailHandlerKeys(),
      posix,
      [],
      [makeDeployTool("exotic_tool", false)],
    );

    const result = await runner.run(
      { id: "c3", name: "exotic_tool", arguments: {} },
      signal,
    );

    expect(result.callId).toBe("c3");
    expect(result.isError).toBe(true);
    expect(result.content).toContain("declared in the deploy tree");
    expect(result.content).toContain("does not match a built-in tool");
  });

  test("returns 'unknown tool' for a name not in any source", async () => {
    const posix = makePosixRunner([]);
    const runner = buildCombinedRunner(makeMailHandlerKeys(), posix, [], []);

    const result = await runner.run(
      { id: "c4", name: "nonexistent", arguments: {} },
      signal,
    );

    expect(result.callId).toBe("c4");
    expect(result.isError).toBe(true);
    expect(result.content).toContain("Unknown tool");
  });

  // Exercises every arm of the dispatcher inside a single runner that
  // holds caller-, deploy-with-handler-, and deploy-without-handler-
  // declared tools simultaneously. Names are disjoint (the startup
  // collision check rejects anything else), so this is a coverage
  // test for the four routing branches living in one assembly, not a
  // precedence/order test — order is irrelevant when names cannot
  // overlap. The complementary order-sensitive scenario (a name
  // appearing in two sources at once) is exercised by the collision
  // tests below, which assert it throws at construction.
  test("routes each name to its source when all four arms coexist", async () => {
    const posix = makePosixRunner([TOOL_DEF("read_file")]);
    const runner = buildCombinedRunner(
      makeMailHandlerKeys(),
      posix,
      posix.definitions,
      [makeDeployTool("custom", true), makeDeployTool("exotic_tool", false)],
    );

    const caller = await runner.run(
      { id: "cc-1", name: "read_file", arguments: {} },
      signal,
    );
    expect(caller.callId).toBe("cc-1");
    expect(caller.isError).not.toBe(true);
    expect(caller.content).toBe("posix:read_file");

    const handler = await runner.run(
      { id: "cc-2", name: "custom", arguments: {} },
      signal,
    );
    expect(handler.callId).toBe("cc-2");
    expect(handler.isError).toBe(true);
    expect(handler.content).toContain("not yet implemented");

    const declared = await runner.run(
      { id: "cc-3", name: "exotic_tool", arguments: {} },
      signal,
    );
    expect(declared.callId).toBe("cc-3");
    expect(declared.isError).toBe(true);
    expect(declared.content).toContain("declared in the deploy tree");

    const unknown = await runner.run(
      { id: "cc-4", name: "nonexistent", arguments: {} },
      signal,
    );
    expect(unknown.callId).toBe("cc-4");
    expect(unknown.isError).toBe(true);
    expect(unknown.content).toContain("Unknown tool");
  });
});

describe("buildCombinedRunner collision detection", () => {
  test("mail tool vs caller tool — throws naming both sources", () => {
    const posix = makePosixRunner([TOOL_DEF("mail_send")]);

    expect(() =>
      buildCombinedRunner(makeMailHandlerKeys(), posix, posix.definitions, []),
    ).toThrow(
      'Tool name collision on "mail_send": registered by both the mail tools and the caller-supplied ToolRunner',
    );
  });

  test("mail tool vs deploy tool — throws naming both sources", () => {
    const posix = makePosixRunner([]);

    expect(() =>
      buildCombinedRunner(
        makeMailHandlerKeys(),
        posix,
        [],
        [makeDeployTool("mail_send", false)],
      ),
    ).toThrow(
      'Tool name collision on "mail_send": registered by both the mail tools and the deploy tree',
    );
  });

  test("caller tool vs deploy tool — throws naming both sources", () => {
    const posix = makePosixRunner([TOOL_DEF("read_file")]);

    expect(() =>
      buildCombinedRunner(makeMailHandlerKeys(), posix, posix.definitions, [
        makeDeployTool("read_file", false),
      ]),
    ).toThrow(
      'Tool name collision on "read_file": registered by both the caller-supplied ToolRunner and the deploy tree',
    );
  });

  // A name in both `tools.definitions` and `deployTools` is a startup
  // collision rather than a runtime precedence rule. The original
  // sidecar's buildToolDispatch had handler-bearing deploy tools win
  // over a same-name posix tool at dispatch time; in the new design
  // that conflict surfaces at construction so the operator sees it
  // before any inference happens.
  test("caller tool vs deploy tool with handler — throws at construction (no runtime precedence)", () => {
    const posix = makePosixRunner([TOOL_DEF("read_file")]);

    expect(() =>
      buildCombinedRunner(makeMailHandlerKeys(), posix, posix.definitions, [
        makeDeployTool("read_file", true),
      ]),
    ).toThrow('Tool name collision on "read_file"');
  });
});
