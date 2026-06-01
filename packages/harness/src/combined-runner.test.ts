import { describe, test, expect } from "bun:test";
import type {
  ToolCall,
  ToolDefinition,
  ToolResult,
  ToolRunner,
} from "@intx/types/runtime";

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
    );

    const result = await runner.run(
      { id: "c1", name: "read_file", arguments: {} },
      signal,
    );

    expect(result.callId).toBe("c1");
    expect(result.isError).not.toBe(true);
    expect(result.content).toBe("posix:read_file");
  });

  test("returns 'unknown tool' for a name not in any source", async () => {
    const posix = makePosixRunner([]);
    const runner = buildCombinedRunner(makeMailHandlerKeys(), posix, []);

    const result = await runner.run(
      { id: "c4", name: "nonexistent", arguments: {} },
      signal,
    );

    expect(result.callId).toBe("c4");
    expect(result.isError).toBe(true);
    expect(result.content).toContain("Unknown tool");
  });
});

describe("buildCombinedRunner collision detection", () => {
  test("mail tool vs caller tool — throws naming both sources", () => {
    const posix = makePosixRunner([TOOL_DEF("mail_send")]);

    expect(() =>
      buildCombinedRunner(makeMailHandlerKeys(), posix, posix.definitions),
    ).toThrow(
      'Tool name collision on "mail_send": registered by both the mail tools and the caller-supplied ToolRunner',
    );
  });
});
