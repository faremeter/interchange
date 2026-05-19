// Tool registration and dispatch.
//
// Two registration shapes are supported:
//
//   `tool({ definition, handler })`        — handler receives the full
//                                            ToolCall and returns the full
//                                            ToolResult. Use when the
//                                            handler needs the callId or
//                                            wants to set isError/detail/
//                                            pendingMarker.
//
//   `stringTool({ definition, handler })`  — sugar for the common case of
//                                            "compute a string from the
//                                            parsed arguments." The callId
//                                            is filled in from the
//                                            surrounding ToolCall, and
//                                            isError is false unless the
//                                            handler throws.
//
// `createToolRunner(tools)` builds a `ToolRunner` that dispatches by tool
// name. Per the ToolRunner contract (packages/types/src/runtime.ts), `run`
// must not throw — unknown tool names and handler exceptions are surfaced
// as `ToolResult` with `isError: true` so the model sees them and can
// recover.

import type {
  ToolCall,
  ToolDefinition,
  ToolResult,
  ToolRunner,
} from "@intx/types/runtime";

export type ToolHandler = (
  call: ToolCall,
  signal: AbortSignal,
) => Promise<ToolResult>;

export type StringToolHandler = (
  args: Record<string, unknown>,
  signal: AbortSignal,
) => Promise<string>;

export type AgentTool =
  | { kind: "full"; definition: ToolDefinition; handler: ToolHandler }
  | {
      kind: "string";
      definition: ToolDefinition;
      handler: StringToolHandler;
    };

export function tool(args: {
  definition: ToolDefinition;
  handler: ToolHandler;
}): AgentTool {
  return { kind: "full", definition: args.definition, handler: args.handler };
}

export function stringTool(args: {
  definition: ToolDefinition;
  handler: StringToolHandler;
}): AgentTool {
  return {
    kind: "string",
    definition: args.definition,
    handler: args.handler,
  };
}

/**
 * Adapt a pre-built ToolRunner (e.g. the one returned by
 * `createPosixTools`) into a list of AgentTools that can be passed to
 * `createAgent({ tools })`. Each definition becomes a full-handler
 * AgentTool that delegates to the runner's `run`.
 *
 * Use this when integrating tool packages whose public surface is a
 * single ToolRunner rather than individual handlers.
 */
export function fromToolRunner(runner: {
  readonly definitions: readonly ToolDefinition[];
  run: ToolRunner["run"];
}): AgentTool[] {
  return runner.definitions.map((definition) => ({
    kind: "full",
    definition,
    handler: (call, signal) => runner.run(call, signal),
  }));
}

export class DuplicateToolError extends Error {
  readonly toolName: string;

  constructor(toolName: string) {
    super(`duplicate tool name: ${toolName}`);
    this.name = "DuplicateToolError";
    this.toolName = toolName;
  }
}

export type AgentToolRunner = ToolRunner & {
  readonly definitions: readonly ToolDefinition[];
};

/**
 * Build a `ToolRunner` that dispatches by tool name. Throws
 * `DuplicateToolError` at construction if any two tools share a name.
 *
 * At call time, unknown tool names and exceptions from handlers are
 * converted to `ToolResult { isError: true }` so the contract on
 * `ToolRunner.run` ("must not throw") is upheld.
 */
export function createToolRunner(tools: AgentTool[]): AgentToolRunner {
  const byName = new Map<string, AgentTool>();
  for (const t of tools) {
    if (byName.has(t.definition.name)) {
      throw new DuplicateToolError(t.definition.name);
    }
    byName.set(t.definition.name, t);
  }

  const definitions: readonly ToolDefinition[] = tools.map((t) => t.definition);

  return {
    definitions,
    async run(call, signal): Promise<ToolResult> {
      const found = byName.get(call.name);
      if (found === undefined) {
        return {
          callId: call.id,
          content: `unknown tool: ${call.name}`,
          isError: true,
        };
      }
      try {
        if (found.kind === "full") {
          return await found.handler(call, signal);
        }
        const text = await found.handler(call.arguments, signal);
        return { callId: call.id, content: text };
      } catch (err) {
        return {
          callId: call.id,
          content: err instanceof Error ? err.message : String(err),
          isError: true,
        };
      }
    },
  };
}
