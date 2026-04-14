import type {
  ToolRunner,
  ToolCall,
  ToolResult,
  ToolDefinition,
} from "@interchange/types/runtime";
import { TOOL_DEFINITIONS, getHandler } from "./registry";

export type { ReadFileArgs } from "./read-file";
export type { WriteFileArgs } from "./write-file";
export type { RunShellArgs } from "./run-shell";

export type PosixTools = ToolRunner & {
  definitions: ToolDefinition[];
};

export function createPosixTools(): PosixTools {
  return {
    definitions: TOOL_DEFINITIONS,

    async run(call: ToolCall, signal: AbortSignal): Promise<ToolResult> {
      const handler = getHandler(call.name);
      if (handler === undefined) {
        return {
          callId: call.id,
          content: `unknown tool: ${call.name}`,
          isError: true,
        };
      }
      return handler(call, signal);
    },
  };
}
