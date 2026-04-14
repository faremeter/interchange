import type {
  ToolDefinition,
  ToolCall,
  ToolResult,
} from "@interchange/types/runtime";
import { runReadFile } from "./read-file";
import { runWriteFile } from "./write-file";
import { runShell } from "./run-shell";

export const TOOL_DEFINITIONS: ToolDefinition[] = [
  {
    name: "read_file",
    description:
      "Read a file from the filesystem. Returns file content with line numbers.",
    inputSchema: {
      type: "object",
      properties: {
        path: {
          type: "string",
          description: "Absolute or relative path to the file to read",
        },
        offset: {
          type: "number",
          description: "Zero-based line offset to start reading from",
        },
        limit: {
          type: "number",
          description: "Maximum number of lines to return",
        },
      },
      required: ["path"],
    },
  },
  {
    name: "write_file",
    description:
      "Write content to a file. Creates parent directories if needed.",
    inputSchema: {
      type: "object",
      properties: {
        path: {
          type: "string",
          description: "Absolute or relative path to the file to write",
        },
        content: {
          type: "string",
          description: "Full content to write to the file",
        },
      },
      required: ["path", "content"],
    },
  },
  {
    name: "run_shell",
    description:
      "Execute a shell command. Returns combined stdout and stderr with exit code.",
    inputSchema: {
      type: "object",
      properties: {
        command: {
          type: "string",
          description: "Shell command to execute",
        },
        timeout: {
          type: "number",
          description: "Timeout in milliseconds (default: 30000)",
        },
      },
      required: ["command"],
    },
  },
];

type ToolHandler = (call: ToolCall, signal: AbortSignal) => Promise<ToolResult>;

function makeResult(callId: string, content: string): ToolResult {
  return { callId, content };
}

function makeErrorResult(callId: string, err: unknown): ToolResult {
  const message =
    err instanceof Error ? err.message : `unknown error: ${String(err)}`;
  return { callId, content: message, isError: true };
}

function requireString(args: Record<string, unknown>, key: string): string {
  const val = args[key];
  if (typeof val !== "string") {
    throw new Error(`required argument "${key}" must be a string`);
  }
  return val;
}

function optionalNumber(
  args: Record<string, unknown>,
  key: string,
): number | undefined {
  const val = args[key];
  if (val === undefined) return undefined;
  if (typeof val !== "number") {
    throw new Error(`argument "${key}" must be a number`);
  }
  return val;
}

const handlers = new Map<string, ToolHandler>([
  [
    "read_file",
    async (call, signal) => {
      try {
        const path = requireString(call.arguments, "path");
        const offset = optionalNumber(call.arguments, "offset");
        const limit = optionalNumber(call.arguments, "limit");
        const readFileArgs =
          offset !== undefined
            ? limit !== undefined
              ? { path, offset, limit }
              : { path, offset }
            : limit !== undefined
              ? { path, limit }
              : { path };
        const content = await runReadFile(readFileArgs, signal);
        return makeResult(call.id, content);
      } catch (err) {
        return makeErrorResult(call.id, err);
      }
    },
  ],
  [
    "write_file",
    async (call, signal) => {
      try {
        const path = requireString(call.arguments, "path");
        const content = requireString(call.arguments, "content");
        const result = await runWriteFile({ path, content }, signal);
        return makeResult(call.id, result);
      } catch (err) {
        return makeErrorResult(call.id, err);
      }
    },
  ],
  [
    "run_shell",
    async (call, signal) => {
      try {
        const command = requireString(call.arguments, "command");
        const timeout = optionalNumber(call.arguments, "timeout");
        const shellArgs =
          timeout !== undefined ? { command, timeout } : { command };
        const { output, exitCode } = await runShell(shellArgs, signal);
        const content =
          exitCode === 0 ? output : `exit code ${exitCode}\n${output}`;
        return makeResult(call.id, content);
      } catch (err) {
        return makeErrorResult(call.id, err);
      }
    },
  ],
]);

export function getHandler(name: string): ToolHandler | undefined {
  return handlers.get(name);
}
