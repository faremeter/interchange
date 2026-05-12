import type {
  ToolDefinition,
  ToolCall,
  ToolResult,
} from "@interchange/types/runtime";
import { runEditFile } from "./edit-file";
import { runGrep } from "./grep";
import { runReadFile } from "./read-file";
import { runSearchFiles } from "./search-files";
import { runShell } from "./run-shell";
import { runWriteFile } from "./write-file";

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
  {
    name: "edit_file",
    description:
      "Make a surgical text replacement in a file. Finds old_string in the file and replaces it with new_string. The old_string must appear exactly once unless replace_all is true.",
    inputSchema: {
      type: "object",
      properties: {
        path: {
          type: "string",
          description: "Absolute or relative path to the file to edit",
        },
        old_string: {
          type: "string",
          description:
            "Exact string to find in the file (must be unique unless replace_all is true)",
        },
        new_string: {
          type: "string",
          description: "String to replace old_string with",
        },
        replace_all: {
          type: "boolean",
          description:
            "If true, replace all occurrences instead of requiring uniqueness",
        },
      },
      required: ["path", "old_string", "new_string"],
    },
  },
  {
    name: "search_files",
    description:
      "Find files matching a glob pattern. Returns matching file paths, one per line. Searches recursively from the given path or current directory. Skips node_modules and .git directories.",
    inputSchema: {
      type: "object",
      properties: {
        pattern: {
          type: "string",
          description:
            'Glob pattern (e.g., "**/*.ts", "src/**/*.test.ts", "*.json")',
        },
        path: {
          type: "string",
          description:
            "Directory to search in (default: current working directory)",
        },
        max_results: {
          type: "number",
          description: "Maximum number of results to return (default: 1000)",
        },
      },
      required: ["pattern"],
    },
  },
  {
    name: "grep",
    description:
      "Search file contents for a regex pattern. Returns matching lines with file paths and line numbers. Skips node_modules and .git directories.",
    inputSchema: {
      type: "object",
      properties: {
        pattern: {
          type: "string",
          description: "Regex pattern to search for",
        },
        path: {
          type: "string",
          description:
            "File or directory to search in (default: current working directory)",
        },
        glob: {
          type: "string",
          description:
            'Glob pattern to filter files. Use **/ prefix to match at any depth (e.g., "**/*.ts"). Without **, matches only at the top level (e.g., "*.json").',
        },
        context: {
          type: "number",
          description:
            "Number of context lines before and after each match (like grep -C)",
        },
        max_results: {
          type: "number",
          description:
            "Maximum number of matching lines to return (default: 500)",
        },
      },
      required: ["pattern"],
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

function optionalString(
  args: Record<string, unknown>,
  key: string,
): string | undefined {
  const val = args[key];
  if (val === undefined) return undefined;
  if (typeof val !== "string") {
    throw new Error(`argument "${key}" must be a string`);
  }
  return val;
}

function optionalBoolean(
  args: Record<string, unknown>,
  key: string,
): boolean | undefined {
  const val = args[key];
  if (val === undefined) return undefined;
  if (typeof val !== "boolean") {
    throw new Error(`argument "${key}" must be a boolean`);
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
  [
    "edit_file",
    async (call, signal) => {
      try {
        const path = requireString(call.arguments, "path");
        const old_string = requireString(call.arguments, "old_string");
        const new_string = requireString(call.arguments, "new_string");
        const replace_all = optionalBoolean(call.arguments, "replace_all");
        const editArgs =
          replace_all !== undefined
            ? { path, old_string, new_string, replace_all }
            : { path, old_string, new_string };
        const result = await runEditFile(editArgs, signal);
        return makeResult(call.id, result);
      } catch (err) {
        return makeErrorResult(call.id, err);
      }
    },
  ],
  [
    "search_files",
    async (call, signal) => {
      try {
        const pattern = requireString(call.arguments, "pattern");
        const path = optionalString(call.arguments, "path");
        const max_results = optionalNumber(call.arguments, "max_results");
        const searchArgs = {
          pattern,
          ...(path !== undefined ? { path } : {}),
          ...(max_results !== undefined ? { max_results } : {}),
        };
        const result = await runSearchFiles(searchArgs, signal);
        return makeResult(call.id, result);
      } catch (err) {
        return makeErrorResult(call.id, err);
      }
    },
  ],
  [
    "grep",
    async (call, signal) => {
      try {
        const pattern = requireString(call.arguments, "pattern");
        const path = optionalString(call.arguments, "path");
        const glob = optionalString(call.arguments, "glob");
        const context = optionalNumber(call.arguments, "context");
        const max_results = optionalNumber(call.arguments, "max_results");
        const grepArgs = {
          pattern,
          ...(path !== undefined ? { path } : {}),
          ...(glob !== undefined ? { glob } : {}),
          ...(context !== undefined ? { context } : {}),
          ...(max_results !== undefined ? { max_results } : {}),
        };
        const result = await runGrep(grepArgs, signal);
        return makeResult(call.id, result);
      } catch (err) {
        return makeErrorResult(call.id, err);
      }
    },
  ],
]);

export function getHandler(name: string): ToolHandler | undefined {
  return handlers.get(name);
}
