import path from "node:path";
import type {
  BlobReader,
  ToolDefinition,
  ToolResult,
} from "@interchange/types/runtime";
import { runEditFile } from "./edit-file";
import { runGrep } from "./grep";
import { runReadFile } from "./read-file";
import { runSearchFiles } from "./search-files";
import { runShell } from "./run-shell";
import { runWriteFile } from "./write-file";
import type { ToolHandler } from "./plugin";

const TOOL_OUTPUT_URI_PREFIX = "tool-output:";

export const TOOL_NAMES = {
  READ_FILE: "read_file",
  WRITE_FILE: "write_file",
  EDIT_FILE: "edit_file",
  RUN_SHELL: "run_shell",
  SEARCH_FILES: "search_files",
  GREP: "grep",
} as const;

export const TOOL_DEFINITIONS: ToolDefinition[] = [
  {
    name: TOOL_NAMES.READ_FILE,
    description:
      "Read a file and return its content with line numbers. The path argument accepts either a filesystem path or a tool-output URI of the form tool-output:///{callId} that references a prior tool result.",
    inputSchema: {
      type: "object",
      properties: {
        path: {
          type: "string",
          description:
            "Absolute or relative filesystem path, or a tool-output:///{callId} URI",
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
    name: TOOL_NAMES.WRITE_FILE,
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
    name: TOOL_NAMES.RUN_SHELL,
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
    name: TOOL_NAMES.EDIT_FILE,
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
    name: TOOL_NAMES.SEARCH_FILES,
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
    name: TOOL_NAMES.GREP,
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

const resolvePath = (raw: string, cwd: string) =>
  path.isAbsolute(raw) ? raw : path.resolve(cwd, raw);

interface RegistryContext {
  cwd: string;
  blobReader?: BlobReader;
}

export function makeHandlerRegistry(
  ctx: RegistryContext,
): Map<string, ToolHandler> {
  const m = new Map<string, ToolHandler>();

  m.set(TOOL_NAMES.READ_FILE, async (call, signal) => {
    try {
      const rawPath = requireString(call.arguments, "path");
      const p = rawPath.startsWith(TOOL_OUTPUT_URI_PREFIX)
        ? rawPath
        : resolvePath(rawPath, ctx.cwd);
      const offset = optionalNumber(call.arguments, "offset");
      const limit = optionalNumber(call.arguments, "limit");
      const readFileArgs =
        offset !== undefined
          ? limit !== undefined
            ? { path: p, offset, limit }
            : { path: p, offset }
          : limit !== undefined
            ? { path: p, limit }
            : { path: p };
      const content = await runReadFile(readFileArgs, signal, ctx.blobReader);
      return makeResult(call.id, content);
    } catch (err) {
      return makeErrorResult(call.id, err);
    }
  });

  m.set(TOOL_NAMES.WRITE_FILE, async (call, signal) => {
    try {
      const p = resolvePath(requireString(call.arguments, "path"), ctx.cwd);
      const content = requireString(call.arguments, "content");
      const result = await runWriteFile({ path: p, content }, signal);
      return makeResult(call.id, result);
    } catch (err) {
      return makeErrorResult(call.id, err);
    }
  });

  m.set(TOOL_NAMES.RUN_SHELL, async (call, signal) => {
    try {
      const command = requireString(call.arguments, "command");
      const timeout = optionalNumber(call.arguments, "timeout");
      const shellArgs = {
        command,
        cwd: ctx.cwd,
        ...(timeout !== undefined ? { timeout } : {}),
      };
      const { output, exitCode } = await runShell(shellArgs, signal);
      const content =
        exitCode === 0 ? output : `exit code ${exitCode}\n${output}`;
      return makeResult(call.id, content);
    } catch (err) {
      return makeErrorResult(call.id, err);
    }
  });

  m.set(TOOL_NAMES.EDIT_FILE, async (call, signal) => {
    try {
      const p = resolvePath(requireString(call.arguments, "path"), ctx.cwd);
      const old_string = requireString(call.arguments, "old_string");
      const new_string = requireString(call.arguments, "new_string");
      const replace_all = optionalBoolean(call.arguments, "replace_all");
      const editArgs =
        replace_all !== undefined
          ? { path: p, old_string, new_string, replace_all }
          : { path: p, old_string, new_string };
      const result = await runEditFile(editArgs, signal);
      return makeResult(call.id, result);
    } catch (err) {
      return makeErrorResult(call.id, err);
    }
  });

  m.set(TOOL_NAMES.SEARCH_FILES, async (call, signal) => {
    try {
      const pattern = requireString(call.arguments, "pattern");
      const rawPath = optionalString(call.arguments, "path");
      const p = rawPath !== undefined ? resolvePath(rawPath, ctx.cwd) : ctx.cwd;
      const max_results = optionalNumber(call.arguments, "max_results");
      const searchArgs = {
        pattern,
        path: p,
        ...(max_results !== undefined ? { max_results } : {}),
      };
      const result = await runSearchFiles(searchArgs, signal);
      return makeResult(call.id, result);
    } catch (err) {
      return makeErrorResult(call.id, err);
    }
  });

  m.set(TOOL_NAMES.GREP, async (call, signal) => {
    try {
      const pattern = requireString(call.arguments, "pattern");
      const rawPath = optionalString(call.arguments, "path");
      const p = rawPath !== undefined ? resolvePath(rawPath, ctx.cwd) : ctx.cwd;
      const glob = optionalString(call.arguments, "glob");
      const context = optionalNumber(call.arguments, "context");
      const max_results = optionalNumber(call.arguments, "max_results");
      const grepArgs = {
        pattern,
        path: p,
        ...(glob !== undefined ? { glob } : {}),
        ...(context !== undefined ? { context } : {}),
        ...(max_results !== undefined ? { max_results } : {}),
      };
      const result = await runGrep(grepArgs, signal);
      return makeResult(call.id, result);
    } catch (err) {
      return makeErrorResult(call.id, err);
    }
  });

  return m;
}
