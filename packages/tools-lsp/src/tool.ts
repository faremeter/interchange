import path from "node:path";
import { pathToFileURL } from "node:url";
import type {
  ToolCall,
  ToolResult,
  ToolDefinition,
} from "@interchange/types/runtime";
import type { LSPManager } from "./lsp";

const OPERATIONS = [
  "goToDefinition",
  "findReferences",
  "hover",
  "goToImplementation",
  "documentSymbol",
  "workspaceSymbol",
  "prepareCallHierarchy",
  "incomingCalls",
  "outgoingCalls",
] as const;
type Operation = (typeof OPERATIONS)[number];

export const LSP_TOOL_DEFINITION: ToolDefinition = {
  name: "lsp",
  description:
    "Run a language-server operation against a file. Operations: " +
    "goToDefinition, findReferences, hover, goToImplementation (position-based); " +
    "documentSymbol (whole-file, line/character ignored); " +
    "workspaceSymbol (uses query, filePath/line/character ignored but required " +
    "by the schema -- pass any valid file in the workspace and 1,1); " +
    "prepareCallHierarchy, incomingCalls, outgoingCalls (position-based). " +
    "Line and character are 1-based as shown in editors.",
  inputSchema: {
    type: "object",
    properties: {
      operation: { type: "string", enum: [...OPERATIONS] },
      filePath: { type: "string" },
      line: { type: "number", description: "1-based line" },
      character: { type: "number", description: "1-based column" },
      query: {
        type: "string",
        description: "Required for workspaceSymbol; ignored otherwise",
      },
    },
    required: ["operation", "filePath", "line", "character"],
  },
};

export function makeLSPToolHandler(lsp: LSPManager, cwd: string) {
  return async (call: ToolCall, _signal: AbortSignal): Promise<ToolResult> => {
    try {
      const operation = requireEnum(call.arguments, "operation", OPERATIONS);
      const rawPath = requireString(call.arguments, "filePath");
      const filePath = path.isAbsolute(rawPath)
        ? rawPath
        : path.join(cwd, rawPath);
      const line = requireNumber(call.arguments, "line");
      const character = requireNumber(call.arguments, "character");

      const has = await lsp.hasClients(filePath);
      if (!has) {
        return {
          callId: call.id,
          content: "no LSP server available for this file type",
          isError: true,
        };
      }

      await lsp.touchFile(filePath, "document");

      const position = {
        file: filePath,
        line: line - 1,
        character: character - 1,
      };
      const uri = pathToFileURL(filePath).href;
      const query =
        typeof call.arguments["query"] === "string"
          ? call.arguments["query"]
          : "";

      const value = await dispatch(lsp, operation, position, uri, query);

      const empty =
        value === null ||
        value === undefined ||
        (Array.isArray(value) && value.length === 0);

      return {
        callId: call.id,
        content: empty
          ? `no results for ${operation}`
          : JSON.stringify(value, null, 2),
      };
    } catch (err) {
      const message =
        err instanceof Error ? err.message : `unknown error: ${String(err)}`;
      return { callId: call.id, content: message, isError: true };
    }
  };
}

type Position = { file: string; line: number; character: number };

function dispatch(
  lsp: LSPManager,
  op: Operation,
  pos: Position,
  uri: string,
  query: string,
): Promise<unknown> {
  switch (op) {
    case "goToDefinition":
      return lsp.definition(pos);
    case "findReferences":
      return lsp.references(pos);
    case "hover":
      return lsp.hover(pos);
    case "goToImplementation":
      return lsp.implementation(pos);
    case "documentSymbol":
      return lsp.documentSymbol(uri);
    case "workspaceSymbol":
      return lsp.workspaceSymbol(query);
    case "prepareCallHierarchy":
      return lsp.prepareCallHierarchy(pos);
    case "incomingCalls":
      return lsp.incomingCalls(pos);
    case "outgoingCalls":
      return lsp.outgoingCalls(pos);
  }
}

function requireString(args: Record<string, unknown>, key: string): string {
  const v = args[key];
  if (typeof v !== "string") {
    throw new Error(`argument "${key}" must be a string`);
  }
  return v;
}

function requireNumber(args: Record<string, unknown>, key: string): number {
  const v = args[key];
  if (typeof v !== "number") {
    throw new Error(`argument "${key}" must be a number`);
  }
  return v;
}

function requireEnum<const T extends readonly string[]>(
  args: Record<string, unknown>,
  key: string,
  options: T,
): T[number] {
  const v = args[key];
  if (typeof v !== "string" || !(options as readonly string[]).includes(v)) {
    throw new Error(`argument "${key}" must be one of: ${options.join(", ")}`);
  }
  return v;
}
