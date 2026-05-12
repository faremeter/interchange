import path from "node:path";
import type { Middleware } from "@interchange/tools-posix";
import { TOOL_NAMES } from "@interchange/tools-posix";
import type { LSPManager } from "./lsp";
import { report } from "./diagnostic";

export interface LSPMiddlewareOptions {
  cwd: string;
  minSeverity?: number;
}

export function createLSPMiddleware(
  lsp: LSPManager,
  opts: LSPMiddlewareOptions,
): Middleware {
  const minSeverity = opts.minSeverity ?? 1;

  return (next) => async (call, signal) => {
    const result = await next(call, signal);
    if (result.isError === true) return result;

    const arg = call.arguments["path"];
    if (typeof arg !== "string") return result;
    const filePath = path.isAbsolute(arg) ? arg : path.resolve(opts.cwd, arg);

    if (call.name === TOOL_NAMES.READ_FILE) {
      // Fire-and-forget: warm the LSP server so it is ready for follow-up
      // operations. The lsp tool handler does its own touchFile before
      // dispatching, so immediate follow-up operations still work.
      void lsp.touchFile(filePath).catch(() => undefined);
      return result;
    }

    if (
      call.name === TOOL_NAMES.EDIT_FILE ||
      call.name === TOOL_NAMES.WRITE_FILE
    ) {
      await lsp.touchFile(filePath, "document").catch(() => undefined);
      const diags = await lsp
        .diagnostics()
        .catch(() => ({}) as Record<string, never>);
      const block = report(filePath, diags[filePath] ?? [], minSeverity);
      if (block === "") return result;
      const content =
        typeof result.content === "string"
          ? `${result.content}\n${block}`
          : result.content;
      return { ...result, content };
    }

    return result;
  };
}
