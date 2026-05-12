import type { ToolPlugin } from "@interchange/tools-posix";
import { createLSPManager } from "./lsp";
import { createLSPMiddleware } from "./middleware";
import { LSP_TOOL_DEFINITION, makeLSPToolHandler } from "./tool";

export type { LSPManager, LSPManagerOptions } from "./lsp";
export type { LSPMiddlewareOptions } from "./middleware";
export { LSP_TOOL_DEFINITION } from "./tool";

export interface LSPPluginOptions {
  cwd: string;
  worktree?: string;
  minSeverity?: number;
}

export function createLSPPlugin(opts: LSPPluginOptions): ToolPlugin {
  const lsp = createLSPManager({
    cwd: opts.cwd,
    ...(opts.worktree !== undefined ? { worktree: opts.worktree } : {}),
  });

  return {
    tools: [
      {
        definition: LSP_TOOL_DEFINITION,
        handler: makeLSPToolHandler(lsp, opts.cwd),
      },
    ],
    middleware: createLSPMiddleware(lsp, {
      cwd: opts.cwd,
      ...(opts.minSeverity !== undefined
        ? { minSeverity: opts.minSeverity }
        : {}),
    }),
    dispose: () => lsp.dispose(),
  };
}
