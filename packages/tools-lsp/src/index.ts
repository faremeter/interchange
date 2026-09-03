import type { ToolPlugin } from "@intx/tools-posix";
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

export interface LSPPlugin extends ToolPlugin {
  /**
   * The working directory the plugin's LSP roots its servers under,
   * surfaced from the manager so a caller can confirm which tree the
   * plugin is scoped to.
   */
  readonly cwd: string;
}

export function createLSPPlugin(opts: LSPPluginOptions): LSPPlugin {
  const lsp = createLSPManager({
    cwd: opts.cwd,
    ...(opts.worktree !== undefined ? { worktree: opts.worktree } : {}),
  });

  return {
    cwd: lsp.cwd,
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
