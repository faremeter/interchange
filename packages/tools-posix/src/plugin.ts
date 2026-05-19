import type { ToolCall, ToolResult, ToolDefinition } from "@intx/types/runtime";

export type ToolHandler = (
  call: ToolCall,
  signal: AbortSignal,
) => Promise<ToolResult>;

export type Middleware = (next: ToolHandler) => ToolHandler;

export interface ExtraTool {
  definition: ToolDefinition;
  handler: ToolHandler;
}

export interface ToolPlugin {
  tools?: ExtraTool[];
  middleware?: Middleware;
  dispose?(): Promise<void>;
}

export function composeMiddleware(
  middleware: readonly Middleware[],
  base: ToolHandler,
): ToolHandler {
  let handler = base;
  for (let i = middleware.length - 1; i >= 0; i--) {
    const mw = middleware[i];
    if (mw !== undefined) handler = mw(handler);
  }
  return handler;
}
