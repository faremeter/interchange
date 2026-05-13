import { statSync } from "node:fs";
import type {
  BlobReader,
  ToolRunner,
  ToolDefinition,
} from "@interchange/types/runtime";
import { TOOL_DEFINITIONS, makeHandlerRegistry } from "./registry";
import {
  composeMiddleware,
  type Middleware,
  type ToolPlugin,
  type ToolHandler,
} from "./plugin";

export type { EditFileArgs } from "./edit-file";
export type { GrepArgs } from "./grep";
export type { ReadFileArgs } from "./read-file";
export type { SearchFilesArgs } from "./search-files";
export type { RunShellArgs } from "./run-shell";
export type { WriteFileArgs } from "./write-file";

export type { Middleware, ToolHandler, ToolPlugin, ExtraTool } from "./plugin";
export { composeMiddleware } from "./plugin";
export { TOOL_NAMES } from "./registry";

export interface PosixToolsOptions {
  cwd: string;
  plugins?: ToolPlugin[];
  /**
   * Optional blob reader used to resolve `tool-output:///{callId}` URIs passed
   * to the read tool. When omitted, attempting to read a `tool-output:` URI
   * throws a clear error; filesystem reads are unaffected.
   */
  blobReader?: BlobReader;
}

export interface PosixTools extends ToolRunner {
  readonly definitions: ToolDefinition[];
  dispose(): Promise<void>;
}

export function createPosixTools(opts: PosixToolsOptions): PosixTools {
  try {
    const stat = statSync(opts.cwd);
    if (!stat.isDirectory()) {
      throw new Error(`cwd is not a directory: ${opts.cwd}`);
    }
  } catch (err) {
    if (err instanceof Error && "code" in err && err.code === "ENOENT") {
      throw new Error(`cwd does not exist: ${opts.cwd}`);
    }
    throw err;
  }

  const handlers = makeHandlerRegistry({
    cwd: opts.cwd,
    ...(opts.blobReader !== undefined ? { blobReader: opts.blobReader } : {}),
  });

  const definitions = [...TOOL_DEFINITIONS];
  const middleware: Middleware[] = [];
  const disposers: (() => Promise<void>)[] = [];

  for (const plugin of opts.plugins ?? []) {
    for (const t of plugin.tools ?? []) {
      if (handlers.has(t.definition.name)) {
        throw new Error(
          `plugin registers tool "${t.definition.name}" which is already registered`,
        );
      }
      definitions.push(t.definition);
      handlers.set(t.definition.name, t.handler);
    }
    if (plugin.middleware !== undefined) middleware.push(plugin.middleware);
    if (plugin.dispose !== undefined) disposers.push(plugin.dispose);
  }

  const base: ToolHandler = async (call, signal) => {
    const handler = handlers.get(call.name);
    if (handler === undefined) {
      return {
        callId: call.id,
        content: `unknown tool: ${call.name}`,
        isError: true,
      };
    }
    return handler(call, signal);
  };

  const composed = composeMiddleware(middleware, base);

  let disposed = false;

  return {
    definitions,
    async run(call, signal) {
      try {
        return await composed(call, signal);
      } catch (err) {
        const message =
          err instanceof Error ? err.message : `unknown error: ${String(err)}`;
        return { callId: call.id, content: message, isError: true };
      }
    },
    async dispose() {
      if (disposed) return;
      disposed = true;
      const errors: unknown[] = [];
      for (const cb of [...disposers].reverse()) {
        try {
          await cb();
        } catch (err) {
          errors.push(err);
        }
      }
      if (errors.length > 0) {
        throw new AggregateError(
          errors,
          "one or more plugin dispose callbacks failed",
        );
      }
    },
  };
}
