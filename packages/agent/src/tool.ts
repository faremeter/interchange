// Tool registration and dispatch.
//
// Two registration shapes are supported for per-tool authoring:
//
//   `tool({ definition, handler })`        - handler receives the full
//                                            ToolCall and returns the full
//                                            ToolResult. Use when the
//                                            handler needs the callId or
//                                            wants to set isError/detail/
//                                            pendingMarker.
//
//   `stringTool({ definition, handler })`  - sugar for the common case of
//                                            "compute a string from the
//                                            parsed arguments." The callId
//                                            is filled in from the
//                                            surrounding ToolCall, and
//                                            isError is false unless the
//                                            handler throws.
//
// `createToolRunner(tools)` builds a `ToolRunner` that dispatches by tool
// name. Per the ToolRunner contract (packages/types/src/runtime.ts), `run`
// must not throw -- unknown tool names and handler exceptions are surfaced
// as `ToolResult` with `isError: true` so the model sees them and can
// recover.
//
// `defineTool({ id, requires?, factory })` is the env-DI factory shape.
// It produces an
// `AnnotatedToolFactory` whose `factory(env)` returns a `ToolBundle`
// exposing a set of tool definitions, a dispatcher, and an optional
// disposer. Bundle-style (rather than per-tool) factory shapes match
// the existing posix-tools and mail-tools ergonomics; a package that
// wants per-tool granularity wraps each tool in its own single-
// definition bundle.

import type {
  ToolCall,
  ToolDefinition,
  ToolResult,
  ToolRunner,
} from "@intx/types/runtime";

import type { BaseEnv } from "./env";
import { validateNamespacedId } from "./namespace";

export type ToolHandler = (
  call: ToolCall,
  signal: AbortSignal,
) => Promise<ToolResult>;

export type StringToolHandler = (
  args: Record<string, unknown>,
  signal: AbortSignal,
) => Promise<string>;

export type AgentTool =
  | { kind: "full"; definition: ToolDefinition; handler: ToolHandler }
  | {
      kind: "string";
      definition: ToolDefinition;
      handler: StringToolHandler;
    };

export function tool(args: {
  definition: ToolDefinition;
  handler: ToolHandler;
}): AgentTool {
  return { kind: "full", definition: args.definition, handler: args.handler };
}

export function stringTool(args: {
  definition: ToolDefinition;
  handler: StringToolHandler;
}): AgentTool {
  return {
    kind: "string",
    definition: args.definition,
    handler: args.handler,
  };
}

/**
 * Adapt a pre-built ToolRunner (e.g. the one returned by
 * `createPosixTools`) into a list of AgentTools that can be passed to
 * `createAgent({ tools })`. Each definition becomes a full-handler
 * AgentTool that delegates to the runner's `run`.
 *
 * Use this when integrating tool packages whose public surface is a
 * single ToolRunner rather than individual handlers.
 */
export function fromToolRunner(runner: {
  readonly definitions: readonly ToolDefinition[];
  run: ToolRunner["run"];
}): AgentTool[] {
  return runner.definitions.map((definition) => ({
    kind: "full",
    definition,
    handler: (call, signal) => runner.run(call, signal),
  }));
}

export class DuplicateToolError extends Error {
  readonly toolName: string;

  constructor(toolName: string) {
    super(`duplicate tool name: ${toolName}`);
    this.name = "DuplicateToolError";
    this.toolName = toolName;
  }
}

export type AgentToolRunner = ToolRunner & {
  readonly definitions: readonly ToolDefinition[];
};

/**
 * A bundle of tools constructed by an `AnnotatedToolFactory`. Exposes
 * the set of tool definitions the model sees, a single dispatcher
 * (`run`), and an optional disposer the caller invokes after the agent
 * closes.
 *
 * Disposer ownership lives with the caller -- the env is the agent's
 * dependency contract; the caller owns the lifetime of what it puts in
 * env. The agent does not invoke `dispose` itself.
 */
export interface ToolBundle {
  readonly definitions: readonly ToolDefinition[];
  run(call: ToolCall, signal: AbortSignal): Promise<ToolResult>;
  dispose?(): Promise<void>;
}

/**
 * Factory function shape -- consumes an env extending `BaseEnv` and
 * produces a `ToolBundle`. The factory is invoked once per agent
 * instantiation.
 */
export type ToolFactory<EnvReq extends BaseEnv = BaseEnv> = (
  env: EnvReq,
) => ToolBundle;

/**
 * Runtime metadata attached to a `ToolFactory` by `defineTool`. `id` is
 * package-namespaced; `requires` enumerates env keys the factory touches
 * beyond `BaseEnv`'s six core fields.
 */
export interface ToolFactoryMeta {
  readonly id: string;
  readonly requires: readonly string[];
}

/**
 * A tool factory carrying its runtime metadata. `defineTool` is the only
 * sanctioned construction path.
 *
 * The intersection `ToolFactory<EnvReq> & ToolFactoryMeta` is the
 * type-level surface; at runtime the meta fields are attached to the
 * factory function via `Object.assign`.
 */
export type AnnotatedToolFactory<EnvReq extends BaseEnv = BaseEnv> =
  ToolFactory<EnvReq> & ToolFactoryMeta;

/**
 * Define a tool bundle factory.
 *
 *   - `id` must be package-namespaced ("@vendor/pkg/name" or
 *     "pkg/name"). Bare ids throw `Error` at definition time.
 *   - `requires` enumerates the env keys this factory touches beyond
 *     `BaseEnv`'s six core fields. The runtime `validateEnv` checks
 *     presence; the factory itself may also fail loud at construction
 *     if the env contents are structurally wrong.
 *   - `factory(env)` returns a `ToolBundle`. Invoked once per agent
 *     instantiation; the bundle's lifetime is tied to that agent.
 *
 * The returned object is the same callable as the supplied `factory`
 * with `id` and a frozen `requires` array attached.
 */
export function defineTool<EnvReq extends BaseEnv = BaseEnv>(opts: {
  id: string;
  requires?: readonly string[];
  factory: ToolFactory<EnvReq>;
}): AnnotatedToolFactory<EnvReq> {
  validateNamespacedId(opts.id);
  const requires = Object.freeze([
    ...(opts.requires ?? []),
  ]) as readonly string[];
  // Wrap the caller's factory rather than mutating it. A caller that
  // shares a factory function across multiple `defineTool` calls
  // (e.g. registering the same constructor under two ids in different
  // bundles) needs each `AnnotatedToolFactory` to be a distinct
  // identity with its own metadata; a direct `Object.assign` on
  // `opts.factory` would let the second call silently overwrite the
  // first's annotations.
  const wrapped: ToolFactory<EnvReq> = (env) => opts.factory(env);
  return Object.assign(wrapped, {
    id: opts.id,
    requires,
  });
}

/**
 * Build a `ToolRunner` that dispatches by tool name. Throws
 * `DuplicateToolError` at construction if any two tools share a name.
 *
 * At call time, unknown tool names and exceptions from handlers are
 * converted to `ToolResult { isError: true }` so the contract on
 * `ToolRunner.run` ("must not throw") is upheld.
 */
export function createToolRunner(tools: AgentTool[]): AgentToolRunner {
  const byName = new Map<string, AgentTool>();
  for (const t of tools) {
    if (byName.has(t.definition.name)) {
      throw new DuplicateToolError(t.definition.name);
    }
    byName.set(t.definition.name, t);
  }

  const definitions: readonly ToolDefinition[] = tools.map((t) => t.definition);

  return {
    definitions,
    async run(call, signal): Promise<ToolResult> {
      const found = byName.get(call.name);
      if (found === undefined) {
        return {
          callId: call.id,
          content: `unknown tool: ${call.name}`,
          isError: true,
        };
      }
      try {
        if (found.kind === "full") {
          return await found.handler(call, signal);
        }
        const text = await found.handler(call.arguments, signal);
        return { callId: call.id, content: text };
      } catch (err) {
        return {
          callId: call.id,
          content: err instanceof Error ? err.message : String(err),
          isError: true,
        };
      }
    },
  };
}
