// Tool registration and dispatch.
//
// Two registration shapes: `tool({ definition, handler })` for
// handlers that need the callId or want to set isError/detail/
// pendingMarker, and `stringTool({ definition, handler })` for the
// common "compute a string from the parsed arguments" case.
// `createToolRunner(tools)` builds a `ToolRunner` that dispatches by
// tool name; per the ToolRunner contract, `run` must not throw --
// unknown names and handler exceptions surface as `ToolResult` with
// `isError: true`.
//
// `defineTool({ id, requires?, factory })` is the env-DI factory shape:
// `factory(env)` returns a `ToolBundle` exposing tool definitions, a
// dispatcher, and an optional disposer.

import type { GrantEffect } from "@intx/types";
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
 * Adapt a pre-built `ToolRunner` (e.g. `createPosixTools`) into
 * `AgentTool[]` for `createAgent({ tools })`; each definition becomes
 * a full-handler tool that delegates to the runner's `run`.
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
 * A bundle of tools constructed by an `AnnotatedToolFactory`: the
 * definitions the model sees, a single dispatcher (`run`), and an
 * optional disposer. Disposer ownership lives with the caller -- the
 * env is the agent's dependency contract.
 */
export interface ToolBundle {
  readonly definitions: readonly ToolDefinition[];
  run(call: ToolCall, signal: AbortSignal): Promise<ToolResult>;
  dispose?(): Promise<void>;
}

/**
 * Factory function shape: consumes an env extending `BaseEnv` and
 * produces a `ToolBundle`. Invoked once per agent instantiation.
 */
export type ToolFactory<EnvReq extends BaseEnv = BaseEnv> = (
  env: EnvReq,
) => ToolBundle;

/**
 * Static, per-definition declaration a tool factory carries so callers
 * (e.g. the deploy-time capability walk) can enumerate contributed
 * tool names WITHOUT instantiating the factory. `approval: "ask"`
 * marks a tool as requiring per-invocation approval; a declaration can
 * request a gate, never a pre-deny.
 */
export interface ToolDeclaration {
  readonly name: string;
  readonly approval?: "ask";
}

/**
 * Map a tool's static approval mark to the `GrantEffect` floor its
 * `tool:<name>` grant carries: `ask` floors at `ask`, everything else
 * at `allow`. Single canonical derivation: both the deploy-time
 * capability walk and the per-step tool authorization route through
 * here so a pinned tool derives the same floor an inline declaration
 * would.
 */
export function toolApprovalEffect(
  declaration: Pick<ToolDeclaration, "approval">,
): GrantEffect {
  return declaration.approval === "ask" ? "ask" : "allow";
}

/**
 * Runtime metadata attached to a `ToolFactory` by `defineTool`: the
 * package-namespaced `id`, the env keys the factory touches beyond
 * `BaseEnv`, and the static `definitions` it contributes.
 */
export interface ToolFactoryMeta {
  readonly id: string;
  readonly requires: readonly string[];
  readonly definitions: readonly ToolDeclaration[];
}

/**
 * A tool factory carrying its runtime metadata. `defineTool` is the
 * only sanctioned construction path; the meta fields are attached to
 * the factory function via `Object.assign`.
 */
export type AnnotatedToolFactory<EnvReq extends BaseEnv = BaseEnv> =
  ToolFactory<EnvReq> & ToolFactoryMeta;

/**
 * Define a tool bundle factory. `id` must be package-namespaced;
 * `requires` enumerates env keys beyond `BaseEnv` that `validateEnv`
 * checks; `definitions` statically declares contributed tool names;
 * `factory(env)` returns a `ToolBundle` tied to the agent's lifetime.
 *
 * The returned object is the supplied `factory` with `id`, a frozen
 * `requires` array, and a frozen `definitions` array attached.
 */
export function defineTool<EnvReq extends BaseEnv = BaseEnv>(opts: {
  id: string;
  requires?: readonly string[];
  definitions: readonly ToolDeclaration[];
  factory: ToolFactory<EnvReq>;
}): AnnotatedToolFactory<EnvReq> {
  validateNamespacedId(opts.id);
  const requires = Object.freeze([
    ...(opts.requires ?? []),
  ]) as readonly string[];
  const definitions = Object.freeze([
    ...opts.definitions,
  ]) as readonly ToolDeclaration[];
  // Wrap the caller's factory rather than mutating it so a factory
  // shared across multiple `defineTool` calls keeps a distinct identity
  // with its own metadata.
  const wrapped: ToolFactory<EnvReq> = (env) => opts.factory(env);
  return Object.assign(wrapped, {
    id: opts.id,
    requires,
    definitions,
  });
}

/**
 * A plugin contributes capabilities (extra tools, middleware, anything
 * a host plugin protocol defines) without producing a `ToolBundle`
 * itself. The factory's result shape is host-defined; tool packages
 * that accept plugins read `env.plugins` and dispatch by structural
 * shape or an explicit kind marker. The agent runtime only delivers
 * plugin shapes; it does not interpret them.
 */
export type PluginFactory<EnvReq extends BaseEnv, Result> = (
  env: EnvReq,
) => Result;

/**
 * Registered symbol tagging `AnnotatedPluginFactory` values, exported
 * so consumers can introspect plugin factories without re-registering
 * the key. A registered symbol (not a string key) prevents third-party
 * objects from accidentally satisfying the marker check.
 */
export const PLUGIN_MARKER: unique symbol = Symbol.for("@intx/agent.plugin");

export interface AnnotatedPluginMeta {
  readonly id: string;
  readonly requires: readonly string[];
  /**
   * Static declaration of the tool names this plugin contributes at
   * runtime, so callers can enumerate the plugin's tool grant surface
   * WITHOUT instantiating it (which for a plugin like LSP would start
   * a language-server subprocess). The plugin adds its tools indirectly
   * through the tool package that consumes `env.plugins`, so they are
   * otherwise invisible until run time. Empty for middleware-only
   * plugins.
   */
  readonly definitions: readonly ToolDeclaration[];
  readonly [PLUGIN_MARKER]: true;
}

export type AnnotatedPluginFactory<
  EnvReq extends BaseEnv = BaseEnv,
  Result = unknown,
> = PluginFactory<EnvReq, Result> & AnnotatedPluginMeta;

/**
 * Constant on every plugin instance returned by `definePlugin`. Hosts
 * distinguish plugin instances from arbitrary objects via this field
 * before duck-typing on shape; the string value travels through
 * pure-JSON inspection, unlike the factory-side symbol marker.
 */
export const TOOL_PLUGIN_KIND = "tool-plugin" as const;

/** Type-level form of the kind marker. */
export type ToolPluginKind = typeof TOOL_PLUGIN_KIND;

/**
 * Predicate hosts use to confirm a value off `env.plugins` was minted
 * by `definePlugin`. True iff the value carries the literal
 * `kind: "tool-plugin"` marker.
 */
export function isToolPluginInstance(
  value: unknown,
): value is Record<string, unknown> & { kind: ToolPluginKind } {
  if (value === null || typeof value !== "object") return false;
  if (!("kind" in value)) return false;
  return (value as { kind: unknown }).kind === TOOL_PLUGIN_KIND;
}

/**
 * Define a plugin factory. The plugin's `Result` is host-defined and
 * surfaces in `env.plugins`; the returned instance is tagged with
 * `kind: "tool-plugin"` so hosts can identify it structurally. `id`
 * must be package-namespaced, same rule as `defineTool`.
 */
export function definePlugin<
  Result extends object,
  EnvReq extends BaseEnv = BaseEnv,
>(opts: {
  id: string;
  requires?: readonly string[];
  /**
   * Static declaration of the tool names this plugin contributes at run
   * time. Omit for a middleware-only plugin that adds no standalone tool.
   * See `AnnotatedPluginMeta.definitions`.
   */
  definitions?: readonly ToolDeclaration[];
  factory: PluginFactory<EnvReq, Result>;
}): AnnotatedPluginFactory<EnvReq, Result & { kind: ToolPluginKind }> {
  validateNamespacedId(opts.id);
  const requires = Object.freeze([
    ...(opts.requires ?? []),
  ]) as readonly string[];
  const definitions = Object.freeze([
    ...(opts.definitions ?? []),
  ]) as readonly ToolDeclaration[];
  const wrapped: PluginFactory<EnvReq, Result & { kind: ToolPluginKind }> = (
    env,
  ) => {
    const instance = opts.factory(env);
    // Re-stamping the marker is harmless if the factory set it itself.
    return Object.assign(instance, { kind: TOOL_PLUGIN_KIND });
  };
  return Object.assign(wrapped, {
    id: opts.id,
    requires,
    definitions,
    [PLUGIN_MARKER]: true as const,
  });
}

/** Type predicate distinguishing plugin factories from tool factories. */
export function isAnnotatedPluginFactory(
  value: unknown,
): value is AnnotatedPluginFactory {
  if (typeof value !== "function") return false;
  if (!(PLUGIN_MARKER in value)) return false;
  // `PLUGIN_MARKER in value` narrows `value` to include the symbol
  // key, so the index access below is type-safe without a cast.
  return value[PLUGIN_MARKER] === true;
}

/**
 * Build a `ToolRunner` that dispatches by tool name. Throws
 * `DuplicateToolError` on duplicate names; unknown names and handler
 * exceptions become `ToolResult { isError: true }` so `run` never
 * throws.
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
