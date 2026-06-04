// Type-only surface for the director registry.
//
// The runtime implementations -- `createDirectorRegistry`,
// `defineDirector`, and the built-in default factory -- live in
// adjacent files. This module holds only the type-level shapes the env
// contract (`BaseEnv`) depends on, so the env primitives can typecheck
// independently.
//
// `DirectorFactory` returns a `ReactorDirector` directly. The only
// director that flows through the registry today is the built-in
// default, which is already `ReactorDirector`-shaped; no translation
// layer is needed.

import type { ReactorDirector, ToolDefinition } from "@intx/types/runtime";

import type { BaseEnv } from "./env";

/**
 * Agent-instance properties a director factory needs at construction.
 * Sourced from the `AgentDefinition` the agent harness is instantiating:
 * the system prompt and the resolved tool definitions the model will
 * see. Held separately from `BaseEnv` because these values are derived
 * from the agent definition, not supplied by the caller as runtime env.
 */
export interface DirectorAgentContext {
  readonly systemPrompt: string;
  readonly toolDefinitions: readonly ToolDefinition[];
}

/**
 * Reference to a director shipped with a bundle. The package-namespaced
 * id is the identity. The bundle that ships the director includes the
 * factory that maps the id back to runtime code; same bundle = same
 * factory, so no separate `factoryHash` is needed.
 *
 * `config` is canonical-JSON-serializable so deploy-hash consumers can
 * stably hash the ref via `canonicalizeForHash(ref.config)`.
 */
export interface DirectorRef<Config = unknown> {
  readonly id: string;
  readonly config: Config;
}

/**
 * Factory function shape that produces a `ReactorDirector` from a
 * validated config, the agent's runtime env, and the agent-instance
 * context (system prompt + tool definitions). The implementation lives
 * in the same bundle as the agent definition; the registry resolves it
 * from `DirectorRef.id`.
 */
export type DirectorFactory<
  Config = unknown,
  EnvReq extends BaseEnv = BaseEnv,
> = (
  config: Config,
  env: EnvReq,
  agent: DirectorAgentContext,
) => ReactorDirector;

/**
 * Arktype validator for a director's config. Stored as `unknown` at the
 * type level so this module does not have to import arktype; the
 * concrete `defineDirector` runtime validates it.
 */
export type DirectorConfigSchema = unknown;

/**
 * Runtime metadata attached to a `DirectorFactory` by `defineDirector`.
 * The factory carries its package-namespaced id, its env-key
 * requirements, and the arktype schema that validates its config.
 */
export interface DirectorFactoryMeta {
  readonly id: string;
  readonly requires: readonly string[];
  readonly configSchema: DirectorConfigSchema;
}

/**
 * A director factory with its runtime metadata attached. The registry
 * stores these; `defineDirector` produces them.
 */
export type AnnotatedDirectorFactory<
  Config = unknown,
  EnvReq extends BaseEnv = BaseEnv,
> = DirectorFactory<Config, EnvReq> & DirectorFactoryMeta;

/**
 * Per-runtime director registry. Populated explicitly at startup from
 * the bundle's `defineDirector` calls plus built-ins from `@intx/agent`.
 * No module-load side effects.
 *
 * `resolve` returns the factory for a given ref; `defaultFactory` is the
 * canonical built-in; `buildDefaultRef` constructs the default ref on
 * demand (each call constructs a fresh object, no module-load constant).
 */
export interface DirectorRegistry {
  resolve(ref: DirectorRef): AnnotatedDirectorFactory;
  defaultFactory(): AnnotatedDirectorFactory;
  buildDefaultRef(): DirectorRef;
}
