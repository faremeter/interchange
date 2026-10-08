// Type-only surface for the director registry. The runtime
// implementations (`createDirectorRegistry`, `defineDirector`, and the
// built-in default factory) live in adjacent files; this module holds
// only the type-level shapes `BaseEnv` depends on.

import type { ReactorDirector, ToolDefinition } from "@intx/types/runtime";

import type { BaseEnv } from "./env";

/**
 * Agent-instance properties a director factory needs at construction,
 * sourced from the `AgentDefinition` being instantiated: the system
 * prompt, the resolved tool definitions, and the compactor names the
 * deployer registered on `env.compactors` (empty when omitted). Held
 * separately from `BaseEnv` because they derive from the definition,
 * not the caller's runtime env.
 */
export interface DirectorAgentContext {
  readonly systemPrompt: string;
  readonly toolDefinitions: readonly ToolDefinition[];
  readonly compactorNames: readonly string[];
}

/**
 * Reference to a director shipped with a bundle; the package-namespaced
 * id is the identity. Same bundle = same factory, so no separate
 * `factoryHash` is needed. `config` is canonical-JSON-serializable so
 * deploy-hash consumers can stably hash the ref.
 */
export interface DirectorRef<Config = unknown> {
  readonly id: string;
  readonly config: Config;
}

/**
 * Factory function shape that produces a `ReactorDirector` from a
 * validated config, the runtime env, and the agent-instance context.
 * The implementation lives in the same bundle as the agent
 * definition; the registry resolves it from `DirectorRef.id`.
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
 * Arktype validator for a director's config. Typed as `unknown` so
 * this module does not import arktype; `defineDirector` validates it.
 */
export type DirectorConfigSchema = unknown;

/**
 * Runtime metadata attached to a `DirectorFactory` by `defineDirector`:
 * the package-namespaced id, env-key requirements, and config schema.
 */
export interface DirectorFactoryMeta {
  readonly id: string;
  readonly requires: readonly string[];
  readonly configSchema: DirectorConfigSchema;
}

/**
 * A director factory with its runtime metadata attached; the registry
 * stores these, `defineDirector` produces them.
 */
export type AnnotatedDirectorFactory<
  Config = unknown,
  EnvReq extends BaseEnv = BaseEnv,
> = DirectorFactory<Config, EnvReq> & DirectorFactoryMeta;

/**
 * Per-runtime director registry, populated explicitly at startup from
 * the bundle's `defineDirector` calls plus built-ins; no module-load
 * side effects. `resolve` returns the factory for a ref;
 * `defaultFactory` is the canonical built-in; `buildDefaultRef`
 * constructs the default ref on demand.
 */
export interface DirectorRegistry {
  resolve(ref: DirectorRef): AnnotatedDirectorFactory;
  defaultFactory(): AnnotatedDirectorFactory;
  buildDefaultRef(): DirectorRef;
}
