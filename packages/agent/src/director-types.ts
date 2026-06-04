// Type-only surface for the director registry.
//
// The runtime implementations -- `createDirectorRegistry`,
// `defineDirector`, the director-to-reactor adapter, and the built-in
// default factory -- arrive in a separate commit. This module holds
// only the type-level shapes the env contract (`BaseEnv`) depends on, so
// the env primitives can typecheck independently.
//
// (Workflow framework design spec v1, section 2 -- director registry.)

import type { BaseEnv } from "./env";

/**
 * Result the author-facing director returns from a `step` call. The
 * three shapes form a closed set for Phase 1: the assembly orchestrates
 * `emit`, `reply`, `checkpoint`, and `compact` actions itself; author
 * directors only express the control-flow choices below.
 *
 *  - `continue`     - run the next inference call
 *  - `invoke-tool`  - dispatch a tool call before the next inference
 *  - `terminate`    - end the loop with a result
 *
 * Anything outside this set is a framework concern; the adapter rejects
 * other shapes with a typed error.
 */
export type DirectorDecision =
  | { kind: "continue" }
  | { kind: "invoke-tool"; toolName: string; args: unknown }
  | { kind: "terminate"; result: AgentResult };

/**
 * Terminal result a director can return through
 * `DirectorDecision.terminate`. The author-facing shape is intentionally
 * narrow; richer reactor results (tool outputs, intermediate turns) are
 * surfaced through the agent's event stream, not the result.
 */
export type AgentResult =
  | { kind: "text"; text: string }
  | { kind: "tool-result"; toolName: string; args: unknown };

/**
 * Author-facing director interface. The agent harness adapts this onto
 * the reactor's `ReactorDirector.decide(state, capabilities)` shape;
 * `@intx/inference` is not touched.
 */
export interface Director {
  step(event: unknown): Promise<DirectorDecision>;
  close(): Promise<void>;
}

/**
 * Reference to a director shipped with a bundle. The package-namespaced
 * id is the identity. The bundle the workflow ships with includes the
 * factory that maps the id back to runtime code; same bundle = same
 * factory, so no separate `factoryHash` is needed.
 *
 * `config` is canonicalized via `canonicalizeForHash` for deploy-time
 * hashing.
 */
export interface DirectorRef<Config = unknown> {
  readonly id: string;
  readonly config: Config;
}

/**
 * Factory function shape that produces a `Director` from a config and
 * the agent's runtime env. The implementation lives in the same bundle
 * as the agent definition; the registry resolves it from `DirectorRef.id`.
 */
export type DirectorFactory<
  Config = unknown,
  EnvReq extends BaseEnv = BaseEnv,
> = (config: Config, env: EnvReq) => Director;

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
