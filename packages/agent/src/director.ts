// `defineDirector` -- the env-DI factory shape for author-defined
// directors.
//
// `defineDirector({ id, configSchema, requires?, factory })` returns
// `{ build, factory }`. The `factory` is an `AnnotatedDirectorFactory`
// the registry stores by id; the bundle that ships the director
// re-exports it so a caller can pass it into `createDirectorRegistry`.
// The `build(config)` constructor produces a `DirectorRef` from a
// config that the schema validates. Both halves are needed: the
// registry stores the factory, the agent definition stores the ref.
//
// The `defineDirector` runtime does not register the factory as a
// module-load side effect. Each runtime instance constructs its
// registry explicitly via `createDirectorRegistry`, listing the
// factories it wants rather than relying on import-order.

import { type } from "arktype";

import type {
  AnnotatedDirectorFactory,
  DirectorConfigSchema,
  DirectorFactory,
  DirectorRef,
} from "./director-types";
import type { BaseEnv } from "./env";
import { validateNamespacedId } from "./namespace";

/**
 * Result of `defineDirector`. The `factory` half is what the registry
 * stores; the `build` half is what the agent-definition author calls
 * to construct a `DirectorRef` referencing this director.
 *
 * The `factory` field is **type-erased** in its `Config` parameter. The
 * registry stores factories of heterogeneous config types alongside
 * each other; if `factory` carried the narrow `Config`, contravariant
 * function-parameter variance would prevent the assignment. The agent
 * only invokes the factory with `ref.config: unknown` sourced from a
 * `DirectorRef`, and the schema has already validated that config at
 * `build` time, so the erasure is safe at the call site.
 */
export interface DefinedDirector<Config, EnvReq extends BaseEnv = BaseEnv> {
  readonly factory: AnnotatedDirectorFactory<unknown, EnvReq>;
  build(config: Config): DirectorRef<Config>;
}

/**
 * Define a director factory.
 *
 *   - `id` must be package-namespaced. Bare ids throw at definition
 *     time.
 *   - `configSchema` is an arktype validator. The schema validates the
 *     config at `build(config)` time. Consumers that compute a deploy
 *     hash over the ref call `canonicalizeForHash(ref.config)`
 *     themselves; `build` does not run that check.
 *   - `requires` enumerates the env keys the factory touches beyond
 *     `BaseEnv`. `validateEnv` checks presence at instantiation.
 *   - `factory(config, env, agentContext)` returns a `ReactorDirector`.
 *     `agentContext` carries the agent definition's resolved system
 *     prompt and tool definitions; the factory uses them when its
 *     director needs to see the model's tools or seed prompt.
 *
 * Two-stage construction (factory + build) lets the registry index the
 * factory by id while callers stamp configs into refs as data. Same
 * bundle = same factory; refs hash by id and config.
 */
export function defineDirector<Config, EnvReq extends BaseEnv = BaseEnv>(opts: {
  readonly id: string;
  readonly configSchema: DirectorConfigSchema;
  readonly requires?: readonly string[];
  readonly factory: DirectorFactory<Config, EnvReq>;
}): DefinedDirector<Config, EnvReq> {
  validateNamespacedId(opts.id);

  const requires = Object.freeze([
    ...(opts.requires ?? []),
  ]) as readonly string[];

  // Wrap the caller's factory rather than mutating it. A caller that
  // shares a factory function across multiple `defineDirector` calls
  // (e.g. registering the same factory under two ids) needs each
  // annotated factory to be a distinct identity with its own metadata;
  // a direct `Object.assign` on `opts.factory` would let the second
  // call silently overwrite the first's annotations.
  const wrapped: DirectorFactory<Config, EnvReq> = (config, env, agent) =>
    opts.factory(config, env, agent);
  const annotatedTyped: AnnotatedDirectorFactory<Config, EnvReq> =
    Object.assign(wrapped, {
      id: opts.id,
      requires,
      configSchema: opts.configSchema,
    });
  // Erase the Config parameter for registry storage. The factory body
  // continues to expect the narrow Config (via the closure on
  // `opts.factory`); the registry just sees `(config: unknown, ...)`.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional contravariant erasure for heterogeneous registry storage
  const annotated = annotatedTyped as unknown as AnnotatedDirectorFactory<
    unknown,
    EnvReq
  >;

  function build(config: Config): DirectorRef<Config> {
    validateConfig(config, opts.configSchema);
    return { id: opts.id, config };
  }

  return { factory: annotated, build };
}

function validateConfig(config: unknown, schema: DirectorConfigSchema): void {
  // The schema is typed as `unknown` at the type level so this module
  // does not have to import arktype. At runtime it must be an arktype
  // validator -- a callable that returns either the validated value or
  // a `type.errors` instance. If the schema is not callable, treat
  // that as a definition-time author error.
  if (typeof schema !== "function") {
    throw new Error(
      "defineDirector: configSchema must be an arktype validator (callable)",
    );
  }
  const result: unknown = schema(config);
  if (result instanceof type.errors) {
    throw new Error(`director config validation failed: ${result.summary}`);
  }
}
