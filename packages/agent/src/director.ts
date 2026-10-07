// `defineDirector` -- the env-DI factory shape for author-defined
// directors. Returns `{ build, factory }`: the factory is an
// `AnnotatedDirectorFactory` the registry stores by id; `build(config)`
// produces a `DirectorRef` from a schema-validated config. The runtime
// does not register factories as a module-load side effect; each
// runtime instance constructs its registry explicitly.

import { type } from "arktype";

import type {
  AnnotatedDirectorFactory,
  DirectorConfigSchema,
  DirectorFactory,
  DirectorRef,
} from "./director-types";
import type { BaseEnv } from "./env";
import { validateNamespacedId } from "./namespace";
import { isAnnotatedPluginFactory } from "./tool";

/**
 * Result of `defineDirector`. The `factory` field is type-erased in
 * its `Config` parameter so heterogeneous config types coexist in the
 * registry; the schema has already validated the config at `build`
 * time, so the erasure is safe at the call site.
 */
export interface DefinedDirector<Config, EnvReq extends BaseEnv = BaseEnv> {
  readonly factory: AnnotatedDirectorFactory<unknown, EnvReq>;
  build(config: Config): DirectorRef<Config>;
}

/**
 * Define a director factory. `id` must be package-namespaced;
 * `configSchema` is an arktype validator run at `build(config)` time;
 * `requires` enumerates env keys beyond `BaseEnv`; `factory(config,
 * env, agentContext)` returns a `ReactorDirector`, with
 * `agentContext` carrying the definition's system prompt and tool
 * definitions.
 *
 * Two-stage construction (factory + build) lets the registry index the
 * factory by id while callers stamp configs into refs as data.
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

  // Wrap the caller's factory rather than mutating it so a factory
  // shared across multiple `defineDirector` calls keeps a distinct
  // identity with its own metadata.
  const wrapped: DirectorFactory<Config, EnvReq> = (config, env, agent) =>
    opts.factory(config, env, agent);
  const annotatedTyped: AnnotatedDirectorFactory<Config, EnvReq> =
    Object.assign(wrapped, {
      id: opts.id,
      requires,
      configSchema: opts.configSchema,
    });
  // Erase the Config parameter for registry storage; the factory body
  // still expects the narrow Config via the closure on `opts.factory`.
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
  // The schema is typed as `unknown` so this module does not import
  // arktype; a non-callable schema is a definition-time author error.
  if (typeof schema !== "function") {
    throw new Error(
      "director configSchema must be an arktype validator (callable)",
    );
  }
  const result: unknown = schema(config);
  if (result instanceof type.errors) {
    throw new Error(`director config validation failed: ${result.summary}`);
  }
}

/**
 * Run the registered config schema against a `DirectorRef.config`.
 * Throws on schema rejection or a non-callable schema. `createAgent`
 * re-validates at resolve time because the ref type is public and
 * nothing forces refs through `build`.
 */
export function validateDirectorConfig(
  config: unknown,
  schema: DirectorConfigSchema,
): void {
  validateConfig(config, schema);
}

/**
 * Structural check for an `AnnotatedDirectorFactory` export: callable
 * plus `{ id, requires, configSchema }`. The `configSchema` field
 * discriminates against tool factories (which carry only `id` and
 * `requires`). Shared by the tool-package loader and the
 * workflow-closure director loader so both accept and reject exactly
 * the same shapes.
 */
export function isAnnotatedDirectorFactory(
  value: unknown,
): value is AnnotatedDirectorFactory<unknown, BaseEnv> {
  if (typeof value !== "function") return false;
  if (isAnnotatedPluginFactory(value)) return false;
  if (!("id" in value) || !("requires" in value)) return false;
  if (!("configSchema" in value)) return false;
  const id = (value as { id: unknown }).id;
  const requires = (value as { requires: unknown }).requires;
  const configSchema = (value as { configSchema: unknown }).configSchema;
  if (typeof id !== "string") return false;
  if (!Array.isArray(requires)) return false;
  if (!requires.every((r) => typeof r === "string")) return false;
  // A non-callable schema would crash later inside config validation;
  // reject here so the failure surfaces at load time.
  if (typeof configSchema !== "function") return false;
  return true;
}
