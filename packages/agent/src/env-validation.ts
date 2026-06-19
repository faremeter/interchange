// Presence-only env validation.
//
// `validateEnv(def, env)` walks the env keys every contributor in the
// definition declared and asserts each is present and non-nullish on
// the supplied env. The check is structural-shallow: a key is "present"
// when `env[key] !== undefined && env[key] !== null`. Value shape is
// not validated -- tool factories whose env contents are structurally
// wrong are expected to fail loud at construction.
//
// `getRequiredEnvKeys(def, registry)` returns the env-key surface in
// a `RequiredEnvKeys` struct alongside an `unresolvedDirectorId` field
// that surfaces the registry's inability to resolve the definition's
// director (so a UI consumer learns both pieces of information in a
// single call). `validateEnv` walks the key set inline rather than
// delegating to this helper -- it needs per-key blame metadata that
// the flat key list does not carry -- so the two functions stay in
// sync through the shared `BASE_ENV_KEYS` constant and the
// `effectiveDirectorRef` helper below.
//
// `effectiveDirectorRef(def, registry)` is the shared helper both
// `validateEnv` and `getRequiredEnvKeys` use to normalize the
// absent-director case. Defined once so the absent-director shape
// stays consistent across callers.

import type { AgentDefinition } from "./definition";
import { UnknownDirectorIdError } from "./director-registry";
import type { DirectorRef, DirectorRegistry } from "./director-types";
import { type BaseEnv, AgentEnvError } from "./env";

const BASE_ENV_KEYS = [
  "sources",
  "defaultSource",
  "storage",
  "workdir",
  "audit",
  "authorize",
  "directors",
] as const;

/**
 * The director ref the agent will resolve against the registry. Falls
 * back to the registry's canonical default when the definition omits a
 * director. Used identically by `validateEnv` and
 * `getRequiredEnvKeys` so the absent-director normalization is
 * consistent.
 *
 * The parameter is typed as `Pick<AgentDefinition<BaseEnv>, "director">`
 * rather than the full `AgentDefinition<EnvReq>` because the function
 * only reads `def.director`, which is invariant in `EnvReq`. The
 * `Pick` is a structural supertype of every `AgentDefinition<EnvReq>`
 * so every caller passes its own narrower generic without an unsafe
 * cast at the call site.
 */
export function effectiveDirectorRef(
  def: Pick<AgentDefinition<BaseEnv>, "director">,
  registry: DirectorRegistry,
): DirectorRef {
  return def.director ?? registry.buildDefaultRef();
}

/**
 * The result of `getRequiredEnvKeys`. `keys` is the env-key set the
 * supplied definition's tools and director declare (plus the
 * `BaseEnv` core keys). `unresolvedDirectorId` is `null` when the
 * director resolved cleanly and the keys list is complete; non-null
 * when the registry could not resolve the definition's director, in
 * which case `keys` is the best partial answer (BaseEnv + tool keys
 * only -- the director's `requires` could not be enumerated).
 *
 * `unresolvedDirectorId` is `string | null` rather than an optional
 * field so the caller has to acknowledge it exists; an optional that
 * resolves to `undefined` is too easy to ignore.
 */
export interface RequiredEnvKeys {
  readonly keys: readonly string[];
  readonly unresolvedDirectorId: string | null;
}

/**
 * Returns the env-key surface the supplied definition declares via
 * `BaseEnv`, tool factory `requires`, and the resolved director's
 * `requires`. When the registry does not contain the definition's
 * director, the returned `keys` is the best partial answer (BaseEnv
 * + tool keys only) and the unresolved id surfaces on
 * `unresolvedDirectorId` so a single call answers both "what env
 * keys must I populate?" and "did the director resolve?".
 */
export function getRequiredEnvKeys(
  def: AgentDefinition<BaseEnv>,
  registry: DirectorRegistry,
): RequiredEnvKeys {
  const keys = new Set<string>(BASE_ENV_KEYS);
  for (const factory of def.toolFactories) {
    for (const key of factory.requires) {
      keys.add(key);
    }
  }
  const ref = effectiveDirectorRef(def, registry);
  let unresolvedDirectorId: string | null = null;
  try {
    const directorFactory = registry.resolve(ref);
    for (const key of directorFactory.requires) {
      keys.add(key);
    }
  } catch (cause) {
    // Same policy as `validateEnv`: only swallow the documented
    // unknown-id case. Other faults from a custom registry propagate
    // so the caller sees the real exception.
    if (!(cause instanceof UnknownDirectorIdError)) throw cause;
    unresolvedDirectorId = ref.id;
  }
  return Object.freeze({
    keys: Object.freeze([...keys]),
    unresolvedDirectorId,
  });
}

/**
 * Presence-only env validation. Throws `AgentEnvError` listing every
 * missing key, the contributors that declared each one, and any
 * director ids the registry could not resolve.
 *
 * `BaseEnv` contributes its core keys; each tool factory
 * contributes under the label `tool:<id>`; the director contributes
 * under `director:<id>`. Multiple contributors blaming the same
 * missing key collapse into a single error. Unknown director ids land
 * on the error's separate `unresolvedDirectors` field rather than
 * being mixed into `missing` (env keys) so consumers can distinguish
 * the two failure modes.
 */
export function validateEnv<EnvReq extends BaseEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
): void {
  const missing = new Set<string>();
  const blame = new Set<string>();
  // Per-contributor map of the keys that contributor declared as
  // missing. Built in parallel with the flat `missing` / `blame`
  // sets so we can surface the contributor → key association without
  // changing the flat-array shape callers already consume.
  const byContributor = new Map<string, Set<string>>();
  const noteMissing = (key: string, contributor: string): void => {
    missing.add(key);
    blame.add(contributor);
    let bucket = byContributor.get(contributor);
    if (bucket === undefined) {
      bucket = new Set<string>();
      byContributor.set(contributor, bucket);
    }
    bucket.add(key);
  };
  const unresolvedDirectors = new Set<string>();
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- shape-erase env to index by key without enumerating the union of generic env keys
  const envRecord = env as unknown as Record<string, unknown>;

  for (const key of BASE_ENV_KEYS) {
    const value = envRecord[key];
    if (value === undefined || value === null) {
      noteMissing(key, "BaseEnv");
    }
  }

  for (const factory of def.toolFactories) {
    for (const key of factory.requires) {
      const value = envRecord[key];
      if (value === undefined || value === null) {
        noteMissing(key, `tool:${factory.id}`);
      }
    }
  }

  // Director resolution can throw on unknown ids. Presence of the
  // director registry itself is already covered by the BaseEnv loop
  // above. If the registry is missing here, we have already recorded
  // it and cannot dereference -- short-circuit on that case. If
  // resolve throws (unknown director id), surface the id through
  // AgentEnvError's `unresolvedDirectors` field so the caller gets a
  // single uniform exception path while still being able to
  // distinguish "missing env key" from "unknown director id"
  // programmatically.
  if (env.directors !== undefined && env.directors !== null) {
    // `effectiveDirectorRef` is `Pick<AgentDefinition, "director">`-shaped
    // -- every `AgentDefinition<EnvReq>` is a structural supertype of
    // that pick, so no cast is needed here.
    const ref = effectiveDirectorRef(def, env.directors);
    try {
      const directorFactory = env.directors.resolve(ref);
      for (const key of directorFactory.requires) {
        const value = envRecord[key];
        if (value === undefined || value === null) {
          noteMissing(key, `director:${directorFactory.id}`);
        }
      }
    } catch (cause) {
      // Only catch the documented unknown-id case. Other faults from a
      // custom registry (TypeError on a malformed ref, an internal
      // Map failure, etc.) propagate so the caller sees the real
      // exception rather than a silently-relabelled "unresolved
      // director id."
      if (!(cause instanceof UnknownDirectorIdError)) throw cause;
      unresolvedDirectors.add(ref.id);
    }
  }

  if (missing.size > 0 || unresolvedDirectors.size > 0) {
    const frozenByContributor = new Map<string, readonly string[]>();
    for (const [contributor, keys] of byContributor) {
      frozenByContributor.set(contributor, Object.freeze([...keys]));
    }
    throw new AgentEnvError([...missing], [...blame], frozenByContributor, [
      ...unresolvedDirectors,
    ]);
  }
}
