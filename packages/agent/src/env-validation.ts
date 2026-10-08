// Presence-only env validation.
//
// `validateEnv(def, env)` walks the env keys every contributor in the
// definition declared and asserts each is present and non-nullish on
// the supplied env. The check is structural-shallow; value shape is
// not validated -- tool factories whose env contents are structurally
// wrong are expected to fail loud at construction.
//
// `getRequiredEnvKeys(def, registry)` returns the env-key surface in a
// `RequiredEnvKeys` struct alongside an `unresolvedDirectorId` field
// that surfaces the registry's inability to resolve the definition's
// director. `validateEnv` walks the key set inline (it needs per-key
// blame metadata the flat key list does not carry), so the two stay in
// sync through the shared `BASE_ENV_KEYS` constant and
// `effectiveDirectorRef` below.

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
 * The director ref the agent will resolve against the registry,
 * falling back to the registry's canonical default when the
 * definition omits one. Shared by `validateEnv` and
 * `getRequiredEnvKeys` so the absent-director normalization is
 * consistent. Typed as `Pick<AgentDefinition, "director">` because the
 * function only reads `def.director`, which is invariant in `EnvReq`.
 */
export function effectiveDirectorRef(
  def: Pick<AgentDefinition<BaseEnv>, "director">,
  registry: DirectorRegistry,
): DirectorRef {
  return def.director ?? registry.buildDefaultRef();
}

/**
 * The result of `getRequiredEnvKeys`. `keys` is the env-key set the
 * definition's tools and director declare (plus the `BaseEnv` core
 * keys). `unresolvedDirectorId` is `null` when the director resolved
 * cleanly; non-null when the registry could not resolve it, in which
 * case `keys` is the best partial answer (BaseEnv + tool keys only).
 * `string | null` rather than optional so the caller has to
 * acknowledge it.
 */
export interface RequiredEnvKeys {
  readonly keys: readonly string[];
  readonly unresolvedDirectorId: string | null;
}

/**
 * Returns the env-key surface the definition declares via `BaseEnv`,
 * tool factory `requires`, and the resolved director's `requires`.
 * When the registry cannot resolve the director, `keys` is the best
 * partial answer and the unresolved id surfaces on
 * `unresolvedDirectorId`.
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
    // Only swallow the documented unknown-id case; other faults from a
    // custom registry propagate.
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
 * director ids the registry could not resolve. `BaseEnv` contributes
 * its core keys; each tool factory under `tool:<id>`; the director
 * under `director:<id>`. Multiple contributors blaming the same key
 * collapse into one error.
 */
export function validateEnv<EnvReq extends BaseEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
): void {
  const missing = new Set<string>();
  const blame = new Set<string>();
  // Per-contributor map of the keys that contributor declared missing,
  // built in parallel with the flat `missing` / `blame` sets.
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

  // Director resolution can throw on unknown ids; surface the id
  // through `unresolvedDirectors` so the caller gets one uniform
  // exception path while still distinguishing the failure modes.
  if (env.directors !== undefined && env.directors !== null) {
    // `effectiveDirectorRef` is `Pick<AgentDefinition, "director">`-shaped;
    // every `AgentDefinition<EnvReq>` is a structural supertype of it.
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
      // Only catch the documented unknown-id case; other faults from a
      // custom registry propagate so the caller sees the real exception.
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
