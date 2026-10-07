// Per-runtime director registry implementation.
//
// `createDirectorRegistry({ factories, defaultId })` builds a registry
// from a flat factory list and a designated default id; id collisions
// and a missing default fail at construction. `createDefaultDirectorRegistry()`
// is the built-ins-only registry the agent harness ships.

import type {
  AnnotatedDirectorFactory,
  DirectorRef,
  DirectorRegistry,
} from "./director-types";
import type { BaseEnv } from "./env";
import { defaultDirectorFactory } from "./default-director";

/**
 * Erased annotated-factory shape the registry stores: `Config` widened
 * to `unknown` so factories with different config types coexist
 * without contravariant assignment failures.
 */
type RegisteredFactory = AnnotatedDirectorFactory<unknown, BaseEnv>;

/**
 * Thrown by `DirectorRegistry.resolve` for an id the registry does not
 * contain. Named separately so `validateEnv` can distinguish an
 * unknown-id failure from other runtime faults; custom registry
 * implementations are expected to throw it on the unknown-id path.
 */
export class UnknownDirectorIdError extends Error {
  readonly directorId: string;

  constructor(directorId: string) {
    super(`unknown director in registry: ${directorId}`);
    this.name = "UnknownDirectorIdError";
    this.directorId = directorId;
  }
}

/**
 * Build a director registry from a flat list of factories. Throws at
 * construction on duplicate ids or when `defaultId` is absent.
 */
export function createDirectorRegistry(opts: {
  readonly factories: readonly RegisteredFactory[];
  readonly defaultId: string;
}): DirectorRegistry {
  const byId = new Map<string, RegisteredFactory>();
  for (const factory of opts.factories) {
    if (byId.has(factory.id)) {
      throw new Error(`director id collision in registry: ${factory.id}`);
    }
    byId.set(factory.id, factory);
  }

  const defaultFactory = byId.get(opts.defaultId);
  if (defaultFactory === undefined) {
    throw new Error(
      `default director ${opts.defaultId} not in registry factories`,
    );
  }

  return {
    resolve(ref: DirectorRef): RegisteredFactory {
      const factory = byId.get(ref.id);
      if (factory === undefined) {
        throw new UnknownDirectorIdError(ref.id);
      }
      return factory;
    },
    defaultFactory(): RegisteredFactory {
      return defaultFactory;
    },
    buildDefaultRef(): DirectorRef {
      // Fresh object per call; no module-load constant (the spec
      // avoids implicit module-load side effects in the director surface).
      return { id: defaultFactory.id, config: {} };
    },
  };
}

/**
 * The canonical built-ins-only registry, for callers that do not ship
 * their own director factories.
 */
export function createDefaultDirectorRegistry(): DirectorRegistry {
  return createDirectorRegistry({
    factories: [defaultDirectorFactory],
    defaultId: defaultDirectorFactory.id,
  });
}

/**
 * Build the director registry for a workflow closure: the built-in
 * default plus the closure's own `defineDirector` factories. A closure
 * that ships no directors composes to just the default; an id that
 * shadows the built-in or another loaded director throws at
 * construction.
 */
export function createWorkflowDirectorRegistry(
  loaded: readonly AnnotatedDirectorFactory<unknown, BaseEnv>[],
): DirectorRegistry {
  return createDirectorRegistry({
    factories: [defaultDirectorFactory, ...loaded],
    defaultId: defaultDirectorFactory.id,
  });
}
