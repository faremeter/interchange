// Per-runtime director registry implementation.
//
// `createDirectorRegistry({ factories, defaultId })` builds a registry
// from a flat list of `AnnotatedDirectorFactory` values and a designated
// default id. Id collisions and a missing default fail at construction
// rather than first lookup. `createDefaultDirectorRegistry()` is the
// canonical built-ins-only registry the agent harness ships for callers
// that do not author their own directors.

import type {
  AnnotatedDirectorFactory,
  DirectorRef,
  DirectorRegistry,
} from "./director-types";
import type { BaseEnv } from "./env";
import { defaultDirectorFactory } from "./default-director";

/**
 * Erased annotated-factory shape the registry stores. `Config` is
 * widened to `unknown` so factories with different configuration types
 * can coexist in the same registry without contravariant assignment
 * failures.
 */
type RegisteredFactory = AnnotatedDirectorFactory<unknown, BaseEnv>;

/**
 * Build a director registry from a flat list of factories. Throws
 * `Error` at construction on duplicate ids or when `defaultId` is not
 * present in `factories`.
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
        throw new Error(`unknown director in registry: ${ref.id}`);
      }
      return factory;
    },
    defaultFactory(): RegisteredFactory {
      return defaultFactory;
    },
    buildDefaultRef(): DirectorRef {
      // Construct fresh each call. There is no module-load constant for
      // the default ref; the spec is explicit about avoiding implicit
      // module-load side effects in the director surface.
      return { id: defaultFactory.id, config: {} };
    },
  };
}

/**
 * The canonical built-ins-only registry. Convenience for callers that
 * do not ship their own director factories. Callers with custom
 * factories pass them into `createDirectorRegistry` directly.
 */
export function createDefaultDirectorRegistry(): DirectorRegistry {
  return createDirectorRegistry({
    factories: [defaultDirectorFactory],
    defaultId: defaultDirectorFactory.id,
  });
}
