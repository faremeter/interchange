// Built-in default director, packaged through the new env-DI surface.
//
// `defaultDirectorFactory` is the `AnnotatedDirectorFactory` the agent
// harness registers under id `@intx/agent/default`. It is the canonical
// entry point for callers that do not author their own directors.
//
// The factory delegates to `@intx/inference`'s `createDefaultDirector`,
// which is already a `ReactorDirector`. The registry's director shape
// is `ReactorDirector` directly (see director-types.ts); no
// translation layer is involved.
//
// Configuration: `DefaultDirectorConfig` maps the existing
// `DefaultDirectorPolicy` fields the factory accepts. The arktype
// schema validates incoming config from `defineDirector.build(config)`.

import { type } from "arktype";

import {
  createDefaultDirector,
  type DefaultDirectorPolicy,
} from "@intx/inference";

import { defineDirector } from "./director";

/**
 * Config the default director accepts via `defineDirector.build`. The
 * shape mirrors `DefaultDirectorPolicy` from `@intx/inference` modulo
 * fields that are not yet exposed at the author-facing surface (the
 * `afterInferenceDone` hook is a function and cannot canonicalize, so
 * it stays off the public ref shape).
 */
export interface DefaultDirectorConfig {
  mode?: "conversational" | "reactive";
}

const DefaultDirectorConfigSchema = type({
  "mode?": '"conversational" | "reactive"',
});

const defined = defineDirector<DefaultDirectorConfig>({
  id: "@intx/agent/default",
  configSchema: DefaultDirectorConfigSchema,
  factory: (config, _env, agent) => {
    const policy: DefaultDirectorPolicy = {};
    if (config.mode !== undefined) {
      policy.mode = config.mode;
    }
    return createDefaultDirector(
      agent.systemPrompt,
      [...agent.toolDefinitions],
      policy,
    );
  },
});

/**
 * The default director factory the agent harness registers. The id is
 * `@intx/agent/default`.
 */
export const defaultDirectorFactory = defined.factory;

/**
 * Convenience constructor for a `DirectorRef` referencing the default
 * director with the supplied config (or `{}` for "no overrides").
 *
 * The registry's `buildDefaultRef()` constructs the same ref shape; this
 * export exists so author-defined `AgentDefinition` values can name the
 * default director explicitly when they want to pass non-default config.
 */
export const buildDefaultDirectorRef = defined.build;
