// Built-in default director, packaged through the env-DI surface.
//
// `defaultDirectorFactory` is the `AnnotatedDirectorFactory` registered
// under id `@intx/agent/default`, delegating to `@intx/inference`'s
// `createDefaultDirector` (already a `ReactorDirector`; no translation
// layer). `DefaultDirectorConfig` maps the accepted policy fields; the
// arktype schema validates incoming config from `defineDirector.build`.

import { type } from "arktype";

import {
  createDefaultDirector,
  type DefaultDirectorPolicy,
} from "@intx/inference";

import { defineDirector } from "./director";

/**
 * Config the default director accepts via `defineDirector.build`,
 * mirroring `DefaultDirectorPolicy` minus fields not exposed at the
 * author-facing surface (the `afterInferenceDone` hook is a function
 * and cannot canonicalize).
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
 * The default director factory the agent harness registers, under id
 * `@intx/agent/default`.
 */
export const defaultDirectorFactory = defined.factory;

/**
 * Convenience constructor for a `DirectorRef` referencing the default
 * director with the supplied config (or `{}` for "no overrides").
 * Exists so author-defined `AgentDefinition` values can name the
 * default director explicitly when passing non-default config.
 */
export const buildDefaultDirectorRef = defined.build;
