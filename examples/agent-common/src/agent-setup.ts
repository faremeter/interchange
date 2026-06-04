// Higher-level setup helpers for the agent-* examples.
//
// `resolveAgentSource` collapses the source-resolution + error-print
// pattern every single-source example has into one call site.
// `openExampleAgent` further wraps construction so the seven
// non-quickstart examples can focus on the feature they demonstrate
// rather than re-deriving boilerplate around `defineAgent`,
// `createIsogitStore`, and the env shape.
//
// `agent-quickstart` deliberately keeps the surface inline as the
// canonical "here is the full shape" reference; every other example
// uses these helpers.

import { mkdirSync } from "node:fs";

import {
  createAgent,
  createDefaultDirectorRegistry,
  createToolRunner,
  defineAgent,
  defineTool,
  type Agent,
  type AgentTool,
  type BaseEnv,
} from "@intx/agent";
import { permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit";
import type { InferenceSource } from "@intx/types/runtime";

import { resolveSource } from "./env-source";
import {
  type CommonMainOptions,
  type SingleSourceMainOptions,
} from "./main-options";
import { defaultContextDir } from "./paths";

/**
 * Resolve a single inference source from `opts.sourceOverride` or the
 * surrounding env, writing the help text to `stderr` and returning
 * `null` when neither is present. Callers map `null` to a non-zero
 * exit code.
 */
export function resolveAgentSource(
  opts: SingleSourceMainOptions,
  env: NodeJS.ProcessEnv,
  exampleName: string,
  stderr: (chunk: string) => void,
): InferenceSource | null {
  const resolved = resolveSource({
    env,
    exampleName,
    ...(opts.sourceOverride !== undefined
      ? { sourceOverride: opts.sourceOverride }
      : {}),
  });
  if (!resolved.ok) {
    stderr(resolved.help);
    return null;
  }
  return resolved.source;
}

/**
 * What `openExampleAgent` needs from the caller. `sources` is the list
 * of preferred inference sources for this run (the entry whose `id`
 * matches `defaultSource` becomes the active env source); `tools` is
 * the list of per-tool registrations -- the helper wraps them as a
 * single bundle factory internally so example authors can keep writing
 * `tool({ definition, handler })` / `stringTool({ ... })` calls without
 * having to know about `defineTool`.
 */
export type OpenExampleAgentSpec = {
  exampleName: string;
  systemPrompt: string;
  tools: readonly AgentTool[];
  sources: readonly InferenceSource[];
  defaultSource: string;
};

/**
 * Construct an `@intx/agent` Agent from the example's `MainOptions`
 * and a per-example spec. The helper builds:
 *
 *   - An `AgentDefinition` whose `toolFactories` carry one bundle
 *     factory wrapping the example's `AgentTool[]`.
 *   - An `AgentEnv` carrying the active source (the entry whose `id`
 *     matches `defaultSource`), an isogit-backed `ContextStore` at the
 *     example's context dir, no-op audit, and a permissive authorize
 *     (per `@intx/agent/testing`).
 *
 * `opts.deps` (test fetch-stub) threads onto env.deps so harness-driven
 * tests can swap inference dependencies without dragging the spread
 * idiom into every example call site.
 *
 * Multi-source examples (`agent-multi-provider`) pass both providers
 * via `sources`; the example rotates to the other entry via
 * `agent.setSource(...)` once the agent exists.
 */
export async function openExampleAgent<T extends CommonMainOptions>(
  opts: T,
  spec: OpenExampleAgentSpec,
): Promise<Agent> {
  const contextDir = opts.contextDir ?? defaultContextDir(spec.exampleName);
  mkdirSync(contextDir, { recursive: true });
  const storage = await createIsogitStore(contextDir);

  const active = spec.sources.find((s) => s.id === spec.defaultSource);
  if (active === undefined) {
    throw new Error(
      `defaultSource ${spec.defaultSource} did not match any source in spec.sources`,
    );
  }

  // Wrap the example's per-tool registrations as a single bundle
  // factory. Empty-tool examples get an empty bundle; the agent
  // dispatches "unknown tool" responses if the model invokes anything.
  const exampleToolsFactory = defineTool({
    id: `@intx/example-agent-common/${spec.exampleName}-tools`,
    factory: () => {
      const runner = createToolRunner([...spec.tools]);
      return {
        definitions: runner.definitions,
        run: (call, signal) => runner.run(call, signal),
      };
    },
  });

  const def = defineAgent({
    id: spec.exampleName,
    systemPrompt: spec.systemPrompt,
    tools: [exampleToolsFactory],
    capabilities: [],
    inference: {
      sources: spec.sources.map((s) => ({
        provider: s.provider,
        model: s.model,
      })),
    },
  });

  const env: BaseEnv = {
    source: active,
    storage,
    workdir: contextDir,
    audit: storage,
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
    ...(opts.deps !== undefined ? { deps: opts.deps } : {}),
  };

  return createAgent(def, env);
}
