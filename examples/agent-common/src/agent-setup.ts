// Higher-level setup helpers for the agent-* examples.
//
// `resolveAgentSource` collapses the source-resolution +
// error-print pattern every single-source example has into one
// call site; `openExampleAgent` further wraps the `createAgent`
// invocation so the seven non-quickstart examples can focus on the
// feature they demonstrate rather than re-deriving construction
// boilerplate.
//
// `agent-quickstart` deliberately keeps `createAgent` inline as the
// canonical "here is the full surface" reference; every other
// example uses these helpers.

import { createAgent, type Agent, type AgentTool } from "@intx/agent";
import type { InferenceSource } from "@intx/types/runtime";

import { resolveSource } from "./env-source";
import {
  type CommonMainOptions,
  type SingleSourceMainOptions,
} from "./main-options";
import { optional } from "./optional";
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
    ...optional("sourceOverride", opts.sourceOverride),
  });
  if (!resolved.ok) {
    stderr(resolved.help);
    return null;
  }
  return resolved.source;
}

/**
 * What `openExampleAgent` needs from the caller. `sources` is supplied
 * as a list so multi-source examples can pass both primary and
 * fallback; `defaultSource` selects the active one by id.
 */
export type OpenExampleAgentSpec = {
  exampleName: string;
  systemPrompt: string;
  tools: AgentTool[];
  sources: InferenceSource[];
  defaultSource: string;
};

/**
 * Construct an `@intx/agent` Agent from the example's
 * `MainOptions` and a per-example spec. The helper:
 *
 *   - Defaults `contextDir` to `defaultContextDir(spec.exampleName)`
 *     unless `opts.contextDir` is set.
 *   - Forwards `opts.deps` via `optional("deps", ...)` so the
 *     harness-driven tests can swap fetch without dragging the
 *     conditional-spread idiom into every call.
 *
 * Anything example-specific lives in `spec` (system prompt, tools,
 * sources, defaultSource). The `@intx/agent` API surface
 * (`createAgent`, `agent.send`, `agent.close`, etc.) is otherwise
 * unchanged; the helper exists to remove repetition, not to wrap
 * the surface.
 */
export async function openExampleAgent<T extends CommonMainOptions>(
  opts: T,
  spec: OpenExampleAgentSpec,
): Promise<Agent> {
  return createAgent({
    contextDir: opts.contextDir ?? defaultContextDir(spec.exampleName),
    sources: spec.sources,
    defaultSource: spec.defaultSource,
    systemPrompt: spec.systemPrompt,
    tools: spec.tools,
    ...optional("deps", opts.deps),
  });
}
