// Higher-level setup helpers for the agent-* examples.
//
// `resolveAgentProvider` collapses the provider-resolution +
// error-print pattern every single-provider example has into one
// call site; `openExampleAgent` further wraps the `createAgent`
// invocation so the seven non-quickstart examples can focus on the
// feature they demonstrate rather than re-deriving construction
// boilerplate.
//
// `agent-quickstart` deliberately keeps `createAgent` inline as the
// canonical "here is the full surface" reference; every other
// example uses these helpers.

import { createAgent, type Agent, type AgentTool } from "@interchange/agent";
import type { ProviderConfig } from "@interchange/types/runtime";

import { resolveProvider } from "./env-provider";
import {
  type CommonMainOptions,
  type SingleProviderMainOptions,
} from "./main-options";
import { optional } from "./optional";
import { defaultContextDir } from "./paths";

/**
 * Resolve a single provider from `opts.providerOverride` or the
 * surrounding env, writing the help text to `stderr` and returning
 * `null` when neither is present. Callers map `null` to a non-zero
 * exit code.
 */
export function resolveAgentProvider(
  opts: SingleProviderMainOptions,
  env: NodeJS.ProcessEnv,
  exampleName: string,
  stderr: (chunk: string) => void,
): { provider: ProviderConfig; model: string } | null {
  const resolved = resolveProvider({
    env,
    exampleName,
    ...optional("providerOverride", opts.providerOverride),
  });
  if (!resolved.ok) {
    stderr(resolved.help);
    return null;
  }
  return { provider: resolved.provider, model: resolved.model };
}

/**
 * What `openExampleAgent` needs from the caller. `providers` is
 * supplied as a list so multi-provider examples can pass both
 * primary and fallback; `defaultModel` selects the active one.
 */
export type OpenExampleAgentSpec = {
  exampleName: string;
  systemPrompt: string;
  tools: AgentTool[];
  providers: ProviderConfig[];
  defaultModel: string;
};

/**
 * Construct an `@interchange/agent` Agent from the example's
 * `MainOptions` and a per-example spec. The helper:
 *
 *   - Defaults `contextDir` to `defaultContextDir(spec.exampleName)`
 *     unless `opts.contextDir` is set.
 *   - Forwards `opts.deps` via `optional("deps", ...)` so the
 *     harness-driven tests can swap fetch without dragging the
 *     conditional-spread idiom into every call.
 *
 * Anything example-specific lives in `spec` (system prompt, tools,
 * providers, model). The `@interchange/agent` API surface
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
    providers: spec.providers,
    defaultModel: spec.defaultModel,
    systemPrompt: spec.systemPrompt,
    tools: spec.tools,
    ...optional("deps", opts.deps),
  });
}
