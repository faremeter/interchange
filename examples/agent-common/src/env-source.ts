// Environment-driven `InferenceSource` resolution for the agent-* examples.
//
// Production users invoke each example with `ANTHROPIC_API_KEY` set;
// `resolveSource` reads that variable, builds an Anthropic inference
// source, and returns it. Tests bypass the env entirely by supplying
// `sourceOverride` — the helper short-circuits and returns the
// override unchanged.
//
// When the env is incomplete and no override was supplied, the helper
// returns `{ ok: false, help }` carrying a multi-line message the
// caller writes to stderr before exiting non-zero. This is gentler
// than throwing: first-time users skim past stack traces, but they
// will read a 5-line "set these variables to run me" block.

import type { InferenceSource } from "@intx/types/runtime";

export const DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com";
export const DEFAULT_ANTHROPIC_MODEL = "claude-sonnet-4-6";

export type ResolveSourceOpts = {
  /**
   * The process env (typically `process.env`). Accepted as a parameter
   * rather than read from `process.env` directly so callers can pass a
   * synthetic env in tests without monkey-patching globals.
   */
  env: NodeJS.ProcessEnv;
  /**
   * Skip env resolution entirely and use this inference source. Tests
   * pass the stub source here; the resolver returns it unchanged.
   */
  sourceOverride?: InferenceSource;
  /**
   * Override the default model name. Ignored when `sourceOverride`
   * is supplied (the override's model wins).
   */
  model?: string;
  /**
   * Name of the example, used in the help message ("example-name: ...").
   * Required so the failure message names the binary the user invoked.
   */
  exampleName: string;
};

export type ResolveSourceResult =
  | {
      ok: true;
      source: InferenceSource;
    }
  | { ok: false; help: string };

/**
 * Resolve an `InferenceSource` for an example. Order of precedence:
 *
 * 1. `sourceOverride` — returned unchanged.
 * 2. `ANTHROPIC_API_KEY` in env — built into an Anthropic source with
 *    the default base URL and `opts.model ?? DEFAULT_ANTHROPIC_MODEL`.
 *    The `id` is synthesized as `${provider}:${model}`.
 * 3. Neither present — returns `{ ok: false, help }` so the caller can
 *    print the help text and exit non-zero.
 */
export function resolveSource(opts: ResolveSourceOpts): ResolveSourceResult {
  if (opts.sourceOverride !== undefined) {
    return { ok: true, source: opts.sourceOverride };
  }

  const apiKey = opts.env["ANTHROPIC_API_KEY"];
  if (apiKey === undefined || apiKey === "") {
    return { ok: false, help: formatHelp(opts.exampleName) };
  }

  const model = opts.model ?? DEFAULT_ANTHROPIC_MODEL;
  return {
    ok: true,
    source: {
      id: `anthropic:${model}`,
      provider: "anthropic",
      baseURL: DEFAULT_ANTHROPIC_BASE_URL,
      apiKey,
      model,
    },
  };
}

function formatHelp(exampleName: string, detail?: string): string {
  const reason =
    detail ?? "the ANTHROPIC_API_KEY environment variable is not set";
  return [
    `${exampleName}: missing provider configuration (${reason}).`,
    "",
    "This example needs an Anthropic API key to call the model. Set:",
    "",
    "  export ANTHROPIC_API_KEY=sk-...",
    "",
    "then re-run the example. Pass --model on the command line where",
    "the example supports it to override the default model. Tests",
    "bypass this check by passing a sourceOverride directly to",
    "main().",
    "",
  ].join("\n");
}
