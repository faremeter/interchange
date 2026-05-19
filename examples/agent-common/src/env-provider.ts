// Environment-driven `ProviderConfig` resolution for the agent-* examples.
//
// Production users invoke each example with `ANTHROPIC_API_KEY` set;
// `resolveProvider` reads that variable, builds an Anthropic provider
// config, and returns it alongside the chosen model. Tests bypass the
// env entirely by supplying `providerOverride` — the helper short-
// circuits and returns the override unchanged.
//
// When the env is incomplete and no override was supplied, the helper
// returns `{ ok: false, help }` carrying a multi-line message the
// caller writes to stderr before exiting non-zero. This is gentler
// than throwing: first-time users skim past stack traces, but they
// will read a 5-line "set these variables to run me" block.

import type { ProviderConfig } from "@intx/types/runtime";

export const DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com";
export const DEFAULT_ANTHROPIC_MODEL = "claude-3-5-sonnet-20241022";

export type ResolveProviderOpts = {
  /**
   * The process env (typically `process.env`). Accepted as a parameter
   * rather than read from `process.env` directly so callers can pass a
   * synthetic env in tests without monkey-patching globals.
   */
  env: NodeJS.ProcessEnv;
  /**
   * Skip env resolution entirely and use this provider config. Tests
   * pass the stub provider here; the resolver returns it unchanged.
   */
  providerOverride?: ProviderConfig;
  /**
   * Override the default model name. Ignored when `providerOverride`
   * is supplied (the override's model wins).
   */
  model?: string;
  /**
   * Name of the example, used in the help message ("example-name: ...").
   * Required so the failure message names the binary the user invoked.
   */
  exampleName: string;
};

export type ResolveProviderResult =
  | {
      ok: true;
      provider: ProviderConfig;
      /**
       * Guaranteed-non-undefined copy of `provider.model`. Examples pass
       * this directly into `createAgent({ defaultModel })` without
       * having to re-check the optional field.
       */
      model: string;
    }
  | { ok: false; help: string };

/**
 * Resolve a `ProviderConfig` for an example. Order of precedence:
 *
 * 1. `providerOverride` — returned unchanged. Its `model` field must
 *    be set; the resolver returns `{ ok: false, help }` otherwise so
 *    the failure mode matches the env-missing case.
 * 2. `ANTHROPIC_API_KEY` in env — built into an Anthropic config with
 *    the default base URL and `opts.model ?? DEFAULT_ANTHROPIC_MODEL`.
 * 3. Neither present — returns `{ ok: false, help }` so the caller can
 *    print the help text and exit non-zero.
 */
export function resolveProvider(
  opts: ResolveProviderOpts,
): ResolveProviderResult {
  if (opts.providerOverride !== undefined) {
    const override = opts.providerOverride;
    if (override.model === undefined || override.model === "") {
      return {
        ok: false,
        help: formatHelp(
          opts.exampleName,
          "providerOverride was supplied but its `model` field is unset",
        ),
      };
    }
    return { ok: true, provider: override, model: override.model };
  }

  const apiKey = opts.env["ANTHROPIC_API_KEY"];
  if (apiKey === undefined || apiKey === "") {
    return { ok: false, help: formatHelp(opts.exampleName) };
  }

  const model = opts.model ?? DEFAULT_ANTHROPIC_MODEL;
  return {
    ok: true,
    provider: {
      provider: "anthropic",
      baseURL: DEFAULT_ANTHROPIC_BASE_URL,
      apiKey,
      model,
    },
    model,
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
    "bypass this check by passing a providerOverride directly to",
    "main().",
    "",
  ].join("\n");
}
