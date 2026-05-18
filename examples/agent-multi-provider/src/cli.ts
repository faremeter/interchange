// agent-multi-provider: combine three flavors of routing policy on
// top of @interchange/agent's `providers` + `setProvider` surface:
//
//   * model-per-task — route each prompt to a "cheap" or "smart"
//     model based on a per-prompt heuristic before each send.
//   * failover       — if the primary provider's call rejects, swap
//                      to a fallback provider and retry once.
//   * cost-routing   — the model-per-task heuristic doubles as a
//                      cost-routing knob; longer prompts go to the
//                      smarter (more expensive) model, shorter ones
//                      go to the cheap one.
//
// The agent package owns the `providers` registry and exposes
// `setProvider` to rotate credentials/model in place. Everything in
// this file lives in user-land: the agent surface stays uncluttered.

import {
  openExampleAgent,
  optional,
  resolveProvider,
  resolveStdio,
  type CommonMainOptions,
} from "@interchange/example-agent-common";
import type { ProviderConfig } from "@interchange/types/runtime";

import {
  routeProvider,
  withFailover,
  type ModelTier,
  type ProviderEntry,
} from "./policy";

const EXAMPLE_NAME = "agent-multi-provider";

export const DEFAULT_CHEAP_MODEL = "claude-3-5-haiku-20241022";
export const DEFAULT_SMART_MODEL = "claude-3-5-sonnet-20241022";

export type MainOptions = CommonMainOptions & {
  /** Override the primary provider (tests use this). */
  primaryOverride?: ProviderConfig;
  /** Override the fallback provider (tests use this). */
  fallbackOverride?: ProviderConfig;
  /** Replace the default cheap/smart model identifiers. */
  models?: { cheap: string; smart: string };
};

/**
 * Resolve one of the two providers this example wires. Each call
 * routes through the same `resolveProvider` helper as the other
 * examples but with its own label and override so the help text
 * names the right role when configuration is missing.
 */
function resolveRole(
  env: NodeJS.ProcessEnv,
  role: "primary" | "fallback",
  override: ProviderConfig | undefined,
  stderr: (chunk: string) => void,
): { provider: ProviderConfig; model: string } | null {
  const resolved = resolveProvider({
    env,
    exampleName: `${EXAMPLE_NAME} (${role})`,
    ...optional("providerOverride", override),
  });
  if (!resolved.ok) {
    stderr(resolved.help);
    return null;
  }
  return { provider: resolved.provider, model: resolved.model };
}

export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const { stdout, stderr } = resolveStdio(opts);

  if (argv.length === 0) {
    stderr("usage: bun run start <prompt-1> [prompt-2] ...\n");
    return 1;
  }

  const primary = resolveRole(env, "primary", opts.primaryOverride, stderr);
  if (primary === null) return 1;

  // The fallback uses the same env var by default — production
  // callers point it at a different provider/key. The integration
  // test passes `fallbackOverride` explicitly so the swap path is
  // exercised regardless of env state.
  const fallback = resolveRole(env, "fallback", opts.fallbackOverride, stderr);
  if (fallback === null) return 1;

  const models = opts.models ?? {
    cheap: DEFAULT_CHEAP_MODEL,
    smart: DEFAULT_SMART_MODEL,
  };

  const primaryEntry: ProviderEntry = {
    name: "primary",
    config: primary.provider,
  };
  const fallbackEntry: ProviderEntry = {
    name: "fallback",
    config: fallback.provider,
  };

  // The agent's `providers` array is the union; `defaultModel` is
  // overwritten by setProvider() before each send. The reactor reads
  // the active provider lazily at start-of-inference, so swapping
  // takes effect on the next send().
  const agent = await openExampleAgent(opts, {
    exampleName: EXAMPLE_NAME,
    systemPrompt:
      "You are a routing-aware assistant. Reply concisely; never mention the model name.",
    tools: [],
    providers: [primary.provider, fallback.provider],
    defaultModel: primary.model,
  });
  try {
    for (const prompt of argv) {
      const routed = routeProvider({
        prompt,
        primary: primaryEntry,
        models,
      });
      stdout(`> ${prompt}\n`);
      stdout(`  routed to: tier=${routed.tier} model=${routed.model}\n`);

      // Apply the routed model on top of the primary provider's
      // credentials, then run the send with failover. The fallback
      // entry's config is left as-is from resolveProvider (it
      // already carries its own model). Production callers would
      // typically overlay the same model on both for parity, but
      // the example leaves the choice to the caller.
      const primaryWithModel: ProviderEntry = {
        name: primaryEntry.name,
        config: routed.provider,
      };

      const { served, attempts, primaryError } = await withFailover({
        primary: primaryWithModel,
        fallback: fallbackEntry,
        applyProvider: (cfg) => {
          agent.setProvider(cfg);
        },
        invoke: () => agent.send(prompt),
      });
      const attemptNames = attempts.map((a) => a.name).join(" -> ");
      stdout(`  attempts:  ${attemptNames}\n`);
      stdout(`  served by: ${served.name}\n`);
      // When failover engaged the primary's failure must surface so
      // the operator can see what went wrong, even though the request
      // itself ultimately succeeded against the fallback.
      if (primaryError !== undefined) {
        const message =
          primaryError instanceof Error
            ? primaryError.message
            : String(primaryError);
        stderr(`  primary error: ${message}\n`);
      }

      const turns = await agent.history();
      const lastReplyTurn = turns.at(-1);
      const replyRole = lastReplyTurn?.role ?? "?";
      const replyText =
        lastReplyTurn === undefined
          ? "(no reply)"
          : lastReplyTurn.content
              .map((b) => (b.type === "text" ? b.text : ""))
              .join("")
              .trim();
      stdout(`  reply:     ${replyRole}\n`);
      stdout(`  text:      ${replyText}\n\n`);
    }
    return 0;
  } finally {
    await agent.close();
  }
}

export type { ModelTier };

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
