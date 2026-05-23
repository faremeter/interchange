#!/usr/bin/env bun
/* eslint-disable no-console */

import { resolve } from "node:path";
import {
  INTENTS,
  SUPPORT_MATRIX,
  getFixtureDir,
  type SupportEntry,
} from "@intx/inference-discovery/catalog";
import {
  assertNotCI,
  parseCLI,
  requireEnvSet,
  runCapture,
  type ParsedCLIRun,
  type ProviderPlugin,
} from "@intx/inference-discovery";
import { createAnthropicPlugin } from "@intx/inference-discovery-anthropic";
import { createGoogleGenaiPlugin } from "@intx/inference-discovery-google-genai";
import { createOpencodeZenPlugin } from "@intx/inference-discovery-openai";

const ROOT = resolve(import.meta.dirname, "..");

interface RegisteredPlugin {
  name: string;
  requiredEnv: readonly string[];
  create(env: Record<string, string>): ProviderPlugin;
}

function anthropicCreate(env: Record<string, string>): ProviderPlugin {
  const apiKey = env.ANTHROPIC_API_KEY;
  if (apiKey === undefined) {
    throw new Error("ANTHROPIC_API_KEY missing from validated env");
  }
  return createAnthropicPlugin({ apiKey });
}

function googleGenaiCreate(env: Record<string, string>): ProviderPlugin {
  const apiKey = env.GOOGLE_API_KEY;
  if (apiKey === undefined) {
    throw new Error("GOOGLE_API_KEY missing from validated env");
  }
  return createGoogleGenaiPlugin({ apiKey });
}

function opencodeZenCreate(env: Record<string, string>): ProviderPlugin {
  const apiKey = env.OPENAI_API_KEY;
  const baseUrl = env.OPENAI_BASE_URL;
  if (apiKey === undefined || baseUrl === undefined) {
    throw new Error(
      "OPENAI_API_KEY or OPENAI_BASE_URL missing from validated env",
    );
  }
  return createOpencodeZenPlugin({ apiKey, baseUrl });
}

const PLUGIN_REGISTRY: readonly RegisteredPlugin[] = [
  {
    name: "anthropic",
    requiredEnv: ["ANTHROPIC_API_KEY"],
    create: anthropicCreate,
  },
  {
    name: "google-genai",
    requiredEnv: ["GOOGLE_API_KEY"],
    create: googleGenaiCreate,
  },
  {
    name: "opencode-zen",
    requiredEnv: ["OPENAI_API_KEY", "OPENAI_BASE_URL"],
    create: opencodeZenCreate,
  },
];

function findPlugin(name: string): RegisteredPlugin | undefined {
  return PLUGIN_REGISTRY.find((entry) => entry.name === name);
}

function buildHelpText(): string {
  const providers = PLUGIN_REGISTRY.map((entry) => {
    const envList = entry.requiredEnv.join(", ");
    return `  ${entry.name}\n    requires env: ${envList}`;
  }).join("\n");

  return `Usage: bun bin/discover.ts --provider <name> [--all | --only <capability>] [--model <name>] [-h]

Captures live inference responses from a provider plug-in and writes
fixture bundles under packages/inference-testing/wire/<provider>/<model>/<capability>/.

Options:
  --provider <name>     Required. Selects the provider plug-in to invoke.
  --model <name>        Restrict to this model. Repeatable.
  --only <capability>   Restrict to this capability. Repeatable.
  --all                 Run every supported model x capability combination
                        for the chosen provider. Mutually exclusive with
                        --model and --only.
  --help, -h            Show this message.

Available providers:
${providers}

CI guard:
  Discovery makes real, paid network calls and must never run in CI.
  If the CI environment variable is set (to any non-empty value), the
  command aborts before any plug-in is constructed.

Regeneration prerequisites:
  - A funded billing account with the upstream provider.
  - The required environment variables for the chosen provider, exported
    in the shell that invokes this command.
  - Network access to the provider's API endpoint.
  - Awareness that each invocation incurs per-request usage charges; an
    --all run touches every (model, capability) pair in the support matrix
    for the selected provider.
`;
}

function describeSelection(entries: readonly SupportEntry[]): string {
  return `${entries.length} (model, capability) pair${entries.length === 1 ? "" : "s"}`;
}

function selectEntries(parsed: ParsedCLIRun): SupportEntry[] {
  const modelSet = new Set(parsed.models);
  const capabilitySet = new Set(parsed.capabilities);
  return SUPPORT_MATRIX.filter((entry) => {
    if (entry.provider !== parsed.provider) return false;
    // captured and misled rows both have fixtures on disk and should
    // be exercised by re-runs: captured to refresh, misled to retry
    // (the documented behavior may have started materializing on the
    // provider side since the last capture).
    if (entry.outcome !== "captured" && entry.outcome !== "misled") {
      return false;
    }
    if (parsed.all) return true;
    if (modelSet.size > 0 && !modelSet.has(entry.model)) return false;
    if (capabilitySet.size > 0 && !capabilitySet.has(entry.capability))
      return false;
    return true;
  });
}

async function main(): Promise<number> {
  const parsed = parseCLI(process.argv.slice(2));

  if (parsed.kind === "help") {
    console.log(buildHelpText());
    return 0;
  }

  if (parsed.kind === "error") {
    console.error(`error: ${parsed.message}`);
    console.error("");
    console.error("Run with --help for usage.");
    return 1;
  }

  assertNotCI();

  const registered = findPlugin(parsed.provider);
  if (registered === undefined) {
    const known = PLUGIN_REGISTRY.map((entry) => entry.name).join(", ");
    console.error(`error: unknown provider '${parsed.provider}'`);
    console.error(`available providers: ${known}`);
    return 1;
  }

  const env = requireEnvSet(registered.requiredEnv);
  const plugin = registered.create(env);

  const entries = selectEntries(parsed);
  if (entries.length === 0) {
    console.error(
      `error: no captured entries in SUPPORT_MATRIX match the selection for provider '${parsed.provider}'`,
    );
    return 1;
  }

  console.error(
    `[discover] provider=${parsed.provider} selection=${describeSelection(entries)}`,
  );

  for (const entry of entries) {
    const intent = INTENTS[entry.capability];
    const relDir = getFixtureDir(entry);
    if (relDir === null) {
      throw new Error(
        `getFixtureDir returned null for captured entry ${entry.provider}/${entry.model}/${entry.capability}`,
      );
    }
    const outDir = resolve(ROOT, relDir);
    console.error(
      `[discover] start  model=${entry.model} capability=${entry.capability}`,
    );
    await runCapture({
      plugin,
      model: entry.model,
      capability: entry.capability,
      intent,
      outDir,
    });
    console.error(
      `[discover] done   model=${entry.model} capability=${entry.capability}`,
    );
  }

  return 0;
}

const exitCode = await main();
process.exit(exitCode);
