// The discover CLI's provider registry: provider name → required env and the
// factory that builds its plug-in. The colocated discover-registry.test.ts
// pins the name→requiredEnv contract and which variables each factory reads,
// so an edit cannot advertise one credential while reading another, or drop a
// required variable. No CLI parsing or capture loop lives here, so the guard
// test can import it without the network machinery in discover.ts.

import type { ProviderPlugin } from "@intx/inference-discovery";
import { createAnthropicPlugin } from "@intx/inference-discovery-anthropic";
import {
  createGoogleGenaiPlugin,
  type GeminiModelClass,
} from "@intx/inference-discovery-google-genai";
import {
  createOpenAIPlugin,
  createOpencodeZenPlugin,
  createXaiPlugin,
} from "@intx/inference-discovery-openai";

// Options for plug-in construction. Only the probe passes modelClass, to
// declare the request shape of a google-genai model absent from the known
// text/image sets.
export interface PluginCreateOptions {
  modelClass?: GeminiModelClass | undefined;
}

export interface RegisteredPlugin {
  name: string;
  requiredEnv: readonly string[];
  create(
    env: Record<string, string>,
    options?: PluginCreateOptions,
  ): ProviderPlugin;
}

function anthropicCreate(env: Record<string, string>): ProviderPlugin {
  const apiKey = env.ANTHROPIC_API_KEY;
  if (apiKey === undefined) {
    throw new Error("ANTHROPIC_API_KEY missing from validated env");
  }
  return createAnthropicPlugin({ apiKey });
}

function googleGenaiCreate(
  env: Record<string, string>,
  options?: PluginCreateOptions,
): ProviderPlugin {
  const apiKey = env.GOOGLE_API_KEY;
  if (apiKey === undefined) {
    throw new Error("GOOGLE_API_KEY missing from validated env");
  }
  return createGoogleGenaiPlugin({ apiKey, modelClass: options?.modelClass });
}

function openaiCreate(env: Record<string, string>): ProviderPlugin {
  const apiKey = env.OPENAI_API_KEY;
  if (apiKey === undefined) {
    throw new Error("OPENAI_API_KEY missing from validated env");
  }
  return createOpenAIPlugin({ apiKey });
}

function opencodeZenCreate(env: Record<string, string>): ProviderPlugin {
  const apiKey = env.OPENCODE_API_KEY;
  const baseUrl = env.OPENCODE_BASE_URL;
  if (apiKey === undefined || baseUrl === undefined) {
    throw new Error(
      "OPENCODE_API_KEY or OPENCODE_BASE_URL missing from validated env",
    );
  }
  return createOpencodeZenPlugin({ apiKey, baseUrl });
}

function xaiCreate(env: Record<string, string>): ProviderPlugin {
  const apiKey = env.XAI_API_KEY;
  if (apiKey === undefined) {
    throw new Error("XAI_API_KEY missing from validated env");
  }
  return createXaiPlugin({ apiKey });
}

export const PLUGIN_REGISTRY: readonly RegisteredPlugin[] = [
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
    requiredEnv: ["OPENCODE_API_KEY", "OPENCODE_BASE_URL"],
    create: opencodeZenCreate,
  },
  {
    name: "openai",
    requiredEnv: ["OPENAI_API_KEY"],
    create: openaiCreate,
  },
  {
    name: "xai",
    requiredEnv: ["XAI_API_KEY"],
    create: xaiCreate,
  },
];

export function findPlugin(name: string): RegisteredPlugin | undefined {
  return PLUGIN_REGISTRY.find((entry) => entry.name === name);
}

// Shared by bin/discover and bin/probe help text, so both advertise providers
// and their required env identically.
export function formatProviderHelp(): string {
  return PLUGIN_REGISTRY.map((entry) => {
    const envList = entry.requiredEnv.join(", ");
    return `  ${entry.name}\n    requires env: ${envList}`;
  }).join("\n");
}
