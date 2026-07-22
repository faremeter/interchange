// The discover CLI's provider registry: the table mapping each provider name to
// the environment it requires and the factory that builds its plug-in.
//
// bin/discover.ts drives these entries — buildHelpText advertises each row's
// requiredEnv, and main() looks a row up by name, validates its env, and calls
// create. The colocated discover-registry.test.ts is the guard. It pins the
// provider to requiredEnv contract and the create factory's read-site wiring
// (which variables each factory actually consumes) so an edit cannot advertise
// one credential while reading another, or drop a required variable.
//
// This module holds only the registry and its plug-in constructors — no CLI
// argument parsing, no capture loop — so the guard test can import it without
// the network machinery in discover.ts.

import type { ProviderPlugin } from "@intx/inference-discovery";
import { createAnthropicPlugin } from "@intx/inference-discovery-anthropic";
import { createGoogleGenaiPlugin } from "@intx/inference-discovery-google-genai";
import {
  createOpenAIPlugin,
  createOpencodeZenPlugin,
} from "@intx/inference-discovery-openai";

export interface RegisteredPlugin {
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
];

export function findPlugin(name: string): RegisteredPlugin | undefined {
  return PLUGIN_REGISTRY.find((entry) => entry.name === name);
}
