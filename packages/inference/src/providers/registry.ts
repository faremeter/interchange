import type { LastCycleSource } from "@intx/types/runtime";
import type { ProviderAdapter } from "../adapter";
import { createAnthropicAdapter } from "./anthropic";
import { createGoogleGenAIAdapter } from "./google-genai";
import { createOpenAIAdapter } from "./openai";

type AdapterFactory = (source: LastCycleSource) => ProviderAdapter;

const registry = new Map<string, AdapterFactory>();

registry.set("anthropic", createAnthropicAdapter);
registry.set("openai", createOpenAIAdapter);
registry.set("openai-compatible", createOpenAIAdapter);
registry.set("google-genai", createGoogleGenAIAdapter);

export function registerProvider(id: string, factory: AdapterFactory): void {
  registry.set(id, factory);
}

export function hasProvider(id: string): boolean {
  return registry.has(id);
}

export function lookupProvider(
  id: string,
  source: LastCycleSource,
): ProviderAdapter {
  const factory = registry.get(id);
  if (factory === undefined) {
    throw new Error(`Unknown inference provider: ${id}`);
  }
  return factory(source);
}
