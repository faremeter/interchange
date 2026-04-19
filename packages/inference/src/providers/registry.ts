import type { ProviderAdapter } from "../adapter";
import { createAnthropicAdapter } from "./anthropic";
import { createOpenAIAdapter } from "./openai";

const registry = new Map<string, ProviderAdapter>();

registry.set("anthropic", createAnthropicAdapter());
registry.set("openai", createOpenAIAdapter());
registry.set("openai-compatible", createOpenAIAdapter());

export function registerProvider(id: string, adapter: ProviderAdapter): void {
  registry.set(id, adapter);
}

export function hasProvider(id: string): boolean {
  return registry.has(id);
}

export function lookupProvider(id: string): ProviderAdapter {
  const adapter = registry.get(id);
  if (adapter === undefined) {
    throw new Error(`Unknown inference provider: ${id}`);
  }
  return adapter;
}
