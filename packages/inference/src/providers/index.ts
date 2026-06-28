import { createAdapterRegistry } from "../adapter";
import type { AdapterFactory, AdapterRegistry } from "../adapter";
import { loadAdapterFactories } from "../manifest";
import type { AdapterManifest, ModuleImporter } from "../manifest";
import { createAnthropicAdapter } from "./anthropic";
import { createGoogleGenAIAdapter } from "./google-genai";
import { createOpenAIAdapter } from "./openai";

export { createAnthropicAdapter } from "./anthropic";
export { createGoogleGenAIAdapter } from "./google-genai";
export { createOpenAIAdapter } from "./openai";

function builtinFactories(): Record<string, AdapterFactory> {
  return {
    anthropic: createAnthropicAdapter,
    openai: createOpenAIAdapter,
    "openai-compatible": createOpenAIAdapter,
    "google-genai": createGoogleGenAIAdapter,
  };
}

/**
 * Builds a registry of the adapters this package ships with, statically linked
 * and resolved synchronously. The per-call factory invariant (a fresh adapter
 * minted on every `resolve`) lives in {@link createAdapterRegistry}.
 *
 * @returns A registry resolving the built-in providers
 */
export function createBuiltinRegistry(): AdapterRegistry {
  return createAdapterRegistry(builtinFactories());
}

/**
 * Builds a registry of the built-in adapters merged with custom adapters loaded
 * from an operator-configured manifest. Custom adapters override built-ins
 * sharing a provider key. With an empty manifest this returns just the
 * built-ins. The per-call factory invariant lives in
 * {@link createAdapterRegistry}.
 *
 * @param manifest - Validated custom adapter manifest entries
 * @param opts - Optional injected module importer
 * @returns A registry resolving built-in and custom providers
 */
export async function loadAdapterRegistry(
  manifest: AdapterManifest,
  opts?: { import?: ModuleImporter },
): Promise<AdapterRegistry> {
  return createAdapterRegistry({
    ...builtinFactories(),
    ...(await loadAdapterFactories(manifest, opts)),
  });
}
