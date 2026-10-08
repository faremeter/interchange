import { createAdapterRegistry } from "../adapter";
import type { AdapterFactory, AdapterRegistry } from "../adapter";
import { createDependencies, type Dependencies } from "../harness";
import { loadAdapterFactories } from "../manifest";
import type { AdapterManifest, ModuleImporter } from "../manifest";
import { createAnthropicAdapter } from "./anthropic";
import { createGoogleGenAIAdapter } from "./google-genai";
import { createOpenAIAdapter } from "./openai";

export {
  createAnthropicAdapter,
  AnthropicQuirks,
  ADAPTIVE_THINKING_MODELS,
  ADAPTIVE_THINKING_EFFORT,
} from "./anthropic";
export { createGoogleGenAIAdapter, GoogleGenAIQuirks } from "./google-genai";
export { createOpenAIAdapter, OpenAIQuirks } from "./openai";

function builtinFactories(): Record<string, AdapterFactory> {
  return {
    anthropic: createAnthropicAdapter,
    openai: createOpenAIAdapter,
    "openai-compatible": createOpenAIAdapter,
    "google-genai": createGoogleGenAIAdapter,
  };
}

/** Registry of the adapters this package ships with, statically linked and
 *  resolved synchronously. The per-call factory invariant (a fresh adapter
 *  per `resolve`) lives in {@link createAdapterRegistry}. */
export function createBuiltinRegistry(): AdapterRegistry {
  return createAdapterRegistry(builtinFactories());
}

/**
 * Runtime dependencies wired to the built-in adapters: binds
 * globalThis.fetch and the production scheduler. Hosts with custom adapters
 * build a registry via {@link loadAdapterRegistry} and pass it to
 * {@link createDependencies}.
 */
export function createDefaultDependencies(): Dependencies {
  return createDependencies(createBuiltinRegistry());
}

/**
 * Built-in adapters merged with custom adapters loaded from an
 * operator-configured manifest; custom adapters override built-ins on the
 * same provider key.
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
