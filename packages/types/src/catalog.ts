import { type } from "arktype";

export const capabilities = [
  "vision",
  "audio-input",
  "tool-use",
  "extended-thinking",
  "structured-output",
  "long-context",
  "prompt-caching",
] as const;
export type Capability = (typeof capabilities)[number];
export const Capability = type
  .enumerated(...capabilities)
  .describe(
    "A curated platform capability tag. `long-context` denotes models advertising a context window of at least ~200k tokens; the threshold is a curation criterion, not a stored limit.",
  );

export const modelProviderPlugins = [
  "anthropic",
  "openai",
  "openai-compatible",
  "google-genai",
] as const;
export type ModelProviderPlugin = (typeof modelProviderPlugins)[number];
export const ModelProviderPlugin = type
  .enumerated(...modelProviderPlugins)
  .describe(
    "The inference adapter that serves this provider's models, dispatched by the runtime provider registry.",
  );

export const ModelResponse = type({
  id: "string",
  tenantId: "string",
  canonicalName: "string",
  "displayName?": "string | null",
  "description?": "string | null",
  disabled: "boolean",
  createdAt: "string",
  updatedAt: "string",
});

export const ModelProviderResponse = type({
  id: "string",
  tenantId: "string",
  name: "string",
  plugin: ModelProviderPlugin,
  baseURL: "string",
  // Exactly one of these is set (enforced at the database). They are opaque
  // references to a credential or wallet row, not secret material.
  "credentialId?": "string | null",
  "walletId?": "string | null",
  disabled: "boolean",
  createdAt: "string",
  updatedAt: "string",
});

export const ModelOfferingResponse = type({
  id: "string",
  tenantId: "string",
  modelId: "string",
  providerId: "string",
  priority: type("number").describe(
    "Ordering hint for source resolution; lower values are preferred first.",
  ),
  deploymentTags: "string[]",
  capabilities: Capability.array().describe(
    "Curated capability tags this provider advertises for this model.",
  ),
  disabled: "boolean",
  createdAt: "string",
  updatedAt: "string",
});

const priceDescription = (axis: string): string =>
  `${axis} as a decimal string in the row's \`currency\`, or null if this provider does not charge for it.`;

export const PricingRowResponse = type({
  id: "string",
  tenantId: "string",
  offeringId: "string",
  currency: type("string").describe(
    "Fiat currency code or opaque credit unit this row prices in.",
  ),
  "inputTokenPrice?": type("string | null").describe(
    priceDescription("Cost per input token"),
  ),
  "outputTokenPrice?": type("string | null").describe(
    priceDescription("Cost per output token"),
  ),
  "cacheReadTokenPrice?": type("string | null").describe(
    priceDescription("Cost per cached-read token"),
  ),
  "cacheWriteTokenPrice?": type("string | null").describe(
    priceDescription("Cost per cached-write token"),
  ),
  "thinkingTokenPrice?": type("string | null").describe(
    priceDescription("Cost per thinking token"),
  ),
  "perRequestFee?": type("string | null").describe(
    priceDescription("Flat fee per request"),
  ),
  "perImageFee?": type("string | null").describe(
    priceDescription("Fee per image"),
  ),
  "perAudioFee?": type("string | null").describe(
    priceDescription("Fee per audio unit"),
  ),
  effectiveFrom: type("string").describe(
    "ISO-8601 timestamp from which this price applies. Cost attribution at a past time uses the latest row whose effectiveFrom is at or before that time.",
  ),
  createdAt: "string",
});
