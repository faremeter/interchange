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

export const providerPreferenceModes = ["pin", "prefer"] as const;
export type ProviderPreferenceMode = (typeof providerPreferenceModes)[number];

export const ProviderPreference = type({
  mode: type
    .enumerated(...providerPreferenceModes)
    .describe(
      "`pin` restricts resolution to the listed providers and fails over only among them; `prefer` orders the listed providers first but keeps the rest of the tenant's providers as fallback.",
    ),
  order: type("string[]").describe(
    "Model-provider names in preferred order, most preferred first.",
  ),
});
export type ProviderPreference = typeof ProviderPreference.infer;

export const ModelRequirement = type({
  model: type("string").describe(
    "Canonical model name the agent requires for inference.",
  ),
  "capabilities?": Capability.array().describe(
    "An offering must advertise every one of these capabilities to be eligible to serve this requirement.",
  ),
  "providers?": ProviderPreference.describe(
    "The definition author's provider preference for this model. Resolution applies it over the tenant-visible providers; it cannot introduce a provider the tenant catalog does not contain.",
  ),
});
export type ModelRequirement = typeof ModelRequirement.infer;

// A definition declares at most one requirement per canonical model: two
// requirements for the same model would resolve the same offering twice and
// produce duplicate inference-source ids. Reject the ambiguity here, at the
// boundary, rather than letting it surface deep in source resolution.
export const ModelRequirements = ModelRequirement.array().narrow(
  (reqs, ctx) => {
    const seen = new Set<string>();
    for (const req of reqs) {
      if (seen.has(req.model)) {
        return ctx.mustBe(
          `an array with no duplicate model requirements; "${req.model}" appears more than once`,
        );
      }
      seen.add(req.model);
    }
    return true;
  },
);
export type ModelRequirements = typeof ModelRequirements.infer;

export const InvokerModelPreference = type({
  model: type("string").describe(
    "Canonical model name this launch-time preference applies to.",
  ),
  providers: ProviderPreference,
});
export type InvokerModelPreference = typeof InvokerModelPreference.infer;

export const InvokerModelPreferences = InvokerModelPreference.array();
export type InvokerModelPreferences = typeof InvokerModelPreferences.infer;

export const CreateModel = type({
  canonicalName: type("string").describe(
    "Tenant-unique canonical model name agents match their requirements against.",
  ),
  "displayName?": "string | null",
  "description?": "string | null",
});

export const UpdateModel = type({
  "displayName?": "string | null",
  "description?": "string | null",
  "disabled?": "boolean",
});

export const CreateModelProvider = type({
  name: type("string").describe("Tenant-unique model-provider name."),
  plugin: ModelProviderPlugin,
  baseURL: "string",
  // Exactly one of these must be set; the route rejects a body that sets
  // both or neither before touching the database.
  "credentialId?": "string | null",
  "walletId?": "string | null",
});

export const UpdateModelProvider = type({
  "name?": "string",
  "baseURL?": "string",
  "disabled?": "boolean",
});

export const CreateModelOffering = type({
  modelId: type("string").describe(
    "Catalog id of a model owned by this tenant.",
  ),
  providerId: type("string").describe(
    "Catalog id of a model-provider owned by this tenant.",
  ),
  "priority?": type("number").describe(
    "Ordering hint for source resolution; lower values are preferred first. Defaults to 0.",
  ),
  "deploymentTags?": "string[]",
  "capabilities?": Capability.array(),
  "quirks?": type("Record<string, unknown>").describe(
    "Opaque per-deployment adapter accommodations for this offering; the adapter factory validates the provider-specific shape. Omit when the deployment needs none.",
  ),
});

export const UpdateModelOffering = type({
  "priority?": "number",
  "deploymentTags?": "string[]",
  "capabilities?": Capability.array(),
  "quirks?": type("Record<string, unknown> | null").describe(
    "Replacement quirks bag, or null to clear it back to the adapter's default behavior. Omit to leave unchanged.",
  ),
  "disabled?": "boolean",
});

const createPriceDescription = (axis: string): string =>
  `${axis} as a decimal string in this row's \`currency\`, or null if this provider does not charge for it.`;

export const CreatePricingRow = type({
  currency: type("string").describe(
    "Fiat currency code or opaque credit unit this row prices in.",
  ),
  "effectiveFrom?": type("string").describe(
    "ISO-8601 timestamp from which this price applies. Defaults to the time of the request.",
  ),
  "inputTokenPrice?": type("string | null").describe(
    createPriceDescription("Cost per input token"),
  ),
  "outputTokenPrice?": type("string | null").describe(
    createPriceDescription("Cost per output token"),
  ),
  "cacheReadTokenPrice?": type("string | null").describe(
    createPriceDescription("Cost per cached-read token"),
  ),
  "cacheWriteTokenPrice?": type("string | null").describe(
    createPriceDescription("Cost per cached-write token"),
  ),
  "thinkingTokenPrice?": type("string | null").describe(
    createPriceDescription("Cost per thinking token"),
  ),
  "perRequestFee?": type("string | null").describe(
    createPriceDescription("Flat fee per request"),
  ),
  "perImageFee?": type("string | null").describe(
    createPriceDescription("Fee per image"),
  ),
  "perAudioFee?": type("string | null").describe(
    createPriceDescription("Fee per audio unit"),
  ),
});

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
  quirks: type("Record<string, unknown> | null").describe(
    "Opaque per-deployment adapter accommodations, or null when the deployment needs none.",
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
