import { type } from "arktype";

import { Capability } from "./capabilities";
import { ModelProviderPlugin, PricingRowResponse } from "./catalog";

export const ModelOfferingInfo = type({
  offeringId: type("string").describe(
    "Catalog primary key of the model-provider offering this entry describes.",
  ),
  providerId: "string",
  providerName: type("string").describe(
    "The model-provider's catalog name, as shown to operators.",
  ),
  plugin: ModelProviderPlugin,
  priority: type("number").describe(
    "Source-resolution ordering hint for this offering; lower values are preferred first.",
  ),
  deploymentTags: "string[]",
  capabilities: Capability.array().describe(
    "Curated capability tags this provider advertises for this model.",
  ),
  pricing: PricingRowResponse.array().describe(
    "The active price per currency for this offering: for each currency, the latest pricing row in effect at the time of the discovery request.",
  ),
});
export type ModelOfferingInfo = typeof ModelOfferingInfo.infer;

export const ModelInfo = type({
  id: "string",
  canonicalName: type("string").describe(
    "The model's tenant-unique canonical name, matched against an agent's model requirements.",
  ),
  "displayName?": "string | null",
  "description?": "string | null",
  offerings: ModelOfferingInfo.array().describe(
    "One entry per provider that offers this model in the tenant's resolved catalog, ordered by resolution priority.",
  ),
});
export type ModelInfo = typeof ModelInfo.infer;
