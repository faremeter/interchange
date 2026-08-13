import type { ProviderPlugin } from "@intx/inference-discovery";
import { buildAuthHeaders } from "../protocol/auth";
import { createOpenaiIterator } from "../protocol/iterator";

const PROVIDER_NAME = "xai";

// First-party xAI Chat Completions endpoint. Like the first-party OpenAI
// deployment and unlike the OpenCode Zen relay, there is no configurable base
// URL: the deployment reads only XAI_API_KEY and always talks to api.x.ai. This
// string must stay byte-identical to CATALOG_TO_BASE_URL["xai"] in
// @intx/inference-discovery's provider-adapter, which stamps the manifest's
// source.baseURL; the request URL comes from here and the recorded provenance
// comes from there, and nothing cross-checks them at capture time.
const XAI_BASE_URL = "https://api.x.ai/v1";

const XAI_MODELS: readonly string[] = [
  "grok-4.20-0309-non-reasoning",
  "grok-4.20-0309-reasoning",
  "grok-4.3",
  "grok-4.5",
  "grok-4.6",
  "grok-build-0.1",
];

const REDACT_REQUEST_HEADERS: readonly string[] = ["authorization"];
const REDACT_RESPONSE_HEADERS: readonly string[] = [
  "set-cookie",
  "x-request-id",
];

export interface CreateXaiPluginOpts {
  apiKey: string;
}

export function createXaiPlugin(opts: CreateXaiPluginOpts): ProviderPlugin {
  const { apiKey } = opts;
  return {
    name: PROVIDER_NAME,
    models: XAI_MODELS,
    redactRequestHeaders: REDACT_REQUEST_HEADERS,
    redactResponseHeaders: REDACT_RESPONSE_HEADERS,
    buildAuthHeaders: () => buildAuthHeaders(apiKey),
    iterateCaptureSteps: createOpenaiIterator(XAI_BASE_URL),
  };
}
