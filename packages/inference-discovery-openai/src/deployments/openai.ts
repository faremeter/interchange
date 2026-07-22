import type { ProviderPlugin } from "@intx/inference-discovery";
import { buildAuthHeaders } from "../protocol/auth";
import { createOpenaiIterator } from "../protocol/iterator";

const PROVIDER_NAME = "openai";

// First-party OpenAI Chat Completions endpoint. Unlike the OpenCode Zen relay,
// there is no configurable base URL: the deployment reads only OPENAI_API_KEY
// and always talks to api.openai.com.
const OPENAI_BASE_URL = "https://api.openai.com/v1";

const OPENAI_MODELS: readonly string[] = ["gpt-5.5"];

const REDACT_REQUEST_HEADERS: readonly string[] = ["authorization"];
const REDACT_RESPONSE_HEADERS: readonly string[] = [
  "set-cookie",
  "x-request-id",
  "openai-organization",
];

export interface CreateOpenAIPluginOpts {
  apiKey: string;
}

// No reasoning-trace extractor is wired: a live gpt-5.5 capture confirmed that
// first-party api.openai.com Chat Completions responses carry no reasoning or
// reasoning_content field (OpenAI surfaces reasoning only via the Responses
// API). If a future first-party model exposes one on this wire, the shared
// extractor belongs in protocol/, consumed by both this deployment and
// OpenCode Zen — not reached across from the sibling deployment.
export function createOpenAIPlugin(
  opts: CreateOpenAIPluginOpts,
): ProviderPlugin {
  const { apiKey } = opts;
  return {
    name: PROVIDER_NAME,
    models: OPENAI_MODELS,
    redactRequestHeaders: REDACT_REQUEST_HEADERS,
    redactResponseHeaders: REDACT_RESPONSE_HEADERS,
    buildAuthHeaders: () => buildAuthHeaders(apiKey),
    iterateCaptureSteps: createOpenaiIterator(OPENAI_BASE_URL),
  };
}
