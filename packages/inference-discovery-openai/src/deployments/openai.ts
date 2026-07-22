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

// No reasoning-trace extractor is wired: the OpenAI Chat Completions reasoning
// path is unverified until a live capture confirms whether a reasoning field
// appears on the response. If one does, the shared extractor belongs in
// protocol/, consumed by both this deployment and OpenCode Zen — not reached
// across from the sibling deployment.
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
