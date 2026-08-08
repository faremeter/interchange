import type { ProviderPlugin } from "@intx/inference-discovery";
import { buildAuthHeaders } from "../protocol/auth";
import { createOpenaiIterator } from "../protocol/iterator";

const PROVIDER_NAME = "openai";

// First-party OpenAI Chat Completions endpoint. Unlike the OpenCode Zen relay,
// there is no configurable base URL: the deployment reads only OPENAI_API_KEY
// and always talks to api.openai.com.
const OPENAI_BASE_URL = "https://api.openai.com/v1";

const OPENAI_MODELS: readonly string[] = [
  "gpt-5.5",
  "gpt-5.6-sol",
  "gpt-5.6-terra",
  "gpt-5.6-luna",
  "gpt-5",
  "gpt-5-mini",
  "gpt-5-nano",
  "gpt-5.1",
  "gpt-5.2",
  "gpt-5.4",
  "gpt-5.4-mini",
  "gpt-5.4-nano",
  "o1",
  "o3",
  "o3-mini",
  "o4-mini",
  "gpt-4",
  "gpt-4-turbo",
  "gpt-4.1",
  "gpt-4.1-mini",
  "gpt-4.1-nano",
  "gpt-4o",
  "gpt-4o-mini",
];

const REDACT_REQUEST_HEADERS: readonly string[] = ["authorization"];
const REDACT_RESPONSE_HEADERS: readonly string[] = [
  "set-cookie",
  "x-request-id",
  "openai-organization",
];

export interface CreateOpenAIPluginOpts {
  apiKey: string;
}

// A live gpt-5.5 capture confirmed that first-party api.openai.com Chat
// Completions responses carry no reasoning or reasoning_content field: OpenAI
// surfaces reasoning only via the Responses API, not this Chat Completions wire.
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
