import type { ProviderPlugin } from "@intx/inference-discovery";
import { buildAuthHeaders } from "../protocol/auth";
import { createResponsesIterator } from "../protocol/responses-iterator";

const PROVIDER_NAME = "openai-responses";

// First-party OpenAI Responses endpoint. Like the first-party Chat
// Completions deployment, there is no configurable base URL: the deployment
// reads only OPENAI_API_KEY and always talks to api.openai.com.
const OPENAI_BASE_URL = "https://api.openai.com/v1";

// Reasoning-capable first-party models on the Responses wire. The gpt-4
// family is excluded pending verification that those models serve
// `/responses`; advertising a model here only routes captures to it, while
// the support matrix stays the authority on what each model was proven to
// do.
const OPENAI_RESPONSES_MODELS: readonly string[] = [
  "gpt-6-astra",
  "gpt-6.1-sol",
  "gpt-6.1-terra",
  "gpt-6-luna",
  "gpt-6-sol",
  "gpt-5.6-sol",
  "gpt-5.6-terra",
  "gpt-5.6-luna",
  "gpt-5.5",
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
];

const REDACT_REQUEST_HEADERS: readonly string[] = ["authorization"];
const REDACT_RESPONSE_HEADERS: readonly string[] = [
  "set-cookie",
  "x-request-id",
  "openai-organization",
  "openai-project",
];

export interface CreateOpenAIResponsesPluginOpts {
  apiKey: string;
}

export function createOpenAIResponsesPlugin(
  opts: CreateOpenAIResponsesPluginOpts,
): ProviderPlugin {
  const { apiKey } = opts;
  return {
    name: PROVIDER_NAME,
    models: OPENAI_RESPONSES_MODELS,
    redactRequestHeaders: REDACT_REQUEST_HEADERS,
    redactResponseHeaders: REDACT_RESPONSE_HEADERS,
    buildAuthHeaders: () => buildAuthHeaders(apiKey),
    iterateCaptureSteps: createResponsesIterator(OPENAI_BASE_URL),
  };
}
