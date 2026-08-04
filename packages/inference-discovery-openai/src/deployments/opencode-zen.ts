import type { ProviderPlugin } from "@intx/inference-discovery";
import { buildAuthHeaders } from "../protocol/auth";
import { createOpenaiIterator } from "../protocol/iterator";

const PROVIDER_NAME = "opencode-zen";

const OPENCODE_ZEN_MODELS: readonly string[] = [
  "kimi-k2.6",
  "kimi-k2.7-code",
  "kimi-k3",
  "glm-5.2",
  "deepseek-v4-pro",
  "deepseek-v4-flash",
  "qwen3.7-plus",
  "mimo-v2.5",
  "gpt-5.4-mini",
];

const REDACT_REQUEST_HEADERS: readonly string[] = ["authorization"];
const REDACT_RESPONSE_HEADERS: readonly string[] = [
  "set-cookie",
  "x-request-id",
];

export interface CreateOpencodeZenPluginOpts {
  apiKey: string;
  baseUrl: string;
}

export function createOpencodeZenPlugin(
  opts: CreateOpencodeZenPluginOpts,
): ProviderPlugin {
  const { apiKey, baseUrl } = opts;
  return {
    name: PROVIDER_NAME,
    models: OPENCODE_ZEN_MODELS,
    redactRequestHeaders: REDACT_REQUEST_HEADERS,
    redactResponseHeaders: REDACT_RESPONSE_HEADERS,
    buildAuthHeaders: () => buildAuthHeaders(apiKey),
    iterateCaptureSteps: createOpenaiIterator(baseUrl),
  };
}
