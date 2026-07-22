import type { ProviderPlugin } from "@intx/inference-discovery";
import { buildAuthHeaders } from "../protocol/auth";
import { createOpenaiIterator } from "../protocol/iterator";

const PROVIDER_NAME = "opencode-zen";

const OPENCODE_ZEN_MODELS: readonly string[] = [
  "kimi-k2.6",
  "kimi-k2.7-code",
  "kimi-k3",
  "glm-5.1",
  "deepseek-v4-pro",
  "qwen3.6-plus",
  "mimo-v2-omni",
];

const REDACT_REQUEST_HEADERS: readonly string[] = ["authorization"];
const REDACT_RESPONSE_HEADERS: readonly string[] = [
  "set-cookie",
  "x-request-id",
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function lookupPath(
  value: unknown,
  path: readonly (string | number)[],
): unknown {
  let cursor: unknown = value;
  for (const segment of path) {
    if (cursor === null || cursor === undefined) return undefined;
    if (typeof segment === "number") {
      if (!Array.isArray(cursor)) return undefined;
      cursor = cursor[segment];
    } else {
      if (!isRecord(cursor)) return undefined;
      cursor = cursor[segment];
    }
  }
  return cursor;
}

function isNonEmpty(value: unknown): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === "string") return value.length > 0;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value).length > 0;
  return true;
}

export interface ReasoningTrace {
  fieldPath: string;
  sample: unknown;
}

// kimi-k2.6 silently routes between two upstream backends that emit
// reasoning under different field paths. Recording which path a given
// capture hit is the cheapest way to detect routing changes later.
const REASONING_FIELD_PATHS: readonly (readonly (string | number)[])[] = [
  ["choices", 0, "message", "reasoning_content"],
  ["choices", 0, "message", "reasoning"],
  ["choices", 0, "message", "reasoning_details"],
];

export function extractReasoningTrace(parsed: unknown): ReasoningTrace | null {
  for (const path of REASONING_FIELD_PATHS) {
    const value = lookupPath(parsed, path);
    if (isNonEmpty(value)) {
      return { fieldPath: path.join("."), sample: value };
    }
  }
  return null;
}

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
    extractReasoningTrace,
    iterateCaptureSteps: createOpenaiIterator(baseUrl),
  };
}
