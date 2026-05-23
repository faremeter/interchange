import { type } from "arktype";
import { Capability } from "./capability";

export const SupportEntry = type({
  provider: "string",
  model: "string",
  capability: Capability,
  outcome: "'captured' | 'refused' | 'http-error' | 'unsupported'",
  "notes?": "string",
});
export type SupportEntry = typeof SupportEntry.infer;

const GEMINI_PROVIDER = "google-genai";
const GEMINI_TEXT_MODEL = "gemini-2.5-flash";
const GEMINI_IMAGE_MODEL = "gemini-2.5-flash-image";

const OPENCODE_PROVIDER = "opencode-zen";

const GEMINI_TEXT_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling-multi-turn",
  "function-calling-multi-turn-streaming",
  "function-calling-with-thinking",
  "function-calling-with-thinking-streaming",
  "vision-input",
  "vision-input-streaming",
  "audio-input",
  "audio-input-streaming",
  "video-input",
  "video-input-streaming",
  "document-input",
  "document-input-streaming",
  "code-execution",
  "code-execution-streaming",
  "grounding",
  "grounding-streaming",
  "files-api-reference",
  "files-api-reference-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const GEMINI_IMAGE_CAPABILITIES = [
  "image-output",
  "image-output-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const OPENCODE_FULL_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
  "reasoning-content",
  "reasoning-content-streaming",
  "vision-input",
] as const satisfies readonly SupportEntry["capability"][];

const OPENCODE_NON_VISION_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
  "reasoning-content",
  "reasoning-content-streaming",
] as const satisfies readonly SupportEntry["capability"][];

function gemini(
  model: string,
  capabilities: readonly SupportEntry["capability"][],
): SupportEntry[] {
  return capabilities.map((capability) => ({
    provider: GEMINI_PROVIDER,
    model,
    capability,
    outcome: "captured",
  }));
}

function opencode(
  model: string,
  capabilities: readonly SupportEntry["capability"][],
): SupportEntry[] {
  return capabilities.map((capability) => ({
    provider: OPENCODE_PROVIDER,
    model,
    capability,
    outcome: "captured",
  }));
}

const MATRIX: SupportEntry[] = [
  ...gemini(GEMINI_TEXT_MODEL, GEMINI_TEXT_CAPABILITIES),
  ...gemini(GEMINI_IMAGE_MODEL, GEMINI_IMAGE_CAPABILITIES),
  ...opencode("kimi-k2.6", OPENCODE_FULL_CAPABILITIES),
  ...opencode("mimo-v2-omni", OPENCODE_FULL_CAPABILITIES),
  ...opencode("qwen3.6-plus", OPENCODE_FULL_CAPABILITIES),
  ...opencode("glm-5.1", OPENCODE_NON_VISION_CAPABILITIES),
  ...opencode("deepseek-v4-pro", OPENCODE_NON_VISION_CAPABILITIES),
  {
    provider: OPENCODE_PROVIDER,
    model: "glm-5.1",
    capability: "vision-input",
    outcome: "refused",
    notes:
      "Probe returned HTTP 200 with the textual refusal 'Please provide an image so I can describe it for you' rather than a real image description; recorded as 'refused' here so no capture is attempted.",
  },
  {
    provider: OPENCODE_PROVIDER,
    model: "deepseek-v4-pro",
    capability: "vision-input",
    outcome: "http-error",
    notes:
      "OpenAI-style multimodal messages[].content elicits HTTP 400 invalid_request_error \"unknown variant 'image_url', expected 'text'\"; recorded as 'http-error' here so no capture is attempted.",
  },
];

export const SUPPORT_MATRIX: readonly SupportEntry[] = MATRIX;

const FIXTURE_ROOT = "packages/inference-testing/wire";

export function getFixtureDir(entry: SupportEntry): string | null {
  if (entry.outcome !== "captured") return null;
  return `${FIXTURE_ROOT}/${entry.provider}/${entry.model}/${entry.capability}`;
}
