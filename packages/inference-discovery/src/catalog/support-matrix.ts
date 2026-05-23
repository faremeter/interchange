import { type } from "arktype";
import { Capability } from "./capability";

// Outcome vocabulary:
//
// - captured: HTTP succeeded and the response contains the wire shape
//   the capability's name implies. Fixture on disk; smoke test validates.
//
// - misled: HTTP succeeded and the model responded normally, but the
//   provider's documented contract for the input did not materialize.
//   The model did not refuse — there is no statement of inability in
//   the response — the documented behavior just did not fire. Used when
//   the wire shape is conditional on external state we do not control
//   (e.g. Anthropic's safety classifier not engaging on the documented
//   redacted-thinking canary). The fixture on disk documents what was
//   actually returned; smoke test validates file presence. A future
//   re-capture may flip the row to captured.
//
// - refused: the provider's response contains an explicit refusal of
//   the requested task. The model told us it would not do the thing —
//   sometimes via HTTP non-2xx, sometimes via a successful HTTP body
//   carrying a textual refusal. No fixture by convention; the refusal
//   detail goes in notes.
//
// - http-error: the provider returned a non-2xx HTTP status. No fixture.
//
// - unsupported: the provider does not support this capability. No
//   fixture, no attempt made.
export const SupportEntry = type({
  provider: "string",
  model: "string",
  capability: Capability,
  outcome: "'captured' | 'misled' | 'refused' | 'http-error' | 'unsupported'",
  "notes?": "string",
});
export type SupportEntry = typeof SupportEntry.infer;

const ANTHROPIC_PROVIDER = "anthropic";
const ANTHROPIC_MODELS = [
  "claude-sonnet-4-5-20250929",
  "claude-opus-4-1-20250805",
  "claude-haiku-4-5-20251001",
] as const;

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

const ANTHROPIC_CAPTURED_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
  "function-calling-multi-turn-streaming",
  "function-calling-with-thinking",
  "function-calling-with-thinking-streaming",
  "vision-input",
  "vision-input-streaming",
  "document-input",
  "document-input-streaming",
  "code-execution",
  "code-execution-streaming",
  "reasoning-content",
  "reasoning-content-streaming",
  "files-api-reference",
  "files-api-reference-streaming",
  "grounding",
  "grounding-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const ANTHROPIC_MISLED_CAPABILITIES = [
  "redacted-thinking",
  "redacted-thinking-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const ANTHROPIC_UNSUPPORTED_INPUT_MODALITIES = [
  "audio-input",
  "audio-input-streaming",
  "video-input",
  "video-input-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const ANTHROPIC_UNSUPPORTED_OUTPUT_MODALITIES = [
  "image-output",
  "image-output-streaming",
] as const satisfies readonly SupportEntry["capability"][];

function anthropic(
  model: string,
  capabilities: readonly SupportEntry["capability"][],
): SupportEntry[] {
  return capabilities.map((capability) => ({
    provider: ANTHROPIC_PROVIDER,
    model,
    capability,
    outcome: "captured",
  }));
}

function anthropicUnsupported(
  model: string,
  capabilities: readonly SupportEntry["capability"][],
  notes: string,
): SupportEntry[] {
  return capabilities.map((capability) => ({
    provider: ANTHROPIC_PROVIDER,
    model,
    capability,
    outcome: "unsupported",
    notes,
  }));
}

function anthropicMisled(
  model: string,
  capabilities: readonly SupportEntry["capability"][],
  notes: string,
): SupportEntry[] {
  return capabilities.map((capability) => ({
    provider: ANTHROPIC_PROVIDER,
    model,
    capability,
    outcome: "misled",
    notes,
  }));
}

const MATRIX: SupportEntry[] = [
  ...ANTHROPIC_MODELS.flatMap((model) =>
    anthropic(model, ANTHROPIC_CAPTURED_CAPABILITIES),
  ),
  ...ANTHROPIC_MODELS.flatMap((model) =>
    anthropicMisled(
      model,
      ANTHROPIC_MISLED_CAPABILITIES,
      "Anthropic's documentation describes the canary prompt as a deterministic trigger for a redacted_thinking content block. On capture day the safety classifier did not fire on any first-party model; the assistant response carries a regular thinking block instead. The fixture on disk documents what the wire actually returned for the documented input. The plug-in and SSE parser already accept redacted_thinking blocks, so a future re-capture on a day the classifier does fire will flip this row to captured without code changes.",
    ),
  ),
  ...ANTHROPIC_MODELS.flatMap((model) =>
    anthropicUnsupported(
      model,
      ANTHROPIC_UNSUPPORTED_INPUT_MODALITIES,
      "Anthropic's first-party Claude models do not accept audio or video inputs; the Messages API content array only permits text, image, document, tool_use, and tool_result blocks. No equivalent server-side ingestion path exists today.",
    ),
  ),
  ...ANTHROPIC_MODELS.flatMap((model) =>
    anthropicUnsupported(
      model,
      ANTHROPIC_UNSUPPORTED_OUTPUT_MODALITIES,
      "Anthropic's first-party Claude models do not emit images; the Messages API surface is text-only on the output side and has no responseModalities-style toggle.",
    ),
  ),
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
  // captured and misled rows both carry fixtures on disk; the smoke
  // test validates either flavor for file presence. refused / http-
  // error / unsupported rows do not.
  if (entry.outcome !== "captured" && entry.outcome !== "misled") return null;
  return `${FIXTURE_ROOT}/${entry.provider}/${entry.model}/${entry.capability}`;
}
