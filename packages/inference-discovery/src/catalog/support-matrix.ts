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
  "claude-sonnet-5",
  "claude-sonnet-4-5-20250929",
  "claude-opus-4-1-20250805",
  "claude-haiku-4-5-20251001",
] as const;

const GEMINI_PROVIDER = "google-genai";
const GEMINI_TEXT_MODEL = "gemini-2.5-flash";
const GEMINI_IMAGE_MODEL = "gemini-2.5-flash-image";

const OPENCODE_PROVIDER = "opencode-zen";
const OPENAI_PROVIDER = "openai";

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
  "structured-output",
  "structured-output-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const GEMINI_TEXT_MISLED_CAPABILITIES = [
  "safety-classification",
  "safety-classification-streaming",
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

// The first-party api.openai.com deployment covers what the OpenAI-protocol
// body builder builds. The streaming multi-turn and vision-streaming variants
// are not built by that builder, so they carry no rows at all: their absence is
// a rig limitation, not a provider outcome, and marking them unsupported would
// wrongly attribute it to the model.
const OPENAI_CAPTURED_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
  "vision-input",
  "structured-output",
  "structured-output-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const OPENAI_UNSUPPORTED_REASONING = [
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

function geminiMisled(
  model: string,
  capabilities: readonly SupportEntry["capability"][],
  notes: string,
): SupportEntry[] {
  return capabilities.map((capability) => ({
    provider: GEMINI_PROVIDER,
    model,
    capability,
    outcome: "misled",
    notes,
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

function openai(
  model: string,
  capabilities: readonly SupportEntry["capability"][],
): SupportEntry[] {
  return capabilities.map((capability) => ({
    provider: OPENAI_PROVIDER,
    model,
    capability,
    outcome: "captured",
  }));
}

function openaiUnsupported(
  model: string,
  capabilities: readonly SupportEntry["capability"][],
  notes: string,
): SupportEntry[] {
  return capabilities.map((capability) => ({
    provider: OPENAI_PROVIDER,
    model,
    capability,
    outcome: "unsupported",
    notes,
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

const ANTHROPIC_UNSUPPORTED_STRUCTURED_OUTPUTS = [
  "structured-output",
  "structured-output-streaming",
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
  ...ANTHROPIC_MODELS.flatMap((model) =>
    anthropicUnsupported(
      model,
      ANTHROPIC_UNSUPPORTED_STRUCTURED_OUTPUTS,
      "Anthropic's Messages API has no native structured-outputs surface. The internal adapter rejects responseFormat values of json and json-schema at the marshaling boundary rather than synthesizing a hidden tool-input wrapper; callers needing structured output route through a provider with native support.",
    ),
  ),
  ...gemini(GEMINI_TEXT_MODEL, GEMINI_TEXT_CAPABILITIES),
  ...gemini(GEMINI_IMAGE_MODEL, GEMINI_IMAGE_CAPABILITIES),
  ...geminiMisled(
    GEMINI_TEXT_MODEL,
    GEMINI_TEXT_MISLED_CAPABILITIES,
    'Probe prompt did not engage Gemini\'s structured safety classifier on capture day. The model self-refused via response text content but `safetyRatings`, `promptFeedback`, and `finishReason: "SAFETY"` are all absent from the response. The fixture on disk documents what the wire actually returned for the documented probe input. A future re-capture (different prompt, different classifier thresholds, or different model behavior) may flip this row to captured without code changes once a structured safety signal materializes.',
  ),
  ...gemini("gemini-2.5-pro", GEMINI_TEXT_CAPABILITIES),
  ...geminiMisled(
    "gemini-2.5-pro",
    GEMINI_TEXT_MISLED_CAPABILITIES,
    'Probe prompt did not engage gemini-2.5-pro\'s structured safety classifier on capture day. The model self-refused via response text content but `safetyRatings`, `promptFeedback`, and `finishReason: "SAFETY"` are all absent from the response. The fixture on disk documents what the wire actually returned for the documented probe input. A future re-capture may flip this row to captured once a structured safety signal materializes.',
  ),
  ...opencode("kimi-k2.6", OPENCODE_FULL_CAPABILITIES),
  ...opencode("mimo-v2-omni", OPENCODE_FULL_CAPABILITIES),
  ...opencode("qwen3.6-plus", OPENCODE_FULL_CAPABILITIES),
  ...opencode("glm-5.1", OPENCODE_NON_VISION_CAPABILITIES),
  ...opencode("deepseek-v4-pro", OPENCODE_NON_VISION_CAPABILITIES),
  ...opencode("gpt-5.4-mini", [
    "structured-output",
    "structured-output-streaming",
  ]),
  ...opencode("kimi-k2.6", [
    "structured-output",
    "structured-output-streaming",
  ]),
  ...opencode("kimi-k3", OPENCODE_FULL_CAPABILITIES),
  ...opencode("kimi-k3", ["structured-output", "structured-output-streaming"]),
  ...opencode("kimi-k2.7-code", OPENCODE_FULL_CAPABILITIES),
  ...opencode("kimi-k2.7-code", [
    "structured-output",
    "structured-output-streaming",
  ]),
  ...opencode("glm-5.1", ["structured-output", "structured-output-streaming"]),
  ...opencode("qwen3.6-plus", [
    "structured-output",
    "structured-output-streaming",
  ]),
  {
    provider: OPENCODE_PROVIDER,
    model: "deepseek-v4-pro",
    capability: "structured-output",
    outcome: "http-error",
    notes:
      "Probe against /zen/v1 returned HTTP 401 with body {type:'error',error:{type:'ModelError',message:'Model deepseek-v4-pro is not supported'}}. The HTTP status is the relay's chosen code for the routing miss, not an auth failure; deepseek-v4-pro's reasoning-content captures live on the older /zen/go/v1 tier and the v1 tier does not route the model.",
  },
  {
    provider: OPENCODE_PROVIDER,
    model: "deepseek-v4-pro",
    capability: "structured-output-streaming",
    outcome: "http-error",
    notes:
      "Probe against /zen/v1 returned HTTP 401 with body {type:'error',error:{type:'ModelError',message:'Model deepseek-v4-pro is not supported'}}. The HTTP status is the relay's chosen code for the routing miss, not an auth failure; deepseek-v4-pro's reasoning-content captures live on the older /zen/go/v1 tier and the v1 tier does not route the model.",
  },
  {
    provider: OPENCODE_PROVIDER,
    model: "mimo-v2-omni",
    capability: "structured-output",
    outcome: "http-error",
    notes:
      "Probe against /zen/v1 returned HTTP 401 with body {type:'error',error:{type:'ModelError',message:'Model mimo-v2-omni is not supported'}}. The HTTP status is the relay's chosen code for the routing miss, not an auth failure; mimo-v2-omni's other captures live on the older /zen/go/v1 tier and the v1 tier does not route the model.",
  },
  {
    provider: OPENCODE_PROVIDER,
    model: "mimo-v2-omni",
    capability: "structured-output-streaming",
    outcome: "http-error",
    notes:
      "Probe against /zen/v1 returned HTTP 401 with body {type:'error',error:{type:'ModelError',message:'Model mimo-v2-omni is not supported'}}. The HTTP status is the relay's chosen code for the routing miss, not an auth failure; mimo-v2-omni's other captures live on the older /zen/go/v1 tier and the v1 tier does not route the model.",
  },
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
  ...openai("gpt-5.5", OPENAI_CAPTURED_CAPABILITIES),
  ...openaiUnsupported(
    "gpt-5.5",
    OPENAI_UNSUPPORTED_REASONING,
    "OpenAI's first-party api.openai.com Chat Completions responses for the gpt-5 series carry no reasoning or reasoning_content field; the assistant message holds only role, content, refusal, and annotations. OpenAI exposes reasoning tokens solely through the Responses API, which this Chat-Completions plug-in does not probe. The OpenAI-protocol opencode-zen relays do surface reasoning_content on this same wire, so this is a first-party OpenAI behavior, not a protocol limitation.",
  ),
];

export const SUPPORT_MATRIX: readonly SupportEntry[] = MATRIX;

// Each provider's captured corpus lives inside the discovery package that
// probes it. This map is the single owner of "which package holds which
// provider's fixtures"; getFixtureDir composes the per-provider root with the
// same {provider}/{model}/{capability} layout every package's wire/ dir uses.
const FIXTURE_ROOTS: Record<string, string> = {
  anthropic: "packages/inference-discovery-anthropic/wire",
  "google-genai": "packages/inference-discovery-google-genai/wire",
  "opencode-zen": "packages/inference-discovery-openai/wire",
  openai: "packages/inference-discovery-openai/wire",
};

const FIXTURE_BEARING_OUTCOMES = new Set<SupportEntry["outcome"]>([
  "captured",
  "misled",
]);

// captured and misled rows both point to a captured wire flow on disk that the
// smoke tests replay, so both are empirical proof the capability works; refused,
// http-error, and unsupported rows carry no fixture. This is the single owner of
// "which outcomes are fixture-bearing" — getFixtureDir and the catalog capability
// expansion both read it rather than re-deciding the outcome set.
export function isFixtureBearing(entry: SupportEntry): boolean {
  return FIXTURE_BEARING_OUTCOMES.has(entry.outcome);
}

export function getFixtureDir(entry: SupportEntry): string | null {
  if (!isFixtureBearing(entry)) return null;
  const root = FIXTURE_ROOTS[entry.provider];
  if (root === undefined) {
    throw new Error(
      `no fixture root registered for provider '${entry.provider}' ` +
        `(${entry.model}/${entry.capability}); add it to FIXTURE_ROOTS`,
    );
  }
  return `${root}/${entry.provider}/${entry.model}/${entry.capability}`;
}
