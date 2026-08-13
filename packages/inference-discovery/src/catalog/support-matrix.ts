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
  "claude-opus-5",
  "claude-fable-5",
  "claude-haiku-4-5-20251001",
  "claude-opus-4-8",
  "claude-opus-4-5-20251101",
  "claude-opus-4-6",
  "claude-opus-4-7",
  "claude-sonnet-4-5-20250929",
  "claude-sonnet-4-6",
] as const;

const GEMINI_PROVIDER = "google-genai";
const GEMINI_TEXT_MODELS = [
  "gemini-2.5-flash",
  "gemini-2.5-pro",
  "gemini-3.6-flash",
  "gemini-3.5-flash",
  "gemini-3-flash-preview",
  "gemini-3.1-pro-preview",
] as const;
const GEMINI_IMAGE_MODELS = [
  "gemini-2.5-flash-image",
  "gemini-3.1-flash-image",
] as const;

const OPENCODE_PROVIDER = "opencode-zen";
const OPENAI_PROVIDER = "openai";
const OPENAI_FIRST_PARTY_MODELS = [
  "gpt-5.5",
  "gpt-5.6-sol",
  "gpt-5.6-terra",
  "gpt-5.6-luna",
] as const;

// The structured-output capability pair, shared by the rows whose entire
// capability list is exactly this pair — several opencode models with outcome
// captured, and Anthropic with outcome unsupported — so they need not re-spell
// it. These two capabilities also appear as members of the larger
// GEMINI_TEXT_CAPABILITIES and OPENAI_CAPTURED_CAPABILITIES lists, where they
// stay inline as part of those providers' full captured sets.
export const STRUCTURED_OUTPUT_CAPABILITIES = [
  "structured-output",
  "structured-output-streaming",
] as const satisfies readonly SupportEntry["capability"][];

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

const GEMINI_TEXT_SAFETY_CAPABILITIES = [
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

// Text + tool-calling with neither reasoning_content nor vision. Several
// net-new relay models emit no reasoning_content field (they reason in-band
// with <think> tags in content, or not at all) and soft-decline or return
// empty on image input, so they carry only this base set plus whatever else
// they demonstrated.
const OPENCODE_BASE_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
] as const satisfies readonly SupportEntry["capability"][];

// The first-party api.openai.com deployment covers what the OpenAI-protocol
// body builder builds. The streaming multi-turn, vision-streaming, and
// document-input-streaming variants are not built by that builder, so they
// carry no rows at all: their absence is a rig limitation, not a provider
// outcome, and marking them unsupported would wrongly attribute it to the model.
const OPENAI_CAPTURED_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
  "vision-input",
  "document-input",
  "structured-output",
  "structured-output-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const OPENAI_UNSUPPORTED_REASONING = [
  "reasoning-content",
  "reasoning-content-streaming",
] as const satisfies readonly SupportEntry["capability"][];

// Net-new first-party OpenAI models probed on the Chat Completions wire. The
// gpt-5.x line, the o-series reasoning models, and the gpt-4.1/gpt-4o families
// all carry the full captured surface (OPENAI_CAPTURED_CAPABILITIES). The two
// oldest models carry less and get their own rows below.
const OPENAI_NETNEW_FULL_MODELS = [
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
  "gpt-4.1",
  "gpt-4.1-mini",
  "gpt-4.1-nano",
  "gpt-4o",
  "gpt-4o-mini",
] as const;

// gpt-4-turbo takes image and document input but rejects json_schema
// structured output.
const OPENAI_MULTIMODAL_NO_STRUCTURED_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
  "vision-input",
  "document-input",
] as const satisfies readonly SupportEntry["capability"][];

// The original gpt-4 is text-only and predates structured output.
const OPENAI_TEXT_ONLY_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
] as const satisfies readonly SupportEntry["capability"][];

const OPENAI_GPT4_MULTIMODAL_UNSUPPORTED = [
  "vision-input",
  "document-input",
] as const satisfies readonly SupportEntry["capability"][];

const OPENAI_NETNEW_REASONING_NOTE =
  "First-party api.openai.com Chat Completions responses for this model carry " +
  "no reasoning or reasoning_content field; the assistant message holds only " +
  "role, content, refusal, and annotations. OpenAI exposes reasoning tokens " +
  "solely through the Responses API, which this Chat-Completions plug-in does " +
  "not probe. This holds for the o-series reasoning models too: they reason " +
  "internally but surface no reasoning field on this wire.";

const OPENAI_STRUCTURED_UNSUPPORTED_NOTE =
  "This model rejects response_format of type json_schema with an HTTP 400 " +
  "invalid_request_error. Strict structured output is a later-model feature " +
  "on the Chat Completions wire; gpt-4 and gpt-4-turbo predate it.";

const OPENAI_GPT4_MULTIMODAL_NOTE =
  "The original text-only gpt-4 returns an HTTP 500 server_error when sent " +
  "image or document content parts. Multimodal input on this Chat Completions " +
  "wire arrived with gpt-4o and gpt-4-turbo, not gpt-4.";

// Builds one SupportEntry per capability for a single (provider, model,
// outcome). notes is included only when supplied, so captured rows stay
// notes-free while misled/unsupported rows carry their explanation. rows does
// not itself check the notes/outcome pairing; assertNotesDiscipline enforces it
// over the assembled matrix at module load.
function rows(
  provider: string,
  model: string,
  capabilities: readonly SupportEntry["capability"][],
  outcome: SupportEntry["outcome"],
  notes?: string,
): SupportEntry[] {
  return capabilities.map((capability) => ({
    provider,
    model,
    capability,
    outcome,
    ...(notes === undefined ? {} : { notes }),
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

const ANTHROPIC_REDACTED_THINKING_CAPABILITIES = [
  "redacted-thinking",
  "redacted-thinking-streaming",
] as const satisfies readonly SupportEntry["capability"][];

// Models that returned real redacted_thinking blocks. Adaptive-thinking
// models (claude-sonnet-5, and any new model that follows the same wire)
// remain misled when the canary still yields regular thinking only.
const ANTHROPIC_REDACTED_CAPTURED_MODELS = [
  "claude-haiku-4-5-20251001",
] as const;

// Adaptive models re-probed for redacted_thinking that still returned
// regular thinking blocks rather than redacted_thinking (HTTP 200 with
// a normal text reply and no refusal).
const ANTHROPIC_REDACTED_MISLED_MODELS = [
  "claude-sonnet-5",
  "claude-opus-5",
] as const;

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

// Net-new opencode-zen models probed live, grouped by the capability profile
// each actually demonstrated on the wire. grok-4.5 (the relay reports its
// upstream endpoint unavailable, returning empty or error bodies) and the
// deprecated mimo-v2-omni / mimo-v2-pro (relay returns a 404 deprecation
// notice) were probed but excluded. Vision and reasoning_content vary per
// model, so the groups differ.
const OPENCODE_FULL_STRUCTURED_MODELS = [
  "kimi-k2.5",
  "qwen3.5-plus",
  "qwen3.6-plus",
  "qwen3.8-max",
] as const;
const OPENCODE_REASONING_ONLY_MODELS = ["glm-5", "glm-5.1"] as const;
const OPENCODE_BASE_STRUCTURED_MODELS = [
  "hy3",
  "mimo-v2.5-pro",
  "minimax-m2.5",
  "minimax-m2.7",
] as const;
const OPENCODE_NONVISION_STRUCTURED_MODELS = ["qwen3.7-max"] as const;

const XAI_PROVIDER = "xai";

// Every xAI model probed against the first-party api.x.ai Chat Completions wire.
const XAI_MODELS = [
  "grok-4.20-0309-non-reasoning",
  "grok-4.20-0309-reasoning",
  "grok-4.3",
  "grok-4.5",
  "grok-4.6",
  "grok-build-0.1",
] as const;

// The five xAI models that stream real reasoning_content on the
// reasoning-content canary. grok-4.20-0309-non-reasoning, xAI's explicit
// non-reasoning variant, is the sole omission — it returns a normal text reply
// with no reasoning_content, so its reasoning rows are unsupported below.
const XAI_REASONING_MODELS = [
  "grok-4.20-0309-reasoning",
  "grok-4.3",
  "grok-4.5",
  "grok-4.6",
  "grok-build-0.1",
] as const;

// Captured on every xAI model regardless of reasoning support.
const XAI_BASE_CAPTURED_CAPABILITIES = [
  "plain-text",
  "plain-text-streaming",
  "function-calling",
  "function-calling-multi-turn",
  "vision-input",
  "structured-output",
  "structured-output-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const XAI_REASONING_CAPABILITIES = [
  "reasoning-content",
  "reasoning-content-streaming",
] as const satisfies readonly SupportEntry["capability"][];

const XAI_NON_REASONING_NOTE =
  "grok-4.20-0309-non-reasoning returns an HTTP 200 text reply with no " +
  "reasoning_content field on the reasoning-content canary (the assistant " +
  "message carries only role, content, and refusal), and the streaming variant " +
  "emits no reasoning_content delta. It is xAI's explicit non-reasoning model, " +
  "so reasoning is unsupported on this wire; the other grok models probed here " +
  "stream real reasoning_content.";

const XAI_REFUSAL_STREAMING_NOTE =
  "Under a strict json_schema plus a policy-declining prompt, the model never " +
  "emitted a non-null delta.refusal field. It streamed schema-conformant JSON " +
  "whose steps array carried a textual decline (content fragments; finish_reason " +
  "stop). The fixture is retained as evidence of this wire behavior; synthetic " +
  "SSE unit tests cover the adapter's delta.refusal parser independently.";

const XAI_DOCUMENT_INPUT_NOTE =
  "xAI Chat Completions rejects a file content part with HTTP 400 and body " +
  "{error:'File content is not supported on /v1/chat/completions. Please use " +
  "/v1/responses instead.'}. Document input requires the Responses API, which " +
  "this Chat-Completions plug-in does not probe. No fixture.";

const MATRIX: SupportEntry[] = [
  ...ANTHROPIC_MODELS.flatMap((model) =>
    rows(
      ANTHROPIC_PROVIDER,
      model,
      ANTHROPIC_CAPTURED_CAPABILITIES,
      "captured",
    ),
  ),
  ...ANTHROPIC_REDACTED_CAPTURED_MODELS.flatMap((model) =>
    rows(
      ANTHROPIC_PROVIDER,
      model,
      ANTHROPIC_REDACTED_THINKING_CAPABILITIES,
      "captured",
    ),
  ),
  ...ANTHROPIC_REDACTED_MISLED_MODELS.flatMap((model) =>
    rows(
      ANTHROPIC_PROVIDER,
      model,
      ANTHROPIC_REDACTED_THINKING_CAPABILITIES,
      "misled",
      "Redacted-thinking canary returned a regular thinking block (empty or partial thinking text plus signature and a normal text reply), not redacted_thinking. These models use adaptive thinking with output_config.effort; Haiku 4.5 on classic budget_tokens thinking still captures real redacted_thinking. Fixture documents the wire that was returned.",
    ),
  ),
  ...rows(
    ANTHROPIC_PROVIDER,
    "claude-fable-5",
    ANTHROPIC_REDACTED_THINKING_CAPABILITIES,
    "refused",
    "Redacted-thinking canary ended with stop_reason refusal (stop_details.type=refusal, category=cyber) under adaptive thinking. No normal text reply and no redacted_thinking block. Anthropic Usage Policy cyber restriction; no fixture by convention.",
  ),
  ...ANTHROPIC_MODELS.flatMap((model) =>
    rows(
      ANTHROPIC_PROVIDER,
      model,
      ANTHROPIC_UNSUPPORTED_INPUT_MODALITIES,
      "unsupported",
      "Anthropic's first-party Claude models do not accept audio or video inputs; the Messages API content array only permits text, image, document, tool_use, and tool_result blocks. No equivalent server-side ingestion path exists today.",
    ),
  ),
  ...ANTHROPIC_MODELS.flatMap((model) =>
    rows(
      ANTHROPIC_PROVIDER,
      model,
      ANTHROPIC_UNSUPPORTED_OUTPUT_MODALITIES,
      "unsupported",
      "Anthropic's first-party Claude models do not emit images; the Messages API surface is text-only on the output side and has no responseModalities-style toggle.",
    ),
  ),
  ...ANTHROPIC_MODELS.flatMap((model) =>
    rows(
      ANTHROPIC_PROVIDER,
      model,
      STRUCTURED_OUTPUT_CAPABILITIES,
      "unsupported",
      "Anthropic's Messages API has no native structured-outputs surface. The internal adapter rejects responseFormat values of json and json-schema at the marshaling boundary rather than synthesizing a hidden tool-input wrapper; callers needing structured output route through a provider with native support.",
    ),
  ),
  ...GEMINI_TEXT_MODELS.flatMap((model) =>
    rows(GEMINI_PROVIDER, model, GEMINI_TEXT_CAPABILITIES, "captured"),
  ),
  ...GEMINI_TEXT_MODELS.flatMap((model) =>
    rows(GEMINI_PROVIDER, model, GEMINI_TEXT_SAFETY_CAPABILITIES, "captured"),
  ),
  ...GEMINI_IMAGE_MODELS.flatMap((model) =>
    rows(GEMINI_PROVIDER, model, GEMINI_IMAGE_CAPABILITIES, "captured"),
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "kimi-k2.6",
    OPENCODE_FULL_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "kimi-k2.6",
    STRUCTURED_OUTPUT_CAPABILITIES,
    "captured",
  ),
  ...rows(OPENCODE_PROVIDER, "kimi-k3", OPENCODE_FULL_CAPABILITIES, "captured"),
  ...rows(
    OPENCODE_PROVIDER,
    "kimi-k3",
    STRUCTURED_OUTPUT_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "kimi-k2.7-code",
    OPENCODE_FULL_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "kimi-k2.7-code",
    STRUCTURED_OUTPUT_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "qwen3.7-plus",
    OPENCODE_FULL_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "qwen3.7-plus",
    STRUCTURED_OUTPUT_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "mimo-v2.5",
    OPENCODE_FULL_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "glm-5.2",
    OPENCODE_NON_VISION_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "glm-5.2",
    STRUCTURED_OUTPUT_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "deepseek-v4-pro",
    OPENCODE_NON_VISION_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "deepseek-v4-flash",
    OPENCODE_NON_VISION_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "gpt-5.4-mini",
    STRUCTURED_OUTPUT_CAPABILITIES,
    "captured",
  ),
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
    model: "deepseek-v4-pro",
    capability: "vision-input",
    outcome: "http-error",
    notes:
      "OpenAI-style multimodal messages[].content elicits HTTP 400 invalid_request_error \"unknown variant 'image_url', expected 'text'\"; recorded as 'http-error' here so no capture is attempted.",
  },
  ...rows(
    OPENCODE_PROVIDER,
    "mimo-v2.5",
    STRUCTURED_OUTPUT_CAPABILITIES,
    "captured",
  ),
  {
    provider: OPENCODE_PROVIDER,
    model: "deepseek-v4-flash",
    capability: "structured-output",
    outcome: "http-error",
    notes:
      "Probe against the configured OpenCode base URL returned invalid_request_error with message 'Error from provider (Console Go): Upstream request failed'. No fixture; the upstream/routing failure is recorded as http-error.",
  },
  {
    provider: OPENCODE_PROVIDER,
    model: "deepseek-v4-flash",
    capability: "structured-output-streaming",
    outcome: "http-error",
    notes:
      "Probe against the configured OpenCode base URL returned invalid_request_error with message 'Error from provider (Console Go): Upstream request failed'. No fixture; the upstream/routing failure is recorded as http-error.",
  },
  ...OPENCODE_FULL_STRUCTURED_MODELS.flatMap((model) =>
    rows(OPENCODE_PROVIDER, model, OPENCODE_FULL_CAPABILITIES, "captured"),
  ),
  ...OPENCODE_FULL_STRUCTURED_MODELS.flatMap((model) =>
    rows(OPENCODE_PROVIDER, model, STRUCTURED_OUTPUT_CAPABILITIES, "captured"),
  ),
  ...OPENCODE_REASONING_ONLY_MODELS.flatMap((model) =>
    rows(
      OPENCODE_PROVIDER,
      model,
      OPENCODE_NON_VISION_CAPABILITIES,
      "captured",
    ),
  ),
  ...OPENCODE_BASE_STRUCTURED_MODELS.flatMap((model) =>
    rows(OPENCODE_PROVIDER, model, OPENCODE_BASE_CAPABILITIES, "captured"),
  ),
  ...OPENCODE_BASE_STRUCTURED_MODELS.flatMap((model) =>
    rows(OPENCODE_PROVIDER, model, STRUCTURED_OUTPUT_CAPABILITIES, "captured"),
  ),
  ...OPENCODE_NONVISION_STRUCTURED_MODELS.flatMap((model) =>
    rows(
      OPENCODE_PROVIDER,
      model,
      OPENCODE_NON_VISION_CAPABILITIES,
      "captured",
    ),
  ),
  ...OPENCODE_NONVISION_STRUCTURED_MODELS.flatMap((model) =>
    rows(OPENCODE_PROVIDER, model, STRUCTURED_OUTPUT_CAPABILITIES, "captured"),
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "minimax-m3",
    OPENCODE_BASE_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENCODE_PROVIDER,
    "minimax-m3",
    ["vision-input"] as const,
    "captured",
  ),
  ...OPENAI_FIRST_PARTY_MODELS.flatMap((model) =>
    rows(OPENAI_PROVIDER, model, OPENAI_CAPTURED_CAPABILITIES, "captured"),
  ),
  ...OPENAI_FIRST_PARTY_MODELS.flatMap((model) =>
    rows(
      OPENAI_PROVIDER,
      model,
      OPENAI_UNSUPPORTED_REASONING,
      "unsupported",
      "OpenAI's first-party api.openai.com Chat Completions responses for the gpt-5 series carry no reasoning or reasoning_content field; the assistant message holds only role, content, refusal, and annotations. OpenAI exposes reasoning tokens solely through the Responses API, which this Chat-Completions plug-in does not probe. The OpenAI-protocol opencode-zen relays do surface reasoning_content on this same wire, so this is a first-party OpenAI behavior, not a protocol limitation.",
    ),
  ),
  ...OPENAI_FIRST_PARTY_MODELS.flatMap((model) =>
    rows(
      OPENAI_PROVIDER,
      model,
      ["structured-output-refusal-streaming"] as const,
      "misled",
      "Live stream under strict json_schema + a policy-declining prompt never emitted a non-null delta.refusal field. The assistant instead streamed schema-conformant JSON whose steps array carried a textual decline (content fragments; finish_reason stop). The fixture is retained as evidence of this wire behavior; synthetic SSE unit tests cover the adapter's delta.refusal parser independently.",
    ),
  ),
  ...OPENAI_NETNEW_FULL_MODELS.flatMap((model) =>
    rows(OPENAI_PROVIDER, model, OPENAI_CAPTURED_CAPABILITIES, "captured"),
  ),
  ...OPENAI_NETNEW_FULL_MODELS.flatMap((model) =>
    rows(
      OPENAI_PROVIDER,
      model,
      OPENAI_UNSUPPORTED_REASONING,
      "unsupported",
      OPENAI_NETNEW_REASONING_NOTE,
    ),
  ),
  ...rows(
    OPENAI_PROVIDER,
    "gpt-4-turbo",
    OPENAI_MULTIMODAL_NO_STRUCTURED_CAPABILITIES,
    "captured",
  ),
  ...rows(
    OPENAI_PROVIDER,
    "gpt-4-turbo",
    OPENAI_UNSUPPORTED_REASONING,
    "unsupported",
    OPENAI_NETNEW_REASONING_NOTE,
  ),
  ...rows(
    OPENAI_PROVIDER,
    "gpt-4-turbo",
    STRUCTURED_OUTPUT_CAPABILITIES,
    "unsupported",
    OPENAI_STRUCTURED_UNSUPPORTED_NOTE,
  ),
  ...rows(OPENAI_PROVIDER, "gpt-4", OPENAI_TEXT_ONLY_CAPABILITIES, "captured"),
  ...rows(
    OPENAI_PROVIDER,
    "gpt-4",
    OPENAI_UNSUPPORTED_REASONING,
    "unsupported",
    OPENAI_NETNEW_REASONING_NOTE,
  ),
  ...rows(
    OPENAI_PROVIDER,
    "gpt-4",
    STRUCTURED_OUTPUT_CAPABILITIES,
    "unsupported",
    OPENAI_STRUCTURED_UNSUPPORTED_NOTE,
  ),
  ...rows(
    OPENAI_PROVIDER,
    "gpt-4",
    OPENAI_GPT4_MULTIMODAL_UNSUPPORTED,
    "unsupported",
    OPENAI_GPT4_MULTIMODAL_NOTE,
  ),
  ...XAI_MODELS.flatMap((model) =>
    rows(XAI_PROVIDER, model, XAI_BASE_CAPTURED_CAPABILITIES, "captured"),
  ),
  ...XAI_REASONING_MODELS.flatMap((model) =>
    rows(XAI_PROVIDER, model, XAI_REASONING_CAPABILITIES, "captured"),
  ),
  ...rows(
    XAI_PROVIDER,
    "grok-4.20-0309-non-reasoning",
    XAI_REASONING_CAPABILITIES,
    "unsupported",
    XAI_NON_REASONING_NOTE,
  ),
  ...XAI_MODELS.flatMap((model) =>
    rows(
      XAI_PROVIDER,
      model,
      ["structured-output-refusal-streaming"] as const,
      "misled",
      XAI_REFUSAL_STREAMING_NOTE,
    ),
  ),
  ...XAI_MODELS.flatMap((model) =>
    rows(
      XAI_PROVIDER,
      model,
      ["document-input"] as const,
      "http-error",
      XAI_DOCUMENT_INPUT_NOTE,
    ),
  ),
];

export const SUPPORT_MATRIX: readonly SupportEntry[] = MATRIX;

// Each provider's captured corpus lives inside the discovery package that
// probes it. This map is the single owner of "which package holds which
// provider's sessions"; getSessionDir composes the per-provider root with the
// same {provider}/{model}/{capability} layout every package's sessions/ dir
// uses.
const SESSION_ROOTS: Record<string, string> = {
  anthropic: "packages/inference-discovery-anthropic/sessions",
  "google-genai": "packages/inference-discovery-google-genai/sessions",
  "opencode-zen": "packages/inference-discovery-openai/sessions",
  openai: "packages/inference-discovery-openai/sessions",
  xai: "packages/inference-discovery-openai/sessions",
};

const FIXTURE_BEARING_OUTCOMES = new Set<SupportEntry["outcome"]>([
  "captured",
  "misled",
]);

// captured is the sole self-explanatory outcome: its fixture is the evidence, so
// the row carries no notes. Every other outcome is a deviation that must justify
// itself, so it requires a non-empty notes explanation. This is a DIFFERENT axis
// from FIXTURE_BEARING_OUTCOMES — misled is fixture-bearing yet still requires
// notes — so the two sets are intentionally distinct; do not unify them.
const NOTES_FREE_OUTCOMES = new Set<SupportEntry["outcome"]>(["captured"]);

// captured and misled rows both point to a captured session on disk that the
// smoke tests replay, so both are empirical proof the capability works; refused,
// http-error, and unsupported rows carry no fixture. This is the single owner of
// "which outcomes are fixture-bearing" — getSessionDir and the catalog capability
// expansion both read it rather than re-deciding the outcome set.
export function isFixtureBearing(entry: SupportEntry): boolean {
  return FIXTURE_BEARING_OUTCOMES.has(entry.outcome);
}

// The session corpus lives inside each provider's discovery package under a
// `sessions/` tree with the {provider}/{model}/{capability} layout.
// `session.json` inside each leaf is authoritative for the capability, model,
// and origin the directory holds; the brand provider is NOT in the manifest
// (its `source.provider` is the adapter-registry name, and both `openai` and
// `opencode-zen` map to `openai-compatible`), so the brand is composed here
// from the matrix entry.
export function getSessionDir(entry: SupportEntry): string | null {
  if (!isFixtureBearing(entry)) return null;
  const root = SESSION_ROOTS[entry.provider];
  if (root === undefined) {
    throw new Error(
      `no session root registered for provider '${entry.provider}' ` +
        `(${entry.model}/${entry.capability}); add it to SESSION_ROOTS`,
    );
  }
  return `${root}/${entry.provider}/${entry.model}/${entry.capability}`;
}

// Enforces the notes/outcome pairing on a single entry: a notes-free outcome
// must carry no notes; every other outcome must carry a non-empty notes
// explanation. Applied to every entry at module load below, so it covers the
// inline refused/http-error rows that never flow through rows() as well.
export function assertNotesDiscipline(entry: SupportEntry): void {
  const site = `${entry.provider}/${entry.model}/${entry.capability}`;
  if (NOTES_FREE_OUTCOMES.has(entry.outcome)) {
    if (entry.notes !== undefined) {
      throw new Error(
        `support-matrix: ${site} is ${entry.outcome} and must not carry ` +
          `notes (got ${JSON.stringify(entry.notes)})`,
      );
    }
    return;
  }
  if (entry.notes === undefined || entry.notes.trim().length === 0) {
    throw new Error(
      `support-matrix: ${site} is ${entry.outcome} and requires a non-empty ` +
        `notes explanation`,
    );
  }
}

for (const entry of SUPPORT_MATRIX) {
  assertNotesDiscipline(entry);
}
