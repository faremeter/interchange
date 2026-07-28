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
] as const;

const GEMINI_PROVIDER = "google-genai";
const GEMINI_TEXT_MODELS = [
  "gemini-2.5-flash",
  "gemini-2.5-pro",
  "gemini-3.6-flash",
  "gemini-3.5-flash",
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

// captured is the sole self-explanatory outcome: its fixture is the evidence, so
// the row carries no notes. Every other outcome is a deviation that must justify
// itself, so it requires a non-empty notes explanation. This is a DIFFERENT axis
// from FIXTURE_BEARING_OUTCOMES — misled is fixture-bearing yet still requires
// notes — so the two sets are intentionally distinct; do not unify them.
const NOTES_FREE_OUTCOMES = new Set<SupportEntry["outcome"]>(["captured"]);

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
