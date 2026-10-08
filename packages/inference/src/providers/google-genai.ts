import { type } from "arktype";

import type {
  CodeExecutionRequestBlock,
  CodeExecutionResultBlock,
  ConversationTurn,
  ContentBlock,
  InferenceEvent,
  InferenceOptions,
  LastCycleSource,
  MediaSource,
  PartialMessage,
  TokenUsage,
} from "@intx/types/runtime";
import { formatSafetyRatingText } from "@intx/types/runtime";
import type { ProviderAdapter, BuiltRequest } from "../adapter";
import { CREDENTIAL_SENTINEL } from "../auth";
import { ProtocolMismatchError } from "../errors";
import {
  decodeToolName,
  encodeToolName,
  type ToolNameLimit,
} from "../tool-name";

// Gemini requires a letter/underscore-leading function name; the raw
// package-qualified names fail that check. Documented limit is 64 characters.
const GOOGLE_TOOL_NAME_LIMIT: ToolNameLimit = {
  provider: "google-genai",
  maxLength: 64,
};

// Models that reject a zero thinking budget with HTTP 400. Keep aligned with
// the discovery plug-in's THINKING_MANDATORY_MODELS.
const THINKING_MANDATORY_MODELS: ReadonlySet<string> = new Set([
  "gemini-2.5-pro",
  "gemini-3.6-flash",
]);

// Sentinel meaning "let the model decide".
const DYNAMIC_THINKING_BUDGET = -1;

function minimalThinkingBudget(model: string): number {
  return THINKING_MANDATORY_MODELS.has(model) ? DYNAMIC_THINKING_BUDGET : 0;
}

// Plain-object validation; avoids an `as` assertion that would lie about
// runtime shape.
const ParsedJSONObject = type("Record<string, unknown>");

// ---------------------------------------------------------------------------
// Request building
//
// Translates ConversationTurn[] into a Gemini `generateContent` body. The
// harness always streams, so the URL pins `:streamGenerateContent?alt=sse`.
// ---------------------------------------------------------------------------

function buildRequest(
  messages: ConversationTurn[],
  model: string,
  options: InferenceOptions,
): BuiltRequest {
  const systemMessages = messages.filter((m) => m.role === "system");
  const conversationMessages = messages.filter((m) => m.role !== "system");

  // History system text, overridden by `options.systemPrompt`; non-text
  // blocks error rather than drop.
  const systemText = systemMessages
    .flatMap((m) =>
      m.content.map((b) => {
        if (b.type !== "text") {
          throw new Error(
            `Google GenAI adapter: system turn must contain only text blocks; got ${JSON.stringify(b.type)}.`,
          );
        }
        return b.text;
      }),
    )
    .join("\n\n");
  const effectiveSystem = options.systemPrompt
    ? options.systemPrompt
    : systemText || undefined;

  // `callId -> functionName` from prior assistant `tool_call` blocks.
  // Gemini's `functionResponse` needs the name, but `ToolResultBlock` carries
  // only the callId; built once to avoid an O(N^2) per-block walk.
  const callIdToFunctionName = buildCallIdToFunctionName(messages);

  // safety_rating is output-only; rewrite to text so history keeps role
  // alternation.
  const contents: GeminiContent[] = conversationMessages.map((msg) => {
    const rewritten: ConversationTurn = {
      ...msg,
      content: msg.content.map((b) =>
        b.type === "safety_rating"
          ? { type: "text" as const, text: formatSafetyRatingText(b) }
          : b,
      ),
    };
    return toGeminiContent(rewritten, callIdToFunctionName);
  });

  const body: Record<string, unknown> = { contents };

  if (effectiveSystem !== undefined) {
    body["systemInstruction"] = { parts: [{ text: effectiveSystem }] };
  }

  if (options.tools !== undefined && options.tools.length > 0) {
    body["tools"] = [
      {
        functionDeclarations: options.tools.map((t) => ({
          name: encodeToolName(t.name, GOOGLE_TOOL_NAME_LIMIT),
          description: t.description,
          parameters: t.inputSchema,
        })),
      },
    ];
  }

  const generationConfig = buildGenerationConfig(model, options);
  if (generationConfig !== undefined) {
    body["generationConfig"] = generationConfig;
  }

  // Caller escape hatch: shallow-merge `providerOptions` over the body, so
  // passing `generationConfig` wholesale replaces the object built above.
  if (options.providerOptions !== undefined) {
    Object.assign(body, options.providerOptions);
  }

  // Escape the model name; the trailing `:streamGenerateContent?alt=sse`
  // stays outside the substitution so its colon and query string survive.
  const encodedModel = encodeURIComponent(model);

  return {
    url: `/v1beta/models/${encodedModel}:streamGenerateContent?alt=sse`,
    headers: {
      "content-type": "application/json",
      "x-goog-api-key": CREDENTIAL_SENTINEL,
    },
    body: JSON.stringify(body),
  };
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

// Round-trip wire shapes. `thought` marks thinking text; `thoughtSignature`
// rides each block's signature back on that block's own part.
interface GeminiTextPart {
  text: string;
  thought?: boolean;
  thoughtSignature?: string;
}
interface GeminiInlineDataPart {
  inlineData: { mimeType: string; data: string };
  thoughtSignature?: string;
}
interface GeminiFileDataPart {
  fileData: { mimeType: string; fileUri: string };
  thoughtSignature?: string;
}
interface GeminiFunctionCallPart {
  functionCall: { name: string; args: Record<string, unknown> };
  thoughtSignature?: string;
}
interface GeminiFunctionResponsePart {
  functionResponse: { name: string; response: Record<string, unknown> };
}
type GeminiPart =
  | GeminiTextPart
  | GeminiInlineDataPart
  | GeminiFileDataPart
  | GeminiFunctionCallPart
  | GeminiFunctionResponsePart;

interface GeminiContent {
  role: "user" | "model";
  parts: GeminiPart[];
}

// ---------------------------------------------------------------------------
// Conversation-turn translation
// ---------------------------------------------------------------------------

function buildCallIdToFunctionName(
  messages: ConversationTurn[],
): Map<string, string> {
  const map = new Map<string, string>();
  for (const msg of messages) {
    if (msg.role !== "assistant") continue;
    for (const block of msg.content) {
      if (block.type === "tool_call") {
        map.set(block.id, block.name);
      }
    }
  }
  return map;
}

function toGeminiContent(
  msg: ConversationTurn,
  callIdToFunctionName: Map<string, string>,
): GeminiContent {
  const role: "user" | "model" = msg.role === "assistant" ? "model" : "user";
  // Gemini wants `functionCall` on model turns and `functionResponse` on
  // user turns; the internal union does not enforce that. Catch misrouted
  // blocks here instead of an opaque 400 from Gemini.
  for (const block of msg.content) {
    if (role === "user" && block.type === "tool_call") {
      throw new Error(
        `Google GenAI adapter: tool_call blocks must appear on assistant turns, ` +
          `found one on a ${JSON.stringify(msg.role)} turn (id ${JSON.stringify(block.id)}).`,
      );
    }
    if (role === "model" && block.type === "tool_result") {
      throw new Error(
        `Google GenAI adapter: tool_result blocks must appear on user turns, ` +
          `found one on a ${JSON.stringify(msg.role)} turn (callId ${JSON.stringify(block.callId)}).`,
      );
    }
  }

  const parts = msg.content.map((block) =>
    toGeminiPart(block, callIdToFunctionName),
  );

  return { role, parts };
}

function toGeminiPart(
  block: ContentBlock,
  callIdToFunctionName: Map<string, string>,
): GeminiPart {
  switch (block.type) {
    case "text":
      return {
        text: block.text,
        ...(block.signature !== undefined
          ? { thoughtSignature: block.signature }
          : {}),
      };

    case "image": {
      // Only ImageBlock among the media kinds has a signature field.
      const part = toGeminiMediaPart(block.source);
      return block.signature !== undefined
        ? { ...part, thoughtSignature: block.signature }
        : part;
    }

    case "document":
    case "audio":
    case "video":
      return toGeminiMediaPart(block.source);

    case "tool_call":
      return {
        functionCall: {
          name: encodeToolName(block.name, GOOGLE_TOOL_NAME_LIMIT),
          args: block.arguments,
        },
        ...(block.signature !== undefined
          ? { thoughtSignature: block.signature }
          : {}),
      };

    case "tool_result":
      return toGeminiFunctionResponse(block, callIdToFunctionName);

    case "thinking":
      // Gemini usually signs the follow-on functionCall part instead; a
      // signed thinking part is the rare case where it signed the thought.
      return {
        text: block.thinking,
        thought: true,
        ...(block.signature !== undefined
          ? { thoughtSignature: block.signature }
          : {}),
      };

    case "redacted_thinking":
      // Gemini does not emit these; a caller passing one is mixing wire
      // formats.
      throw new Error(
        "Google GenAI adapter does not handle redacted_thinking blocks; " +
          "they are Anthropic-specific.",
      );

    case "safety_rating":
      // Rewritten to text in buildRequest before this is reached.
      throw new Error(
        "Google GenAI adapter: safety_rating blocks must be rewritten " +
          "to text before toGeminiPart.",
      );
    case "citation":
      // Citations are output-only; echoing one into an input turn has no
      // defined wire shape.
      throw new Error(
        "Google GenAI adapter does not echo citation blocks; citations " +
          "are emitted by the model, not sent to it.",
      );

    case "code_execution_request":
    case "code_execution_result":
      // The adapter emits no `executableCode`/`codeExecutionResult` shapes;
      // surface the gap rather than drop the block.
      throw new Error(
        `Google GenAI adapter does not handle ${block.type} content blocks.`,
      );

    case "refusal":
      // OpenAI strict-mode output shape with no Gemini wire equivalent; fail
      // loudly rather than drop.
      throw new Error(
        "Google GenAI adapter does not handle refusal content blocks; " +
          "they are emitted by OpenAI strict-mode structured outputs.",
      );
  }
}

// `base64` inlines bytes; `file-reference` and `url` both map to
// `fileData.fileUri` (Files API URIs and public URLs).
function toGeminiMediaPart(
  source: MediaSource,
): GeminiInlineDataPart | GeminiFileDataPart {
  if (source.kind === "base64") {
    return {
      inlineData: { mimeType: source.mimeType, data: source.data },
    };
  }
  if (source.kind === "file-reference") {
    return {
      fileData: { mimeType: source.mimeType, fileUri: source.reference },
    };
  }
  if (source.kind === "url") {
    return {
      fileData: { mimeType: source.mimeType, fileUri: source.url },
    };
  }
  source satisfies never;
  throw new Error(`unreachable: unknown MediaSource kind`);
}

// Gemini's `response` must be a JSON object: one text block that parses as a
// plain object becomes `response`, else `{ result: text }` (`{ error: text }`
// when isError). Zero or multiple text blocks, non-text blocks, or an
// unknown callId throw here instead of surfacing as an opaque 400 later.
function toGeminiFunctionResponse(
  block: Extract<ContentBlock, { type: "tool_result" }>,
  callIdToFunctionName: Map<string, string>,
): GeminiFunctionResponsePart {
  const name = callIdToFunctionName.get(block.callId);
  if (name === undefined) {
    const known = Array.from(callIdToFunctionName.keys());
    throw new Error(
      `Google GenAI adapter: tool_result.callId ${JSON.stringify(block.callId)} ` +
        `has no matching tool_call in the conversation history. ` +
        `Known callIds: ${known.length === 0 ? "(none)" : known.map((k) => JSON.stringify(k)).join(", ")}.`,
    );
  }

  if (block.content.length !== 1) {
    throw new Error(
      `Google GenAI adapter: tool_result must contain exactly one text block, ` +
        `got ${String(block.content.length)} blocks for callId ` +
        `${JSON.stringify(block.callId)}.`,
    );
  }
  const only = block.content[0];
  if (only === undefined || only.type !== "text") {
    const seenType = only?.type ?? "undefined";
    throw new Error(
      `Google GenAI adapter: tool_result content block must be of type "text", ` +
        `got ${JSON.stringify(seenType)} for callId ${JSON.stringify(block.callId)}.`,
    );
  }

  const text = only.text;
  const parsed = tryParseJSONObject(text);

  let response: Record<string, unknown>;
  if (parsed !== null) {
    response = parsed;
  } else if (block.isError === true) {
    response = { error: text };
  } else {
    response = { result: text };
  }

  return {
    functionResponse: {
      name: encodeToolName(name, GOOGLE_TOOL_NAME_LIMIT),
      response,
    },
  };
}

function tryParseJSONObject(text: string): Record<string, unknown> | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  // arktype's `Record<string, unknown>` accepts arrays (arrays are records
  // with numeric-string keys), so reject arrays before validating; otherwise
  // `"[1,2,3]"` would become a `response` shape Gemini cannot consume.
  if (Array.isArray(parsed)) {
    return null;
  }
  const validated = ParsedJSONObject(parsed);
  if (validated instanceof type.errors) {
    return null;
  }
  return validated;
}

// ---------------------------------------------------------------------------
// generationConfig
// ---------------------------------------------------------------------------

function buildGenerationConfig(
  model: string,
  options: InferenceOptions,
): Record<string, unknown> | undefined {
  const config: Record<string, unknown> = {};

  if (options.maxTokens !== undefined) {
    config["maxOutputTokens"] = options.maxTokens;
  }
  if (options.temperature !== undefined) {
    config["temperature"] = options.temperature;
  }

  // enabled  -> budget (default 1024) plus includeThoughts
  // disabled -> budget 0, or the dynamic sentinel on mandatory-thinking models
  // absent   -> omit thinkingConfig; the model's default applies
  if (options.thinking !== undefined) {
    if (options.thinking.enabled) {
      const thinkingBudget = options.thinking.budgetTokens ?? 1024;
      config["thinkingConfig"] = {
        thinkingBudget,
        includeThoughts: true,
      };
    } else {
      config["thinkingConfig"] = {
        thinkingBudget: minimalThinkingBudget(model),
      };
    }
  }

  if (
    options.responseModalities !== undefined &&
    options.responseModalities.length > 0
  ) {
    config["responseModalities"] =
      options.responseModalities.map(toGeminiModality);
  }

  if (options.responseFormat !== undefined) {
    applyResponseFormat(config, options.responseFormat);
  }

  return Object.keys(config).length === 0 ? undefined : config;
}

// Gemini exposes structured output via (`responseMimeType`, `responseSchema`):
// MIME alone gives free-form JSON, the pair constrains it. The OpenAI
// `name`/`strict` fields are ignored; the schema is forwarded verbatim and
// Gemini rejects anything outside its JSON Schema subset.
function applyResponseFormat(
  config: Record<string, unknown>,
  format: NonNullable<InferenceOptions["responseFormat"]>,
): void {
  switch (format.kind) {
    case "text":
      // Free-form text is the default; omit the MIME type.
      return;
    case "json":
      config["responseMimeType"] = "application/json";
      return;
    case "json-schema":
      config["responseMimeType"] = "application/json";
      config["responseSchema"] = format.schema;
      return;
  }
}

function toGeminiModality(m: "text" | "image" | "audio"): string {
  switch (m) {
    case "text":
      return "TEXT";
    case "image":
      return "IMAGE";
    case "audio":
      return "AUDIO";
  }
}

// ---------------------------------------------------------------------------
// Response parsing
//
// Each SSE event is one complete JSON object; a partial means the `parseSSE`
// framing broke, not a Gemini protocol violation. Per the adapter contract,
// `ProtocolMismatchError` is the only throw type the parser may raise. Text
// deltas are incremental; the harness accumulates `EMPTY_PARTIAL` placeholders.
// ---------------------------------------------------------------------------

const EMPTY_PARTIAL: PartialMessage = { text: "" };

// The five payload kinds (`text`, `functionCall`, `inlineData`,
// `executableCode`, `codeExecutionResult`) are mutually exclusive; arktype's
// open-object semantics would admit multiples, so `parseResponse` enforces
// exclusivity via `assertSinglePayload`. `inlineData` is further constrained
// to `image/*` at the `emitPart` boundary.
//
// `thought: true` is only valid on a `text` part; `thoughtSignature` is the
// opaque per-thinking-block signature Gemini requires echoed back on
// follow-up turns.
const GeminiFunctionCallPayload = type({
  name: "string",
  args: "Record<string, unknown>",
});

const GeminiInlineDataPayload = type({
  mimeType: "string",
  data: "string",
});

const GeminiExecutableCodePayload = type({
  language: "string",
  code: "string",
});

const GeminiCodeExecutionResultPayload = type({
  outcome: "string",
  // Combined stdout+stderr; Gemini does not split them, so it lands in
  // `stdout` with `stderr` empty (see `CodeExecutionResultBlock`).
  "output?": "string",
});

const GeminiPart = type({
  "text?": "string",
  "thought?": "boolean",
  "thoughtSignature?": "string",
  "functionCall?": GeminiFunctionCallPayload,
  "inlineData?": GeminiInlineDataPayload,
  "executableCode?": GeminiExecutableCodePayload,
  "codeExecutionResult?": GeminiCodeExecutionResultPayload,
});

const GeminiContent = type({
  "parts?": GeminiPart.array(),
  "role?": "string",
});

// Grounding citation metadata. `groundingChunks[].web` are the sources
// (`{uri, title}`); `groundingSupports[]` pair an output text span
// (`segment`) with chunk indices. `searchEntryPoint` and `webSearchQueries`
// carry no per-span attribution and are not surfaced.
const GeminiGroundingChunk = type({
  // Only `web` chunks appear in the captured corpus; other kinds are
  // admitted as absences and skipped at emission.
  "web?": type({ uri: "string", title: "string" }),
});

const GeminiGroundingSupport = type({
  segment: {
    startIndex: "number",
    endIndex: "number",
    text: "string",
  },
  groundingChunkIndices: "number[]",
});

const GeminiGroundingMetadata = type({
  "groundingChunks?": GeminiGroundingChunk.array(),
  "groundingSupports?": GeminiGroundingSupport.array(),
});

const GeminiCandidate = type({
  "content?": GeminiContent,
  "finishReason?": "string",
  "index?": "number",
  "groundingMetadata?": GeminiGroundingMetadata,
});

// `thoughtsTokenCount` -> `TokenUsage.thinking`; `cachedContentTokenCount`
// -> `TokenUsage.cacheRead`. Absent when the feature is unused; the parser
// treats absence as zero.
const GeminiUsageMetadata = type({
  "promptTokenCount?": "number",
  "candidatesTokenCount?": "number",
  "totalTokenCount?": "number",
  "thoughtsTokenCount?": "number",
  "cachedContentTokenCount?": "number",
});

// Prompt-level safety signal. Captured 2026-07-28: `{ blockReason:
// "PROHIBITED_CONTENT" }` with no candidates. Only consumed fields are
// validated.
const GeminiPromptFeedback = type({
  "blockReason?": "string > 0",
});

const GeminiSSEEvent = type({
  "candidates?": GeminiCandidate.array(),
  "usageMetadata?": GeminiUsageMetadata,
  "promptFeedback?": GeminiPromptFeedback,
  // `modelVersion` and `responseId` are dropped: `AssistantTurn.model` comes
  // from the requested string, not the served `modelVersion` — which can
  // differ (requesting `gemini-2.5-flash` may return `gemini-2.5-flash-001`).
  "modelVersion?": "string",
  "responseId?": "string",
});

// Gemini provides no content-block index on the wire; block boundaries are
// positional. `currentBlock` extends consecutive same-kind parts and resets
// when a different kind appears; function-call blocks are atomic (one part =
// one tool call) and never become `currentBlock`.
interface GeminiParserState {
  nextBlockIndex: number;
  currentBlock: { kind: "text" | "thinking"; index: number } | null;
  // Depth-1 LIFO slot: the synthetic request id lands here on an
  // `executableCode` part and is consumed by the next `codeExecutionResult`.
  pendingExecutionRequestId: string | null;
}

function createParserState(): GeminiParserState {
  return {
    nextBlockIndex: 0,
    currentBlock: null,
    pendingExecutionRequestId: null,
  };
}

// Emit a signature event against the block's own index.
function emitBlockSignature(
  signature: string | undefined,
  index: number,
  seq: number,
  out: InferenceEvent[],
): void {
  if (signature === undefined) return;
  out.push({
    type: "inference.block.signature",
    seq,
    data: { signature, index },
  });
}

function openOrExtendBlock(
  state: GeminiParserState,
  kind: "text" | "thinking",
): number {
  if (state.currentBlock !== null && state.currentBlock.kind === kind) {
    return state.currentBlock.index;
  }
  closeCurrentBlock(state);
  const index = state.nextBlockIndex++;
  state.currentBlock = { kind, index };
  return index;
}

function closeCurrentBlock(state: GeminiParserState): void {
  state.currentBlock = null;
}

// Enforce payload exclusivity and `thought` placement, which arktype's
// open-object semantics would otherwise admit. Zero-payload parts pass only
// with a `thoughtSignature` present.
function assertSinglePayload(
  part: typeof GeminiPart.infer,
  raw: unknown,
): void {
  const payloads: string[] = [];
  if (part.text !== undefined) payloads.push("text");
  if (part.functionCall !== undefined) payloads.push("functionCall");
  if (part.inlineData !== undefined) payloads.push("inlineData");
  if (part.executableCode !== undefined) payloads.push("executableCode");
  if (part.codeExecutionResult !== undefined) {
    payloads.push("codeExecutionResult");
  }

  if (payloads.length > 1) {
    throw new ProtocolMismatchError(
      `google-genai parseResponse: part has multiple payload fields set ` +
        `(${payloads.join("+")}); exactly one of ` +
        `{text, functionCall, inlineData, executableCode, ` +
        `codeExecutionResult} must be present per Gemini wire convention.`,
      raw,
    );
  }
  if (payloads.length === 0 && part.thoughtSignature === undefined) {
    throw new ProtocolMismatchError(
      `google-genai parseResponse: part has no payload and no ` +
        `thoughtSignature; an empty part is not a defined wire shape.`,
      raw,
    );
  }
  // `thought` only discriminates thinking text from regular text.
  if (part.thought === true && part.text === undefined) {
    throw new ProtocolMismatchError(
      `google-genai parseResponse: \`thought: true\` set on a part with ` +
        `no \`text\` payload; the flag is only valid on text parts.`,
      raw,
    );
  }
}

function emitPart(
  part: typeof GeminiPart.infer,
  state: GeminiParserState,
  seq: number,
  out: InferenceEvent[],
  raw: unknown,
): void {
  assertSinglePayload(part, raw);

  if (part.text !== undefined && part.thought === true) {
    const index = openOrExtendBlock(state, "thinking");
    // Anchor the block so a later signature event targets an index the
    // harness has seen (same pattern as the Anthropic adapter).
    out.push({
      type: "inference.thinking.delta",
      seq,
      data: {
        token: part.text,
        partial: EMPTY_PARTIAL,
        index,
      },
    });
    emitBlockSignature(part.thoughtSignature, index, seq, out);
    return;
  }

  // Unsigned empty text is a true no-op: it neither opens nor closes a
  // block; signed empty text still opens one so the signature has a block.
  if (part.text !== undefined) {
    if (part.text === "" && part.thoughtSignature === undefined) {
      return;
    }
    const index = openOrExtendBlock(state, "text");
    out.push({
      type: "inference.text.delta",
      seq,
      data: {
        token: part.text,
        partial: EMPTY_PARTIAL,
        index,
      },
    });
    emitBlockSignature(part.thoughtSignature, index, seq, out);
    return;
  }

  if (part.functionCall !== undefined) {
    closeCurrentBlock(state);
    const fc = part.functionCall;
    const index = state.nextBlockIndex++;
    // Gemini's `functionCall` has no wire id; the harness keys end-to-end on
    // this synthetic callId (same fallback as the Anthropic adapter).
    const callId = String(index);

    out.push({
      type: "inference.tool_call.start",
      seq,
      data: {
        callId,
        name: decodeToolName(fc.name),
        partial: EMPTY_PARTIAL,
        index,
      },
    });
    // Args arrive complete in one part; emit them in one delta so
    // end-of-stream finalization produces the right `tool_call.end`.
    out.push({
      type: "inference.tool_call.delta",
      seq,
      data: {
        callId,
        argumentFragment: JSON.stringify(fc.args),
        partial: EMPTY_PARTIAL,
        index,
      },
    });
    emitBlockSignature(part.thoughtSignature, index, seq, out);
    return;
  }

  if (part.inlineData !== undefined) {
    // inlineData becomes an ImageBlock, so a non-image MIME would silently
    // mistype the payload; reject at the boundary.
    if (!part.inlineData.mimeType.startsWith("image/")) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: inlineData part has non-image ` +
          `mimeType ${JSON.stringify(part.inlineData.mimeType)}; the ` +
          `parser wraps inlineData as an ImageBlock and does not ` +
          `handle other modalities on this code path.`,
        raw,
      );
    }
    closeCurrentBlock(state);
    const index = state.nextBlockIndex++;
    out.push({
      type: "inference.image_output",
      seq,
      data: {
        image: {
          type: "image",
          source: {
            kind: "base64",
            mimeType: part.inlineData.mimeType,
            data: part.inlineData.data,
          },
        },
        index,
      },
    });
    emitBlockSignature(part.thoughtSignature, index, seq, out);
    return;
  }

  // Atomic request block. Synthetic id `gemini-exec-<index>` is
  // deterministic per response so replays match (`CodeExecutionRequestBlock.id`
  // contract); it lands in `pendingExecutionRequestId` for the result part.
  if (part.executableCode !== undefined) {
    // Check before mutating state so the throw rejects the part cleanly.
    if (state.pendingExecutionRequestId !== null) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: encountered a second executableCode ` +
          `part while the prior code-execution request ` +
          `${JSON.stringify(state.pendingExecutionRequestId)} is still ` +
          `unmatched. The wire convention is strict LIFO with depth 1 ` +
          `(request, then result); no fixture exercises depth > 1.`,
        raw,
      );
    }
    closeCurrentBlock(state);
    const index = state.nextBlockIndex++;

    const requestId = `gemini-exec-${String(index)}`;
    state.pendingExecutionRequestId = requestId;

    const ec = part.executableCode;
    const request: CodeExecutionRequestBlock = {
      type: "code_execution_request",
      id: requestId,
      code: ec.code,
      // Passed verbatim; Gemini emits SCREAMING_CASE (e.g. `"PYTHON"`) and
      // the type contract forbids defaulting it.
      language: ec.language,
    };
    out.push({
      type: "inference.code_execution.start",
      seq,
      data: { request, index },
    });
    emitBlockSignature(part.thoughtSignature, index, seq, out);
    return;
  }

  // Atomic result block; pairs with the preceding request via the slot
  // (Gemini's wire carries no back-pointer).
  if (part.codeExecutionResult !== undefined) {
    const requestId = state.pendingExecutionRequestId;
    if (requestId === null) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: codeExecutionResult part has no ` +
          `preceding executableCode part in this request to pair against.`,
        raw,
      );
    }
    // Run before mutating state so an unknown-outcome throw rejects the part
    // cleanly.
    const cer = part.codeExecutionResult;
    const status = outcomeToStatus(cer.outcome, raw);
    // Result blocks are not signable; a signature here is an unmodeled wire
    // shape.
    if (part.thoughtSignature !== undefined) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: codeExecutionResult part carries a ` +
          `thoughtSignature; the code_execution_result block is not signable.`,
        raw,
      );
    }

    closeCurrentBlock(state);
    const index = state.nextBlockIndex++;
    state.pendingExecutionRequestId = null;

    const result: CodeExecutionResultBlock = {
      type: "code_execution_result",
      requestId,
      status,
      ...(cer.output !== undefined ? { stdout: cer.output } : {}),
      providerOutcome: cer.outcome,
    };
    out.push({
      type: "inference.code_execution.result",
      seq,
      data: { result, index },
    });
    return;
  }

  // A signature needs a block to authenticate; with no payload there is none.
  if (part.thoughtSignature !== undefined) {
    throw new ProtocolMismatchError(
      `google-genai parseResponse: part carries a thoughtSignature but no ` +
        `payload; there is no block for the signature to authenticate.`,
      raw,
    );
  }

  // The schema admitted a payload field with no matching branch here.
  throw new ProtocolMismatchError(
    `google-genai parseResponse: unhandled part shape; the schema admits ` +
      `a payload field that emitPart has no branch for.`,
    raw,
  );
}

// One citation per referenced chunk: a span citing four sources yields four
// citations sharing `citedText`/`textOffset` with distinct `source` entries.
// Anchored to the current text block; grounding without a text anchor is a
// protocol mismatch rather than a synthesized index. Non-web chunks are skipped.
function emitGroundingCitations(
  metadata: typeof GeminiGroundingMetadata.infer,
  state: GeminiParserState,
  seq: number,
  out: InferenceEvent[],
  raw: unknown,
): void {
  const supports = metadata.groundingSupports ?? [];
  const chunks = metadata.groundingChunks ?? [];
  if (supports.length === 0) {
    return;
  }

  const anchor = state.currentBlock;
  if (anchor === null || anchor.kind !== "text") {
    throw new ProtocolMismatchError(
      `google-genai parseResponse: groundingMetadata arrived without a ` +
        `current text block to anchor citations against (currentBlock=` +
        `${anchor === null ? "null" : JSON.stringify(anchor.kind)}). The ` +
        `wire convention places groundingMetadata on the terminal event ` +
        `alongside the text it grounds.`,
      raw,
    );
  }
  const index = anchor.index;

  for (const support of supports) {
    const { segment, groundingChunkIndices } = support;
    for (const chunkIdx of groundingChunkIndices) {
      const chunk = chunks[chunkIdx];
      if (chunk === undefined) {
        throw new ProtocolMismatchError(
          `google-genai parseResponse: groundingSupport references ` +
            `chunk index ${String(chunkIdx)} but the response has only ` +
            `${String(chunks.length)} grounding chunk(s).`,
          raw,
        );
      }
      const web = chunk.web;
      if (web === undefined) {
        // Non-web chunks have no `uri`/`title`; skip rather than synthesize.
        continue;
      }
      const citation = {
        type: "citation" as const,
        citedText: segment.text,
        source: {
          uri: web.uri,
          title: web.title,
        },
        textOffset: {
          start: segment.startIndex,
          end: segment.endIndex,
        },
      };
      out.push({
        type: "inference.citation",
        seq,
        data: { citation, index },
      });
    }
  }
}

// Exhaustive over the three outcomes Gemini documents; an unknown value
// throws rather than falling into a default, so new outcomes are deliberate
// changes.
function outcomeToStatus(
  outcome: string,
  raw: unknown,
): "ok" | "error" | "aborted" | "timeout" {
  switch (outcome) {
    case "OUTCOME_OK":
      return "ok";
    case "OUTCOME_FAILED":
      return "error";
    case "OUTCOME_DEADLINE_EXCEEDED":
      return "timeout";
    default:
      throw new ProtocolMismatchError(
        `google-genai parseResponse: unknown codeExecutionResult.outcome ` +
          `${JSON.stringify(outcome)}; the mapping recognizes ` +
          `OUTCOME_OK, OUTCOME_FAILED, OUTCOME_DEADLINE_EXCEEDED. ` +
          `A new outcome value is a deliberate adapter change, not a ` +
          `silent fallback.`,
        raw,
      );
  }
}

function parseResponse(
  sseData: string,
  state: GeminiParserState,
  source: LastCycleSource,
): InferenceEvent[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(sseData);
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new ProtocolMismatchError(
      `google-genai parseResponse: malformed JSON in SSE data payload: ${message}`,
      sseData,
    );
  }

  const event = GeminiSSEEvent(parsed);
  if (event instanceof type.errors) {
    throw new ProtocolMismatchError(
      `google-genai parseResponse: SSE event failed schema validation: ${event.summary}`,
      parsed,
    );
  }

  const candidates = event.candidates ?? [];

  // `buildRequest` never asks for more than one candidate; a multi-candidate
  // response diverges from the request, so fail loudly rather than pick
  // `[0]`.
  if (candidates.length > 1) {
    throw new ProtocolMismatchError(
      `google-genai parseResponse: expected at most one candidate, got ${String(candidates.length)}.`,
      parsed,
    );
  }

  // Placeholder; the harness assigns real sequence numbers.
  const seq = 0;
  const out: InferenceEvent[] = [];

  const candidate = candidates[0];
  if (candidate?.content?.parts !== undefined) {
    for (const part of candidate.content.parts) {
      emitPart(part, state, seq, out, parsed);
    }
  }

  // After parts, so same-event text deltas settle currentBlock first;
  // citations precede the terminal usage emission.
  if (candidate?.groundingMetadata !== undefined) {
    emitGroundingCitations(
      candidate.groundingMetadata,
      state,
      seq,
      out,
      parsed,
    );
  }

  // Captured shape (2026-07-28): HTTP 200 with `blockReason` and zero
  // candidates. Terminal path: emit the safety event, then usage. Not an
  // `inference.error` — the transport succeeded.
  const blockReason = event.promptFeedback?.blockReason;
  if (blockReason !== undefined) {
    out.push({
      type: "inference.safety_rating",
      seq,
      data: {
        safetyRating: {
          type: "safety_rating",
          blockReason,
        },
      },
    });
    const usage = event.usageMetadata;
    if (usage === undefined) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: promptFeedback.blockReason terminal event missing usageMetadata.`,
        parsed,
      );
    }
    out.push({
      type: "inference.usage",
      seq,
      data: {
        usage: {
          input: usage.promptTokenCount ?? 0,
          output: usage.candidatesTokenCount ?? 0,
          cacheRead: usage.cachedContentTokenCount ?? 0,
          cacheWrite: 0,
          thinking: usage.thoughtsTokenCount ?? 0,
        },
        source,
      },
    });
    return out;
  }

  // `usageMetadata` is cumulative, so the terminal snapshot is the final
  // count. `MAX_TOKENS`/`SAFETY`/`RECITATION`/`OTHER` don't yet surface as
  // `inference.error` — that needs fixtures showing the full error envelope;
  // candidate-level `safetyRatings` likewise.
  if (candidate?.finishReason !== undefined) {
    const usage = event.usageMetadata;
    if (usage === undefined) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: terminal event (finishReason=${JSON.stringify(candidate.finishReason)}) missing usageMetadata.`,
        parsed,
      );
    }
    const tokenUsage: TokenUsage = {
      input: usage.promptTokenCount ?? 0,
      output: usage.candidatesTokenCount ?? 0,
      // Single counter; the API does not split read/write the way Anthropic does.
      cacheRead: usage.cachedContentTokenCount ?? 0,
      cacheWrite: 0,
      thinking: usage.thoughtsTokenCount ?? 0,
    };
    out.push({
      type: "inference.usage",
      seq,
      data: { usage: tokenUsage, source },
    });

    if (state.pendingExecutionRequestId !== null) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: response terminated with an ` +
          `unmatched code-execution request ` +
          `${JSON.stringify(state.pendingExecutionRequestId)}; the wire ` +
          `must deliver a codeExecutionResult part before the terminal ` +
          `finishReason.`,
        parsed,
      );
    }
  }

  return out;
}

// A non-streaming response is one terminal SSE event, so decode it through
// the same parser with fresh per-call state — parity by construction, since
// nothing in the parser branches on event boundaries.
function parseJSONResponse(
  body: string,
  source: LastCycleSource,
): InferenceEvent[] {
  const events = parseResponse(body, createParserState(), source);
  // A complete body must be terminal; both terminal paths emit
  // `inference.usage`, so its absence marks a truncated or malformed
  // capture.
  if (!events.some((e) => e.type === "inference.usage")) {
    throw new ProtocolMismatchError(
      `google-genai parseJSONResponse: non-streaming body carried no terminal ` +
        `finishReason or promptFeedback.blockReason; a complete ` +
        `generateContent response must be terminal and emit usage.`,
      body,
    );
  }
  return events;
}

// No per-source accommodations today; rejecting unknown keys makes a
// misconfigured bag fail loudly at the boundary.
export const GoogleGenAIQuirks = type({ "+": "reject" });
export type GoogleGenAIQuirks = typeof GoogleGenAIQuirks.infer;

export function createGoogleGenAIAdapter(
  source: LastCycleSource,
  quirks?: unknown,
): ProviderAdapter {
  const parsedQuirks = GoogleGenAIQuirks(quirks ?? {});
  if (parsedQuirks instanceof type.errors) {
    throw new Error(
      `google-genai adapter: invalid quirks: ${parsedQuirks.summary}`,
    );
  }

  // Block-index allocation and code-execution pairing span SSE events, so
  // state lives in the closure; only `parseResponse` touches it.
  const state = createParserState();
  return {
    buildRequest,
    parseResponse: (sseData) => parseResponse(sseData, state, source),
    parseJSONResponse: (body) => parseJSONResponse(body, source),
  };
}
