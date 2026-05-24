import { type } from "arktype";

import type {
  ConversationTurn,
  ContentBlock,
  InferenceEvent,
  InferenceOptions,
  MediaSource,
  PartialMessage,
  TokenUsage,
} from "@intx/types/runtime";
import type { ProviderAdapter, BuiltRequest } from "../adapter";
import { CREDENTIAL_SENTINEL } from "../auth";
import { ProtocolMismatchError } from "../errors";

// Runtime validator for "parsed JSON value is a plain object." Used
// by `tryParseJSONObject` to narrow `JSON.parse(string)` from its
// declared `unknown` return into a `Record<string, unknown>` without a
// type assertion -- the assertion would be a compile-time lie about
// runtime shape (per the project style guide), and arktype gives an
// honest runtime check.
const ParsedJSONObject = type("Record<string, unknown>");

// ---------------------------------------------------------------------------
// Request building
//
// Translates the internal ConversationTurn[] format into Gemini's
// `generateContent` / `streamGenerateContent` request body. The harness
// always streams, so the URL pins `:streamGenerateContent?alt=sse`.
//
// `parseResponse` throws unconditionally: a live call surfaces the
// missing parser via the harness's standard inference.error path
// rather than silently dropping events.
// ---------------------------------------------------------------------------

function buildRequest(
  messages: ConversationTurn[],
  model: string,
  options: InferenceOptions,
): BuiltRequest {
  const systemMessages = messages.filter((m) => m.role === "system");
  const conversationMessages = messages.filter((m) => m.role !== "system");

  // System text: concatenated from any system turns in history, unless
  // the caller overrides via `options.systemPrompt`. Matches the
  // precedence used by the Anthropic adapter. Non-text blocks in a
  // system turn surface as an error rather than a silent drop -- the
  // rest of the file fails loudly on unsupported block kinds and this
  // boundary holds the same discipline.
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

  // A `callId -> functionName` lookup, built once per request from
  // every prior assistant `tool_call` block. Gemini's
  // `functionResponse` part requires the function name (Anthropic
  // requires the callId); the internal `ToolResultBlock` carries only
  // the callId, so the name comes from the assistant turn that
  // produced the matching `tool_call`. Built once because a per-block
  // walk would be O(N^2) in turn count.
  const callIdToFunctionName = buildCallIdToFunctionName(messages);

  const contents: GeminiContent[] = conversationMessages.map((msg) =>
    toGeminiContent(msg, callIdToFunctionName),
  );

  const body: Record<string, unknown> = { contents };

  if (effectiveSystem !== undefined) {
    body["systemInstruction"] = { parts: [{ text: effectiveSystem }] };
  }

  if (options.tools !== undefined && options.tools.length > 0) {
    body["tools"] = [
      {
        functionDeclarations: options.tools.map((t) => ({
          name: t.name,
          description: t.description,
          parameters: t.inputSchema,
        })),
      },
    ];
  }

  const generationConfig = buildGenerationConfig(options);
  if (generationConfig !== undefined) {
    body["generationConfig"] = generationConfig;
  }

  // Caller escape hatch. Documented as shallow-merge over the body
  // top-level: a caller passing `providerOptions.generationConfig`
  // wholesale replaces the object built above. Same shape semantics as
  // the `InferenceOptions.providerOptions` contract on every other
  // adapter -- the caller owns the consequences of clobbering a
  // structured key.
  if (options.providerOptions !== undefined) {
    Object.assign(body, options.providerOptions);
  }

  // Escape the model name in the URL path. `encodeURIComponent` is a
  // no-op on the legitimate Gemini model names in use today
  // (alphanumerics, hyphens, periods are all reserved-safe), but
  // guards against future model values that arrive from outside
  // trusted configuration. The trailing `:streamGenerateContent?alt=sse`
  // sits outside the substitution so its colon and query string
  // survive intact.
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

interface GeminiTextPart {
  text: string;
}
interface GeminiInlineDataPart {
  inlineData: { mimeType: string; data: string };
}
interface GeminiFileDataPart {
  fileData: { mimeType: string; fileUri: string };
}
interface GeminiFunctionCallPart {
  functionCall: { name: string; args: Record<string, unknown> };
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
  // Role/block pairing: Gemini wants `functionCall` parts only on
  // `model`-role contents and `functionResponse` parts only on
  // `user`-role contents. The internal `ContentBlock` union does not
  // enforce the pairing on its own, so misrouted blocks (a `tool_call`
  // on a user turn, a `tool_result` on an assistant turn) would
  // otherwise reach Gemini and return an opaque 400. Catch them at
  // the marshaling boundary with diagnostic context instead.
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
      return { text: block.text };

    case "image":
    case "document":
    case "audio":
    case "video":
      return toGeminiMediaPart(block.source);

    case "tool_call":
      return {
        functionCall: { name: block.name, args: block.arguments },
      };

    case "tool_result":
      return toGeminiFunctionResponse(block, callIdToFunctionName);

    case "thinking":
      // Echoing assistant thinking back requires Gemini's
      // `thoughtSignature` round-trip, which the adapter does not
      // emit. Surface the gap rather than send a request that
      // silently strips thinking content.
      throw new Error(
        "Google GenAI adapter does not handle thinking content blocks " +
          "in incoming turns.",
      );

    case "redacted_thinking":
      // Gemini does not emit redacted-thinking blocks; a caller
      // passing one in is mixing wire formats. Surface the mismatch
      // loudly rather than dropping it silently.
      throw new Error(
        "Google GenAI adapter does not handle redacted_thinking blocks; " +
          "they are Anthropic-specific.",
      );

    case "citation":
      // Citations are output-only blocks: the model produces them as
      // grounding/source references for its own text. Echoing one
      // back in an input turn has no defined wire shape and is almost
      // certainly a caller bug -- fail rather than send a nonsense
      // request.
      throw new Error(
        "Google GenAI adapter does not echo citation blocks; citations " +
          "are emitted by the model, not sent to it.",
      );

    case "code_execution_request":
    case "code_execution_result":
      // Code-execution round-trip needs Gemini's
      // `executableCode`/`codeExecutionResult` part shapes, which
      // the adapter does not emit. Surface the gap rather than
      // produce a request with these blocks missing.
      throw new Error(
        `Google GenAI adapter does not handle ${block.type} content blocks.`,
      );
  }
}

// Marshal an internal MediaSource into a Gemini part. `base64`
// inlines the bytes; `file-reference` and `url` both target Gemini's
// `fileData` with `fileUri` -- the Files API returns URIs, and Gemini
// also accepts public HTTP(S) URLs through the same field.
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
  // Exhaustiveness: a new MediaSource variant added without a case
  // here fails this compile-time check.
  source satisfies never;
  throw new Error(`unreachable: unknown MediaSource kind`);
}

// Marshal a tool_result into Gemini's functionResponse part shape.
// The contract is deliberately strict: Gemini's `response` is a JSON
// object, and a permissive "guess at the shape" mapping silently
// reshapes payloads when callers don't intend it. The four accepted
// shapes are:
//
//   - exactly one text block whose text parses as a plain JSON object
//     -> that object becomes `response`
//   - exactly one text block whose text does not parse as an object
//     -> `{ result: text }` (or `{ error: text }` when isError is true)
//   - zero or multiple text blocks -> throw; the caller must collapse
//     to a single text block before handing the tool_result to the
//     adapter
//   - any non-text block (image/audio/video/document) inside the
//     tool_result -> throw; Gemini's functionResponse accepts no media
//
// The unknown-callId case throws with the unknown id and the set of
// known ids so a malformed conversation surfaces at the marshaling
// site instead of as an opaque HTTP 400 a round-trip later.
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

  return { functionResponse: { name, response } };
}

// Returns the parsed value when `text` is a JSON-encoded plain
// object, or `null` for any other shape: arrays, primitives
// (numbers, strings, booleans, null), and JSON parse errors all map
// to `null`. Wrapping is the responsibility of the caller -- this
// helper only confirms "is the text exactly a JSON object we can use
// verbatim."
function tryParseJSONObject(text: string): Record<string, unknown> | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  // `ParsedJSONObject` (arktype `Record<string, unknown>`) accepts
  // arrays -- in arktype's view an array IS a record with
  // numeric-string keys -- so the array-rejection has to happen
  // before the validator runs. Without this guard, a tool that
  // returns `"[1,2,3]"` would be silently promoted to a `response`
  // shape Gemini cannot consume.
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
  options: InferenceOptions,
): Record<string, unknown> | undefined {
  const config: Record<string, unknown> = {};

  if (options.maxTokens !== undefined) {
    config["maxOutputTokens"] = options.maxTokens;
  }
  if (options.temperature !== undefined) {
    config["temperature"] = options.temperature;
  }

  // thinking.enabled === true  -> include a budget (default 1024) and
  //                                ask Gemini to surface thought parts
  // thinking.enabled === false -> set the budget to 0 to disable
  //                                thinking; Gemini's 2.5-series default
  //                                is NOT zero, so "thinking off" needs
  //                                an explicit signal
  // thinking absent            -> omit thinkingConfig entirely; Gemini
  //                                uses the model's default
  if (options.thinking !== undefined) {
    if (options.thinking.enabled) {
      const thinkingBudget = options.thinking.budgetTokens ?? 1024;
      config["thinkingConfig"] = {
        thinkingBudget,
        includeThoughts: true,
      };
    } else {
      config["thinkingConfig"] = { thinkingBudget: 0 };
    }
  }

  if (
    options.responseModalities !== undefined &&
    options.responseModalities.length > 0
  ) {
    config["responseModalities"] =
      options.responseModalities.map(toGeminiModality);
  }

  return Object.keys(config).length === 0 ? undefined : config;
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
// Each Gemini SSE event is one complete JSON object delivered through
// `parseSSE` (event boundary `\n\n`); a partial JSON would mean the
// SSE framing layer broke its contract, not a Gemini protocol
// violation. Per the adapter contract in
// `packages/inference/src/adapter.ts`, `ProtocolMismatchError` is the
// only throw type the parser is allowed to raise -- the harness's
// `classifyStreamError` recognizes it.
//
// Text deltas on the Gemini wire are incremental: each event carries
// only the new tokens, not the accumulated text. The harness owns
// partial-state accumulation; the parser emits placeholder
// `EMPTY_PARTIAL` and the harness fills the real value in.
// ---------------------------------------------------------------------------

const EMPTY_PARTIAL: PartialMessage = { text: "" };

// Wire shape: every field is optional. Gemini emits candidates without
// content during safety-filter rejections, sends events with only
// `usageMetadata` populated, and may omit `finishReason` on every
// event except the terminal one. The parser handles the absences
// directly rather than via schema-default coercion.
const GeminiPart = type({
  "text?": "string",
});

const GeminiContent = type({
  "parts?": GeminiPart.array(),
  "role?": "string",
});

const GeminiCandidate = type({
  "content?": GeminiContent,
  "finishReason?": "string",
  "index?": "number",
});

// `cachedContentTokenCount` is present when context caching is in
// use; the plain-text streaming path does not exercise caching so
// the field is absent here. The thinking commit will revisit
// `thoughtsTokenCount` once thought-part round-trip lands; for now
// it is also absent on the plain-text wire (thinking budget = 0
// when streaming plain text per the discovery-side body builder).
const GeminiUsageMetadata = type({
  "promptTokenCount?": "number",
  "candidatesTokenCount?": "number",
  "totalTokenCount?": "number",
  "thoughtsTokenCount?": "number",
  "cachedContentTokenCount?": "number",
});

const GeminiSSEEvent = type({
  "candidates?": GeminiCandidate.array(),
  "usageMetadata?": GeminiUsageMetadata,
  // `modelVersion` and `responseId` are dropped at this layer. The
  // harness's `AssistantTurn.model` is set from the requested model
  // string, not from the served `modelVersion` -- which can differ
  // (`gemini-2.5-flash` requested may return `gemini-2.5-flash-001`).
  // Surfacing the served version is a separate concern; for now the
  // request-side identifier is what downstream consumers see.
  "modelVersion?": "string",
  "responseId?": "string",
});

function parseResponse(sseData: string): InferenceEvent[] {
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

  // The adapter's `buildRequest` never requests `candidateCount > 1`,
  // so a multi-candidate response means the wire shape diverged from
  // what was requested. Surface the mismatch loudly with the full
  // payload in `error.raw` rather than silently picking `[0]`.
  if (candidates.length > 1) {
    throw new ProtocolMismatchError(
      `google-genai parseResponse: expected at most one candidate, got ${String(candidates.length)}.`,
      parsed,
    );
  }

  // The seq field is a placeholder 0 -- the harness assigns real
  // sequence numbers.
  const seq = 0;
  const out: InferenceEvent[] = [];

  // Plain-text streaming has a single logical block at index 0. The
  // multi-block indexer (text + image, text + functionCall, ...)
  // arrives when those part kinds land; for now `index: 0` is
  // correct by construction and any future kind would need its own
  // index allocator.
  const blockIndex = 0;

  const candidate = candidates[0];
  if (candidate?.content?.parts !== undefined) {
    for (const part of candidate.content.parts) {
      if (part.text === undefined || part.text === "") {
        // Empty text parts are dropped. The single-block plain-text
        // path needs no per-index anchoring, so there is no reason
        // to emit a zero-token delta. Future multi-block paths may
        // need an anchoring emission for empty parts; revisit when
        // adding those kinds.
        continue;
      }
      out.push({
        type: "inference.text.delta",
        seq,
        data: {
          token: part.text,
          partial: EMPTY_PARTIAL,
          index: blockIndex,
        },
      });
    }
  }

  // `finishReason` arrives only on the terminal event. Emit usage at
  // exactly that point: Gemini's `usageMetadata` is cumulative in
  // every event, so the terminal-event snapshot is the final count
  // and intermediate emissions would be pure noise that the
  // harness's `inference.done` would discard anyway.
  //
  // `MAX_TOKENS`, `SAFETY`, `RECITATION`, and `OTHER` reach this
  // layer but do not yet surface as `inference.error` -- emitting
  // those needs fixtures showing the full error envelope shape,
  // which the plain-text path does not exercise.
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
      // Gemini exposes context caching via `cachedContentTokenCount`
      // (single counter; the API does not distinguish "read" from
      // "write" the way Anthropic does). The plain-text path does
      // not exercise caching, so the field is absent here. A future
      // caching commit decides whether to route the count into
      // `cacheRead` or carry both fields.
      cacheRead: usage.cachedContentTokenCount ?? 0,
      cacheWrite: 0,
      // `thoughtsTokenCount` is absent on the plain-text wire
      // (thinking budget is zero). When the thinking commit lands,
      // it will set this from the wire.
      thinking: usage.thoughtsTokenCount ?? 0,
    };
    out.push({
      type: "inference.usage",
      seq,
      data: { usage: tokenUsage },
    });
  }

  return out;
}

export function createGoogleGenAIAdapter(): ProviderAdapter {
  // Both functions are pure -- no per-request state is needed for
  // the plain-text path. State enters this adapter when the
  // multimodal/function-calling/thinking paths land and require
  // cross-event coordination (e.g., a callId-to-block-index map
  // analogous to the anthropic adapter's `blockIndexToCallId`).
  return { buildRequest, parseResponse };
}
