import { type } from "arktype";

import type {
  CodeExecutionRequestBlock,
  CodeExecutionResultBlock,
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

// Round-trip wire shapes. `thought` and `thoughtSignature` are
// Gemini-specific metadata that ride alongside the payload-bearing
// fields; both are optional on every part. The translation produces
// a `text` part with `thought: true` for `ThinkingBlock`s, and
// stashes signatures onto the follow-on non-thinking part per the
// pairing logic in `toGeminiContent`.
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

  // Positional signature pairing: a `ThinkingBlock` with a signature
  // contributes both a `{text, thought: true}` part (no signature on
  // it) and a stashed signature that attaches to the NEXT
  // non-thinking part in the turn. The wire convention from the
  // captured fixtures places `thoughtSignature` on the follow-on
  // part (typically `functionCall`), not on the thinking text. Two
  // pending signatures in a row, or a turn ending with a signature
  // still pending, are encoded as errors: the corpus contains no
  // fixture for those shapes and a silent drop would corrupt the
  // signed-thinking round-trip Gemini requires.
  const parts: GeminiPart[] = [];
  let pendingSignature: string | null = null;
  for (const block of msg.content) {
    const part = toGeminiPart(block, callIdToFunctionName);
    const isThinkingPart =
      "text" in part && (part as GeminiTextPart).thought === true;
    if (isThinkingPart) {
      if (pendingSignature !== null) {
        throw new Error(
          `Google GenAI adapter: encountered a second thinking block on ` +
            `assistant turn while a prior thinking-block signature is ` +
            `still awaiting a carrier part; the wire convention pairs ` +
            `each signed thinking block 1:1 with the next non-thinking ` +
            `part.`,
        );
      }
      // Stash the signature off the thinking block (if any) for the
      // next non-thinking part to claim. `toGeminiPart` already
      // produced a thinking part WITHOUT the signature on it, per
      // the wire shape.
      if (block.type === "thinking" && block.signature !== undefined) {
        pendingSignature = block.signature;
      }
      parts.push(part);
      continue;
    }

    if (pendingSignature !== null) {
      // Attach the stashed signature to this non-thinking part. The
      // mutation matches Gemini's wire shape exactly: the part keeps
      // its existing payload and grows a `thoughtSignature` field.
      (part as GeminiPart & { thoughtSignature?: string }).thoughtSignature =
        pendingSignature;
      pendingSignature = null;
    }
    parts.push(part);
  }

  if (pendingSignature !== null) {
    throw new Error(
      `Google GenAI adapter: assistant turn ends with a thinking-block ` +
        `signature awaiting a carrier part. Gemini's wire convention ` +
        `requires the signature to ride on a follow-on non-thinking part ` +
        `(typically a functionCall); a signed thinking block with no ` +
        `follow-on part has no defined wire shape.`,
    );
  }

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
      // Thinking text is translated WITHOUT the signature on this
      // part. `toGeminiContent`'s positional pairing logic stashes
      // the signature off the block and attaches it to the next
      // non-thinking part in the same turn (which is where Gemini's
      // wire format expects to see `thoughtSignature`). If the
      // signature were attached here, both this part and the
      // following part would carry it, producing a malformed
      // request.
      return { text: block.thinking, thought: true };

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

    case "refusal":
      // Refusal blocks are an OpenAI strict-mode output shape and have
      // no Gemini wire equivalent. Echoing one back into a Gemini
      // request has no defined translation; fail loudly at the
      // marshaling site rather than silently drop the block.
      throw new Error(
        "Google GenAI adapter does not handle refusal content blocks; " +
          "they are emitted by OpenAI strict-mode structured outputs.",
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
//
// The schema models the five payload kinds the parser handles:
// `text`, `functionCall`, `inlineData` (image output),
// `executableCode` (code-execution request), and
// `codeExecutionResult` (code-execution result). They are mutually
// exclusive on the wire: a single part is one kind of content.
// Arktype's open-object semantics will accept multiple set
// simultaneously, so `parseResponse` enforces the exclusivity at
// the boundary via `assertSinglePayload` and throws
// `ProtocolMismatchError` on a violation. `inlineData` is
// additionally constrained to `image/*` MIME types at the
// `emitPart` boundary; a non-image MIME on `inlineData` is treated
// as a wire shape the parser does not handle (rather than silently
// wrapping arbitrary bytes as an ImageBlock).
//
// `thought` and `thoughtSignature` are metadata that ride alongside
// the payload: `thought: true` is only meaningful on a `text` part
// (a non-text part with `thought: true` is a wire violation rejected
// at the boundary), and `thoughtSignature` carries the opaque
// per-thinking-block signature that Gemini requires echoed back on
// follow-up turns. Both can be absent.
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
  // The combined stdout/stderr stream. Gemini does not split the
  // streams; the parser routes this verbatim into the result
  // block's `stdout` and leaves `stderr` empty (per the contract
  // documented on `CodeExecutionResultBlock`).
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

// Grounding metadata rides on a candidate whenever the request
// enabled `tools: [{googleSearch: {}}]`. The captured fixture
// shows `groundingMetadata: {}` present on every SSE event with
// `groundingChunks`/`groundingSupports` populated only on the
// terminal event; intermediate empty-metadata events short-circuit
// in `emitGroundingCitations` via the `supports.length === 0`
// early return. The two arrays the parser consumes are:
//
//   - `groundingChunks[].web`: per-source `{uri, title}` entries.
//     Indexed positionally; the chunks are the citation sources.
//
//   - `groundingSupports[]`: pairings between an output text span
//     (`segment: {startIndex, endIndex, text}`) and one or more
//     chunk indices (`groundingChunkIndices: number[]`). Each
//     index-into-chunks expands into one CitationBlock during
//     emission.
//
// `searchEntryPoint` (HTML rendering widget) and `webSearchQueries`
// (the model-issued queries) carry no per-text-span attribution and
// are not surfaced as citation blocks. Validating them here would
// pin a wire shape the parser does not consume; the schema admits
// them implicitly via arktype's open-object semantics.
const GeminiGroundingChunk = type({
  // Each chunk currently arrives with a single `web` shape. Other
  // chunk kinds (e.g. document, retrieved-context) are not in the
  // captured corpus; admitting them as schema-validated absences
  // keeps `web`-shaped chunks well-typed without committing to a
  // discriminated union the parser cannot dispatch over.
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

// `thoughtsTokenCount` is populated on responses with thinking
// enabled; it maps directly onto `TokenUsage.thinking`.
// `cachedContentTokenCount` is populated when context caching is in
// use and maps onto `TokenUsage.cacheRead`. Both are absent on
// responses that don't exercise the corresponding feature, and the
// parser treats absence as zero.
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

// Per-request parser state. Gemini provides no explicit content-block
// index on the wire -- block boundaries are positional, derived from
// the order and kind of parts. The parser allocates indices itself
// and coalesces consecutive same-kind parts into one logical block.
//
//   - `nextBlockIndex` is the monotonic counter for newly allocated
//     blocks across the entire request (incremented on each
//     allocation, never reset).
//
//   - `currentBlock` is the in-progress block that subsequent
//     same-kind parts extend. Reset to `null` when a different-kind
//     part appears -- the next part of any kind starts a fresh block.
//     Function-call blocks are atomic (a single part = a complete
//     tool call) and never become the `currentBlock`.
//
//   - `pendingSignatureAnchor` is set when a thinking block closes
//     and cleared when the next non-thinking part that carries
//     `thoughtSignature` attaches its signature to that index. A
//     standalone signature-only part (no payload) also consumes the
//     anchor. The lifecycle is deliberately narrow: keeping a
//     long-lived "most recent thinking block" pointer would let a
//     signature on, say, the third functionCall attach to the first
//     thinking block when two unrelated functionCalls happened in
//     between. The wire convention is "the signature belongs to the
//     immediately preceding thinking," and the state encodes exactly
//     that.
interface GeminiParserState {
  nextBlockIndex: number;
  currentBlock: { kind: "text" | "thinking"; index: number } | null;
  pendingSignatureAnchor: number | null;
  // Unmatched-request stack of depth 1: when the parser emits an
  // `inference.code_execution.start` for an `executableCode` part,
  // the synthetic request id lands here and is consumed by the
  // immediately-following `codeExecutionResult` part. The wire
  // convention (from the captured Gemini fixture) is strict LIFO
  // with depth 1: request, then result, then optional follow-on
  // text. The depth-1 invariant is enforced: a second request
  // arriving while the slot is occupied, a result arriving with
  // the slot empty, and a non-empty slot at the end of a response
  // all throw `ProtocolMismatchError`.
  pendingExecutionRequestId: string | null;
}

function createParserState(): GeminiParserState {
  return {
    nextBlockIndex: 0,
    currentBlock: null,
    pendingSignatureAnchor: null,
    pendingExecutionRequestId: null,
  };
}

// Open or extend a text/thinking block, returning the block index.
// A part of the same kind as the current block extends it; a part of
// a different kind closes the current block and allocates a new
// index. Closing a thinking block stashes its index in
// `pendingSignatureAnchor` so a subsequent non-thinking part's
// `thoughtSignature` can attach to it.
function openOrExtendBlock(
  state: GeminiParserState,
  kind: "text" | "thinking",
  rawForError: unknown,
): number {
  if (state.currentBlock !== null && state.currentBlock.kind === kind) {
    return state.currentBlock.index;
  }
  closeCurrentBlock(state, rawForError);
  const index = state.nextBlockIndex++;
  state.currentBlock = { kind, index };
  return index;
}

// Close the current text/thinking block. A thinking block being
// closed sets `pendingSignatureAnchor` so the next non-thinking part
// can claim it for its `thoughtSignature`. If two thinking blocks
// close in a row without an intervening signature consumer, surface
// it loudly -- the corpus has no fixture exercising that shape and
// silently overwriting the anchor would route a signature to the
// wrong block.
function closeCurrentBlock(
  state: GeminiParserState,
  rawForError: unknown,
): void {
  if (state.currentBlock?.kind === "thinking") {
    if (state.pendingSignatureAnchor !== null) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: second thinking block closed with a ` +
          `prior signature anchor still pending (anchor block index ` +
          `${String(state.pendingSignatureAnchor)}); the wire convention ` +
          `pairs each thinking block 1:1 with the next non-thinking ` +
          `carrier and the corpus contains no fixture for the unpaired ` +
          `case.`,
        rawForError,
      );
    }
    state.pendingSignatureAnchor = state.currentBlock.index;
  }
  state.currentBlock = null;
}

// Enforce mutual exclusivity of payload-bearing fields and correct
// placement of the `thought` flag on a single part. The schema
// models five payload fields (`text`, `functionCall`, `inlineData`,
// `executableCode`, `codeExecutionResult`); arktype's open-object
// semantics would otherwise admit a part with more than one set,
// or with `thought: true` on a non-text part. Both are wire
// violations and surface as `ProtocolMismatchError` here. A part
// with zero payload fields is only legal when a `thoughtSignature`
// is present (signature-carrier-only part, not seen in the current
// corpus but spec-permitted).
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
  // `thought: true` is only meaningful on a text part; the flag's
  // sole purpose is to discriminate thinking text from regular
  // assistant text. A `thought` flag on a `functionCall` part or a
  // payload-free part has no defined wire interpretation.
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

  // text part with `thought: true` -- belongs to a thinking block.
  if (part.text !== undefined && part.thought === true) {
    const index = openOrExtendBlock(state, "thinking", raw);
    // Anchor the block in the harness's per-index map. An empty
    // text part with only a `thoughtSignature` would otherwise route
    // the signature to an index the harness has never seen. The
    // empty-token delta mirrors the Anthropic adapter's anchoring
    // pattern for the same invariant.
    out.push({
      type: "inference.thinking.delta",
      seq,
      data: {
        token: part.text,
        partial: EMPTY_PARTIAL,
        index,
      },
    });
    // A thinking part may itself carry a signature (signature on the
    // thinking part rather than on a follow-on functionCall). Attach
    // it directly to this thinking block's index; it consumes any
    // pending anchor too because the signature on `this` thinking
    // part takes precedence.
    if (part.thoughtSignature !== undefined) {
      out.push({
        type: "inference.thinking.signature",
        seq,
        data: { signature: part.thoughtSignature, index },
      });
      state.pendingSignatureAnchor = null;
    }
    return;
  }

  // text part without `thought` -- belongs to a text block.
  if (part.text !== undefined) {
    if (part.text === "") {
      // Empty text parts emit no delta. A signature-bearing
      // empty-text part is still the carrier opportunity for any
      // open thinking block: close the current block first so the
      // thinking-block index lands in `pendingSignatureAnchor`,
      // then consume the signature against it. Without that claim
      // path, the signature would silently evaporate (the payload
      // has nowhere else to surface) -- the empty payload is the
      // ONLY signal Gemini sends for an authenticated empty-text
      // carrier. An empty-text part without a signature is a true
      // no-op -- it neither closes the current block nor consumes
      // the carrier opportunity, so a follow-on same-kind part
      // extends what was open.
      if (part.thoughtSignature !== undefined) {
        closeCurrentBlock(state, raw);
        consumeSignature(state, part.thoughtSignature, seq, out, raw);
      }
      return;
    }
    const index = openOrExtendBlock(state, "text", raw);
    out.push({
      type: "inference.text.delta",
      seq,
      data: {
        token: part.text,
        partial: EMPTY_PARTIAL,
        index,
      },
    });
    // Settle the carrier opportunity. A `thoughtSignature` on the
    // part consumes the pending anchor (the signature
    // authenticates the preceding thinking, not the text block);
    // a signature-less part still ends the carrier opportunity by
    // discarding the anchor. The wire convention is that the FIRST
    // non-thinking part after a thinking block is the only carrier
    // chance -- a later thinking block cannot retroactively claim
    // a stale anchor.
    settleCarrierOpportunity(state, part.thoughtSignature, seq, out, raw);
    return;
  }

  // functionCall part -- atomic block, allocates a fresh index and
  // does not become the `currentBlock` (a follow-on text or thinking
  // part starts a new block of that kind).
  if (part.functionCall !== undefined) {
    closeCurrentBlock(state, raw);
    const fc = part.functionCall;
    const index = state.nextBlockIndex++;
    // Synthetic callId: Gemini's `functionCall` has no wire-level id
    // field. The harness keys on this id end-to-end (start, delta,
    // round-trip lookup); `String(index)` matches the Anthropic
    // adapter's fallback when its wire id is absent. Block indices
    // are unique within a request by construction.
    const callId = String(index);

    // Settle the carrier opportunity BEFORE the tool_call.start/delta
    // pair. The signature event carries the thinking block's explicit
    // index in its data, so the harness routes it correctly regardless
    // of arrival order; the ordering here is for positional consumers
    // of the event stream (snapshot tests, debuggers, anything reading
    // the sequence by position rather than by index). The same settle
    // call also discards a stale anchor when no signature is present,
    // so a later thinking block does not trip the "two thinking
    // blocks closed" guard on an anchor the current carrier already
    // declined to claim.
    settleCarrierOpportunity(state, part.thoughtSignature, seq, out, raw);

    out.push({
      type: "inference.tool_call.start",
      seq,
      data: {
        callId,
        name: fc.name,
        partial: EMPTY_PARTIAL,
        index,
      },
    });
    // Gemini delivers `args` complete in a single part -- no
    // streaming JSON fragments. Emit the full serialized args in one
    // delta so the harness's end-of-stream finalization (which keys
    // on `openToolCalls` and re-parses the accumulated argsBuffer)
    // produces a `tool_call.end` with the correct arguments. The
    // harness owns the `tool_call.end` emission; adapters emit only
    // `start` + `delta`.
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
    return;
  }

  // inlineData part -- atomic image-output block. The image arrives
  // complete in a single SSE event (no streaming chunks of base64),
  // so a new block index is allocated and the ImageBlock is emitted
  // in one `inference.image_output` event. The signature carrier
  // semantics mirror the functionCall path: any pending thinking
  // signature is settled BEFORE the image_output event so it
  // attaches to the preceding thinking block, not the image block.
  if (part.inlineData !== undefined) {
    // The parser wraps inlineData as an `ImageBlock`, so a non-
    // image MIME (e.g. audio/wav, application/pdf) would silently
    // mistype the payload. Reject at the boundary rather than
    // produce a confidently-wrong ContentBlock.
    if (!part.inlineData.mimeType.startsWith("image/")) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: inlineData part has non-image ` +
          `mimeType ${JSON.stringify(part.inlineData.mimeType)}; the ` +
          `parser wraps inlineData as an ImageBlock and does not ` +
          `handle other modalities on this code path.`,
        raw,
      );
    }
    closeCurrentBlock(state, raw);
    const index = state.nextBlockIndex++;
    settleCarrierOpportunity(state, part.thoughtSignature, seq, out, raw);
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
    return;
  }

  // executableCode part -- atomic code-execution request block.
  // Gemini delivers the full source in one part (no chunked code
  // streaming), so a fresh block index is allocated and the request
  // block is emitted in one `inference.code_execution.start` event.
  // The synthetic id is `gemini-exec-<index>` where `index` is the
  // content-block index allocated within THIS response (deterministic
  // per-response so replays of the same response produce the same
  // ids). It satisfies the `CodeExecutionRequestBlock.id` contract
  // ("synthesized by the adapter for providers that don't emit one,
  // using a deterministic per-response position-based scheme so
  // replays match"). The id then lands in
  // `pendingExecutionRequestId` so the next codeExecutionResult
  // part can back-point its `requestId` to it.
  if (part.executableCode !== undefined) {
    // Precondition first, before any state mutation or event
    // emission: a depth-1 violation must not leave a half-applied
    // close/allocate/settle sequence in `state` and `out`. The
    // caller discards `out` on throw today, so the difference is
    // not observable, but the ordering keeps the throw faithful
    // to "this part was rejected entirely."
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
    closeCurrentBlock(state, raw);
    const index = state.nextBlockIndex++;
    settleCarrierOpportunity(state, part.thoughtSignature, seq, out, raw);

    const requestId = `gemini-exec-${String(index)}`;
    state.pendingExecutionRequestId = requestId;

    const ec = part.executableCode;
    const request: CodeExecutionRequestBlock = {
      type: "code_execution_request",
      id: requestId,
      code: ec.code,
      // Pass `language` through verbatim. Gemini emits SCREAMING_CASE
      // (e.g. `"PYTHON"`); the type contract is "adapters MUST NOT
      // default this -- callers narrow on its presence." Comparing
      // values cross-provider requires case-insensitive logic at
      // the consumer.
      language: ec.language,
    };
    out.push({
      type: "inference.code_execution.start",
      seq,
      data: { request, index },
    });
    return;
  }

  // codeExecutionResult part -- atomic result block. Pairs against
  // the most recently emitted `executableCode` part via
  // `pendingExecutionRequestId` (Gemini's wire carries no explicit
  // back-pointer; the immediately-preceding request is the
  // implicit owner). The slot read is destructive: clearing it
  // here forces the depth-1 invariant on subsequent parts, and a
  // result arriving with the slot empty throws.
  if (part.codeExecutionResult !== undefined) {
    // Precondition first, before any state mutation or event
    // emission: an empty-slot violation must not leave a
    // half-applied close/allocate/settle sequence behind. Same
    // discipline as the executableCode branch above.
    const requestId = state.pendingExecutionRequestId;
    if (requestId === null) {
      throw new ProtocolMismatchError(
        `google-genai parseResponse: codeExecutionResult part has no ` +
          `preceding executableCode part in this request to pair against.`,
        raw,
      );
    }
    // outcomeToStatus throws on an unknown outcome -- run it before
    // any other state mutation so the throw cleanly rejects the
    // part without partial side effects.
    const cer = part.codeExecutionResult;
    const status = outcomeToStatus(cer.outcome, raw);

    closeCurrentBlock(state, raw);
    const index = state.nextBlockIndex++;
    settleCarrierOpportunity(state, part.thoughtSignature, seq, out, raw);
    state.pendingExecutionRequestId = null;

    const result: CodeExecutionResultBlock = {
      type: "code_execution_result",
      requestId,
      status,
      // Gemini's `output` is the combined stdout+stderr stream.
      // Per the `CodeExecutionResultBlock.stdout` comment, providers
      // that don't split the streams map their combined output here
      // and leave `stderr` empty.
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

  // Signature-only part (no payload, signature set). A still-open
  // thinking block is closed first so its index lands in
  // `pendingSignatureAnchor` before `consumeSignature` claims it --
  // same shape as the empty-text-with-signature branch above. No
  // new block is opened.
  if (part.thoughtSignature !== undefined) {
    closeCurrentBlock(state, raw);
    consumeSignature(state, part.thoughtSignature, seq, out, raw);
    return;
  }

  // `assertSinglePayload` above rules out the no-payload-no-signature
  // case, so a part that lands here had a payload that no earlier
  // branch claimed. The schema models five payload fields (`text`,
  // `functionCall`, `inlineData`, `executableCode`,
  // `codeExecutionResult`); all five have their own branches
  // above. Reaching this line implies the schema has grown a new
  // payload field without a matching branch in `emitPart`.
  throw new ProtocolMismatchError(
    `google-genai parseResponse: unhandled part shape; the schema admits ` +
      `a payload field that emitPart has no branch for.`,
    raw,
  );
}

// Emit `inference.citation` events from a candidate's
// `groundingMetadata`. Each `groundingSupport` expands into one
// citation per referenced chunk: a span that cites four sources
// produces four citations with the same `citedText` and
// `textOffset` but distinct `source` entries. Consumers see the
// full attribution list and can de-duplicate by URI if they want
// to collapse identical sources.
//
// The text-block anchor is read from `state.currentBlock` -- the
// just-processed text parts in this same event will have left it
// set to the running text block. If currentBlock is not text (or
// is null), Gemini delivered grounding without a preceding text
// anchor, which has no defined attribution per the
// `CitationBlock` contract; surface as a protocol mismatch
// rather than synthesize an arbitrary index.
//
// `groundingChunks` entries without the `web` shape (a future
// chunk kind) are skipped silently for now -- their source has no
// `uri`/`title` to populate `CitationSource`, and synthesizing a
// placeholder citation would misrepresent the wire. Supports that
// reference an out-of-range chunk index throw -- the wire is
// pointing at a chunk slot the response never delivered, which is
// a wire bug we want to see.
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
        // Non-web chunk kinds (retrieved-context, document, etc.)
        // have no `web.uri`/`web.title` to populate a
        // CitationSource. Skipping rather than synthesizing keeps
        // the citation faithful to the wire shape the parser
        // actually models -- the schema admits non-web chunks
        // implicitly so a wider chunk kind reaching the parser
        // does not fail schema validation, but it has no defined
        // mapping into `CitationSource` until its discriminator
        // is modeled here.
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

// Map Gemini's `codeExecutionResult.outcome` enum onto the
// internal `CodeExecutionResultBlock.status` union. The switch is
// exhaustive over the three values Gemini documents today; an
// unknown outcome string surfaces as a `ProtocolMismatchError`
// naming the value verbatim rather than being bucketed into a
// fallback status. Adding a new outcome to this mapping is a
// deliberate code change, not an implicit acceptance of whatever
// Gemini sends next.
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

// Settle the carrier-opportunity lifecycle for a non-thinking part
// that has just been processed. If the part carries a signature, it
// is consumed against the pending anchor (which must exist, or the
// request is in a corrupt state). If it does not, the anchor is
// discarded: the FIRST non-thinking part after a thinking block is
// the only chance to claim that thinking block's signature, and a
// part that passes without claiming ends the opportunity. A later
// thinking block cannot retroactively re-open the claim, and the
// discard prevents a stale anchor from tripping the
// `closeCurrentBlock` guard when another thinking block closes.
function settleCarrierOpportunity(
  state: GeminiParserState,
  signature: string | undefined,
  seq: number,
  out: InferenceEvent[],
  raw: unknown,
): void {
  if (signature !== undefined) {
    consumeSignature(state, signature, seq, out, raw);
    return;
  }
  state.pendingSignatureAnchor = null;
}

// Emit `inference.thinking.signature` against the pending anchor and
// clear it. A signature with no pending anchor is a state-corruption
// case: Gemini placed a thoughtSignature on a part with no preceding
// thinking block in this request. Surface as a protocol mismatch.
function consumeSignature(
  state: GeminiParserState,
  signature: string,
  seq: number,
  out: InferenceEvent[],
  raw: unknown,
): void {
  if (state.pendingSignatureAnchor === null) {
    throw new ProtocolMismatchError(
      `google-genai parseResponse: thoughtSignature present but no ` +
        `preceding thinking block exists in this request to anchor it.`,
      raw,
    );
  }
  out.push({
    type: "inference.thinking.signature",
    seq,
    data: {
      signature,
      index: state.pendingSignatureAnchor,
    },
  });
  state.pendingSignatureAnchor = null;
}

function parseResponse(
  sseData: string,
  state: GeminiParserState,
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

  const candidate = candidates[0];
  if (candidate?.content?.parts !== undefined) {
    for (const part of candidate.content.parts) {
      emitPart(part, state, seq, out, parsed);
    }
  }

  // `groundingMetadata` rides on the candidate alongside the parts
  // and the finishReason. It is processed AFTER the parts have
  // settled so any text deltas in the same event extend the
  // currentBlock first; `emitGroundingCitations` reads the
  // currentBlock's index to attribute each citation to the right
  // text block. Citations precede the terminal usage emission --
  // they belong to the model's output, not to the bookkeeping
  // signal that closes the response.
  if (candidate?.groundingMetadata !== undefined) {
    emitGroundingCitations(
      candidate.groundingMetadata,
      state,
      seq,
      out,
      parsed,
    );
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
      thinking: usage.thoughtsTokenCount ?? 0,
    };
    out.push({
      type: "inference.usage",
      seq,
      data: { usage: tokenUsage },
    });

    // Terminal events seal the response. A still-pending
    // code-execution request at this point would mean Gemini
    // emitted an executableCode part without a matching
    // codeExecutionResult before stopping -- a wire bug, not a
    // case the harness should silently swallow.
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

export function createGoogleGenAIAdapter(): ProviderAdapter {
  // Per-request state lives in the closure: block-index allocation
  // and signature-anchor pairing both need to span SSE events.
  // `buildRequest` does not touch state; only `parseResponse` does.
  const state = createParserState();
  return {
    buildRequest,
    parseResponse: (sseData) => parseResponse(sseData, state),
  };
}
