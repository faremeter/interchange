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
import { CitationBlock as CitationBlockType } from "@intx/types/runtime";
import type { ProviderAdapter, BuiltRequest } from "../adapter";
import { CREDENTIAL_SENTINEL } from "../auth";
import { ProtocolMismatchError } from "../errors";

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

function buildRequest(
  messages: ConversationTurn[],
  model: string,
  options: InferenceOptions,
): BuiltRequest {
  const systemMessages = messages.filter((m) => m.role === "system");
  const conversationMessages = messages.filter((m) => m.role !== "system");

  const systemText = systemMessages
    .flatMap((m) =>
      m.content
        .filter((b): b is { type: "text"; text: string } => b.type === "text")
        .map((b) => b.text),
    )
    .join("\n\n");

  const effectiveSystem = options.systemPrompt
    ? options.systemPrompt
    : systemText || undefined;

  const body: Record<string, unknown> = {
    model,
    max_tokens: options.maxTokens ?? 4096,
    messages: conversationMessages.map((msg, i) => {
      // Place a cache breakpoint on the last user message so all prior
      // turns are cached on the next request.
      const isLastUser =
        msg.role !== "assistant" &&
        conversationMessages.slice(i + 1).every((m) => m.role === "assistant");
      return toAnthropicMessage(msg, isLastUser);
    }),
    stream: true,
  };

  if (effectiveSystem) {
    body["system"] = [
      {
        type: "text",
        text: effectiveSystem,
        cache_control: { type: "ephemeral" },
      },
    ];
  }

  if (options.thinking?.enabled) {
    body["thinking"] = {
      type: "enabled",
      budget_tokens: options.thinking.budgetTokens ?? 1024,
    };
  }

  if (options.tools !== undefined && options.tools.length > 0) {
    const tools: Record<string, unknown>[] = options.tools.map((t) => ({
      name: t.name,
      description: t.description,
      input_schema: t.inputSchema,
    }));
    const lastTool = tools[tools.length - 1];
    if (lastTool !== undefined) {
      lastTool["cache_control"] = { type: "ephemeral" };
    }
    body["tools"] = tools;
  }

  if (options.temperature !== undefined) {
    body["temperature"] = options.temperature;
  }

  return {
    url: "/v1/messages",
    headers: {
      "content-type": "application/json",
      "x-api-key": CREDENTIAL_SENTINEL,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify(body),
  };
}

function toAnthropicMessage(
  msg: ConversationTurn,
  cacheLastBlock?: boolean,
): Record<string, unknown> {
  const role = msg.role === "assistant" ? "assistant" : "user";
  const content = msg.content.map(toAnthropicBlock);
  if (cacheLastBlock) {
    const lastBlock = content[content.length - 1];
    if (lastBlock !== undefined) {
      lastBlock["cache_control"] = { type: "ephemeral" };
    }
  }
  return { role, content };
}

// Marshal a MediaSource into Anthropic's nested `source` shape.
// Base64 sources carry the mimeType on the wire as `media_type`.
// File-reference and URL sources carry no mimeType: Anthropic
// identifies file-reference content by id alone (encoded server-side
// at upload time) and infers URL-sourced content from the response of
// the fetch it performs. The MediaSource's mimeType is intentionally
// dropped at this layer for both. The internal `mimeType` requirement
// on the non-base64 variants keeps callers honest about what they
// have in hand even when the provider doesn't need it.
function toAnthropicMediaSource(source: MediaSource): Record<string, unknown> {
  if (source.kind === "base64") {
    return {
      type: "base64",
      media_type: source.mimeType,
      data: source.data,
    };
  }
  if (source.kind === "file-reference") {
    return {
      type: "file",
      file_id: source.reference,
    };
  }
  if (source.kind === "url") {
    return {
      type: "url",
      url: source.url,
    };
  }
  // Exhaustiveness: a new MediaSource variant added without a case
  // here fails this compile-time check.
  source satisfies never;
  throw new Error(`unreachable: unknown MediaSource kind`);
}

// Map an Anthropic-streamed citation onto the internal CitationBlock.
// `textOffset` is intentionally not populated for any variant:
// Anthropic's offsets are document-relative (page numbers, doc char
// offsets, doc block indices) rather than text-relative — they don't
// correspond to UTF-16 positions in the preceding TextBlock that
// `CitationBlock.textOffset` describes. Computing text-relative
// offsets from `cited_text` substring search produces wrong answers
// whenever `cited_text` is paraphrased, appears multiple times, or
// spans wire-chunk boundaries; better to leave the field unset than
// guess.
//
// `encrypted_index` (web_search_result_location) has no echo-back
// target in CitationBlock today and is intentionally dropped at this
// layer. When echo-back of citation context lands, this is the layer
// to preserve it from.
function toCitationBlock(
  wire: typeof AnthropicCitation.infer,
  index: number,
): typeof CitationBlockType.infer {
  if (wire.cited_text === undefined) {
    throw new ProtocolMismatchError(
      `anthropic parseResponse: citation at block ${index} missing required \`cited_text\``,
      wire,
    );
  }
  const citedText = wire.cited_text;
  const source: {
    title?: string;
    uri?: string;
    documentRef?: { index: number };
  } = {};
  if (wire.title !== undefined) source.title = wire.title;
  if (wire.url !== undefined) source.uri = wire.url;
  if (wire.document_title !== undefined && source.title === undefined) {
    source.title = wire.document_title;
  }
  if (wire.document_index !== undefined) {
    source.documentRef = { index: wire.document_index };
  }

  switch (wire.type) {
    case "web_search_result_location":
      return { type: "citation", citedText, source };
    case "page_location": {
      // Anthropic page numbers are 1-indexed and inclusive on both
      // ends per the documented PDF citation shape.
      const start = wire.start_page_number;
      const end = wire.end_page_number;
      if (start === undefined || end === undefined) {
        throw new ProtocolMismatchError(
          `anthropic parseResponse: page_location citation at block ${index} missing start_page_number or end_page_number`,
          wire,
        );
      }
      return {
        type: "citation",
        citedText,
        source,
        location: { kind: "page", start, end },
      };
    }
    case "char_location": {
      const start = wire.start_char_index;
      const end = wire.end_char_index;
      if (start === undefined || end === undefined) {
        throw new ProtocolMismatchError(
          `anthropic parseResponse: char_location citation at block ${index} missing start_char_index or end_char_index`,
          wire,
        );
      }
      return {
        type: "citation",
        citedText,
        source,
        location: { kind: "char", start, end },
      };
    }
    case "content_block_location": {
      const start = wire.start_block_index;
      const end = wire.end_block_index;
      if (start === undefined || end === undefined) {
        throw new ProtocolMismatchError(
          `anthropic parseResponse: content_block_location citation at block ${index} missing start_block_index or end_block_index`,
          wire,
        );
      }
      return {
        type: "citation",
        citedText,
        source,
        location: { kind: "content-block", start, end },
      };
    }
    default:
      throw new ProtocolMismatchError(
        `anthropic parseResponse: unrecognized citation variant "${wire.type}" at block ${index}`,
        wire,
      );
  }
}

function toAnthropicBlock(block: ContentBlock): Record<string, unknown> {
  switch (block.type) {
    case "text":
      return { type: "text", text: block.text };

    case "thinking":
      return {
        type: "thinking",
        thinking: block.thinking,
        ...(block.signature !== undefined
          ? { signature: block.signature }
          : {}),
      };

    case "redacted_thinking":
      // The opaque `data` blob must echo back verbatim on every
      // follow-up turn that includes this block as context. Any
      // mutation (truncation, base64-decoding-and-reencoding,
      // whitespace normalization) produces a 400 from Anthropic with
      // "messages.N.content.M.redacted_thinking: Field required" or
      // a context-corruption error on subsequent turns. The
      // RedactedThinkingBlock type carries it as `string` (opaque
      // base64); pass through untouched.
      return { type: "redacted_thinking", data: block.data };

    case "image":
      return { type: "image", source: toAnthropicMediaSource(block.source) };

    case "document":
      return { type: "document", source: toAnthropicMediaSource(block.source) };

    case "audio":
    case "video":
      throw new Error(
        `Anthropic adapter does not yet handle ${block.type} content blocks.`,
      );

    case "citation":
      throw new Error(
        "Anthropic adapter does not yet emit citation content blocks.",
      );

    case "code_execution_request":
    case "code_execution_result":
      throw new Error(
        `Anthropic adapter does not yet emit ${block.type} content blocks.`,
      );

    case "refusal":
      // Refusal blocks are an OpenAI strict-mode output shape. Echoing
      // one back into an Anthropic request has no defined wire shape;
      // surface the mismatch at the marshaling site rather than fall
      // through to a silent drop.
      throw new Error(
        "Anthropic adapter does not handle refusal content blocks; " +
          "they are emitted by OpenAI strict-mode structured outputs.",
      );

    case "tool_call":
      return {
        type: "tool_use",
        id: block.id,
        name: block.name,
        input: block.arguments,
      };

    case "tool_result":
      return {
        type: "tool_result",
        tool_use_id: block.callId,
        content: block.content.map((c) => {
          if (c.type === "text") {
            return { type: "text", text: c.text };
          }
          if (c.type === "image") {
            return {
              type: "image",
              source: toAnthropicMediaSource(c.source),
            };
          }
          // Anthropic's tool_result.content accepts only `text` and
          // `image` blocks today. `document` in particular is rejected
          // at the API edge; surface the failure at the marshaling
          // site with the specific block type so the failure shows
          // where the wrong block type was authored, not as an opaque
          // HTTP 400 a round-trip later. The ContentBlock union allows
          // these so the type system can grow uniformly; the wire
          // surface lags.
          throw new Error(
            `Anthropic adapter does not handle ${c.type} content blocks ` +
              `inside tool_result.content; the API accepts only text and ` +
              `image here.`,
          );
        }),
        ...(block.isError ? { is_error: true } : {}),
      };
  }
}

// ---------------------------------------------------------------------------
// Response parsing
//
// The harness passes one SSE data payload per call. The parser accumulates
// no state — all partial state lives in the harness. The parser emits events
// for the fragments it sees; the harness updates the PartialMessage and
// injects it into the returned events.
//
// Because the harness owns partial state, the parser cannot construct the
// correct `partial` field. We emit raw delta events with a placeholder empty
// partial — the harness will replace it before forwarding. This is the
// design: adapters are pure translators, the harness owns all state.
// ---------------------------------------------------------------------------

const EMPTY_PARTIAL: PartialMessage = { text: "" };

// Internal intermediate type used to communicate Anthropic-specific
// delta information to the harness before it enriches with partial state.
export type AnthropicRawEvent =
  | { kind: "text_delta"; token: string }
  | { kind: "thinking_delta"; token: string }
  | { kind: "tool_call_start"; index: number; callId: string; name: string }
  | { kind: "tool_call_delta"; index: number; argumentFragment: string }
  | { kind: "tool_call_end"; index: number }
  | {
      kind: "usage";
      inputTokens: number;
      outputTokens: number;
      cacheReadTokens: number;
      cacheWriteTokens: number;
      thinkingTokens: number;
    }
  | { kind: "message_stop" }
  | { kind: "skip" };

// Anthropic's SSE protocol guarantees `index` on every content_block_*
// event. Parsing it as required (not optional) means the type system
// carries the guarantee through to every emission site below — no
// defensive `?? 0` fallback that would silently route real protocol
// violations to block 0 and corrupt the `blockIndexToCallId` cache the
// parser uses to resolve input_json_delta lookups across multiple
// tool_use blocks at distinct indices. A malformed upstream missing
// `index` surfaces as a ProtocolMismatchError via the schema-validation
// throw site, with the offending payload preserved in `error.raw` for
// inspection.
// Anthropic's wire shape for a single citation, streamed inside a
// `citations_delta`. The `type` discriminator selects the location
// model:
//   - web_search_result_location: URL + title, no document offsets
//   - page_location: 1-indexed page numbers (inclusive start/end)
//   - char_location: 0-indexed character offsets into the document
//   - content_block_location: index into the document's content blocks
// Fields not relevant to a given variant are absent; the union is
// flat at the wire level. `encrypted_index` (web_search) is recorded
// only on the wire — it has no echo-back target in the internal
// CitationBlock today, so the adapter drops it.
const AnthropicCitation = type({
  type: "string",
  "cited_text?": "string",
  "url?": "string",
  "title?": "string",
  "encrypted_index?": "string",
  "document_index?": "number",
  "document_title?": "string",
  "start_page_number?": "number",
  "end_page_number?": "number",
  "start_char_index?": "number",
  "end_char_index?": "number",
  "start_block_index?": "number",
  "end_block_index?": "number",
});

const ContentBlockDelta = type({
  type: "'content_block_delta'",
  index: "number",
  delta: {
    type: "string",
    "text?": "string",
    "thinking?": "string",
    "partial_json?": "string",
    "signature?": "string",
    "citation?": AnthropicCitation,
  },
});

const ContentBlockStart = type({
  type: "'content_block_start'",
  index: "number",
  // Anthropic sends either content_block (snake_case) or contentBlock
  // (camelCase). `data` is optional on the shared shape because only
  // redacted_thinking blocks carry it; the redacted_thinking branch in
  // the parser asserts presence and throws ProtocolMismatchError when
  // it is missing, rather than synthesizing an empty string that would
  // round-trip back to Anthropic as a corrupted block.
  "content_block?": {
    type: "string",
    "id?": "string",
    "name?": "string",
    "data?": "string",
  },
  "contentBlock?": {
    type: "string",
    "id?": "string",
    "name?": "string",
    "data?": "string",
  },
});

const ContentBlockStop = type({
  type: "'content_block_stop'",
  index: "number",
});

const MessageDelta = type({
  type: "'message_delta'",
  "usage?": { "output_tokens?": "number" },
});

const MessageStart = type({
  type: "'message_start'",
  "message?": {
    "usage?": {
      "input_tokens?": "number",
      "output_tokens?": "number",
      "cache_read_input_tokens?": "number",
      "cache_creation_input_tokens?": "number",
    },
  },
});

const MessageStop = type({ type: "'message_stop'" });
const Ping = type({ type: "'ping'" });

const AnthropicSSEEvent = ContentBlockDelta.or(ContentBlockStart)
  .or(ContentBlockStop)
  .or(MessageDelta)
  .or(MessageStart)
  .or(MessageStop)
  .or(Ping);

function parseResponse(
  sseData: string,
  blockIndexToCallId: Map<number, string>,
): InferenceEvent[] {
  // Same protocol-mismatch posture as the openai adapter: a JSON parse
  // failure or arktype rejection means the upstream emitted bytes that
  // violate the Anthropic streaming protocol. Surface through
  // ProtocolMismatchError so the harness's stream-error catch emits
  // an inference.error with category "protocol_mismatch" carrying the
  // offending data in error.raw, rather than dropping the chunk
  // silently.
  let parsed: unknown;
  try {
    parsed = JSON.parse(sseData);
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new ProtocolMismatchError(
      `anthropic parseResponse: malformed JSON in SSE data payload: ${message}`,
      sseData,
    );
  }

  const event = AnthropicSSEEvent(parsed);
  if (event instanceof type.errors) {
    throw new ProtocolMismatchError(
      `anthropic parseResponse: SSE event failed schema validation: ${event.summary}`,
      parsed,
    );
  }

  // The seq field is a placeholder 0 — the harness assigns real sequence numbers.
  const seq = 0;

  switch (event.type) {
    case "content_block_delta": {
      const { delta, index } = event;

      if (delta.type === "text_delta") {
        const token = delta.text ?? "";
        return [
          {
            type: "inference.text.delta",
            seq,
            data: { token, partial: EMPTY_PARTIAL, index },
          },
        ];
      }

      if (delta.type === "thinking_delta") {
        const token = delta.thinking ?? "";
        return [
          {
            type: "inference.thinking.delta",
            seq,
            data: { token, partial: EMPTY_PARTIAL, index },
          },
        ];
      }

      if (delta.type === "signature_delta") {
        // Anthropic emits the cryptographic signature for a thinking block
        // in a dedicated signature_delta event after the block's
        // thinking_delta stream. The signature must be echoed back on any
        // follow-up turn that includes the thinking block as context —
        // otherwise the API rejects the request with
        // "messages.N.content.M.thinking.signature: Field required".
        const signature = delta.signature ?? "";
        return [
          {
            type: "inference.thinking.signature",
            seq,
            data: { signature, index },
          },
        ];
      }

      if (delta.type === "input_json_delta") {
        const callId = blockIndexToCallId.get(index);
        if (callId === undefined) {
          throw new ProtocolMismatchError(
            `anthropic parseResponse: input_json_delta for content block ${index} with no preceding tool_use start`,
            event,
          );
        }
        const fragment = delta.partial_json ?? "";
        return [
          {
            type: "inference.tool_call.delta",
            seq,
            data: {
              callId,
              argumentFragment: fragment,
              partial: EMPTY_PARTIAL,
              index,
            },
          },
        ];
      }

      if (delta.type === "citations_delta") {
        const wireCitation = delta.citation;
        if (wireCitation === undefined) {
          throw new ProtocolMismatchError(
            `anthropic parseResponse: citations_delta missing citation payload at block ${index}`,
            event,
          );
        }
        const citation = toCitationBlock(wireCitation, index);
        return [
          {
            type: "inference.citation",
            seq,
            data: { citation, index },
          },
        ];
      }

      return [];
    }

    case "content_block_start": {
      const block = event.content_block ?? event.contentBlock;
      if (block === undefined) return [];

      if (block.type === "tool_use") {
        const { index } = event;
        const callId = block.id ?? String(index);
        blockIndexToCallId.set(index, callId);
        const name = block.name ?? "";
        return [
          {
            type: "inference.tool_call.start",
            seq,
            data: { callId, name, partial: EMPTY_PARTIAL, index },
          },
        ];
      }

      if (block.type === "thinking") {
        // Anchor the thinking block in the harness's per-index map
        // via an empty thinking.delta. Anthropic can stream a
        // signature_delta for a thinking block whose visible text is
        // empty (redacted-adjacent flow); without this anchor, the
        // signature would arrive at the harness with no preceding
        // thinking entry at the same index and the per-index router
        // would (correctly) reject it as a protocol violation. The
        // empty-token delta is the parser-side analogue of the wire's
        // `content_block_start` for thinking — it carries no visible
        // content but reserves the index.
        const { index } = event;
        return [
          {
            type: "inference.thinking.delta",
            seq,
            data: { token: "", partial: EMPTY_PARTIAL, index },
          },
        ];
      }

      if (block.type === "redacted_thinking") {
        // Anthropic delivers redacted_thinking as a one-shot inside
        // content_block_start (no delta stream). The opaque `data`
        // blob must echo back verbatim on every follow-up turn —
        // mutating or synthesizing it corrupts the conversation
        // context. A start event missing `data` is a protocol
        // violation, not a default-to-empty case.
        const { index } = event;
        if (block.data === undefined) {
          throw new ProtocolMismatchError(
            `anthropic parseResponse: content_block_start of type redacted_thinking ` +
              `at index ${String(index)} missing required \`data\` field`,
            event,
          );
        }
        return [
          {
            type: "inference.thinking.redacted",
            seq,
            data: {
              redactedThinking: {
                type: "redacted_thinking",
                data: block.data,
              },
              index,
            },
          },
        ];
      }

      // Non-tool_use, non-redacted_thinking content_block_start events
      // (text, thinking) emit nothing here by design: each
      // content_block_delta arrives with a typed delta (text_delta,
      // thinking_delta, signature_delta) that the switch above
      // discriminates on directly, so an upfront start emission would
      // be redundant. Tool calls are the exception because their
      // callId arrives only in the start event and must be cached
      // against the block index for subsequent input_json_delta
      // lookups; redacted_thinking is the exception because the block
      // is delivered start-only with no follow-on deltas.
      return [];
    }

    case "content_block_stop": {
      // The harness handles finalizing tool calls when it sees this — we
      // emit nothing here; the harness knows which blocks are complete.
      return [];
    }

    case "message_delta": {
      const outputTokens = event.usage?.output_tokens ?? 0;
      const inferenceUsage: TokenUsage = {
        input: 0,
        output: outputTokens,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      };
      return [
        { type: "inference.usage", seq, data: { usage: inferenceUsage } },
      ];
    }

    case "message_start": {
      blockIndexToCallId.clear();
      const msgUsage = event.message?.usage;
      if (msgUsage === undefined) return [];

      const inferenceUsage: TokenUsage = {
        input: msgUsage.input_tokens ?? 0,
        output: msgUsage.output_tokens ?? 0,
        cacheRead: msgUsage.cache_read_input_tokens ?? 0,
        cacheWrite: msgUsage.cache_creation_input_tokens ?? 0,
        thinking: 0,
      };
      return [
        { type: "inference.usage", seq, data: { usage: inferenceUsage } },
      ];
    }

    case "message_stop":
    case "ping":
      return [];
  }
}

function extractRetryAfterMs(headers: Headers): number | undefined {
  const raw = headers.get("retry-after");
  if (raw === null) return undefined;
  const seconds = Number(raw);
  if (!Number.isFinite(seconds) || seconds <= 0) return undefined;
  return Math.ceil(seconds * 1000);
}

function extractPacingDelayMs(headers: Headers): number | undefined {
  // Check all rate limit dimensions and return the longest wait needed
  const delays: number[] = [];

  for (const prefix of [
    "anthropic-ratelimit-requests",
    "anthropic-ratelimit-input-tokens",
    "anthropic-ratelimit-output-tokens",
    "anthropic-ratelimit-tokens",
  ]) {
    const remaining = headers.get(`${prefix}-remaining`);
    if (remaining === null) continue;
    const n = Number(remaining);
    if (!Number.isFinite(n) || n > 0) continue;

    const reset = headers.get(`${prefix}-reset`);
    if (reset === null) continue;
    const resetTime = Date.parse(reset);
    if (Number.isNaN(resetTime)) continue;
    const delayMs = resetTime - Date.now();
    if (delayMs > 0) delays.push(delayMs);
  }

  return delays.length > 0 ? Math.max(...delays) : undefined;
}

export function createAnthropicAdapter(): ProviderAdapter {
  const blockIndexToCallId = new Map<number, string>();

  return {
    buildRequest,
    parseResponse: (sseData) => parseResponse(sseData, blockIndexToCallId),
    extractRetryAfterMs,
    extractPacingDelayMs,
  };
}
