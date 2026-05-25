import { type } from "arktype";

import type {
  ConversationTurn,
  ContentBlock,
  InferenceEvent,
  InferenceOptions,
  PartialMessage,
  TokenUsage,
} from "@intx/types/runtime";
import type { ProviderAdapter, BuiltRequest } from "../adapter";
import { BEARER_CREDENTIAL_SENTINEL } from "../auth";
import { ProtocolMismatchError } from "../errors";

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

function buildRequest(
  messages: ConversationTurn[],
  model: string,
  options: InferenceOptions,
): BuiltRequest {
  const convertedMessages: unknown[] = messages.flatMap(toOpenAIMessage);

  const body: Record<string, unknown> = {
    model,
    max_tokens: options.maxTokens ?? 4096,
    messages: convertedMessages,
    stream: true,
  };

  if (options.temperature !== undefined) {
    body["temperature"] = options.temperature;
  }

  if (options.tools !== undefined && options.tools.length > 0) {
    body["tools"] = options.tools.map((t) => ({
      type: "function",
      function: {
        name: t.name,
        description: t.description,
        parameters: t.inputSchema,
      },
    }));
  }

  if (options.systemPrompt) {
    // Prepend a system message if provided via options (takes priority over
    // any system messages already in the history).
    body["messages"] = [
      { role: "system", content: options.systemPrompt },
      ...convertedMessages,
    ];
  }

  if (options.responseFormat !== undefined) {
    body["response_format"] = toOpenAIResponseFormat(options.responseFormat);
  }

  return {
    url: "/chat/completions",
    headers: {
      "content-type": "application/json",
      authorization: BEARER_CREDENTIAL_SENTINEL,
    },
    body: JSON.stringify(body),
  };
}

// Translate the internal `responseFormat` union to OpenAI's
// `response_format` field. The three kinds map one-to-one to OpenAI's
// `text` / `json_object` / `json_schema` types; in `json-schema` mode
// the caller's `name`, `schema`, and (optional) `strict` ride through
// verbatim. Strict mode is the path that produces structured `refusal`
// responses when the model declines a request -- the response-side
// parser handles those refusal chunks below.
function toOpenAIResponseFormat(
  format: NonNullable<InferenceOptions["responseFormat"]>,
): Record<string, unknown> {
  switch (format.kind) {
    case "text":
      return { type: "text" };
    case "json":
      return { type: "json_object" };
    case "json-schema": {
      const jsonSchema: Record<string, unknown> = {
        name: format.name,
        schema: format.schema,
      };
      if (format.strict !== undefined) jsonSchema["strict"] = format.strict;
      return { type: "json_schema", json_schema: jsonSchema };
    }
  }
}

function toOpenAIMessage(msg: ConversationTurn): unknown[] {
  if (msg.role === "system") {
    const text = msg.content
      .filter((b): b is { type: "text"; text: string } => b.type === "text")
      .map((b) => b.text)
      .join("\n\n");
    return [{ role: "system", content: text }];
  }

  if (msg.role === "user") {
    // Check if any block is a tool result — if so, emit as tool role messages.
    const toolResults = msg.content.filter(
      (b): b is Extract<ContentBlock, { type: "tool_result" }> =>
        b.type === "tool_result",
    );
    if (toolResults.length > 0) {
      // One tool role message per result. The OpenAI Chat Completions schema
      // for `role: "tool"` only permits role/tool_call_id/content — there is
      // no `is_error` field — so error status is encoded inside `content`.
      return toolResults.map((r) => {
        const text = r.content
          .filter((c): c is { type: "text"; text: string } => c.type === "text")
          .map((c) => c.text)
          .join("\n");
        return {
          role: "tool",
          tool_call_id: r.callId,
          content: r.isError ? `<error>\n${text}\n</error>` : text,
        };
      });
    }

    const parts = msg.content.map(toOpenAIContentPart);
    // If all parts are plain strings, collapse to a single string.
    if (parts.every((p) => typeof p === "string")) {
      return [{ role: "user", content: parts.join("") }];
    }
    return [{ role: "user", content: parts }];
  }

  if (msg.role === "assistant") {
    // Detect block types that cannot survive the OpenAI assistant
    // message shape and surface the failure rather than silently
    // dropping them. Code execution blocks are first-class semantic
    // content; their loss would corrupt cross-provider conversations.
    for (const block of msg.content) {
      if (
        block.type === "code_execution_request" ||
        block.type === "code_execution_result"
      ) {
        throw new Error(
          `OpenAI adapter does not handle ${block.type} content blocks.`,
        );
      }
    }
    const textBlocks = msg.content.filter(
      (b): b is { type: "text"; text: string } => b.type === "text",
    );
    const thinkingBlocks = msg.content.filter(
      (b): b is { type: "thinking"; thinking: string } => b.type === "thinking",
    );
    const toolCalls = msg.content.filter(
      (b): b is Extract<ContentBlock, { type: "tool_call" }> =>
        b.type === "tool_call",
    );

    const result: Record<string, unknown> = { role: "assistant" };

    if (textBlocks.length > 0) {
      result["content"] = textBlocks.map((b) => b.text).join("");
    } else {
      result["content"] = null;
    }

    // Some providers (e.g. kimi) require reasoning_content on ALL assistant
    // messages when thinking is enabled. If thinking blocks exist anywhere in
    // the conversation, every assistant message must carry reasoning_content —
    // even if empty for that particular turn.
    result["reasoning_content"] =
      thinkingBlocks.length > 0
        ? thinkingBlocks.map((b) => b.thinking).join("")
        : "";

    if (toolCalls.length > 0) {
      result["tool_calls"] = toolCalls.map((tc) => ({
        id: tc.id,
        type: "function",
        function: {
          name: tc.name,
          arguments: JSON.stringify(tc.arguments),
        },
      }));
    }

    return [result];
  }

  return [{ role: msg.role, content: "" }];
}

function toOpenAIContentPart(block: ContentBlock): unknown {
  switch (block.type) {
    case "text":
      return block.text;
    case "image": {
      const source = block.source;
      if (source.kind === "base64") {
        return {
          type: "image_url",
          image_url: {
            url: `data:${source.mimeType};base64,${source.data}`,
          },
        };
      }
      if (source.kind === "url") {
        // OpenAI's image_url accepts a public URL verbatim alongside
        // the data-URL form. The MediaSource's mimeType is not
        // propagated on the wire — OpenAI infers content type from
        // the URL response. The internal mimeType requirement still
        // keeps the caller honest about what they have in hand.
        return {
          type: "image_url",
          image_url: {
            url: source.url,
          },
        };
      }
      if (source.kind === "file-reference") {
        // OpenAI's Chat Completions endpoint accepts images only via
        // `image_url: { url }` (data URL or public URL). It does not
        // accept opaque uploaded-file references the way Anthropic's
        // `{ type: "file", file_id }` does. A `file-reference`
        // handle minted by some other provider (an Anthropic file_id,
        // a Gemini fileUri) is meaningless to OpenAI; the adapter
        // would have to round-trip the bytes through base64 to be
        // useful, which is a caller-level choice, not an adapter one.
        // Surface the constraint loudly with the apparent reference
        // so an operator triaging the failure sees what was sent.
        throw new Error(
          `OpenAI Chat Completions does not accept file-reference image ` +
            `sources; the API only takes base64 data URLs or public URLs ` +
            `via image_url. Received reference: ${source.reference}`,
        );
      }
      source satisfies never;
      throw new Error(`unreachable: unknown MediaSource kind`);
    }
    case "audio":
    case "video":
      throw new Error(
        `OpenAI adapter does not yet handle ${block.type} content blocks.`,
      );
    case "document":
      // OpenAI's Chat Completions added a `file` content type with
      // `file_data`/`file_id` for PDF inputs, but the exact field
      // names and required metadata (filename, content disposition)
      // are version-sensitive and the OpenCode-Zen capture corpus
      // carries no OpenAI document-input fixtures to ground-truth
      // against. Surface the failure with explicit context rather
      // than emitting an unverified wire shape that may 400 or — worse
      // — silently land as malformed input the model ignores.
      throw new Error(
        "OpenAI adapter does not yet emit document content blocks; the " +
          "Chat Completions file-content-type wire shape needs a captured " +
          "fixture before the adapter can be wired against it.",
      );
    case "citation":
      // Citation blocks are server-emitted attribution metadata for
      // content the model already produced; they're not part of the
      // active conversation state the next turn needs to make sense
      // of. OpenAI's Chat Completions has no input wire shape for
      // citations either, so re-uploading them on a follow-up turn
      // would be ignored at best. Drop them when serializing history
      // to OpenAI; a downstream consumer that wants to preserve them
      // across provider switches reads the finalized turn's content[]
      // directly. See INFERENCE.md § Cross-Provider Message
      // Transformation for the general policy on history-drop fields.
      return "";
    case "code_execution_request":
    case "code_execution_result":
      // Code execution blocks are first-class semantic content; silently
      // dropping them would lose the model's tool invocation entirely.
      // OpenAI has no first-class code execution surface today.
      throw new Error(
        `OpenAI adapter does not handle ${block.type} content blocks.`,
      );
    case "thinking":
      // Thinking blocks are not forwarded to OpenAI endpoints.
      return "";
    case "redacted_thinking":
      // Redacted thinking blocks are opaque by design; the cross-
      // provider mapping is meaningless on OpenAI's surface.
      return "";
    case "tool_call":
    case "tool_result":
      // These are handled separately in toOpenAIMessage.
      return "";
  }
}

// ---------------------------------------------------------------------------
// Response parsing
// ---------------------------------------------------------------------------

const EMPTY_PARTIAL: PartialMessage = { text: "" };

// Fireworks (and likely other OpenAI-compatible deployments) emits
// `name: null` and `arguments: null` on tool-call delta fragments AFTER
// the start delta. arktype rejects `null` against `"string"` and would
// drop the whole chunk silently — taking the argument fragments with
// it. Accept `string | null` here and treat null the same as the field
// being absent at the consumer site.
const OpenAIToolCallDelta = type({
  "index?": "number",
  "id?": "string | null",
  "function?": {
    "name?": "string | null",
    "arguments?": "string | null",
  },
});

const OpenAIChunkDelta = type({
  "role?": "string",
  "content?": "string | null",
  "reasoning_content?": "string | null",
  "reasoning?": "string | null",
  // Strict-mode structured-outputs refusal: when the model declines a
  // JSON-schema request on policy grounds, the delta carries the
  // refusal text in this field instead of `content`. Some
  // OpenAI-compatible relays strip it before forwarding; the parser
  // emits refusal events only when the field is present.
  "refusal?": "string | null",
  "tool_calls?": OpenAIToolCallDelta.array(),
});

const PromptTokensDetails = type({ "cached_tokens?": "number" }).or("null");
const CompletionTokensDetails = type({
  "reasoning_tokens?": "number",
}).or("null");

const OpenAIChunkUsage = type({
  "prompt_tokens?": "number",
  "completion_tokens?": "number",
  "prompt_tokens_details?": PromptTokensDetails,
  "completion_tokens_details?": CompletionTokensDetails,
});

const OpenAIChunk = type({
  "choices?": type({
    "index?": "number",
    delta: OpenAIChunkDelta,
    "finish_reason?": "string | null",
  }).array(),
  "usage?": OpenAIChunkUsage.or("null"),
});

// Per-request state for the OpenAI parser. OpenAI's Chat Completions
// has no wire-level content_block index — reasoning_content, content,
// and tool_calls all appear as fields on the same delta chunk
// without per-block positional indices. The harness's per-index
// routing nevertheless requires distinct indices for distinct
// content blocks at distinct positions, so the parser assigns block
// indices on first observation in arrival order, threaded through
// this shared counter. Tool calls share the same counter to avoid
// colliding with text/thinking indices: a tool_call that arrives
// before any text gets the next free block index, NOT zero, so the
// later text doesn't try to land on top of it.
//
// `tcDelta.index` (OpenAI's position in `tool_calls[]`) is a
// tool-call-local index, distinct from a content-block index. The
// indexer maintains a `toolCallBlockIndex` map from tcDelta.index to
// the block index assigned at first observation; subsequent deltas
// for the same tcDelta.index reuse it.
type OpenAIBlockIndexer = {
  nextIndex: number;
  textIndex: number | null;
  thinkingIndex: number | null;
  refusalIndex: number | null;
  toolCallBlockIndex: Map<number, number>;
};

function getOrAssignTextIndex(state: OpenAIBlockIndexer): number {
  if (state.textIndex === null) {
    state.textIndex = state.nextIndex;
    state.nextIndex += 1;
  }
  return state.textIndex;
}

function getOrAssignThinkingIndex(state: OpenAIBlockIndexer): number {
  if (state.thinkingIndex === null) {
    state.thinkingIndex = state.nextIndex;
    state.nextIndex += 1;
  }
  return state.thinkingIndex;
}

function getOrAssignRefusalIndex(state: OpenAIBlockIndexer): number {
  if (state.refusalIndex === null) {
    state.refusalIndex = state.nextIndex;
    state.nextIndex += 1;
  }
  return state.refusalIndex;
}

function getOrAssignToolCallIndex(
  state: OpenAIBlockIndexer,
  toolCallIndex: number,
): number {
  const existing = state.toolCallBlockIndex.get(toolCallIndex);
  if (existing !== undefined) return existing;
  const assigned = state.nextIndex;
  state.nextIndex += 1;
  state.toolCallBlockIndex.set(toolCallIndex, assigned);
  return assigned;
}

function parseResponse(
  sseData: string,
  indexer: OpenAIBlockIndexer,
): InferenceEvent[] {
  // parseSSE strips the `[DONE]` sentinel before yielding payloads, so
  // anything that reaches us here is supposed to be a JSON chunk. A
  // JSON.parse failure or an arktype rejection means the upstream
  // emitted bytes that violate the OpenAI streaming protocol — a
  // protocol mismatch, not a transport flake. Surface it through the
  // harness's stream-error catch via ProtocolMismatchError so the
  // resulting inference.error carries category "protocol_mismatch"
  // and the offending data in error.raw, instead of silently dropping
  // the chunk and leaving the agent to guess why a tool call arrived
  // with empty arguments.
  let parsed: unknown;
  try {
    parsed = JSON.parse(sseData);
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new ProtocolMismatchError(
      `openai parseResponse: malformed JSON in SSE data payload: ${message}`,
      sseData,
    );
  }

  const chunk = OpenAIChunk(parsed);
  if (chunk instanceof type.errors) {
    throw new ProtocolMismatchError(
      `openai parseResponse: SSE chunk failed schema validation: ${chunk.summary}`,
      parsed,
    );
  }

  const seq = 0;

  const { choices } = chunk;
  if (choices === undefined || choices.length === 0) {
    // Check for usage-only events (some providers send a final event with usage).
    const { usage } = chunk;
    if (usage != null) {
      const tokenUsage: TokenUsage = {
        input: usage.prompt_tokens ?? 0,
        output: usage.completion_tokens ?? 0,
        cacheRead: usage.prompt_tokens_details?.cached_tokens ?? 0,
        cacheWrite: 0,
        thinking: usage.completion_tokens_details?.reasoning_tokens ?? 0,
      };
      return [{ type: "inference.usage", seq, data: { usage: tokenUsage } }];
    }
    return [];
  }

  const choice = choices[0];
  if (choice === undefined) return [];
  const { delta } = choice;

  const events: InferenceEvent[] = [];

  // Providers stream reasoning tokens under different field names:
  //   - kimi (via OpenRouter): delta.reasoning
  //   - kimi (direct): delta.reasoning_content
  //   - DeepSeek / others: delta.reasoning_content
  //
  // OpenAI's Chat Completions ships reasoning_content and content as
  // separate logical content blocks without a wire-level block index.
  // The parser assigns indices on first observation in arrival order
  // via the per-request `indexer`: whichever kind streams first lands
  // at 0, the other (if it appears) at 1. This satisfies the harness's
  // per-index routing contract — distinct kinds get distinct indices
  // and the harness's collision detection between block kinds at the
  // same index never fires from a normal OpenAI response.
  const reasoning = delta.reasoning_content ?? delta.reasoning;
  if (typeof reasoning === "string" && reasoning.length > 0) {
    events.push({
      type: "inference.thinking.delta",
      seq,
      data: {
        token: reasoning,
        partial: EMPTY_PARTIAL,
        index: getOrAssignThinkingIndex(indexer),
      },
    });
  }

  const { content } = delta;
  if (typeof content === "string" && content.length > 0) {
    events.push({
      type: "inference.text.delta",
      seq,
      data: {
        token: content,
        partial: EMPTY_PARTIAL,
        index: getOrAssignTextIndex(indexer),
      },
    });
  }

  // Strict-mode structured-outputs refusal. Allocate a content-block
  // index via the same shared counter that text/thinking/tool_call use
  // so a refusal that arrives interleaved with text (e.g. partial
  // content emitted before the refusal kicks in) lands on its own
  // block index rather than colliding with text.
  const { refusal } = delta;
  if (typeof refusal === "string" && refusal.length > 0) {
    events.push({
      type: "inference.refusal.delta",
      seq,
      data: {
        token: refusal,
        partial: EMPTY_PARTIAL,
        index: getOrAssignRefusalIndex(indexer),
      },
    });
  }

  const { tool_calls: toolCallDeltas } = delta;

  if (toolCallDeltas !== undefined) {
    for (const tcDelta of toolCallDeltas) {
      const toolCallSlot = tcDelta.index ?? 0;
      // The harness's per-index map keys on content-block index, not
      // OpenAI's `tool_calls[]` slot. Map this tool call's slot to a
      // content-block index that doesn't collide with text/thinking:
      // first observation of each unique `tcDelta.index` allocates a
      // fresh content-block index from the shared `nextIndex`
      // counter; subsequent deltas for the same slot reuse it.
      const blockIndex = getOrAssignToolCallIndex(indexer, toolCallSlot);
      // Normalize null → undefined: Fireworks emits literal null on every
      // delta after the first; we treat that the same as the field being
      // absent so the start / fragment branches below remain simple.
      const id = tcDelta.id ?? undefined;
      const fn = tcDelta.function;
      const name = fn?.name ?? undefined;
      const argFragment = fn?.arguments ?? undefined;

      // Different providers shape these deltas differently:
      //   - OpenAI emits id + name + empty arguments in the first delta,
      //     then arguments-only deltas (no id, no name) for the body.
      //   - Fireworks (kimi-k2.6) emits id + index on EVERY delta, with
      //     name populated only on the first and arguments fragments on
      //     subsequent deltas. The non-first deltas carry name: null
      //     (normalized to undefined above) rather than omitting the
      //     field outright.
      // Treat the two signals independently. A single delta may legitimately
      // carry both a start signal (id + non-null name) and an argument
      // fragment; both must be emitted.
      //
      // `data.callId` is the OpenAI-provided id when present
      // (`tcDelta.id`); when absent on continuation deltas, the
      // adapter synthesizes a per-stream placeholder from
      // `toolCallSlot` so the harness's id-keyed accumulator can
      // merge fragments until the real id resolves at finalize time.
      // `data.index` is the content-block index allocated above —
      // namespaced into the same counter as text/thinking indices so
      // a tool_call arriving before any text doesn't collide with a
      // later text block at the same numeric index.
      if (id !== undefined && name !== undefined) {
        events.push({
          type: "inference.tool_call.start",
          seq,
          data: {
            callId: id,
            name,
            partial: EMPTY_PARTIAL,
            index: blockIndex,
          },
        });
      }
      if (argFragment !== undefined && argFragment.length > 0) {
        // The delta's `callId` is a per-stream placeholder used by the
        // harness to resolve fragments to the real id minted on the
        // start event. Use `String(blockIndex)` rather than
        // `String(toolCallSlot)` so the placeholder matches the key
        // the harness registers in `indexToCallId` on start —
        // otherwise a non-zero, non-contiguous `tcDelta.index`
        // (single tool at slot 3, or parallel tools at slots 0/3)
        // would land its fragments under a key the harness never
        // registered, and the harness's accumulator would silently
        // drop them.
        events.push({
          type: "inference.tool_call.delta",
          seq,
          data: {
            callId: String(blockIndex),
            argumentFragment: argFragment,
            partial: EMPTY_PARTIAL,
            index: blockIndex,
          },
        });
      }
    }
  }

  // finish_reason is checked but we emit nothing — the harness handles cleanup.
  // (Keeping the reference here documents the field is intentionally unused.)
  void choice.finish_reason;

  // Usage at end of stream (stream_options: { include_usage: true }).
  const usageInChunk = chunk.usage;
  if (usageInChunk != null) {
    const tokenUsage: TokenUsage = {
      input: usageInChunk.prompt_tokens ?? 0,
      output: usageInChunk.completion_tokens ?? 0,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 0,
    };
    events.push({ type: "inference.usage", seq, data: { usage: tokenUsage } });
  }

  return events;
}

function extractRetryAfterMs(headers: Headers): number | undefined {
  // OpenAI's non-standard millisecond header takes priority
  const retryMs = headers.get("retry-after-ms");
  if (retryMs !== null) {
    const ms = Number(retryMs);
    if (Number.isFinite(ms) && ms > 0) return Math.ceil(ms);
  }
  const raw = headers.get("retry-after");
  if (raw !== null) {
    const seconds = Number(raw);
    if (Number.isFinite(seconds) && seconds > 0) {
      return Math.ceil(seconds * 1000);
    }
  }
  return undefined;
}

function extractPacingDelayMs(headers: Headers): number | undefined {
  const remaining = headers.get("x-ratelimit-remaining-requests");
  if (remaining === null) return undefined;
  const n = Number(remaining);
  if (!Number.isFinite(n) || n > 0) return undefined;

  const reset = headers.get("x-ratelimit-reset-requests");
  if (reset === null) return undefined;
  const ms = parseDuration(reset);
  return ms !== undefined && ms > 0 ? ms : undefined;
}

function parseDuration(value: string): number | undefined {
  let total = 0;
  const pattern = /(\d+(?:\.\d+)?)(ms|s|m|h)/g;
  let match;
  while ((match = pattern.exec(value)) !== null) {
    const num = Number(match[1]);
    switch (match[2]) {
      case "ms":
        total += num;
        break;
      case "s":
        total += num * 1000;
        break;
      case "m":
        total += num * 60_000;
        break;
      case "h":
        total += num * 3_600_000;
        break;
    }
  }
  return total > 0 ? Math.ceil(total) : undefined;
}

export function createOpenAIAdapter(): ProviderAdapter {
  // Per-request indexer state. Adapter instances are created per
  // request (see `adapter.ts`), so each call to `createOpenAIAdapter`
  // gets a fresh counter for assigning block indices to reasoning vs.
  // content streams in arrival order.
  const indexer: OpenAIBlockIndexer = {
    nextIndex: 0,
    textIndex: null,
    thinkingIndex: null,
    refusalIndex: null,
    toolCallBlockIndex: new Map<number, number>(),
  };
  return {
    buildRequest,
    parseResponse: (sseData) => parseResponse(sseData, indexer),
    extractRetryAfterMs,
    extractPacingDelayMs,
  };
}
