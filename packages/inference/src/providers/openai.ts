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

  return {
    url: "/chat/completions",
    headers: {
      "content-type": "application/json",
      authorization: "", // Filled by the harness: "Bearer <apiKey>"
    },
    body: JSON.stringify(body),
  };
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
      if (source.kind === "file-reference") {
        throw new Error(
          "OpenAI adapter does not yet handle file-reference image " +
            "sources.",
        );
      }
      source satisfies never;
      throw new Error(`unreachable: unknown MediaSource kind`);
    }
    case "thinking":
      // Thinking blocks are not forwarded to OpenAI endpoints.
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

function parseResponse(sseData: string): InferenceEvent[] {
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
  const reasoning = delta.reasoning_content ?? delta.reasoning;
  if (typeof reasoning === "string" && reasoning.length > 0) {
    events.push({
      type: "inference.thinking.delta",
      seq,
      data: { token: reasoning, partial: EMPTY_PARTIAL },
    });
  }

  const { content } = delta;
  if (typeof content === "string" && content.length > 0) {
    events.push({
      type: "inference.text.delta",
      seq,
      data: { token: content, partial: EMPTY_PARTIAL },
    });
  }

  const { tool_calls: toolCallDeltas } = delta;

  if (toolCallDeltas !== undefined) {
    for (const tcDelta of toolCallDeltas) {
      const index = tcDelta.index ?? 0;
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
      if (id !== undefined && name !== undefined) {
        events.push({
          type: "inference.tool_call.start",
          seq,
          data: { callId: id, name, partial: EMPTY_PARTIAL },
        });
      }
      if (argFragment !== undefined && argFragment.length > 0) {
        // Argument fragment. We use the index as a temporary callId placeholder
        // since the upstream harness merges fragments by index — the real id is
        // resolved at finalize time.
        events.push({
          type: "inference.tool_call.delta",
          seq,
          data: {
            callId: String(index),
            argumentFragment: argFragment,
            partial: EMPTY_PARTIAL,
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
  return {
    buildRequest,
    parseResponse,
    extractRetryAfterMs,
    extractPacingDelayMs,
  };
}
