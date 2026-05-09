import { type } from "arktype";

import { getLogger } from "@interchange/log";
import type {
  ConversationMessage,
  ContentBlock,
  InferenceEvent,
  InferenceOptions,
  PartialMessage,
  TokenUsage,
} from "@interchange/types/runtime";
import type { ProviderAdapter, BuiltRequest } from "../adapter";

const logger = getLogger(["interchange", "inference", "openai"]);

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

function buildRequest(
  messages: ConversationMessage[],
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

function toOpenAIMessage(msg: ConversationMessage): unknown[] {
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
      // One tool role message per result.
      return toolResults.map((r) => ({
        role: "tool",
        tool_call_id: r.callId,
        content: r.content
          .filter((c): c is { type: "text"; text: string } => c.type === "text")
          .map((c) => c.text)
          .join("\n"),
        ...(r.isError ? { is_error: true } : {}),
      }));
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
    case "image":
      return {
        type: "image_url",
        image_url: { url: `data:${block.mimeType};base64,${block.data}` },
      };
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

const OpenAIToolCallDelta = type({
  "index?": "number",
  "id?": "string",
  "function?": { "name?": "string", "arguments?": "string" },
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
  let parsed: unknown;
  try {
    parsed = JSON.parse(sseData);
  } catch {
    return [];
  }

  const chunk = OpenAIChunk(parsed);
  if (chunk instanceof type.errors) {
    logger.warn`Unexpected SSE chunk shape: ${chunk.summary}`;
    return [];
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
      const { id } = tcDelta;
      const fn = tcDelta.function;
      const name = fn?.name;
      const argFragment = fn?.arguments;

      if (id !== undefined && name !== undefined) {
        // First delta for this tool call — emit start.
        events.push({
          type: "inference.tool_call.start",
          seq,
          data: { callId: id, name, partial: EMPTY_PARTIAL },
        });
      } else if (argFragment !== undefined && argFragment.length > 0) {
        // Argument fragment. We use the index as a temporary callId placeholder
        // since we may not have the real ID yet — the harness resolves this.
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

export function createOpenAIAdapter(): ProviderAdapter {
  return { buildRequest, parseResponse };
}
