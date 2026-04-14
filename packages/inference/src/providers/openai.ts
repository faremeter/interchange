import type {
  ConversationMessage,
  ContentBlock,
  InferenceEvent,
  InferenceOptions,
  PartialMessage,
  TokenUsage,
} from "@interchange/types/runtime";
import type { ProviderAdapter, BuiltRequest } from "../adapter";

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

function buildRequest(
  messages: ConversationMessage[],
  model: string,
  options: InferenceOptions,
): BuiltRequest {
  const body: Record<string, unknown> = {
    model,
    max_tokens: options.maxTokens ?? 4096,
    messages: messages.flatMap(toOpenAIMessage),
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
    const existing = body["messages"] as unknown[];
    body["messages"] = [
      { role: "system", content: options.systemPrompt },
      ...existing,
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

function parseResponse(sseData: string): InferenceEvent[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(sseData);
  } catch {
    return [];
  }

  if (parsed === null || typeof parsed !== "object") {
    return [];
  }

  const event = parsed as Record<string, unknown>;
  const seq = 0;

  const choices = event["choices"] as unknown[] | undefined;
  if (choices === undefined || choices.length === 0) {
    // Check for usage-only events (some providers send a final event with usage).
    const usage = event["usage"] as Record<string, unknown> | undefined;
    if (usage !== undefined) {
      const tokenUsage: TokenUsage = {
        input: (usage["prompt_tokens"] as number | undefined) ?? 0,
        output: (usage["completion_tokens"] as number | undefined) ?? 0,
        cacheRead:
          ((
            usage["prompt_tokens_details"] as
              | Record<string, unknown>
              | undefined
          )?.["cached_tokens"] as number) ?? 0,
        cacheWrite: 0,
        thinking:
          ((
            usage["completion_tokens_details"] as
              | Record<string, unknown>
              | undefined
          )?.["reasoning_tokens"] as number) ?? 0,
      };
      return [{ type: "inference.usage", seq, data: { usage: tokenUsage } }];
    }
    return [];
  }

  const choice = choices[0] as Record<string, unknown>;
  const delta = choice["delta"] as Record<string, unknown> | undefined;
  if (delta === undefined) return [];

  const events: InferenceEvent[] = [];

  // Providers stream reasoning tokens under different field names:
  //   - kimi (via OpenRouter): delta.reasoning
  //   - kimi (direct): delta.reasoning_content
  //   - DeepSeek / others: delta.reasoning_content
  const reasoning = (delta["reasoning_content"] ?? delta["reasoning"]) as
    | string
    | null
    | undefined;
  if (typeof reasoning === "string" && reasoning.length > 0) {
    events.push({
      type: "inference.thinking.delta",
      seq,
      data: { token: reasoning, partial: EMPTY_PARTIAL },
    });
  }

  const content = delta["content"] as string | null | undefined;
  if (typeof content === "string" && content.length > 0) {
    events.push({
      type: "inference.text.delta",
      seq,
      data: { token: content, partial: EMPTY_PARTIAL },
    });
  }

  const toolCallDeltas = delta["tool_calls"] as
    | Record<string, unknown>[]
    | undefined;

  if (toolCallDeltas !== undefined) {
    for (const tcDelta of toolCallDeltas) {
      const index = (tcDelta["index"] as number | undefined) ?? 0;
      const id = tcDelta["id"] as string | undefined;
      const fn = tcDelta["function"] as Record<string, unknown> | undefined;
      const name = fn?.["name"] as string | undefined;
      const argFragment = fn?.["arguments"] as string | undefined;

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

  // Check finish_reason to emit tool_call.end events.
  const finishReason = choice["finish_reason"] as string | null | undefined;
  if (finishReason === "tool_calls") {
    // The harness will finalize open tool calls on stream end.
    // We emit nothing extra here — it's handled by the harness.
  }

  // Usage at end of stream (stream_options: { include_usage: true }).
  const usageInChunk = event["usage"] as Record<string, unknown> | undefined;
  if (usageInChunk !== undefined) {
    const tokenUsage: TokenUsage = {
      input: (usageInChunk["prompt_tokens"] as number | undefined) ?? 0,
      output: (usageInChunk["completion_tokens"] as number | undefined) ?? 0,
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
