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
    messages: conversationMessages.map(toAnthropicMessage),
    stream: true,
  };

  if (effectiveSystem) {
    body["system"] = effectiveSystem;
  }

  if (options.thinking?.enabled) {
    body["thinking"] = {
      type: "enabled",
      budget_tokens: options.thinking.budgetTokens ?? 1024,
    };
  }

  if (options.temperature !== undefined) {
    body["temperature"] = options.temperature;
  }

  return {
    url: "https://api.anthropic.com/v1/messages",
    headers: {
      "content-type": "application/json",
      "x-api-key": "", // Filled by the harness from ProviderConfig.apiKey
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify(body),
  };
}

function toAnthropicMessage(msg: ConversationMessage): unknown {
  const role = msg.role === "assistant" ? "assistant" : "user";
  return {
    role,
    content: msg.content.map(toAnthropicBlock),
  };
}

function toAnthropicBlock(block: ContentBlock): unknown {
  switch (block.type) {
    case "text":
      return { type: "text", text: block.text };

    case "thinking":
      if (block.redacted) {
        return { type: "thinking", thinking: "", thinking_type: "redacted" };
      }
      return {
        type: "thinking",
        thinking: block.thinking,
        ...(block.signature !== undefined
          ? { signature: block.signature }
          : {}),
      };

    case "image":
      return {
        type: "image",
        source: {
          type: "base64",
          media_type: block.mimeType,
          data: block.data,
        },
      };

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
          return {
            type: "image",
            source: { type: "base64", media_type: c.mimeType, data: c.data },
          };
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

function parseResponse(sseData: string): InferenceEvent[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(sseData);
  } catch {
    return [];
  }

  if (
    parsed === null ||
    typeof parsed !== "object" ||
    !("type" in parsed) ||
    typeof (parsed as Record<string, unknown>)["type"] !== "string"
  ) {
    return [];
  }

  const event = parsed as Record<string, unknown>;
  const eventType = event["type"] as string;

  // The seq field is a placeholder 0 — the harness assigns real sequence numbers.
  const seq = 0;

  switch (eventType) {
    case "content_block_delta": {
      const delta = event["delta"] as Record<string, unknown> | undefined;
      if (delta === undefined) return [];

      const deltaType = delta["type"] as string | undefined;

      if (deltaType === "text_delta") {
        const token = (delta["text"] as string | undefined) ?? "";
        return [
          {
            type: "inference.text.delta",
            seq,
            data: { token, partial: EMPTY_PARTIAL },
          },
        ];
      }

      if (deltaType === "thinking_delta") {
        const token = (delta["thinking"] as string | undefined) ?? "";
        return [
          {
            type: "inference.thinking.delta",
            seq,
            data: { token, partial: EMPTY_PARTIAL },
          },
        ];
      }

      if (deltaType === "input_json_delta") {
        const index = (event["index"] as number | undefined) ?? 0;
        const fragment = (delta["partial_json"] as string | undefined) ?? "";
        return [
          {
            type: "inference.tool_call.delta",
            seq,
            data: {
              callId: String(index),
              argumentFragment: fragment,
              partial: EMPTY_PARTIAL,
            },
          },
        ];
      }

      return [];
    }

    case "content_block_start": {
      const block =
        (event["contentBlock"] as Record<string, unknown> | undefined) ??
        (event["content_block"] as Record<string, unknown> | undefined);
      if (block === undefined) return [];

      const blockType = block["type"] as string | undefined;
      if (blockType === "tool_use") {
        const index = (event["index"] as number | undefined) ?? 0;
        const callId = (block["id"] as string | undefined) ?? String(index);
        const name = (block["name"] as string | undefined) ?? "";
        return [
          {
            type: "inference.tool_call.start",
            seq,
            data: { callId, name, partial: EMPTY_PARTIAL },
          },
        ];
      }
      return [];
    }

    case "content_block_stop": {
      // The harness handles finalizing tool calls when it sees this — we
      // emit nothing here; the harness knows which blocks are complete.
      return [];
    }

    case "message_delta": {
      const usage = event["usage"] as Record<string, unknown> | undefined;
      if (usage === undefined) return [];

      const outputTokens = (usage["output_tokens"] as number | undefined) ?? 0;
      const inputTokens = 0;
      const inferenceUsage: TokenUsage = {
        input: inputTokens,
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
      const msgUsage = (
        event["message"] as Record<string, unknown> | undefined
      )?.["usage"] as Record<string, unknown> | undefined;
      if (msgUsage === undefined) return [];

      const inferenceUsage: TokenUsage = {
        input: (msgUsage["input_tokens"] as number | undefined) ?? 0,
        output: (msgUsage["output_tokens"] as number | undefined) ?? 0,
        cacheRead:
          (msgUsage["cache_read_input_tokens"] as number | undefined) ?? 0,
        cacheWrite:
          (msgUsage["cache_creation_input_tokens"] as number | undefined) ?? 0,
        thinking: 0,
      };
      return [
        { type: "inference.usage", seq, data: { usage: inferenceUsage } },
      ];
    }

    case "message_stop":
    case "ping":
    default:
      return [];
  }
}

export function createAnthropicAdapter(): ProviderAdapter {
  return { buildRequest, parseResponse };
}
