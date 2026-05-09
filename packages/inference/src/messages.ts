import type {
  ConversationMessage,
  ContentBlock,
  AssistantMessage,
  InboundMessage,
  ToolCall,
  ToolResult,
} from "@interchange/types/runtime";

export type { ConversationMessage, ContentBlock, AssistantMessage };

export type TextBlock = { type: "text"; text: string };

export type ThinkingBlock = {
  type: "thinking";
  thinking: string;
  signature?: string;
  redacted?: boolean;
};

export type ImageBlock = {
  type: "image";
  mimeType: string;
  data: string;
};

export type ToolCallBlock = {
  type: "tool_call";
  id: string;
  name: string;
  arguments: Record<string, unknown>;
};

export type ToolResultBlock = {
  type: "tool_result";
  callId: string;
  content: (
    | { type: "text"; text: string }
    | { type: "image"; mimeType: string; data: string }
  )[];
  detail?: unknown;
  isError?: boolean;
};

export type { ToolCall, ToolResult };

export function createInboundMessage(
  message: InboundMessage,
): ConversationMessage | null {
  const content = message.content ?? "";
  if (content.length === 0) return null;

  const { from, subject } = message.headers;
  const envelope: string[] = [];
  if (from.length > 0) envelope.push(`[From: ${from}]`);
  if (subject !== undefined && subject.length > 0) {
    envelope.push(`[Subject: ${subject}]`);
  }

  const text =
    envelope.length > 0 ? `${envelope.join("\n")}\n\n${content}` : content;
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: Date.now(),
  };
}

export function createSystemMessage(text: string): ConversationMessage {
  return {
    role: "system",
    content: [{ type: "text", text }],
    timestamp: Date.now(),
  };
}

export function createAssistantMessage(
  blocks: ContentBlock[],
  model: string,
): AssistantMessage {
  return { role: "assistant", content: blocks, model, timestamp: Date.now() };
}

export function createToolResultMessage(
  results: ToolResult[],
): ConversationMessage {
  const blocks: ContentBlock[] = results.map((r) => {
    const block: Extract<ContentBlock, { type: "tool_result" }> = {
      type: "tool_result",
      callId: r.callId,
      content:
        typeof r.content === "string"
          ? [{ type: "text" as const, text: r.content }]
          : [{ type: "text" as const, text: JSON.stringify(r.content) }],
    };
    if (r.detail !== undefined) {
      block.detail = r.detail;
    }
    if (r.isError !== undefined) {
      block.isError = r.isError;
    }
    return block;
  });
  return { role: "user", content: blocks, timestamp: Date.now() };
}
