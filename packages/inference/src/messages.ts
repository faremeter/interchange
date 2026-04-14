import type {
  ConversationMessage,
  ContentBlock,
  AssistantMessage,
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

export function createTextMessage(text: string): ConversationMessage {
  return { role: "user", content: [{ type: "text", text }] };
}

export function createSystemMessage(text: string): ConversationMessage {
  return { role: "system", content: [{ type: "text", text }] };
}

export function createAssistantMessage(
  blocks: ContentBlock[],
  model: string,
): AssistantMessage {
  return { role: "assistant", content: blocks, model };
}

export function createToolResultMessage(
  results: ToolResult[],
): ConversationMessage {
  const blocks: ContentBlock[] = results.map((r) => {
    const block: ContentBlock = {
      type: "tool_result",
      callId: r.callId,
      content:
        typeof r.content === "string"
          ? [{ type: "text" as const, text: r.content }]
          : [{ type: "text" as const, text: JSON.stringify(r.content) }],
    };
    if (r.detail !== undefined) {
      (block as Extract<ContentBlock, { type: "tool_result" }>).detail =
        r.detail;
    }
    if (r.isError !== undefined) {
      (block as Extract<ContentBlock, { type: "tool_result" }>).isError =
        r.isError;
    }
    return block;
  });
  return { role: "user", content: blocks };
}
