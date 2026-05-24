import type {
  ConversationTurn,
  ContentBlock,
  AssistantTurn,
  InboundMessage,
  ToolCall,
  ToolResult,
} from "@intx/types/runtime";

export type { ConversationTurn, ContentBlock, AssistantTurn };

export type { ToolCall, ToolResult };

export function createInboundTurn(
  message: InboundMessage,
): ConversationTurn | null {
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

export function createToolResultTurn(results: ToolResult[]): ConversationTurn {
  const blocks: ContentBlock[] = results.map((r) => {
    const raw =
      typeof r.content === "string" ? r.content : JSON.stringify(r.content);
    const block: Extract<ContentBlock, { type: "tool_result" }> = {
      type: "tool_result",
      callId: r.callId,
      content: [{ type: "text" as const, text: raw }],
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
