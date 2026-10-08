import type {
  ConversationTurn,
  ContentBlock,
  AssistantTurn,
  InboundMessage,
  MediaSource,
  MessageAttachment,
  ToolCall,
  ToolResult,
} from "@intx/types/runtime";
import { attachmentCategory, base64Encode } from "@intx/types";
import { getLogger } from "@intx/log";

const logger = getLogger(["interchange", "inference", "turns"]);

export type { ConversationTurn, ContentBlock, AssistantTurn };

export type { ToolCall, ToolResult };

/**
 * Map a received attachment to a ContentBlock. Dispatch is by major type for
 * image/video/audio and by allowlist category for documents; do not collapse
 * to major type alone, or text/plain would become a text block. Total: a type
 * with no block degrades to a visible text marker instead of throwing, so a
 * malformed remote attachment cannot tear down the session.
 */
function attachmentToContentBlock(att: MessageAttachment): ContentBlock {
  const majorType = att.contentType.split("/")[0];
  if (
    majorType === "image" ||
    majorType === "video" ||
    majorType === "audio" ||
    attachmentCategory(att.contentType) === "document"
  ) {
    const source: MediaSource = {
      kind: "base64",
      mimeType: att.contentType,
      data: base64Encode(att.data),
    };
    if (majorType === "image") return { type: "image", source };
    if (majorType === "video") return { type: "video", source };
    if (majorType === "audio") return { type: "audio", source };
    return { type: "document", source };
  }

  logger.warn`Unsupported attachment content type ${att.contentType}; surfacing as a text marker`;
  return {
    type: "text",
    text: `[Unsupported attachment: ${att.name} (${att.contentType})]`,
  };
}

export function createInboundTurn(
  message: InboundMessage,
): ConversationTurn | null {
  const content = message.content ?? "";
  const attachments = message.attachments ?? [];
  if (content.length === 0 && attachments.length === 0) return null;

  const blocks: ContentBlock[] = [];

  if (content.length > 0) {
    const { from, subject } = message.headers;
    const envelope: string[] = [];
    if (from !== undefined && from.length > 0) envelope.push(`[From: ${from}]`);
    if (subject !== undefined && subject.length > 0) {
      envelope.push(`[Subject: ${subject}]`);
    }
    const text =
      envelope.length > 0 ? `${envelope.join("\n")}\n\n${content}` : content;
    blocks.push({ type: "text", text });
  }

  for (const att of attachments) {
    blocks.push(attachmentToContentBlock(att));
  }

  return {
    role: "user",
    content: blocks,
    timestamp: Date.now(),
  };
}

/**
 * Validate tool_call/tool_result structure before a prompt goes to a
 * provider. Throws on duplicate tool_call ids, results without a preceding
 * call, or duplicate results for one call, naming the offending id and turn.
 * An unanswered tool_call is allowed: a halt/abort can leave one legitimately.
 */
export function assertWellFormedToolSequence(turns: ConversationTurn[]): void {
  const calledIds = new Set<string>();
  const answeredIds = new Set<string>();

  for (let turnIndex = 0; turnIndex < turns.length; turnIndex++) {
    const turn = turns[turnIndex];
    if (turn === undefined) continue;

    for (const block of turn.content) {
      if (block.type === "tool_call") {
        if (calledIds.has(block.id)) {
          throw new Error(
            `Malformed tool sequence: duplicate tool_call id ${JSON.stringify(block.id)} at turn ${String(turnIndex)}`,
          );
        }
        calledIds.add(block.id);
      } else if (block.type === "tool_result") {
        if (!calledIds.has(block.callId)) {
          throw new Error(
            `Malformed tool sequence: tool_result for ${JSON.stringify(block.callId)} at turn ${String(turnIndex)} has no preceding tool_call`,
          );
        }
        if (answeredIds.has(block.callId)) {
          throw new Error(
            `Malformed tool sequence: duplicate tool_result for ${JSON.stringify(block.callId)} at turn ${String(turnIndex)}`,
          );
        }
        answeredIds.add(block.callId);
      }
    }
  }
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
