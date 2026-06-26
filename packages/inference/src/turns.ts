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
 * Map a received attachment to a model ContentBlock.
 *
 * The dispatch uses two policies that look unified but are not, and must
 * stay distinct:
 *   - major-type dispatch for image/*, video/*, audio/* → the matching block;
 *   - allowlist-category dispatch for the document category
 *     (application/pdf, application/json, text/plain, text/csv,
 *     text/markdown) → DocumentBlock.
 * Do NOT collapse this into "always dispatch by major type": that would
 * route text/plain to a text block instead of DocumentBlock.
 *
 * This function is total — it never throws. The hub route allowlist only
 * guards the local user-upload path; inbound attachments arrive from remote
 * senders via fetchFull and are not subtype-filtered here (provider adapters
 * are the contract layer that rejects unsupported media at marshal time, per
 * INTERCHANGE message design). Any major-type media is therefore passed
 * through to the adapter. A type that maps to no block at all (e.g. an
 * archive) degrades to a visible text marker rather than throwing: throwing
 * here would propagate into the reactor's ungoverned delivery path and let a
 * single malformed remote attachment tear down the session.
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
    if (from.length > 0) envelope.push(`[From: ${from}]`);
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
 * Assert that a prompt's tool_call / tool_result blocks are structurally
 * well-formed before it is sent to a provider.
 *
 * Throws if a tool_call id is emitted twice, if a tool_result references a
 * callId with no preceding tool_call, or if two tool_result blocks answer the
 * same callId. None of these are valid in a coherent tool conversation — a
 * tool call has exactly one result. Catching it here surfaces the corruption
 * as an internal error at the assembly boundary, with the offending id and
 * turn index, instead of an opaque downstream provider rejection.
 *
 * This deliberately does NOT require every tool_call to have a result: an
 * unanswered tool_call is left legitimately by an after-inference halt/abort
 * and is repaired downstream for cross-provider replay.
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
