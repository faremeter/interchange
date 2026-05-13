import { writeFileSync, mkdirSync } from "node:fs";
import path from "node:path";
import type {
  ConversationTurn,
  ContentBlock,
  AssistantTurn,
  InboundMessage,
  ToolCall,
  ToolResult,
} from "@interchange/types/runtime";

export type { ConversationTurn, ContentBlock, AssistantTurn };

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

const MAX_TOOL_RESULT_CHARS = 10_000;

function truncateToolResult(
  text: string,
  callId: string,
  outputDir?: string,
): string {
  if (text.length <= MAX_TOOL_RESULT_CHARS) return text;
  const omitted = text.length - MAX_TOOL_RESULT_CHARS;

  if (outputDir !== undefined) {
    const dir = path.join(outputDir, ".tool-output");
    mkdirSync(dir, { recursive: true });
    const filePath = path.join(dir, `${callId}.txt`);
    writeFileSync(filePath, text);
    return `${text.slice(0, MAX_TOOL_RESULT_CHARS)}\n[Tool output truncated: omitted ${omitted} chars. Full output saved to ${filePath} -- use read_file to see the rest.]`;
  }

  return `${text.slice(0, MAX_TOOL_RESULT_CHARS)}\n[Tool output truncated: omitted ${omitted} chars]`;
}

export interface ToolResultTurnOptions {
  outputDir?: string;
}

export function createToolResultTurn(
  results: ToolResult[],
  options?: ToolResultTurnOptions,
): ConversationTurn {
  const blocks: ContentBlock[] = results.map((r) => {
    const raw =
      typeof r.content === "string" ? r.content : JSON.stringify(r.content);
    const block: Extract<ContentBlock, { type: "tool_result" }> = {
      type: "tool_result",
      callId: r.callId,
      content: [
        {
          type: "text" as const,
          text: truncateToolResult(raw, r.callId, options?.outputDir),
        },
      ],
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
