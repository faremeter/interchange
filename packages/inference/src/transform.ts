// Cross-provider message transformation.
//
// When conversations cross provider boundaries the message history must be
// adapted: thinking blocks are stripped for foreign models, orphaned tool
// calls receive synthetic error results, and tool call IDs are normalized to
// a portable format.
//
// Transformation runs automatically when the target model differs from a
// message's originating model. The originating model is tracked per-message,
// not per-conversation.

import type {
  ConversationTurn,
  ContentBlock,
} from "@interchange/types/runtime";

export type TransformOptions = {
  targetModel: string;
  // When true, keep thinking blocks for messages that originated from the
  // same model. When false, strip all thinking blocks (cross-provider replay).
  keepThinkingForSameModel?: boolean;
};

export function transformMessages(
  messages: ConversationTurn[],
  options: TransformOptions,
): ConversationTurn[] {
  const { targetModel, keepThinkingForSameModel = true } = options;

  // First pass: strip thinking blocks and filter aborted assistant messages.
  const filtered = messages
    .map((msg): ConversationTurn | null => {
      if (msg.role === "assistant") {
        const isSameModel = msg.model === targetModel;
        const keepThinking = keepThinkingForSameModel && isSameModel;

        const filteredContent = msg.content.filter((block) => {
          if (block.type === "thinking") {
            return keepThinking;
          }
          return true;
        });

        // Filter out assistant messages that have no text or tool calls
        // (aborted/error messages with only thinking blocks removed).
        const hasUsableContent = filteredContent.some(
          (b) => b.type === "text" || b.type === "tool_call",
        );
        if (!hasUsableContent && filteredContent.length === 0) {
          return null;
        }

        return { ...msg, content: filteredContent };
      }
      return msg;
    })
    .filter((msg): msg is ConversationTurn => msg !== null);

  // Second pass: inject synthetic tool results for orphaned tool calls.
  return injectOrphanedToolResults(filtered);
}

function injectOrphanedToolResults(
  messages: ConversationTurn[],
): ConversationTurn[] {
  const result: ConversationTurn[] = [];

  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i];
    if (msg === undefined) continue;
    result.push(msg);

    if (msg.role !== "assistant") continue;

    const toolCalls = msg.content.filter(
      (b): b is Extract<ContentBlock, { type: "tool_call" }> =>
        b.type === "tool_call",
    );

    if (toolCalls.length === 0) continue;

    // Collect tool call IDs from this assistant message.
    const calledIds = new Set(toolCalls.map((tc) => tc.id));

    // Check the following messages for results that cover these calls.
    const coveredIds = new Set<string>();
    for (let j = i + 1; j < messages.length; j++) {
      const next = messages[j];
      if (next === undefined) break;
      if (next.role !== "user") break;

      for (const block of next.content) {
        if (block.type === "tool_result") {
          coveredIds.add(block.callId);
        }
      }
    }

    // Find which tool calls have no corresponding result.
    const orphanedIds = [...calledIds].filter((id) => !coveredIds.has(id));

    if (orphanedIds.length === 0) continue;

    // Inject a synthetic user message with error tool results for each orphan.
    const syntheticBlocks: ContentBlock[] = orphanedIds.map((id) => ({
      type: "tool_result" as const,
      callId: id,
      content: [
        {
          type: "text" as const,
          text: "Tool execution was interrupted before completion.",
        },
      ],
      isError: true,
    }));

    result.push({
      role: "user",
      content: syntheticBlocks,
      timestamp: Date.now(),
    });
  }

  return result;
}

// ---------------------------------------------------------------------------
// Tool call ID normalization
//
// OpenAI Responses API generates 450+ character IDs with pipes. Anthropic
// has strict format requirements. IDs are normalized to a short portable
// format with a bidirectional map for round-trip fidelity.
// ---------------------------------------------------------------------------

const PORTABLE_ID_PREFIX = "tc_";

export type IDNormalizer = {
  normalize(providerId: string): string;
  resolve(portableId: string): string | undefined;
};

export function createIDNormalizer(): IDNormalizer {
  const portableToProvider = new Map<string, string>();
  const providerToPortable = new Map<string, string>();
  let counter = 0;

  return {
    normalize(providerId: string): string {
      const existing = providerToPortable.get(providerId);
      if (existing !== undefined) return existing;

      const portable = `${PORTABLE_ID_PREFIX}${(++counter).toString(36)}`;
      providerToPortable.set(providerId, portable);
      portableToProvider.set(portable, providerId);
      return portable;
    },

    resolve(portableId: string): string | undefined {
      return portableToProvider.get(portableId);
    },
  };
}
