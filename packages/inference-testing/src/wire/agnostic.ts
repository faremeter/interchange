// Provider-agnostic wire helpers. Compose the per-provider DSLs into
// higher-level operations ("assistant text", "tool call", "usage",
// "complete response"), each returning Uint8Array chunks the caller
// enqueues into a `SimulatedStream`. They stay small; tests that need exact
// event boundaries call the per-provider helpers directly.

import type { TokenUsage } from "@intx/types/runtime";

import * as anthropic from "./anthropic";
import * as openai from "./openai";

/** Wire format to generate. `"openai"` also covers `openai-compatible` (same wire shape). */
export type Provider = "anthropic" | "openai";

/** Emit a single text content block (Anthropic block sequence; OpenAI one `chunk`). */
export function assistantText(provider: Provider, text: string): Uint8Array[] {
  if (provider === "anthropic") return anthropic.textBlock(text);
  return [openai.chunk({ content: text })];
}

/**
 * Emit a complete tool call for the provider. `argsJSON` is a JSON string —
 * pass `JSON.stringify(...)` for a structured value or hand-written bytes
 * for an adversarial case.
 */
export function toolCall(
  provider: Provider,
  callId: string,
  name: string,
  argsJSON: string,
  blockIndex = 0,
): Uint8Array[] {
  if (provider === "anthropic") {
    return anthropic.toolUseBlock(callId, name, argsJSON, blockIndex);
  }
  return openai.toolCallSequence(blockIndex, callId, name, [argsJSON]);
}

/**
 * Emit a usage event for the provider. Anthropic forwards only `output` via
 * `message_delta`; input/cache usage lives on `message_start` — use
 * `usageHead` for that. OpenAI emits a final usage chunk.
 */
export function usage(
  provider: Provider,
  tokenUsage: TokenUsage,
): Uint8Array[] {
  if (provider === "anthropic") {
    return [anthropic.messageDelta({ outputTokens: tokenUsage.output })];
  }
  return [
    openai.usageChunk({
      promptTokens: tokenUsage.input,
      completionTokens: tokenUsage.output,
      cachedTokens: tokenUsage.cacheRead,
      reasoningTokens: tokenUsage.thinking,
    }),
  ];
}

/**
 * Emit the initial usage frame (Anthropic `message_start` only; OpenAI
 * reports usage only in the final chunk, so this is a no-op there).
 */
export function usageHead(
  provider: Provider,
  tokenUsage: TokenUsage,
): Uint8Array[] {
  if (provider === "anthropic") {
    return [
      anthropic.messageStart({
        usage: {
          inputTokens: tokenUsage.input,
          outputTokens: tokenUsage.output,
          cacheReadInputTokens: tokenUsage.cacheRead,
          cacheCreationInputTokens: tokenUsage.cacheWrite,
        },
      }),
    ];
  }
  return [];
}

/**
 * Emit a transcript-shaped response (start + content + done) for the
 * provider. Chunks are emitted at virtual time 0; callers needing staggered
 * delivery enqueue each chunk at the desired offset. `headUsage` (Anthropic
 * only) and `tailUsage` are optional.
 */
export function completeResponse(
  provider: Provider,
  opts: {
    text?: string;
    toolCalls?: { callId: string; name: string; argsJSON: string }[];
    headUsage?: TokenUsage;
    tailUsage?: TokenUsage;
  } = {},
): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  if (provider === "anthropic") {
    if (opts.headUsage !== undefined) {
      chunks.push(...usageHead("anthropic", opts.headUsage));
    } else {
      chunks.push(anthropic.messageStart());
    }
    let nextIndex = 0;
    if (opts.text !== undefined) {
      chunks.push(...anthropic.textBlock(opts.text, nextIndex));
      nextIndex += 1;
    }
    for (const tc of opts.toolCalls ?? []) {
      chunks.push(
        ...anthropic.toolUseBlock(tc.callId, tc.name, tc.argsJSON, nextIndex),
      );
      nextIndex += 1;
    }
    if (opts.tailUsage !== undefined) {
      chunks.push(...usage("anthropic", opts.tailUsage));
    } else {
      chunks.push(anthropic.messageDelta({ stopReason: "end_turn" }));
    }
    chunks.push(anthropic.messageStop());
    return chunks;
  }

  if (opts.text !== undefined) {
    chunks.push(openai.chunk({ content: opts.text }));
  }
  let toolIdx = 0;
  for (const tc of opts.toolCalls ?? []) {
    chunks.push(openai.toolCallStart(toolIdx, tc.callId, tc.name));
    chunks.push(openai.toolCallArgumentsDelta(toolIdx, tc.argsJSON));
    toolIdx += 1;
  }
  if (opts.tailUsage !== undefined) {
    chunks.push(...usage("openai", opts.tailUsage));
  }
  chunks.push(openai.done());
  return chunks;
}
