// Provider-agnostic wire helpers.
//
// These helpers compose the per-provider DSLs into higher-level operations
// — "emit some assistant text", "emit a tool call", "emit usage", "emit a
// complete response". Each returns a sequence of Uint8Array chunks the
// caller can enqueue into a `SimulatedStream` at whatever virtual times the
// scenario dictates.
//
// The helpers stay deliberately small: they produce a minimal byte sequence
// for the common case. Tests that need exact control over event boundaries
// should call the per-provider helpers directly.

import type { TokenUsage } from "@intx/types/runtime";

import * as anthropic from "./anthropic";
import * as openai from "./openai";

/**
 * Identifier of the wire format to generate. Matches the built-in provider
 * keys from `@intx/inference/providers`' `createBuiltinRegistry()`:
 * - `"anthropic"` — Anthropic Messages API SSE
 * - `"openai"` — OpenAI Chat Completions SSE (also covers
 *   `openai-compatible`, which shares the same wire shape)
 */
export type Provider = "anthropic" | "openai";

/**
 * Emit a single text content block for the given provider.
 *
 * - Anthropic: `content_block_start(text)` + `content_block_delta(text_delta)`
 *   + `content_block_stop`.
 * - OpenAI: one `chunk` with `delta.content = text`.
 */
export function assistantText(provider: Provider, text: string): Uint8Array[] {
  if (provider === "anthropic") return anthropic.textBlock(text);
  return [openai.chunk({ content: text })];
}

/**
 * Emit a complete tool call (start + arguments) for the given provider.
 *
 * - Anthropic: tool_use block with `input_json_delta` carrying `argsJSON`.
 * - OpenAI: a start chunk with `id`+`name`, followed by one arguments chunk.
 *
 * `argsJSON` is a JSON string — pass `JSON.stringify(...)` for a structured
 * value or hand-written bytes for an adversarial case.
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
 * Emit a usage event for the given provider.
 *
 * - Anthropic: a `message_delta` carrying `usage.output_tokens` (only the
 *   `output` field is forwarded by the adapter; pass other fields via
 *   `usageHead` below if a full breakdown is needed).
 * - OpenAI: a final usage chunk with `prompt_tokens`/`completion_tokens` and
 *   optional cache/reasoning token details.
 *
 * For Anthropic input/cache usage, use `usageHead` instead — those live on
 * `message_start`, not `message_delta`.
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
 * Emit the initial usage frame (Anthropic's `message_start`'s `usage` block,
 * OpenAI has no equivalent — there the head usage is reported only in the
 * final `usageChunk`). On OpenAI this is a no-op.
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
 * Emit a transcript-shaped response (start + content + done) for the given
 * provider. The blocks are emitted at virtual time 0; callers that need
 * staggered delivery should enqueue each chunk at the desired offset.
 *
 * `headUsage` populates the leading usage frame (Anthropic only). `tailUsage`
 * populates the trailing usage frame. Omit either to skip.
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
