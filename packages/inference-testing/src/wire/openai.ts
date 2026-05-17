// OpenAI SSE wire DSL.
//
// Each helper emits a single SSE event encoded as UTF-8 bytes the existing
// `createOpenAIAdapter()` in `@interchange/inference/providers/openai` will
// parse without error. The DSL produces the same byte shape OpenAI's
// `/v1/chat/completions` stream emits, including the terminal `[DONE]`
// sentinel that `parseSSE` consumes internally.

const encoder = new TextEncoder();

/**
 * Encode a JSON-serializable payload as an OpenAI-style SSE event. OpenAI
 * does not emit an `event:` line — just `data: <json>\n\n`.
 */
function encodeSSE(data: unknown): Uint8Array {
  return encoder.encode(`data: ${JSON.stringify(data)}\n\n`);
}

/**
 * A single tool-call delta within `choices[0].delta.tool_calls[]`. Use `id`
 * + `name` for the first delta of a tool call (the adapter emits
 * `inference.tool_call.start`); subsequent deltas typically carry only
 * `arguments` (the adapter emits `inference.tool_call.delta` keyed by
 * `index`).
 */
export type OpenAIToolCallDeltaOpts = {
  index: number;
  id?: string;
  name?: string;
  argumentsChunk?: string;
};

/**
 * Options for `chunk`. Provides every field the adapter currently consumes:
 * text content, reasoning text (under either `reasoning_content` or
 * `reasoning`), tool-call deltas (index-based), and a usage block.
 *
 * Pass `extra` to layer arbitrary fields onto the emitted `chunk` object —
 * useful for non-standard fields the adapter ignores but a test needs to
 * see surface in the bytes.
 */
export type OpenAIChunkOpts = {
  /** Text content forwarded as `inference.text.delta`. */
  content?: string;
  /** Force `delta.content` to be the wire value `null` (some providers do this). */
  contentNull?: boolean;
  /** Reasoning text in `delta.reasoning_content`. */
  reasoningContent?: string;
  /** Reasoning text in `delta.reasoning` (the OpenRouter shape). */
  reasoning?: string;
  /** Tool-call deltas under `choices[0].delta.tool_calls[]`. */
  toolCalls?: OpenAIToolCallDeltaOpts[];
  /** `finish_reason` placed on the choice. */
  finishReason?: string | null;
  /** Index of the choice; defaults to 0. */
  choiceIndex?: number;
  /** Optional usage block on the chunk (for `stream_options.include_usage`). */
  usage?: {
    promptTokens?: number;
    completionTokens?: number;
    cachedTokens?: number;
    reasoningTokens?: number;
  };
  /** Arbitrary extra fields layered onto the top-level chunk object. */
  extra?: Record<string, unknown>;
};

/**
 * Emit one OpenAI-style streaming chunk. Supply only the fields the test
 * cares about — every option is independent.
 */
export function chunk(opts: OpenAIChunkOpts = {}): Uint8Array {
  const delta: Record<string, unknown> = {};
  if (opts.contentNull === true) {
    delta["content"] = null;
  } else if (opts.content !== undefined) {
    delta["content"] = opts.content;
  }
  if (opts.reasoningContent !== undefined) {
    delta["reasoning_content"] = opts.reasoningContent;
  }
  if (opts.reasoning !== undefined) delta["reasoning"] = opts.reasoning;
  if (opts.toolCalls !== undefined && opts.toolCalls.length > 0) {
    delta["tool_calls"] = opts.toolCalls.map((tc) => {
      const out: Record<string, unknown> = { index: tc.index };
      if (tc.id !== undefined) out["id"] = tc.id;
      const fn: Record<string, unknown> = {};
      if (tc.name !== undefined) fn["name"] = tc.name;
      if (tc.argumentsChunk !== undefined) fn["arguments"] = tc.argumentsChunk;
      if (tc.id !== undefined) out["type"] = "function";
      if (Object.keys(fn).length > 0) out["function"] = fn;
      return out;
    });
  }

  const choice: Record<string, unknown> = {
    index: opts.choiceIndex ?? 0,
    delta,
    finish_reason: opts.finishReason ?? null,
  };

  const payload: Record<string, unknown> = {
    id: "chatcmpl-test",
    object: "chat.completion.chunk",
    choices: [choice],
    ...(opts.extra ?? {}),
  };

  if (opts.usage !== undefined) {
    const u = opts.usage;
    const usage: Record<string, unknown> = {};
    if (u.promptTokens !== undefined) usage["prompt_tokens"] = u.promptTokens;
    if (u.completionTokens !== undefined) {
      usage["completion_tokens"] = u.completionTokens;
    }
    if (u.cachedTokens !== undefined) {
      usage["prompt_tokens_details"] = { cached_tokens: u.cachedTokens };
    }
    if (u.reasoningTokens !== undefined) {
      usage["completion_tokens_details"] = {
        reasoning_tokens: u.reasoningTokens,
      };
    }
    payload["usage"] = usage;
    // Real OpenAI usage chunks include an empty choices array.
    payload["choices"] = [];
  }

  return encodeSSE(payload);
}

/**
 * Emit the `[DONE]` sentinel. `parseSSE` consumes this and terminates the
 * iteration, so adapters never see an event for it. Tests that drive a
 * complete request through `parseSSE` should end with this byte.
 */
export function done(): Uint8Array {
  return encoder.encode("data: [DONE]\n\n");
}

/**
 * Wire-level escape hatch. Emits the supplied string as-is (no `data:` or
 * trailing blank line added). Use when a test needs to model bytes the
 * structured helpers cannot express — split SSE events, malformed framing,
 * or experimental event types not yet covered by a helper.
 */
export function raw(rawSSE: string): Uint8Array {
  return encoder.encode(rawSSE);
}

/**
 * Convenience: emit a tool-call start chunk (carries `id` + `name` for index
 * 0; empty arguments). The adapter emits `inference.tool_call.start`.
 */
export function toolCallStart(
  index: number,
  id: string,
  name: string,
): Uint8Array {
  return chunk({
    toolCalls: [{ index, id, name, argumentsChunk: "" }],
  });
}

/**
 * Convenience: emit an index-based tool-call argument fragment. The adapter
 * emits `inference.tool_call.delta` keyed by `String(index)`; the harness
 * remaps it to the real callId from the corresponding `toolCallStart`.
 */
export function toolCallArgumentsDelta(
  index: number,
  argumentsChunk: string,
): Uint8Array {
  return chunk({
    toolCalls: [{ index, argumentsChunk }],
  });
}

/**
 * Convenience: emit a complete tool-call sequence (start + argument chunks +
 * empty trailing chunk). `argChunks` is broken across multiple delta chunks
 * so consumers exercise their chunked-argument accumulation path.
 *
 * Tests that need a specific chunk boundary should call `toolCallStart` and
 * `toolCallArgumentsDelta` directly.
 */
export function toolCallSequence(
  index: number,
  id: string,
  name: string,
  argChunks: string[],
): Uint8Array[] {
  const chunks = [toolCallStart(index, id, name)];
  for (const argChunk of argChunks) {
    chunks.push(toolCallArgumentsDelta(index, argChunk));
  }
  return chunks;
}

/**
 * Convenience: emit a deprecated `function_call` chunk (pre-`tool_calls`
 * OpenAI shape). The current `parseResponse` does not handle this directly
 * because the adapter only understands the modern `tool_calls` array, but
 * the bytes match what older deployments emit — useful for tests that
 * verify the adapter quietly ignores legacy events.
 */
export function legacyFunctionCall(name: string, args: string): Uint8Array {
  return encodeSSE({
    id: "chatcmpl-test",
    object: "chat.completion.chunk",
    choices: [
      {
        index: 0,
        delta: { function_call: { name, arguments: args } },
        finish_reason: null,
      },
    ],
  });
}

/**
 * Convenience: emit a complete malformed-arguments tool call (start + a
 * single unterminated JSON fragment). The adapter still emits
 * `inference.tool_call.start` and `inference.tool_call.delta`; the harness's
 * final `JSON.parse` falls back to `{ _raw: ... }`.
 */
export function malformedToolCall(
  index: number,
  id: string,
  name: string,
): Uint8Array[] {
  return [
    toolCallStart(index, id, name),
    toolCallArgumentsDelta(index, '{"unterminated":'),
  ];
}

/**
 * Convenience: emit a chunk that mimics a real OpenAI usage-only frame —
 * empty `choices` and a populated `usage`. The adapter emits one
 * `inference.usage`.
 */
export function usageChunk(opts: {
  promptTokens?: number;
  completionTokens?: number;
  cachedTokens?: number;
  reasoningTokens?: number;
}): Uint8Array {
  return chunk({ usage: opts });
}

/**
 * Convenience: emit a chunk with a missing `delta.role` and `null` content.
 * The adapter emits no events. Useful for confirming the adapter tolerates
 * the keep-alive shape some gateways inject.
 */
export function emptyKeepAliveChunk(): Uint8Array {
  return chunk({ contentNull: true });
}
