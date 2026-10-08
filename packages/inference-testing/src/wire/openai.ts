// OpenAI SSE wire DSL. Each helper emits one SSE event as UTF-8 bytes the
// `createOpenAIAdapter()` in `@intx/inference/providers/openai` parses
// without error — the same byte shape OpenAI's `/v1/chat/completions` stream
// emits, including the terminal `[DONE]` sentinel `parseSSE` consumes.

const encoder = new TextEncoder();

/** Encode a payload as an OpenAI-style SSE event: `data: <json>\n\n` (no `event:` line). */
function encodeSSE(data: unknown): Uint8Array {
  return encoder.encode(`data: ${JSON.stringify(data)}\n\n`);
}

/**
 * A single tool-call delta within `choices[0].delta.tool_calls[]`. The
 * first delta carries `id`+`name` (`inference.tool_call.start`); later
 * deltas carry only `argumentsChunk` (`inference.tool_call.delta` by index).
 */
export type OpenAIToolCallDeltaOpts = {
  index: number;
  id?: string;
  name?: string;
  argumentsChunk?: string;
};

/**
 * Options for `chunk`: text content, reasoning (under either
 * `reasoning_content` or `reasoning`), tool-call deltas, and a usage
 * block. `extra` layers arbitrary fields onto the chunk object.
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
  /**
   * Refusal text in `delta.refusal` (OpenAI strict-mode structured
   * outputs). Forwarded as `inference.refusal.delta`.
   */
  refusal?: string;
  /** Force `delta.refusal` to be the wire value `null`. */
  refusalNull?: boolean;
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

/** Emit one OpenAI-style streaming chunk; every option is independent. */
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
  if (opts.refusalNull === true) {
    delta["refusal"] = null;
  } else if (opts.refusal !== undefined) {
    delta["refusal"] = opts.refusal;
  }
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

/** Emit the `[DONE]` sentinel `parseSSE` consumes to end an iteration. */
export function done(): Uint8Array {
  return encoder.encode("data: [DONE]\n\n");
}

/** Wire-level escape hatch: emit the string as-is (no `data:` framing). */
export function raw(rawSSE: string): Uint8Array {
  return encoder.encode(rawSSE);
}

/** Convenience: a tool-call start chunk (`id`+`name`, empty args); the adapter emits `inference.tool_call.start`. */
export function toolCallStart(
  index: number,
  id: string,
  name: string,
): Uint8Array {
  return chunk({
    toolCalls: [{ index, id, name, argumentsChunk: "" }],
  });
}

/** Convenience: an index-keyed tool-call argument fragment (`inference.tool_call.delta`). */
export function toolCallArgumentsDelta(
  index: number,
  argumentsChunk: string,
): Uint8Array {
  return chunk({
    toolCalls: [{ index, argumentsChunk }],
  });
}

/** Convenience: a complete tool-call sequence with `argChunks` split across deltas. */
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

/** Convenience: a legacy `function_call` chunk (pre-`tool_calls` shape) the adapter ignores. */
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

/** Convenience: a tool call with an unterminated JSON args fragment; the harness's `JSON.parse` falls back to `{ _raw: ... }`. */
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

/** Convenience: a usage-only frame (empty `choices`, populated `usage`); the adapter emits one `inference.usage`. */
export function usageChunk(opts: {
  promptTokens?: number;
  completionTokens?: number;
  cachedTokens?: number;
  reasoningTokens?: number;
}): Uint8Array {
  return chunk({ usage: opts });
}

/** Convenience: a keep-alive chunk (`null` content, no `delta.role`) the adapter silently ignores. */
export function emptyKeepAliveChunk(): Uint8Array {
  return chunk({ contentNull: true });
}
