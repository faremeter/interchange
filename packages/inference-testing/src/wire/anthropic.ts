// Anthropic SSE wire DSL. Each helper emits one SSE event as UTF-8 bytes the
// `createAnthropicAdapter()` in `@intx/inference/providers/anthropic` parses
// without error — the same byte shape Anthropic's real `/v1/messages` stream
// emits.

const encoder = new TextEncoder();

/**
 * Encode a payload as an Anthropic-style SSE event: `event:` line plus
 * `data:` payload, as real Anthropic streams emit. The adapter's
 * `parseResponse` reads only the `data:` payload.
 */
function encodeSSE(eventName: string, data: unknown): Uint8Array {
  return encoder.encode(
    `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`,
  );
}

/**
 * Options for `messageStart`. When `usage` is omitted no `message.usage`
 * object is emitted (Anthropic's shape when it does not forward initial
 * usage). With `usage`, the adapter emits one `inference.usage` event.
 */
export type AnthropicMessageStartOpts = {
  /** Usage block in `message.usage`; cache fields map to `cacheRead`/`cacheWrite`. */
  usage?: {
    inputTokens?: number;
    outputTokens?: number;
    cacheReadInputTokens?: number;
    cacheCreationInputTokens?: number;
  };
  /** Override the model id reported in `message.model`. */
  model?: string;
  /** Override the message id reported in `message.id`. */
  id?: string;
};

/** Emit a `message_start` SSE event. */
export function messageStart(opts: AnthropicMessageStartOpts = {}): Uint8Array {
  const message: Record<string, unknown> = {
    id: opts.id ?? "msg_test",
    type: "message",
    role: "assistant",
    model: opts.model ?? "claude-test",
    content: [],
    stop_reason: null,
    stop_sequence: null,
  };
  if (opts.usage !== undefined) {
    const u = opts.usage;
    const usage: Record<string, number> = {};
    if (u.inputTokens !== undefined) usage["input_tokens"] = u.inputTokens;
    if (u.outputTokens !== undefined) usage["output_tokens"] = u.outputTokens;
    if (u.cacheReadInputTokens !== undefined) {
      usage["cache_read_input_tokens"] = u.cacheReadInputTokens;
    }
    if (u.cacheCreationInputTokens !== undefined) {
      usage["cache_creation_input_tokens"] = u.cacheCreationInputTokens;
    }
    message["usage"] = usage;
  }
  return encodeSSE("message_start", { type: "message_start", message });
}

/**
 * Options for `contentBlockStart`. The adapter acts only on `tool_use`
 * blocks (`inference.tool_call.start`); text/thinking blocks are emitted for
 * faithful multi-block transcripts.
 */
export type AnthropicContentBlockStartOpts =
  | { index: number; kind: "text"; text?: string }
  | { index: number; kind: "thinking"; thinking?: string }
  | { index: number; kind: "tool_use"; id: string; name: string }
  | { index: number; kind: "raw"; contentBlock: Record<string, unknown> };

/** Emit a `content_block_start` SSE event. `raw` takes an arbitrary payload. */
export function contentBlockStart(
  opts: AnthropicContentBlockStartOpts,
): Uint8Array {
  let contentBlock: Record<string, unknown>;
  switch (opts.kind) {
    case "text":
      contentBlock = { type: "text", text: opts.text ?? "" };
      break;
    case "thinking":
      contentBlock = { type: "thinking", thinking: opts.thinking ?? "" };
      break;
    case "tool_use":
      contentBlock = { type: "tool_use", id: opts.id, name: opts.name };
      break;
    case "raw":
      contentBlock = opts.contentBlock;
      break;
  }
  return encodeSSE("content_block_start", {
    type: "content_block_start",
    index: opts.index,
    content_block: contentBlock,
  });
}

/** Options for `contentBlockDelta`; kinds mirror the deltas the adapter handles. */
export type AnthropicContentBlockDeltaOpts =
  | { index: number; kind: "text_delta"; text: string }
  | { index: number; kind: "thinking_delta"; thinking: string }
  | { index: number; kind: "signature_delta"; signature: string }
  | { index: number; kind: "input_json_delta"; partialJson: string }
  | { index: number; kind: "raw"; delta: Record<string, unknown> };

/** Emit a `content_block_delta` SSE event. `raw` takes an arbitrary payload. */
export function contentBlockDelta(
  opts: AnthropicContentBlockDeltaOpts,
): Uint8Array {
  let delta: Record<string, unknown>;
  switch (opts.kind) {
    case "text_delta":
      delta = { type: "text_delta", text: opts.text };
      break;
    case "thinking_delta":
      delta = { type: "thinking_delta", thinking: opts.thinking };
      break;
    case "signature_delta":
      delta = { type: "signature_delta", signature: opts.signature };
      break;
    case "input_json_delta":
      delta = { type: "input_json_delta", partial_json: opts.partialJson };
      break;
    case "raw":
      delta = opts.delta;
      break;
  }
  return encodeSSE("content_block_delta", {
    type: "content_block_delta",
    index: opts.index,
    delta,
  });
}

/** Options for `contentBlockStop`. */
export type AnthropicContentBlockStopOpts = { index: number };

/** Emit a `content_block_stop` SSE event. */
export function contentBlockStop(
  opts: AnthropicContentBlockStopOpts,
): Uint8Array {
  return encodeSSE("content_block_stop", {
    type: "content_block_stop",
    index: opts.index,
  });
}

/** Options for `messageDelta`. */
export type AnthropicMessageDeltaOpts = {
  /** Stop reason placed in `delta.stop_reason`. */
  stopReason?: "end_turn" | "tool_use" | "max_tokens" | "stop_sequence";
  /** Output token count for `usage.output_tokens`. */
  outputTokens?: number;
};

/** Emit a `message_delta` SSE event; the adapter forwards `usage.output_tokens` as `inference.usage`. */
export function messageDelta(opts: AnthropicMessageDeltaOpts = {}): Uint8Array {
  const delta: Record<string, unknown> = {};
  if (opts.stopReason !== undefined) delta["stop_reason"] = opts.stopReason;
  const payload: Record<string, unknown> = {
    type: "message_delta",
    delta,
  };
  if (opts.outputTokens !== undefined) {
    payload["usage"] = { output_tokens: opts.outputTokens };
  }
  return encodeSSE("message_delta", payload);
}

/** Emit a `message_stop` SSE event. */
export function messageStop(): Uint8Array {
  return encodeSSE("message_stop", { type: "message_stop" });
}

/** Emit a `ping` SSE event. The adapter ignores it; useful for heartbeats. */
export function ping(): Uint8Array {
  return encodeSSE("ping", { type: "ping" });
}

/**
 * Wire-level escape hatch: emits the string as-is, no `event:`/`data:`
 * framing, for byte shapes the structured helpers cannot express (split
 * events, malformed framing, new event types). Prefer a helper if a pattern
 * recurs.
 */
export function raw(rawSSE: string): Uint8Array {
  return encoder.encode(rawSSE);
}

/**
 * Convenience: emit a thinking block (start + delta + optional
 * `signature_delta` + stop). Deltas forward as `inference.thinking.delta`;
 * a trailing signature becomes `inference.block.signature` on the final
 * ThinkingBlock. `index` defaults to 0.
 */
export function thinkingBlock(
  text: string,
  index = 0,
  signature?: string,
): Uint8Array[] {
  const events: Uint8Array[] = [
    contentBlockStart({ index, kind: "thinking", thinking: "" }),
    contentBlockDelta({ index, kind: "thinking_delta", thinking: text }),
  ];
  if (signature !== undefined) {
    events.push(
      contentBlockDelta({ index, kind: "signature_delta", signature }),
    );
  }
  events.push(contentBlockStop({ index }));
  return events;
}

/**
 * Convenience: emit a complete tool_use block (start + args delta + stop).
 * `argsJSON` is the serialized arguments string; pass malformed JSON to
 * model bad-JSON cases. `index` defaults to 0.
 */
export function toolUseBlock(
  id: string,
  name: string,
  argsJSON: string,
  index = 0,
): Uint8Array[] {
  return [
    contentBlockStart({ index, kind: "tool_use", id, name }),
    contentBlockDelta({
      index,
      kind: "input_json_delta",
      partialJson: argsJSON,
    }),
    contentBlockStop({ index }),
  ];
}

/** Convenience: emit a complete text block; the adapter emits one `inference.text.delta`. */
export function textBlock(text: string, index = 0): Uint8Array[] {
  return [
    contentBlockStart({ index, kind: "text", text: "" }),
    contentBlockDelta({ index, kind: "text_delta", text }),
    contentBlockStop({ index }),
  ];
}

/** Convenience: a tool_use block with unterminated JSON args; the harness's `JSON.parse` falls back to `{ _raw: ... }`. */
export function malformedToolUseBlock(
  id: string,
  name: string,
  index = 0,
): Uint8Array[] {
  return toolUseBlock(id, name, '{"unterminated":', index);
}

/**
 * Convenience: emit a `content_block_delta` with an unknown `delta.type`.
 * The validator accepts any string type, so the event parses; the adapter
 * then emits nothing (forward-compat test).
 */
export function unknownDelta(index = 0): Uint8Array {
  return contentBlockDelta({
    index,
    kind: "raw",
    delta: { type: "garbage_delta", text: "ignored" },
  });
}

/**
 * Convenience: emit a redacted_thinking block, delivered as a one-shot
 * `content_block_start` with an opaque `data` blob (no delta stream).
 * Must echo back verbatim on follow-up turns or the API rejects the
 * request. `index` defaults to 0.
 */
export function redactedThinkingBlock(data: string, index = 0): Uint8Array[] {
  return [
    contentBlockStart({
      index,
      kind: "raw",
      contentBlock: { type: "redacted_thinking", data },
    }),
    contentBlockStop({ index }),
  ];
}

/**
 * Convenience: emit a server_tool_use block (Anthropic's server-side tool
 * shape: code_execution, web_search, etc.). Same wire pattern as `tool_use`
 * with an empty input object. `index` defaults to 0.
 */
export function serverToolUseBlock(
  id: string,
  name: string,
  argsJSON: string,
  index = 0,
): Uint8Array[] {
  return [
    contentBlockStart({
      index,
      kind: "raw",
      contentBlock: { type: "server_tool_use", id, name, input: {} },
    }),
    contentBlockDelta({
      index,
      kind: "input_json_delta",
      partialJson: argsJSON,
    }),
    contentBlockStop({ index }),
  ];
}

/** Inner result shape for `codeExecutionToolResultBlock`, mirroring Anthropic's `code_execution_result` payload. */
export type AnthropicCodeExecutionResult = {
  stdout?: string;
  stderr?: string;
  return_code?: number;
  abort_reason?: string | null;
  content?: Record<string, unknown>[];
};

/**
 * Convenience: emit a code_execution_tool_result block, delivered as a
 * one-shot `content_block_start` with the full payload (no delta stream).
 * `toolUseId` correlates back to the preceding `server_tool_use` id.
 * `index` defaults to 0.
 */
export function codeExecutionToolResultBlock(
  toolUseId: string,
  result: AnthropicCodeExecutionResult,
  index = 0,
): Uint8Array[] {
  return [
    contentBlockStart({
      index,
      kind: "raw",
      contentBlock: {
        type: "code_execution_tool_result",
        tool_use_id: toolUseId,
        content: { type: "code_execution_result", ...result },
      },
    }),
    contentBlockStop({ index }),
  ];
}

/**
 * Convenience: emit a text block with inline citations: start with an
 * empty `citations: []`, stream the body via `text_delta`, then one
 * `citations_delta` per citation (shapes vary by source; pass them
 * already-shaped). `index` defaults to 0.
 */
export function textBlockWithCitations(
  text: string,
  citations: Record<string, unknown>[],
  index = 0,
): Uint8Array[] {
  const events: Uint8Array[] = [
    contentBlockStart({
      index,
      kind: "raw",
      contentBlock: { type: "text", text: "", citations: [] },
    }),
    contentBlockDelta({ index, kind: "text_delta", text }),
  ];
  for (const citation of citations) {
    events.push(
      contentBlockDelta({
        index,
        kind: "raw",
        delta: { type: "citations_delta", citation },
      }),
    );
  }
  events.push(contentBlockStop({ index }));
  return events;
}
