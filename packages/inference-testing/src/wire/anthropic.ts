// Anthropic SSE wire DSL.
//
// Each helper emits a single SSE event encoded as UTF-8 bytes the existing
// `createAnthropicAdapter()` in `@intx/inference/providers/anthropic`
// will parse without error. The DSL never touches adapter internals; it
// produces the same byte shape Anthropic's real `/v1/messages` stream would.

const encoder = new TextEncoder();

/**
 * Encode a JSON-serializable payload as an Anthropic-style SSE event. The
 * adapter's `parseResponse` only looks at the `data:` payload, but real
 * Anthropic streams prefix each event with an `event:` line — emitting both
 * keeps the bytes faithful to production and lets future stricter parsers
 * keep working.
 */
function encodeSSE(eventName: string, data: unknown): Uint8Array {
  return encoder.encode(
    `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`,
  );
}

/**
 * Options for `messageStart`. When `usage` is omitted the event is emitted
 * with no `message.usage` object — exactly the shape Anthropic uses for the
 * `message_start` event when it elects not to forward initial usage. With
 * `usage`, the adapter emits one `inference.usage` event.
 */
export type AnthropicMessageStartOpts = {
  /**
   * Usage block embedded in `message.usage`. Provider-specific cache fields
   * (`cache_read_input_tokens`, `cache_creation_input_tokens`) map to
   * `cacheRead` / `cacheWrite` in the harness's `TokenUsage`.
   */
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

/**
 * Emit a `message_start` SSE event. With a `usage` block, the adapter emits
 * one `inference.usage` event. Without one, it emits nothing.
 */
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
 * Options for `contentBlockStart`. `kind` selects the block shape; the
 * adapter only acts on `tool_use` blocks (emitting `inference.tool_call.start`)
 * but emits text blocks too so multi-block transcripts can be reproduced.
 */
export type AnthropicContentBlockStartOpts =
  | { index: number; kind: "text"; text?: string }
  | { index: number; kind: "thinking"; thinking?: string }
  | { index: number; kind: "tool_use"; id: string; name: string }
  | { index: number; kind: "raw"; contentBlock: Record<string, unknown> };

/**
 * Emit a `content_block_start` SSE event. The `raw` variant accepts an
 * arbitrary `contentBlock` payload for adversarial tests that need to model
 * unknown block kinds.
 */
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

/**
 * Options for `contentBlockDelta`. Three delta kinds correspond to the three
 * the adapter handles: text, thinking, and `input_json_delta` for tool args.
 */
export type AnthropicContentBlockDeltaOpts =
  | { index: number; kind: "text_delta"; text: string }
  | { index: number; kind: "thinking_delta"; thinking: string }
  | { index: number; kind: "input_json_delta"; partialJson: string }
  | { index: number; kind: "raw"; delta: Record<string, unknown> };

/**
 * Emit a `content_block_delta` SSE event. The `raw` variant accepts an
 * arbitrary `delta` payload (including unknown `type` values) for
 * malformed/adversarial scenarios.
 */
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

/**
 * Emit a `message_delta` SSE event. The adapter forwards `usage.output_tokens`
 * as an `inference.usage` event; `stop_reason` is informational.
 */
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
 * Wire-level escape hatch. Emits the supplied string as-is (no `event:`
 * or `data:` framing added). Use this when a test needs to model bytes the
 * structured helpers cannot express — split SSE events, malformed framing,
 * or experimental event types not yet covered by a helper.
 *
 * If a particular adversarial pattern recurs, add a helper instead of
 * sprinkling `raw()` calls across tests.
 */
export function raw(rawSSE: string): Uint8Array {
  return encoder.encode(rawSSE);
}

/**
 * Convenience: emit an Anthropic thinking block (start + delta + stop). The
 * adapter forwards each `thinking_delta` as an `inference.thinking.delta`.
 *
 * `index` defaults to 0; supply a higher index when interleaving thinking
 * with other content blocks (e.g., text at index 1 after thinking at 0).
 */
export function thinkingBlock(text: string, index = 0): Uint8Array[] {
  return [
    contentBlockStart({ index, kind: "thinking", thinking: "" }),
    contentBlockDelta({ index, kind: "thinking_delta", thinking: text }),
    contentBlockStop({ index }),
  ];
}

/**
 * Convenience: emit a complete tool_use content block (start + JSON args
 * delta + stop). `argsJSON` is the serialized arguments string; pass an
 * intentionally malformed string to model bad-JSON cases.
 *
 * `index` defaults to 0; supply a higher index when other content blocks
 * appear before the tool call.
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

/**
 * Convenience: emit a complete text content block (start + delta + stop)
 * at the supplied index. The adapter emits one `inference.text.delta`
 * carrying `text` as the token.
 */
export function textBlock(text: string, index = 0): Uint8Array[] {
  return [
    contentBlockStart({ index, kind: "text", text: "" }),
    contentBlockDelta({ index, kind: "text_delta", text }),
    contentBlockStop({ index }),
  ];
}

/**
 * Convenience: emit a complete malformed-JSON tool_use sequence. The adapter
 * still surfaces `tool_call.start` and `tool_call.delta` events; the
 * downstream harness's JSON.parse later sees an unparseable argument buffer
 * and falls back to `{ _raw: ... }`. Useful for testing recovery paths.
 */
export function malformedToolUseBlock(
  id: string,
  name: string,
  index = 0,
): Uint8Array[] {
  return toolUseBlock(id, name, '{"unterminated":', index);
}

/**
 * Convenience: emit a `content_block_delta` event whose `delta.type` is
 * unknown ("garbage_delta"). The adapter's validator accepts any `string`
 * for `delta.type`, so the event parses cleanly; the switch statement
 * then falls through every known `delta.type` branch and the adapter
 * emits no events. Useful for testing forward-compat.
 */
export function unknownDelta(index = 0): Uint8Array {
  return contentBlockDelta({
    index,
    kind: "raw",
    delta: { type: "garbage_delta", text: "ignored" },
  });
}
