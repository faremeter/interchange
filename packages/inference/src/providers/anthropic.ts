import { type } from "arktype";

import type {
  ConversationTurn,
  ContentBlock,
  InferenceEvent,
  InferenceOptions,
  PartialMessage,
  TokenUsage,
} from "@intx/types/runtime";
import type { ProviderAdapter, BuiltRequest } from "../adapter";
import { ProtocolMismatchError } from "../errors";

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

function buildRequest(
  messages: ConversationTurn[],
  model: string,
  options: InferenceOptions,
): BuiltRequest {
  const systemMessages = messages.filter((m) => m.role === "system");
  const conversationMessages = messages.filter((m) => m.role !== "system");

  const systemText = systemMessages
    .flatMap((m) =>
      m.content
        .filter((b): b is { type: "text"; text: string } => b.type === "text")
        .map((b) => b.text),
    )
    .join("\n\n");

  const effectiveSystem = options.systemPrompt
    ? options.systemPrompt
    : systemText || undefined;

  const body: Record<string, unknown> = {
    model,
    max_tokens: options.maxTokens ?? 4096,
    messages: conversationMessages.map((msg, i) => {
      // Place a cache breakpoint on the last user message so all prior
      // turns are cached on the next request.
      const isLastUser =
        msg.role !== "assistant" &&
        conversationMessages.slice(i + 1).every((m) => m.role === "assistant");
      return toAnthropicMessage(msg, isLastUser);
    }),
    stream: true,
  };

  if (effectiveSystem) {
    body["system"] = [
      {
        type: "text",
        text: effectiveSystem,
        cache_control: { type: "ephemeral" },
      },
    ];
  }

  if (options.thinking?.enabled) {
    body["thinking"] = {
      type: "enabled",
      budget_tokens: options.thinking.budgetTokens ?? 1024,
    };
  }

  if (options.tools !== undefined && options.tools.length > 0) {
    const tools: Record<string, unknown>[] = options.tools.map((t) => ({
      name: t.name,
      description: t.description,
      input_schema: t.inputSchema,
    }));
    const lastTool = tools[tools.length - 1];
    if (lastTool !== undefined) {
      lastTool["cache_control"] = { type: "ephemeral" };
    }
    body["tools"] = tools;
  }

  if (options.temperature !== undefined) {
    body["temperature"] = options.temperature;
  }

  return {
    url: "/v1/messages",
    headers: {
      "content-type": "application/json",
      "x-api-key": "", // Filled by the harness from InferenceSource.apiKey
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify(body),
  };
}

function toAnthropicMessage(
  msg: ConversationTurn,
  cacheLastBlock?: boolean,
): Record<string, unknown> {
  const role = msg.role === "assistant" ? "assistant" : "user";
  const content = msg.content.map(toAnthropicBlock);
  if (cacheLastBlock) {
    const lastBlock = content[content.length - 1];
    if (lastBlock !== undefined) {
      lastBlock["cache_control"] = { type: "ephemeral" };
    }
  }
  return { role, content };
}

function toAnthropicBlock(block: ContentBlock): Record<string, unknown> {
  switch (block.type) {
    case "text":
      return { type: "text", text: block.text };

    case "thinking":
      return {
        type: "thinking",
        thinking: block.thinking,
        ...(block.signature !== undefined
          ? { signature: block.signature }
          : {}),
      };

    case "redacted_thinking":
      throw new Error(
        "Anthropic adapter does not yet echo redacted_thinking content " +
          "blocks back on follow-up turns.",
      );

    case "image": {
      const source = block.source;
      if (source.kind === "base64") {
        return {
          type: "image",
          source: {
            type: "base64",
            media_type: source.mimeType,
            data: source.data,
          },
        };
      }
      if (source.kind === "file-reference") {
        throw new Error(
          "Anthropic adapter does not yet handle file-reference image " +
            "sources.",
        );
      }
      // Exhaustiveness: a new MediaSource variant added without a case
      // here fails this compile-time check.
      source satisfies never;
      throw new Error(`unreachable: unknown MediaSource kind`);
    }

    case "audio":
    case "video":
    case "document":
      throw new Error(
        `Anthropic adapter does not yet handle ${block.type} content blocks.`,
      );

    case "citation":
      throw new Error(
        "Anthropic adapter does not yet emit citation content blocks.",
      );

    case "code_execution_request":
    case "code_execution_result":
      throw new Error(
        `Anthropic adapter does not yet emit ${block.type} content blocks.`,
      );

    case "tool_call":
      return {
        type: "tool_use",
        id: block.id,
        name: block.name,
        input: block.arguments,
      };

    case "tool_result":
      return {
        type: "tool_result",
        tool_use_id: block.callId,
        content: block.content.map((c) => {
          if (c.type === "text") {
            return { type: "text", text: c.text };
          }
          if (c.type !== "image") {
            // audio / video / document in tool_result content. The
            // ContentBlock union allows these so the type system can
            // grow uniformly; no Anthropic wire path exists today.
            throw new Error(
              `Anthropic adapter does not yet handle ${c.type} content ` +
                `blocks in tool results.`,
            );
          }
          const source = c.source;
          if (source.kind === "base64") {
            return {
              type: "image",
              source: {
                type: "base64",
                media_type: source.mimeType,
                data: source.data,
              },
            };
          }
          if (source.kind === "file-reference") {
            throw new Error(
              "Anthropic adapter does not yet handle file-reference image " +
                "sources in tool results.",
            );
          }
          source satisfies never;
          throw new Error(`unreachable: unknown MediaSource kind`);
        }),
        ...(block.isError ? { is_error: true } : {}),
      };
  }
}

// ---------------------------------------------------------------------------
// Response parsing
//
// The harness passes one SSE data payload per call. The parser accumulates
// no state — all partial state lives in the harness. The parser emits events
// for the fragments it sees; the harness updates the PartialMessage and
// injects it into the returned events.
//
// Because the harness owns partial state, the parser cannot construct the
// correct `partial` field. We emit raw delta events with a placeholder empty
// partial — the harness will replace it before forwarding. This is the
// design: adapters are pure translators, the harness owns all state.
// ---------------------------------------------------------------------------

const EMPTY_PARTIAL: PartialMessage = { text: "" };

// Internal intermediate type used to communicate Anthropic-specific
// delta information to the harness before it enriches with partial state.
export type AnthropicRawEvent =
  | { kind: "text_delta"; token: string }
  | { kind: "thinking_delta"; token: string }
  | { kind: "tool_call_start"; index: number; callId: string; name: string }
  | { kind: "tool_call_delta"; index: number; argumentFragment: string }
  | { kind: "tool_call_end"; index: number }
  | {
      kind: "usage";
      inputTokens: number;
      outputTokens: number;
      cacheReadTokens: number;
      cacheWriteTokens: number;
      thinkingTokens: number;
    }
  | { kind: "message_stop" }
  | { kind: "skip" };

// Anthropic's SSE protocol guarantees `index` on every content_block_*
// event. Parsing it as required (not optional) means the type system
// carries the guarantee through to every emission site below — no
// defensive `?? 0` fallback that would silently route real protocol
// violations to block 0 and corrupt the `blockIndexToCallId` cache the
// parser uses to resolve input_json_delta lookups across multiple
// tool_use blocks at distinct indices. A malformed upstream missing
// `index` surfaces as a ProtocolMismatchError via the schema-validation
// throw site, with the offending payload preserved in `error.raw` for
// inspection.
const ContentBlockDelta = type({
  type: "'content_block_delta'",
  index: "number",
  delta: {
    type: "string",
    "text?": "string",
    "thinking?": "string",
    "partial_json?": "string",
    "signature?": "string",
  },
});

const ContentBlockStart = type({
  type: "'content_block_start'",
  index: "number",
  // Anthropic sends either content_block (snake_case) or contentBlock (camelCase).
  "content_block?": { type: "string", "id?": "string", "name?": "string" },
  "contentBlock?": { type: "string", "id?": "string", "name?": "string" },
});

const ContentBlockStop = type({
  type: "'content_block_stop'",
  index: "number",
});

const MessageDelta = type({
  type: "'message_delta'",
  "usage?": { "output_tokens?": "number" },
});

const MessageStart = type({
  type: "'message_start'",
  "message?": {
    "usage?": {
      "input_tokens?": "number",
      "output_tokens?": "number",
      "cache_read_input_tokens?": "number",
      "cache_creation_input_tokens?": "number",
    },
  },
});

const MessageStop = type({ type: "'message_stop'" });
const Ping = type({ type: "'ping'" });

const AnthropicSSEEvent = ContentBlockDelta.or(ContentBlockStart)
  .or(ContentBlockStop)
  .or(MessageDelta)
  .or(MessageStart)
  .or(MessageStop)
  .or(Ping);

function parseResponse(
  sseData: string,
  blockIndexToCallId: Map<number, string>,
): InferenceEvent[] {
  // Same protocol-mismatch posture as the openai adapter: a JSON parse
  // failure or arktype rejection means the upstream emitted bytes that
  // violate the Anthropic streaming protocol. Surface through
  // ProtocolMismatchError so the harness's stream-error catch emits
  // an inference.error with category "protocol_mismatch" carrying the
  // offending data in error.raw, rather than dropping the chunk
  // silently.
  let parsed: unknown;
  try {
    parsed = JSON.parse(sseData);
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    throw new ProtocolMismatchError(
      `anthropic parseResponse: malformed JSON in SSE data payload: ${message}`,
      sseData,
    );
  }

  const event = AnthropicSSEEvent(parsed);
  if (event instanceof type.errors) {
    throw new ProtocolMismatchError(
      `anthropic parseResponse: SSE event failed schema validation: ${event.summary}`,
      parsed,
    );
  }

  // The seq field is a placeholder 0 — the harness assigns real sequence numbers.
  const seq = 0;

  switch (event.type) {
    case "content_block_delta": {
      const { delta, index } = event;

      if (delta.type === "text_delta") {
        const token = delta.text ?? "";
        return [
          {
            type: "inference.text.delta",
            seq,
            data: { token, partial: EMPTY_PARTIAL, index },
          },
        ];
      }

      if (delta.type === "thinking_delta") {
        const token = delta.thinking ?? "";
        return [
          {
            type: "inference.thinking.delta",
            seq,
            data: { token, partial: EMPTY_PARTIAL, index },
          },
        ];
      }

      if (delta.type === "signature_delta") {
        // Anthropic emits the cryptographic signature for a thinking block
        // in a dedicated signature_delta event after the block's
        // thinking_delta stream. The signature must be echoed back on any
        // follow-up turn that includes the thinking block as context —
        // otherwise the API rejects the request with
        // "messages.N.content.M.thinking.signature: Field required".
        const signature = delta.signature ?? "";
        return [
          {
            type: "inference.thinking.signature",
            seq,
            data: { signature, index },
          },
        ];
      }

      if (delta.type === "input_json_delta") {
        const callId = blockIndexToCallId.get(index);
        if (callId === undefined) {
          throw new ProtocolMismatchError(
            `anthropic parseResponse: input_json_delta for content block ${index} with no preceding tool_use start`,
            event,
          );
        }
        const fragment = delta.partial_json ?? "";
        return [
          {
            type: "inference.tool_call.delta",
            seq,
            data: {
              callId,
              argumentFragment: fragment,
              partial: EMPTY_PARTIAL,
              index,
            },
          },
        ];
      }

      return [];
    }

    case "content_block_start": {
      const block = event.content_block ?? event.contentBlock;
      if (block === undefined) return [];

      if (block.type === "tool_use") {
        const { index } = event;
        const callId = block.id ?? String(index);
        blockIndexToCallId.set(index, callId);
        const name = block.name ?? "";
        return [
          {
            type: "inference.tool_call.start",
            seq,
            data: { callId, name, partial: EMPTY_PARTIAL, index },
          },
        ];
      }
      // Non-tool_use content_block_start events (text, thinking) emit
      // nothing here by design: each content_block_delta arrives with
      // a typed delta (text_delta, thinking_delta, signature_delta)
      // that the switch above discriminates on directly, so an upfront
      // start emission would be redundant. Tool calls are the
      // exception because their callId arrives only in the start
      // event and must be cached against the block index for
      // subsequent input_json_delta lookups.
      return [];
    }

    case "content_block_stop": {
      // The harness handles finalizing tool calls when it sees this — we
      // emit nothing here; the harness knows which blocks are complete.
      return [];
    }

    case "message_delta": {
      const outputTokens = event.usage?.output_tokens ?? 0;
      const inferenceUsage: TokenUsage = {
        input: 0,
        output: outputTokens,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      };
      return [
        { type: "inference.usage", seq, data: { usage: inferenceUsage } },
      ];
    }

    case "message_start": {
      blockIndexToCallId.clear();
      const msgUsage = event.message?.usage;
      if (msgUsage === undefined) return [];

      const inferenceUsage: TokenUsage = {
        input: msgUsage.input_tokens ?? 0,
        output: msgUsage.output_tokens ?? 0,
        cacheRead: msgUsage.cache_read_input_tokens ?? 0,
        cacheWrite: msgUsage.cache_creation_input_tokens ?? 0,
        thinking: 0,
      };
      return [
        { type: "inference.usage", seq, data: { usage: inferenceUsage } },
      ];
    }

    case "message_stop":
    case "ping":
      return [];
  }
}

function extractRetryAfterMs(headers: Headers): number | undefined {
  const raw = headers.get("retry-after");
  if (raw === null) return undefined;
  const seconds = Number(raw);
  if (!Number.isFinite(seconds) || seconds <= 0) return undefined;
  return Math.ceil(seconds * 1000);
}

function extractPacingDelayMs(headers: Headers): number | undefined {
  // Check all rate limit dimensions and return the longest wait needed
  const delays: number[] = [];

  for (const prefix of [
    "anthropic-ratelimit-requests",
    "anthropic-ratelimit-input-tokens",
    "anthropic-ratelimit-output-tokens",
    "anthropic-ratelimit-tokens",
  ]) {
    const remaining = headers.get(`${prefix}-remaining`);
    if (remaining === null) continue;
    const n = Number(remaining);
    if (!Number.isFinite(n) || n > 0) continue;

    const reset = headers.get(`${prefix}-reset`);
    if (reset === null) continue;
    const resetTime = Date.parse(reset);
    if (Number.isNaN(resetTime)) continue;
    const delayMs = resetTime - Date.now();
    if (delayMs > 0) delays.push(delayMs);
  }

  return delays.length > 0 ? Math.max(...delays) : undefined;
}

export function createAnthropicAdapter(): ProviderAdapter {
  const blockIndexToCallId = new Map<number, string>();

  return {
    buildRequest,
    parseResponse: (sseData) => parseResponse(sseData, blockIndexToCallId),
    extractRetryAfterMs,
    extractPacingDelayMs,
  };
}
