// Anthropic streams /v1/messages as Server-Sent Events whose payloads
// arrive as named events: message_start, content_block_start,
// content_block_delta, content_block_stop, message_delta, message_stop,
// ping. To build a turn-2 multi-turn body for a streaming capability we
// need the assistant's content blocks reconstructed from those events.
// This module parses the event stream, applies the per-block deltas
// (text_delta, input_json_delta, thinking_delta, signature_delta), and
// returns the resolved content blocks in their original index order.

interface BlockAccumulatorBase {
  type: string;
  index: number;
}

interface TextAcc extends BlockAccumulatorBase {
  type: "text";
  text: string;
}

interface ToolUseAcc extends BlockAccumulatorBase {
  type: "tool_use";
  id: string;
  name: string;
  partialJson: string;
}

interface ThinkingAcc extends BlockAccumulatorBase {
  type: "thinking";
  thinking: string;
  signature?: string;
}

interface RedactedThinkingAcc extends BlockAccumulatorBase {
  type: "redacted_thinking";
  data: string;
}

interface UnknownAcc extends BlockAccumulatorBase {
  type: "unknown";
  contentBlock: Record<string, unknown>;
}

type BlockAcc =
  | TextAcc
  | ToolUseAcc
  | ThinkingAcc
  | RedactedThinkingAcc
  | UnknownAcc;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// These guards omit the offending value from the thrown message: SSE
// payloads can carry large opaque blobs (e.g. a redacted_thinking
// `data` field, a multi-KB signature) and stringifying them into an
// error message produces unreadable output. The typeof + the call-site
// in the stack trace are enough to localise the bad field.

function asString(value: unknown): string {
  if (typeof value !== "string") {
    throw new Error(`anthropic SSE: expected string, got ${typeof value}`);
  }
  return value;
}

function asNumber(value: unknown): number {
  if (typeof value !== "number") {
    throw new Error(`anthropic SSE: expected number, got ${typeof value}`);
  }
  return value;
}

interface ParsedEvent {
  event: string;
  data: unknown;
}

function parseEvents(text: string): ParsedEvent[] {
  const events: ParsedEvent[] = [];
  // Normalize CRLF, then split on the blank-line event separator.
  const normalized = text.replace(/\r\n/g, "\n");
  for (const chunk of normalized.split("\n\n")) {
    if (chunk.length === 0) continue;
    let eventName: string | null = null;
    const dataLines: string[] = [];
    for (const line of chunk.split("\n")) {
      if (line.startsWith(":")) continue;
      if (line.startsWith("event:")) {
        eventName = line.slice("event:".length).trim();
      } else if (line.startsWith("data:")) {
        dataLines.push(line.slice("data:".length).replace(/^ /, ""));
      }
    }
    if (eventName === null || dataLines.length === 0) continue;
    const joined = dataLines.join("\n");
    // Anthropic emits trailing whitespace inside the closing brace on
    // every data: line (e.g. `…}   }`). JSON.parse tolerates it. If
    // this parser is ever swapped for a stricter one, trim here first.
    const data: unknown = JSON.parse(joined);
    events.push({ event: eventName, data });
  }
  return events;
}

function initialAccumulator(index: number, contentBlock: unknown): BlockAcc {
  if (!isRecord(contentBlock)) {
    throw new Error(
      "anthropic SSE: content_block_start missing content_block object",
    );
  }
  const type = contentBlock.type;
  if (type === "text") {
    return { type: "text", index, text: asString(contentBlock.text ?? "") };
  }
  if (type === "tool_use") {
    return {
      type: "tool_use",
      index,
      id: asString(contentBlock.id),
      name: asString(contentBlock.name),
      partialJson: "",
    };
  }
  if (type === "thinking") {
    const acc: ThinkingAcc = {
      type: "thinking",
      index,
      thinking: asString(contentBlock.thinking ?? ""),
    };
    if (typeof contentBlock.signature === "string") {
      acc.signature = contentBlock.signature;
    }
    return acc;
  }
  if (type === "redacted_thinking") {
    return {
      type: "redacted_thinking",
      index,
      data: asString(contentBlock.data),
    };
  }
  // Pass-through for server_tool_use, web_search_tool_result,
  // code_execution_tool_use, etc. The wire round-trip is what matters.
  return { type: "unknown", index, contentBlock };
}

function applyDelta(acc: BlockAcc, delta: unknown): BlockAcc {
  if (!isRecord(delta)) {
    throw new Error("anthropic SSE: delta is not an object");
  }
  const dtype = delta.type;
  if (dtype === "text_delta" && acc.type === "text") {
    return { ...acc, text: acc.text + asString(delta.text) };
  }
  if (dtype === "input_json_delta" && acc.type === "tool_use") {
    return {
      ...acc,
      partialJson: acc.partialJson + asString(delta.partial_json),
    };
  }
  if (dtype === "thinking_delta" && acc.type === "thinking") {
    return { ...acc, thinking: acc.thinking + asString(delta.thinking) };
  }
  if (dtype === "signature_delta" && acc.type === "thinking") {
    return {
      ...acc,
      signature: (acc.signature ?? "") + asString(delta.signature),
    };
  }
  // Anthropic's server-side tool blocks (server_tool_use,
  // web_search_tool_result, code_execution_tool_use) DO arrive with
  // partial deltas in real streams — the grounding-streaming and
  // code-execution-streaming fixtures in this repo carry
  // input_json_delta events building up the tool input field. This
  // parser does not yet implement delta application for those block
  // types; the streaming-multi-turn capabilities currently in scope
  // (function-calling-multi-turn-streaming,
  // function-calling-with-thinking-streaming, redacted-thinking-
  // streaming) do not pair with server-side tools, so the parser is
  // not invoked on those fixtures today. Fail loud here so that any
  // future expansion that does invoke the parser on a server-side
  // tool stream surfaces the gap at parse time rather than silently
  // dropping the delta payloads.
  if (acc.type === "unknown") {
    throw new Error(
      `anthropic SSE: received ${String(dtype)} for a non-enumerated block type at index ${String(acc.index)}; partial-delta streaming for server-side tool blocks is not implemented`,
    );
  }
  throw new Error(
    `anthropic SSE: delta type ${String(dtype)} does not match block type ${acc.type}`,
  );
}

function finalize(acc: BlockAcc): Record<string, unknown> {
  if (acc.type === "text") {
    return { type: "text", text: acc.text };
  }
  if (acc.type === "tool_use") {
    const input: unknown =
      acc.partialJson.length === 0 ? {} : JSON.parse(acc.partialJson);
    return { type: "tool_use", id: acc.id, name: acc.name, input };
  }
  if (acc.type === "thinking") {
    const block: Record<string, unknown> = {
      type: "thinking",
      thinking: acc.thinking,
    };
    if (acc.signature !== undefined) block.signature = acc.signature;
    return block;
  }
  if (acc.type === "redacted_thinking") {
    return { type: "redacted_thinking", data: acc.data };
  }
  return acc.contentBlock;
}

// Parses Anthropic's named-event SSE stream and reconstructs the
// assistant's content blocks in their original index order. Used by
// the streaming multi-turn iterators to build turn-2 bodies that echo
// the assistant content blocks verbatim — without a turn-1 JSON body to
// read from, this is the only path to those blocks.
export function extractContentBlocksFromSSE(
  bytes: Uint8Array,
): Record<string, unknown>[] {
  const text = new TextDecoder().decode(bytes);
  const events = parseEvents(text);
  const accumulators = new Map<number, BlockAcc>();
  for (const { event, data } of events) {
    if (
      event === "ping" ||
      event === "message_start" ||
      event === "message_delta" ||
      event === "message_stop"
    ) {
      continue;
    }
    if (!isRecord(data)) {
      throw new Error(`anthropic SSE: event ${event} payload is not an object`);
    }
    if (event === "content_block_start") {
      const index = asNumber(data.index);
      accumulators.set(index, initialAccumulator(index, data.content_block));
      continue;
    }
    if (event === "content_block_delta") {
      const index = asNumber(data.index);
      const acc = accumulators.get(index);
      if (acc === undefined) {
        throw new Error(
          `anthropic SSE: content_block_delta for index ${String(index)} has no matching content_block_start`,
        );
      }
      accumulators.set(index, applyDelta(acc, data.delta));
      continue;
    }
    if (event === "content_block_stop") {
      // No-op; finalization happens after the loop in index order.
      continue;
    }
    throw new Error(`anthropic SSE: unexpected event ${event}`);
  }
  const sortedIndices = Array.from(accumulators.keys()).sort((a, b) => a - b);
  return sortedIndices.map((i) => {
    const acc = accumulators.get(i);
    if (acc === undefined) {
      throw new Error(
        `anthropic SSE: missing accumulator for index ${String(i)}`,
      );
    }
    return finalize(acc);
  });
}
