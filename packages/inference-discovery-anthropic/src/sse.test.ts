import { describe, test, expect } from "bun:test";
import { extractContentBlocksFromSSE } from "./sse";

function bytes(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

describe("extractContentBlocksFromSSE", () => {
  test("returns a single text block with concatenated text_delta payloads", () => {
    const stream = [
      "event: message_start",
      'data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[]}}',
      "",
      "event: content_block_start",
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":", world"}}',
      "",
      "event: content_block_stop",
      'data: {"type":"content_block_stop","index":0}',
      "",
      "event: message_stop",
      'data: {"type":"message_stop"}',
      "",
    ].join("\n");
    const blocks = extractContentBlocksFromSSE(bytes(stream));
    expect(blocks).toEqual([{ type: "text", text: "Hello, world" }]);
  });

  test("returns a tool_use block with input reconstructed from input_json_delta chunks", () => {
    const stream = [
      "event: content_block_start",
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool_1","name":"get_weather","input":{}}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\\"loc"}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"ation\\":\\"Boston, MA\\"}"}}',
      "",
      "event: content_block_stop",
      'data: {"type":"content_block_stop","index":0}',
      "",
    ].join("\n");
    const blocks = extractContentBlocksFromSSE(bytes(stream));
    expect(blocks).toEqual([
      {
        type: "tool_use",
        id: "tool_1",
        name: "get_weather",
        input: { location: "Boston, MA" },
      },
    ]);
  });

  test("returns a thinking block with concatenated thinking_delta and signature_delta", () => {
    const stream = [
      "event: content_block_start",
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Let me think."}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-abc"}}',
      "",
      "event: content_block_stop",
      'data: {"type":"content_block_stop","index":0}',
      "",
    ].join("\n");
    const blocks = extractContentBlocksFromSSE(bytes(stream));
    expect(blocks).toEqual([
      { type: "thinking", thinking: "Let me think.", signature: "sig-abc" },
    ]);
  });

  test("returns a redacted_thinking block from a one-shot content_block_start (no deltas)", () => {
    const stream = [
      "event: content_block_start",
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"opaque-encrypted-bytes"}}',
      "",
      "event: content_block_stop",
      'data: {"type":"content_block_stop","index":0}',
      "",
    ].join("\n");
    const blocks = extractContentBlocksFromSSE(bytes(stream));
    expect(blocks).toEqual([
      { type: "redacted_thinking", data: "opaque-encrypted-bytes" },
    ]);
  });

  test("returns blocks in their original index order across interleaved deltas", () => {
    const stream = [
      "event: content_block_start",
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}',
      "",
      "event: content_block_start",
      'data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"answer"}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"reasoning"}}',
      "",
      "event: content_block_stop",
      'data: {"type":"content_block_stop","index":0}',
      "",
      "event: content_block_stop",
      'data: {"type":"content_block_stop","index":1}',
      "",
    ].join("\n");
    const blocks = extractContentBlocksFromSSE(bytes(stream));
    expect(blocks).toEqual([
      { type: "thinking", thinking: "reasoning" },
      { type: "text", text: "answer" },
    ]);
  });

  test("ignores ping and message_* envelope events", () => {
    const stream = [
      "event: message_start",
      'data: {"type":"message_start","message":{"id":"msg_1"}}',
      "",
      "event: ping",
      'data: {"type":"ping"}',
      "",
      "event: content_block_start",
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}',
      "",
      "event: content_block_stop",
      'data: {"type":"content_block_stop","index":0}',
      "",
      "event: message_delta",
      'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}',
      "",
      "event: message_stop",
      'data: {"type":"message_stop"}',
      "",
    ].join("\n");
    const blocks = extractContentBlocksFromSSE(bytes(stream));
    expect(blocks).toEqual([{ type: "text", text: "hi" }]);
  });

  test("throws when an unknown block type receives partial-delta events", () => {
    const stream = [
      "event: content_block_start",
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"srv_1","name":"web_search","input":{}}}',
      "",
      "event: content_block_delta",
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}',
      "",
      "event: content_block_stop",
      'data: {"type":"content_block_stop","index":0}',
      "",
    ].join("\n");
    expect(() => extractContentBlocksFromSSE(bytes(stream))).toThrow(
      /non-enumerated block type/,
    );
  });

  test("forwards unknown content_block types verbatim", () => {
    const stream = [
      "event: content_block_start",
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"srv_1","name":"web_search","input":{"query":"x"}}}',
      "",
      "event: content_block_stop",
      'data: {"type":"content_block_stop","index":0}',
      "",
    ].join("\n");
    const blocks = extractContentBlocksFromSSE(bytes(stream));
    expect(blocks).toEqual([
      {
        type: "server_tool_use",
        id: "srv_1",
        name: "web_search",
        input: { query: "x" },
      },
    ]);
  });
});
