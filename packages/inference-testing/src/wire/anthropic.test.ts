import { describe, test, expect } from "bun:test";

import { createAnthropicAdapter, parseSSE } from "@interchange/inference";
import type { InferenceEvent } from "@interchange/types/runtime";

import * as anthropic from "./anthropic";

/**
 * Drive a sequence of wire bytes through the real `parseSSE` + Anthropic
 * adapter pipeline, returning the events the adapter emitted. Tests use
 * this to verify each helper's bytes are parseable.
 */
async function drive(chunks: Uint8Array[]): Promise<InferenceEvent[]> {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  const adapter = createAnthropicAdapter();
  const events: InferenceEvent[] = [];
  for await (const sseData of parseSSE(stream)) {
    for (const evt of adapter.parseResponse(sseData)) {
      events.push(evt);
    }
  }
  return events;
}

describe("anthropic wire DSL", () => {
  test("messageStart with usage emits inference.usage", async () => {
    const events = await drive([
      anthropic.messageStart({
        usage: {
          inputTokens: 100,
          outputTokens: 0,
          cacheReadInputTokens: 25,
          cacheCreationInputTokens: 0,
        },
      }),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.usage");
    if (events[0]?.type === "inference.usage") {
      expect(events[0].data.usage.input).toBe(100);
      expect(events[0].data.usage.cacheRead).toBe(25);
    }
  });

  test("messageStart without usage emits nothing", async () => {
    const events = await drive([anthropic.messageStart()]);
    expect(events).toEqual([]);
  });

  test("textBlock emits inference.text.delta with the supplied text", async () => {
    const events = await drive([
      anthropic.messageStart(),
      ...anthropic.textBlock("Hello"),
    ]);
    const textEvents = events.filter((e) => e.type === "inference.text.delta");
    expect(textEvents).toHaveLength(1);
    if (textEvents[0]?.type === "inference.text.delta") {
      expect(textEvents[0].data.token).toBe("Hello");
    }
  });

  test("thinkingBlock emits inference.thinking.delta", async () => {
    const events = await drive([
      anthropic.messageStart(),
      ...anthropic.thinkingBlock("Let me think..."),
    ]);
    const thinkEvents = events.filter(
      (e) => e.type === "inference.thinking.delta",
    );
    expect(thinkEvents).toHaveLength(1);
    if (thinkEvents[0]?.type === "inference.thinking.delta") {
      expect(thinkEvents[0].data.token).toBe("Let me think...");
    }
  });

  test("toolUseBlock emits start + delta carrying the call id and args", async () => {
    const events = await drive([
      anthropic.messageStart(),
      ...anthropic.toolUseBlock("toolu_abc", "do_thing", '{"x":1}'),
    ]);
    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const deltas = events.filter((e) => e.type === "inference.tool_call.delta");
    expect(starts).toHaveLength(1);
    expect(deltas).toHaveLength(1);
    if (starts[0]?.type === "inference.tool_call.start") {
      expect(starts[0].data.callId).toBe("toolu_abc");
      expect(starts[0].data.name).toBe("do_thing");
    }
    if (deltas[0]?.type === "inference.tool_call.delta") {
      expect(deltas[0].data.callId).toBe("toolu_abc");
      expect(deltas[0].data.argumentFragment).toBe('{"x":1}');
    }
  });

  test("contentBlockDelta with input_json_delta resolves call id via start", async () => {
    const events = await drive([
      anthropic.messageStart(),
      anthropic.contentBlockStart({
        index: 1,
        kind: "tool_use",
        id: "toolu_real",
        name: "search",
      }),
      anthropic.contentBlockDelta({
        index: 1,
        kind: "input_json_delta",
        partialJson: '{"q":',
      }),
      anthropic.contentBlockDelta({
        index: 1,
        kind: "input_json_delta",
        partialJson: '"x"}',
      }),
      anthropic.contentBlockStop({ index: 1 }),
    ]);
    const deltas = events.filter((e) => e.type === "inference.tool_call.delta");
    expect(deltas).toHaveLength(2);
    if (deltas[0]?.type === "inference.tool_call.delta") {
      expect(deltas[0].data.callId).toBe("toolu_real");
    }
  });

  test("messageDelta with outputTokens emits inference.usage", async () => {
    const events = await drive([
      anthropic.messageStart(),
      anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 42 }),
    ]);
    const usageEvents = events.filter((e) => e.type === "inference.usage");
    expect(usageEvents.length).toBeGreaterThanOrEqual(1);
    const last = usageEvents.at(-1);
    if (last?.type === "inference.usage") {
      expect(last.data.usage.output).toBe(42);
    }
  });

  test("messageStop and ping are parsed and produce no events", async () => {
    const events = await drive([
      anthropic.messageStart(),
      anthropic.ping(),
      anthropic.messageStop(),
    ]);
    expect(events).toEqual([]);
  });

  test("unknownDelta is tolerated by the adapter", async () => {
    const events = await drive([
      anthropic.messageStart(),
      anthropic.unknownDelta(0),
    ]);
    expect(events).toEqual([]);
  });

  test("raw escape hatch lets tests inject custom bytes", async () => {
    // Anthropic adapter ignores comment lines because parseSSE strips them.
    const events = await drive([
      anthropic.raw(": keepalive\n\n"),
      anthropic.messageStart(),
    ]);
    expect(events).toEqual([]);
  });

  test("malformedToolUseBlock still emits start + delta (parse failure is downstream)", async () => {
    const events = await drive([
      anthropic.messageStart(),
      ...anthropic.malformedToolUseBlock("toolu_bad", "broken"),
    ]);
    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const deltas = events.filter((e) => e.type === "inference.tool_call.delta");
    expect(starts).toHaveLength(1);
    expect(deltas).toHaveLength(1);
    if (deltas[0]?.type === "inference.tool_call.delta") {
      expect(deltas[0].data.argumentFragment).toBe('{"unterminated":');
    }
  });

  test("out-of-order content blocks parse without throwing", async () => {
    // Adapter is per-event stateless apart from index→callId mapping; a
    // tool_use start at a higher index before a text block at a lower one
    // is malformed but should not crash. Verifies the helpers can express it.
    const events = await drive([
      anthropic.messageStart(),
      anthropic.contentBlockStart({
        index: 1,
        kind: "tool_use",
        id: "toolu_first",
        name: "alpha",
      }),
      anthropic.contentBlockStart({
        index: 0,
        kind: "text",
        text: "",
      }),
      anthropic.contentBlockDelta({
        index: 0,
        kind: "text_delta",
        text: "after",
      }),
    ]);
    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const textDeltas = events.filter((e) => e.type === "inference.text.delta");
    expect(starts).toHaveLength(1);
    expect(textDeltas).toHaveLength(1);
  });
});
