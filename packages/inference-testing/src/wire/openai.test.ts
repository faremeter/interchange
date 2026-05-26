import { describe, test, expect } from "bun:test";

import { createOpenAIAdapter, parseSSE } from "@intx/inference";
import type { InferenceEvent, LastCycleSource } from "@intx/types/runtime";

const TEST_SOURCE: LastCycleSource = {
  sourceId: "test-openai",
  provider: "openai",
  model: "test-openai-model",
};

import * as openai from "./openai";

async function drive(chunks: Uint8Array[]): Promise<InferenceEvent[]> {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  const adapter = createOpenAIAdapter(TEST_SOURCE);
  const events: InferenceEvent[] = [];
  for await (const sseData of parseSSE(stream)) {
    for (const evt of adapter.parseResponse(sseData)) {
      events.push(evt);
    }
  }
  return events;
}

describe("openai wire DSL", () => {
  test("chunk with content emits inference.text.delta", async () => {
    const events = await drive([
      openai.chunk({ content: "hello" }),
      openai.done(),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.text.delta");
    if (events[0]?.type === "inference.text.delta") {
      expect(events[0].data.token).toBe("hello");
    }
  });

  test("emptyKeepAliveChunk produces no events", async () => {
    const events = await drive([openai.emptyKeepAliveChunk(), openai.done()]);
    expect(events).toEqual([]);
  });

  test("toolCallStart emits inference.tool_call.start", async () => {
    const events = await drive([
      openai.toolCallStart(0, "call_xyz", "search"),
      openai.done(),
    ]);
    expect(events).toHaveLength(1);
    if (events[0]?.type === "inference.tool_call.start") {
      expect(events[0].data.callId).toBe("call_xyz");
      expect(events[0].data.name).toBe("search");
    }
  });

  test("toolCallArgumentsDelta emits index-keyed inference.tool_call.delta", async () => {
    const events = await drive([
      openai.toolCallStart(0, "call_a", "tool"),
      openai.toolCallArgumentsDelta(0, '{"q":'),
      openai.toolCallArgumentsDelta(0, '"hi"}'),
      openai.done(),
    ]);
    const deltas = events.filter((e) => e.type === "inference.tool_call.delta");
    expect(deltas).toHaveLength(2);
    if (deltas[0]?.type === "inference.tool_call.delta") {
      // The adapter uses the index as a placeholder callId — the harness
      // remaps to the real id. At the adapter layer the callId is "0".
      expect(deltas[0].data.callId).toBe("0");
      expect(deltas[0].data.argumentFragment).toBe('{"q":');
    }
  });

  test("toolCallSequence emits a complete start+args chain", async () => {
    const events = await drive([
      ...openai.toolCallSequence(0, "call_b", "calc", ['{"x":', "1}"]),
      openai.done(),
    ]);
    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const deltas = events.filter((e) => e.type === "inference.tool_call.delta");
    expect(starts).toHaveLength(1);
    expect(deltas).toHaveLength(2);
  });

  test("usageChunk emits inference.usage from final-frame usage", async () => {
    const events = await drive([
      openai.chunk({ content: "x" }),
      openai.usageChunk({
        promptTokens: 10,
        completionTokens: 20,
        cachedTokens: 5,
        reasoningTokens: 3,
      }),
      openai.done(),
    ]);
    const usage = events.find((e) => e.type === "inference.usage");
    expect(usage).toBeDefined();
    if (usage?.type === "inference.usage") {
      expect(usage.data.usage.input).toBe(10);
      expect(usage.data.usage.output).toBe(20);
      expect(usage.data.usage.cacheRead).toBe(5);
      expect(usage.data.usage.thinking).toBe(3);
    }
  });

  test("reasoning_content delta emits inference.thinking.delta", async () => {
    const events = await drive([
      openai.chunk({ reasoningContent: "musing" }),
      openai.done(),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.thinking.delta");
    if (events[0]?.type === "inference.thinking.delta") {
      expect(events[0].data.token).toBe("musing");
    }
  });

  test("reasoning delta (OpenRouter shape) also emits inference.thinking.delta", async () => {
    const events = await drive([
      openai.chunk({ reasoning: "openrouter-shape" }),
      openai.done(),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.thinking.delta");
  });

  test("done sentinel terminates parseSSE without emitting an event", async () => {
    const events = await drive([openai.done()]);
    expect(events).toEqual([]);
  });

  test("legacyFunctionCall bytes are valid SSE and ignored by the adapter", async () => {
    const events = await drive([
      openai.legacyFunctionCall("old_tool", '{"y":2}'),
      openai.done(),
    ]);
    // The modern adapter does not surface a tool_call from the legacy shape.
    expect(events).toEqual([]);
  });

  test("malformedToolCall emits start + delta carrying the bad fragment", async () => {
    const events = await drive([
      ...openai.malformedToolCall(0, "call_bad", "broken"),
      openai.done(),
    ]);
    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const deltas = events.filter((e) => e.type === "inference.tool_call.delta");
    expect(starts).toHaveLength(1);
    expect(deltas).toHaveLength(1);
    if (deltas[0]?.type === "inference.tool_call.delta") {
      expect(deltas[0].data.argumentFragment).toBe('{"unterminated":');
    }
  });

  test("raw escape hatch supports inline comments parseSSE strips", async () => {
    const events = await drive([
      openai.raw(": keepalive\n\n"),
      openai.chunk({ content: "after" }),
      openai.done(),
    ]);
    expect(events).toHaveLength(1);
    if (events[0]?.type === "inference.text.delta") {
      expect(events[0].data.token).toBe("after");
    }
  });

  test("chunk with no choices and no usage emits no events", async () => {
    const events = await drive([
      openai.chunk({
        extra: {
          /* nothing useful */
        },
      }),
      openai.done(),
    ]);
    // chunk() above still has a default empty-delta choice, so adapter sees
    // a delta with no recognized fields and emits nothing.
    expect(events).toEqual([]);
  });

  test("extra fields are layered onto the chunk and ignored by the adapter", async () => {
    const events = await drive([
      openai.chunk({
        content: "ok",
        extra: { system_fingerprint: "abc" },
      }),
      openai.done(),
    ]);
    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("inference.text.delta");
  });
});
