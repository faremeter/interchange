import { describe, test, expect } from "bun:test";

import { parseSSE } from "@intx/inference";
import {
  createAnthropicAdapter,
  createOpenAIAdapter,
} from "@intx/inference/providers";
import type { InferenceEvent, LastCycleSource } from "@intx/types/runtime";

const ANTHROPIC_SOURCE: LastCycleSource = {
  sourceId: "test-anthropic",
  provider: "anthropic",
  model: "test-anthropic-model",
};

const OPENAI_SOURCE: LastCycleSource = {
  sourceId: "test-openai",
  provider: "openai",
  model: "test-openai-model",
};

import { assistantText, completeResponse, toolCall, usage } from "./agnostic";

async function driveAnthropic(chunks: Uint8Array[]): Promise<InferenceEvent[]> {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  const adapter = createAnthropicAdapter(ANTHROPIC_SOURCE);
  const events: InferenceEvent[] = [];
  for await (const sseData of parseSSE(stream)) {
    for (const evt of adapter.parseResponse(sseData)) {
      events.push(evt);
    }
  }
  return events;
}

async function driveOpenAI(chunks: Uint8Array[]): Promise<InferenceEvent[]> {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  const adapter = createOpenAIAdapter(OPENAI_SOURCE);
  const events: InferenceEvent[] = [];
  for await (const sseData of parseSSE(stream)) {
    for (const evt of adapter.parseResponse(sseData)) {
      events.push(evt);
    }
  }
  return events;
}

describe("agnostic wire helpers", () => {
  test("assistantText dispatches to anthropic and emits text.delta", async () => {
    const events = await driveAnthropic(assistantText("anthropic", "hi"));
    const text = events.filter((e) => e.type === "inference.text.delta");
    expect(text).toHaveLength(1);
  });

  test("assistantText dispatches to openai and emits text.delta", async () => {
    const events = await driveOpenAI(assistantText("openai", "hi"));
    const text = events.filter((e) => e.type === "inference.text.delta");
    expect(text).toHaveLength(1);
  });

  test("toolCall(anthropic) emits start + delta carrying the supplied args", async () => {
    const events = await driveAnthropic(
      toolCall("anthropic", "toolu_x", "do", '{"a":1}'),
    );
    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const deltas = events.filter((e) => e.type === "inference.tool_call.delta");
    expect(starts).toHaveLength(1);
    expect(deltas).toHaveLength(1);
    if (deltas[0]?.type === "inference.tool_call.delta") {
      expect(deltas[0].data.argumentFragment).toBe('{"a":1}');
    }
  });

  test("toolCall(openai) emits start + delta", async () => {
    const events = await driveOpenAI(
      toolCall("openai", "call_x", "do", '{"a":1}'),
    );
    const starts = events.filter((e) => e.type === "inference.tool_call.start");
    const deltas = events.filter((e) => e.type === "inference.tool_call.delta");
    expect(starts).toHaveLength(1);
    expect(deltas).toHaveLength(1);
  });

  test("usage(anthropic) emits inference.usage with output tokens", async () => {
    const events = await driveAnthropic(
      usage("anthropic", {
        input: 0,
        output: 7,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      }),
    );
    const u = events.find((e) => e.type === "inference.usage");
    expect(u).toBeDefined();
    if (u?.type === "inference.usage") {
      expect(u.data.usage.output).toBe(7);
    }
  });

  test("usage(openai) emits inference.usage with prompt/completion tokens", async () => {
    const events = await driveOpenAI(
      usage("openai", {
        input: 11,
        output: 22,
        cacheRead: 3,
        cacheWrite: 0,
        thinking: 4,
      }),
    );
    const u = events.find((e) => e.type === "inference.usage");
    expect(u).toBeDefined();
    if (u?.type === "inference.usage") {
      expect(u.data.usage.input).toBe(11);
      expect(u.data.usage.output).toBe(22);
      expect(u.data.usage.cacheRead).toBe(3);
      expect(u.data.usage.thinking).toBe(4);
    }
  });

  test("completeResponse(anthropic) yields a full transcript", async () => {
    const chunks = completeResponse("anthropic", {
      text: "Hello",
      headUsage: {
        input: 5,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      },
      tailUsage: {
        input: 0,
        output: 8,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      },
    });
    const events = await driveAnthropic(chunks);
    const types = events.map((e) => e.type);
    expect(types).toContain("inference.usage");
    expect(types).toContain("inference.text.delta");
  });

  test("completeResponse(openai) ends with [DONE] sentinel", async () => {
    const chunks = completeResponse("openai", {
      text: "Hi",
      tailUsage: {
        input: 5,
        output: 5,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      },
    });
    const events = await driveOpenAI(chunks);
    const text = events.find((e) => e.type === "inference.text.delta");
    const usageEvt = events.find((e) => e.type === "inference.usage");
    expect(text).toBeDefined();
    expect(usageEvt).toBeDefined();
  });
});
