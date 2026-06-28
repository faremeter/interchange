import { describe, test, expect } from "bun:test";

import { parseSSE } from "@intx/inference";
import { createAnthropicAdapter } from "@intx/inference/providers";
import type { InferenceEvent, LastCycleSource } from "@intx/types/runtime";

const TEST_SOURCE: LastCycleSource = {
  sourceId: "test-anthropic",
  provider: "anthropic",
  model: "test-anthropic-model",
};

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
  const adapter = createAnthropicAdapter(TEST_SOURCE);
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
    // The parser anchors the thinking block at content_block_start
    // with an empty-token thinking.delta so the harness can attach a
    // later signature_delta even when no visible text streams in
    // between. The thinkingBlock wire helper emits content_block_start
    // + content_block_delta, so a single visible-text block yields
    // two thinking.delta events: the empty anchor and the content.
    expect(thinkEvents).toHaveLength(2);
    const anchor = thinkEvents[0];
    const content = thinkEvents[1];
    if (
      anchor?.type !== "inference.thinking.delta" ||
      content?.type !== "inference.thinking.delta"
    ) {
      throw new Error("expected two thinking.delta events");
    }
    expect(anchor.data.token).toBe("");
    expect(content.data.token).toBe("Let me think...");
  });

  test("thinkingBlock with signature emits inference.thinking.signature", async () => {
    const events = await drive([
      anthropic.messageStart(),
      ...anthropic.thinkingBlock("Let me think...", 0, "sig_xyz"),
    ]);
    const sigEvents = events.filter(
      (e) => e.type === "inference.thinking.signature",
    );
    expect(sigEvents).toHaveLength(1);
    if (sigEvents[0]?.type === "inference.thinking.signature") {
      expect(sigEvents[0].data.signature).toBe("sig_xyz");
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

/**
 * Extract decoded JSON event objects from a sequence of wire bytes,
 * bypassing the Anthropic adapter. Used to assert the structural wire
 * shape a helper produces, independently of whether the adapter knows
 * how to interpret it. (Adapter integration is tested elsewhere; here
 * we pin the wire bytes the helper emits.)
 */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function decodeEvents(
  chunks: Uint8Array[],
): Promise<Record<string, unknown>[]> {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  const out: Record<string, unknown>[] = [];
  for await (const sseData of parseSSE(stream)) {
    const parsed: unknown = JSON.parse(sseData);
    if (isRecord(parsed)) {
      out.push(parsed);
    }
  }
  return out;
}

describe("anthropic wire DSL — new content block variants", () => {
  test("redactedThinkingBlock emits start+stop with the opaque data blob", async () => {
    const events = await decodeEvents(
      anthropic.redactedThinkingBlock("EncryptedOpaqueBlobAAAA==", 0),
    );
    expect(events).toHaveLength(2);
    expect(events[0]).toEqual({
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "redacted_thinking",
        data: "EncryptedOpaqueBlobAAAA==",
      },
    });
    expect(events[1]).toEqual({ type: "content_block_stop", index: 0 });
  });

  test("redactedThinkingBlock at a non-zero index", async () => {
    const events = await decodeEvents(
      anthropic.redactedThinkingBlock("blob", 3),
    );
    expect(events[0]).toMatchObject({ index: 3 });
    expect(events[1]).toMatchObject({ index: 3 });
  });

  test("serverToolUseBlock streams its input as JSON deltas", async () => {
    const events = await decodeEvents(
      anthropic.serverToolUseBlock(
        "srvtoolu_01",
        "code_execution",
        '{"code": "print(1)"}',
        2,
      ),
    );
    expect(events).toHaveLength(3);
    expect(events[0]).toEqual({
      type: "content_block_start",
      index: 2,
      content_block: {
        type: "server_tool_use",
        id: "srvtoolu_01",
        name: "code_execution",
        input: {},
      },
    });
    expect(events[1]).toEqual({
      type: "content_block_delta",
      index: 2,
      delta: { type: "input_json_delta", partial_json: '{"code": "print(1)"}' },
    });
    expect(events[2]).toEqual({ type: "content_block_stop", index: 2 });
  });

  test("codeExecutionToolResultBlock bakes the result into start with no deltas", async () => {
    const events = await decodeEvents(
      anthropic.codeExecutionToolResultBlock(
        "srvtoolu_01",
        { stdout: "144", stderr: "", return_code: 0 },
        3,
      ),
    );
    expect(events).toHaveLength(2);
    expect(events[0]).toEqual({
      type: "content_block_start",
      index: 3,
      content_block: {
        type: "code_execution_tool_result",
        tool_use_id: "srvtoolu_01",
        content: {
          type: "code_execution_result",
          stdout: "144",
          stderr: "",
          return_code: 0,
        },
      },
    });
    expect(events[1]).toEqual({ type: "content_block_stop", index: 3 });
  });

  test("codeExecutionToolResultBlock carries an aborted-run shape", async () => {
    const events = await decodeEvents(
      anthropic.codeExecutionToolResultBlock("srvtoolu_02", {
        stdout: "",
        stderr: "killed",
        return_code: 137,
        abort_reason: "timeout",
      }),
    );
    const start = events[0];
    if (start === undefined) throw new Error("missing content_block_start");
    const contentBlock = start["content_block"];
    expect(contentBlock).toMatchObject({
      type: "code_execution_tool_result",
      tool_use_id: "srvtoolu_02",
      content: {
        type: "code_execution_result",
        stderr: "killed",
        return_code: 137,
        abort_reason: "timeout",
      },
    });
  });

  test("textBlockWithCitations interleaves text_delta and citations_delta", async () => {
    const citation1 = {
      type: "web_search_result_location",
      cited_text: "first cited span",
      url: "https://example.com/a",
      title: "A",
      encrypted_index: "enc1",
    };
    const citation2 = {
      type: "char_location",
      cited_text: "second cited span",
      document_index: 0,
      document_title: "doc.pdf",
      start_char_index: 0,
      end_char_index: 10,
    };
    const events = await decodeEvents(
      anthropic.textBlockWithCitations(
        "The body text",
        [citation1, citation2],
        1,
      ),
    );
    // start, text_delta, citations_delta * 2, stop = 5 events.
    expect(events).toHaveLength(5);
    expect(events[0]).toEqual({
      type: "content_block_start",
      index: 1,
      content_block: { type: "text", text: "", citations: [] },
    });
    expect(events[1]).toEqual({
      type: "content_block_delta",
      index: 1,
      delta: { type: "text_delta", text: "The body text" },
    });
    expect(events[2]).toEqual({
      type: "content_block_delta",
      index: 1,
      delta: { type: "citations_delta", citation: citation1 },
    });
    expect(events[3]).toEqual({
      type: "content_block_delta",
      index: 1,
      delta: { type: "citations_delta", citation: citation2 },
    });
    expect(events[4]).toEqual({ type: "content_block_stop", index: 1 });
  });

  test("textBlockWithCitations with no citations matches a plain textBlock", async () => {
    const withEmpty = await decodeEvents(
      anthropic.textBlockWithCitations("Hello", [], 0),
    );
    // start, text_delta, stop = 3 events (no citation deltas).
    expect(withEmpty).toHaveLength(3);
    expect(withEmpty[2]).toEqual({ type: "content_block_stop", index: 0 });
  });
});
