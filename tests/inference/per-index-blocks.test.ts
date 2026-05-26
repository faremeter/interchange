// Per-index block tracking in the inference harness. The harness
// keys block state by `content_block` index (Map<number, BlockState>),
// preserving insertion order so the final assistant turn reproduces
// the wire-arrival order of content blocks even when those blocks
// were authored at non-monotonic or non-contiguous indices.
//
// These tests exercise the harness end-to-end via `runInference` with
// synthesized SSE bytes from the Anthropic wire DSL, covering:
//
//   - Multi-block interleaving: thinking@0 → text@1 → tool_use@2
//     lands in the final turn's content[] in that order.
//   - Map insertion order with non-monotonic keys: index 2 inserted
//     before index 0 still iterates in insertion order.
//   - Signature arrives after the next block's deltas: signature
//     attaches to the right thinking block by index.
//   - Empty thinking block with signature only: emitted, not dropped.
//   - Kind collision at the same index: ProtocolMismatchError.
//   - Missing index at the harness boundary: ProtocolMismatchError.

import { describe, expect, test } from "bun:test";

import {
  runInference,
  type Dependencies,
  type Scheduler,
} from "@intx/inference";
import { wire } from "@intx/inference-testing";
import type {
  ContentBlock,
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

// ThinkingBlock is not exported from @intx/types/runtime, so narrow
// to it via Extract on the ContentBlock union.
type ThinkingBlock = Extract<ContentBlock, { type: "thinking" }>;

const SOURCE: InferenceSource = {
  id: "anthropic:claude-test",
  provider: "anthropic",
  baseURL: "https://test.invalid/v1",
  apiKey: "test",
  model: "claude-test",
};

const inertScheduler: Scheduler = {
  setTimeout: () => () => {
    /* tests do not exercise timer firing */
  },
  now: () => 0,
};

async function drain(
  stream: AsyncIterable<InferenceEvent>,
): Promise<InferenceEvent[]> {
  const out: InferenceEvent[] = [];
  for await (const event of stream) {
    out.push(event);
  }
  return out;
}

function streamingFetch(chunks: Uint8Array[]): Dependencies["fetch"] {
  return () => {
    return Promise.resolve(
      new Response(
        new ReadableStream({
          start(controller) {
            for (const chunk of chunks) controller.enqueue(chunk);
            controller.close();
          },
        }),
        { status: 200, headers: { "content-type": "text/event-stream" } },
      ),
    );
  };
}

async function runWithChunks(chunks: Uint8Array[]): Promise<InferenceEvent[]> {
  const deps: Dependencies = {
    fetch: streamingFetch(chunks),
    scheduler: inertScheduler,
  };
  let seq = 0;
  return drain(
    runInference({
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "x" }],
          timestamp: 0,
        },
      ],
      source: SOURCE,
      nextSeq: () => seq++,
      deps,
    }),
  );
}

function finalTurn(events: InferenceEvent[]): ConversationTurn {
  const done = events.find((e) => e.type === "inference.done");
  if (done?.type !== "inference.done") {
    throw new Error("expected inference.done event");
  }
  return done.data.turn;
}

describe("runInference — per-index final turn assembly", () => {
  test("thinking@0 + text@1 + tool_use@2 lands in arrival order in the final turn", async () => {
    // function-calling-with-thinking-streaming corpus shape: thinking
    // at block 0, text at block 1, tool_use at block 2. The final
    // turn's content[] must reproduce that order.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "thinking_delta",
        thinking: "Reasoning about which tool to call.",
      }),
      wire.anthropic.contentBlockStop({ index: 0 }),
      wire.anthropic.contentBlockDelta({
        index: 1,
        kind: "text_delta",
        text: "I'll call the search tool. ",
      }),
      wire.anthropic.contentBlockStop({ index: 1 }),
      wire.anthropic.contentBlockStart({
        index: 2,
        kind: "tool_use",
        id: "toolu_search",
        name: "search",
      }),
      wire.anthropic.contentBlockDelta({
        index: 2,
        kind: "input_json_delta",
        partialJson: '{"q":"hi"}',
      }),
      wire.anthropic.contentBlockStop({ index: 2 }),
      wire.anthropic.messageDelta({ stopReason: "tool_use", outputTokens: 1 }),
      wire.anthropic.messageStop(),
    ];

    const events = await runWithChunks(chunks);
    const turn = finalTurn(events);
    const kinds = turn.content.map((b) => b.type);
    expect(kinds).toEqual(["thinking", "text", "tool_call"]);
    const thinkingBlock = turn.content[0];
    if (thinkingBlock?.type !== "thinking") {
      throw new Error("expected thinking block at content[0]");
    }
    expect(thinkingBlock.thinking).toBe("Reasoning about which tool to call.");
    const textBlock = turn.content[1];
    if (textBlock?.type !== "text") {
      throw new Error("expected text block at content[1]");
    }
    expect(textBlock.text).toBe("I'll call the search tool. ");
    const toolCallBlock = turn.content[2];
    if (toolCallBlock?.type !== "tool_call") {
      throw new Error("expected tool_call block at content[2]");
    }
    expect(toolCallBlock.name).toBe("search");
    expect(toolCallBlock.arguments).toEqual({ q: "hi" });
  });

  test("non-monotonic block index arrival preserves insertion order", async () => {
    // Anthropic doesn't actually emit non-monotonic indices in
    // practice, but the harness's per-index Map must use insertion
    // order rather than numeric key order. If a future provider
    // streams content_block_start at index 2 before index 0
    // (e.g., for re-ordered server-side rendering), the final turn
    // must reflect arrival order, not key order.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockDelta({
        index: 2,
        kind: "text_delta",
        text: "FIRST_INSERTED",
      }),
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "text_delta",
        text: "SECOND_INSERTED",
      }),
      wire.anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 1 }),
      wire.anthropic.messageStop(),
    ];

    const events = await runWithChunks(chunks);
    const turn = finalTurn(events);
    const textBlocks = turn.content.filter((b) => b.type === "text");
    expect(textBlocks).toHaveLength(2);
    const first = textBlocks[0];
    const second = textBlocks[1];
    if (first?.type !== "text" || second?.type !== "text") {
      throw new Error("expected two text blocks");
    }
    // Verify Map insertion-order semantics: index 2 was inserted
    // first, so it appears first in the final turn even though its
    // numeric key is higher than index 0.
    expect(first.text).toBe("FIRST_INSERTED");
    expect(second.text).toBe("SECOND_INSERTED");
  });

  test("signature attaches to the right thinking block when it arrives after another block's deltas", async () => {
    // Anthropic emits signature_delta for a thinking block after
    // content_block_stop for that block — but the wire-level
    // streaming model permits the signature to arrive after deltas
    // for a subsequent block have already started. The harness must
    // route the signature by index, not by recency.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "thinking_delta",
        thinking: "Reasoning step one.",
      }),
      wire.anthropic.contentBlockDelta({
        index: 1,
        kind: "text_delta",
        text: "Answer text.",
      }),
      // Signature for thinking@0 arrives AFTER text@1 has streamed.
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "signature_delta",
        signature: "sig_for_block_0",
      }),
      wire.anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 1 }),
      wire.anthropic.messageStop(),
    ];

    const events = await runWithChunks(chunks);
    const turn = finalTurn(events);
    const thinkingBlock: ThinkingBlock | undefined = turn.content.find(
      (b): b is ThinkingBlock => b.type === "thinking",
    );
    if (thinkingBlock === undefined) {
      throw new Error("expected a thinking block in the final turn");
    }
    expect(thinkingBlock.thinking).toBe("Reasoning step one.");
    expect(thinkingBlock.signature).toBe("sig_for_block_0");
  });

  test("empty thinking block with signature only is still emitted", async () => {
    // A thinking block whose visible text is empty but whose
    // signature must round-trip on follow-up turns. The signature is
    // load-bearing for multi-turn redacted-thinking-adjacent flows;
    // dropping the empty-text block would lose it.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockStart({
        index: 0,
        kind: "thinking",
        thinking: "",
      }),
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "signature_delta",
        signature: "sig_empty_block",
      }),
      wire.anthropic.contentBlockStop({ index: 0 }),
      wire.anthropic.contentBlockDelta({
        index: 1,
        kind: "text_delta",
        text: "Answer.",
      }),
      wire.anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 1 }),
      wire.anthropic.messageStop(),
    ];

    const events = await runWithChunks(chunks);
    const turn = finalTurn(events);
    const thinkingBlocks: ThinkingBlock[] = turn.content.filter(
      (b): b is ThinkingBlock => b.type === "thinking",
    );
    expect(thinkingBlocks).toHaveLength(1);
    const block = thinkingBlocks[0];
    if (block === undefined) {
      throw new Error("expected one thinking block");
    }
    expect(block.thinking).toBe("");
    expect(block.signature).toBe("sig_empty_block");
  });

  test("partial.thinking is a running concat across multiple thinking blocks under interleaving", async () => {
    // Under per-index, thinking@0 "A" then thinking@2 "B" produces
    // two distinct ThinkingBlocks in the final turn, but
    // `partial.thinking` (the live snapshot) is the running concat
    // "AB" — backwards compatible with the pre-per-index single-
    // buffer semantics where `partial.thinking` was the cumulative
    // thinking text the assistant had emitted so far.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "thinking_delta",
        thinking: "A",
      }),
      wire.anthropic.contentBlockDelta({
        index: 1,
        kind: "text_delta",
        text: "X",
      }),
      wire.anthropic.contentBlockDelta({
        index: 2,
        kind: "thinking_delta",
        thinking: "B",
      }),
      wire.anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 1 }),
      wire.anthropic.messageStop(),
    ];

    const events = await runWithChunks(chunks);
    // The last thinking-delta event's `partial.thinking` should reflect
    // the running concat at that point.
    const thinkingDeltas = events.filter(
      (e) => e.type === "inference.thinking.delta",
    );
    expect(thinkingDeltas).toHaveLength(2);
    const lastThinking = thinkingDeltas[1];
    if (lastThinking?.type !== "inference.thinking.delta") {
      throw new Error("expected two thinking.delta events");
    }
    expect(lastThinking.data.partial.thinking).toBe("AB");
    // And the final turn carries TWO distinct ThinkingBlocks.
    const turn = finalTurn(events);
    const blockKinds = turn.content.map((b) => b.type);
    expect(blockKinds).toEqual(["thinking", "text", "thinking"]);
    const block0 = turn.content[0];
    const block2 = turn.content[2];
    if (block0?.type !== "thinking" || block2?.type !== "thinking") {
      throw new Error("expected thinking blocks at 0 and 2");
    }
    expect(block0.thinking).toBe("A");
    expect(block2.thinking).toBe("B");
  });
});

describe("runInference — per-index protocol violations", () => {
  function errorOf(events: InferenceEvent[]) {
    const err = events.find((e) => e.type === "inference.error");
    if (err?.type !== "inference.error") {
      throw new Error("expected inference.error event");
    }
    return err.data.error;
  }

  test("kind collision at the same index produces inference.error protocol_mismatch", async () => {
    // A text delta arriving at index 0 after a thinking delta at the
    // same index is a wire-level protocol violation — distinct kinds
    // can't share an index. Surface as protocol_mismatch with the
    // colliding kind named in the message so a future regression is
    // immediately diagnosable.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "thinking_delta",
        thinking: "T",
      }),
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "text_delta",
        text: "X",
      }),
    ];

    const events = await runWithChunks(chunks);
    const err = errorOf(events);
    expect(err.category).toBe("protocol_mismatch");
    expect(err.message).toMatch(/text\.delta at index 0 collides/);
  });

  test("signature targeting a non-thinking block at that index throws a kind-specific protocol_mismatch", async () => {
    // The signature error path has two distinct branches with
    // different operator triage signals: "no block at this index"
    // (the empty-slot case) vs. "wrong kind at this index" (the
    // collision case). The second branch fires when a signature_delta
    // arrives at an index already populated by a non-thinking block —
    // e.g., a text block was opened at that index and now a signature
    // tries to attach. The error message must name the colliding
    // kind so an operator knows whether the bug is upstream (text
    // landed at the wrong index) or downstream (signature routed to
    // the wrong index).
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockDelta({
        index: 2,
        kind: "text_delta",
        text: "some text",
      }),
      wire.anthropic.contentBlockDelta({
        index: 2,
        kind: "signature_delta",
        signature: "stray_sig",
      }),
    ];

    const events = await runWithChunks(chunks);
    const err = errorOf(events);
    expect(err.category).toBe("protocol_mismatch");
    expect(err.message).toMatch(
      /thinking\.signature at index 2 targets an existing text block, not a thinking block/,
    );
  });

  test("signature without preceding thinking at that index throws protocol_mismatch", async () => {
    // A signature_delta arriving at an index where no thinking block
    // has been opened is a wire violation — Anthropic always emits
    // signature_delta after at least one thinking_delta for the same
    // block.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockDelta({
        index: 7,
        kind: "signature_delta",
        signature: "stray_sig",
      }),
    ];

    const events = await runWithChunks(chunks);
    const err = errorOf(events);
    expect(err.category).toBe("protocol_mismatch");
    expect(err.message).toMatch(
      /thinking\.signature at index 7 has no preceding thinking block/,
    );
  });
});

// A small synthetic verifier that the harness's tool_use marker in
// the per-index map resolves to the right ContentBlock at assembly
// time. This locks in greybeard's "keep tool_use markers separate
// from the openToolCalls state machine" design.
describe("runInference — OpenAI tool_call slot/blockIndex resolution", () => {
  // Regression: an OpenAI tool_call whose `tcDelta.index` is non-zero
  // and non-contiguous (a single tool call at slot 3, or sparse
  // parallel slots) used to land its argument fragments under a
  // placeholder callId that the harness had never registered in
  // `indexToCallId`. The fragments were silently dropped and the
  // final tool_call.end carried empty arguments. This test exercises
  // the full path: parser emits start at slot 3, delta with
  // placeholder callId, harness resolves to the real id, finalized
  // turn carries the assembled arguments.

  test("OpenAI tool_call at sparse tcDelta.index accumulates argument fragments", async () => {
    const { wire } = await import("@intx/inference-testing");
    const openaiSource: InferenceSource = {
      id: "openai:gpt-test",
      provider: "openai",
      baseURL: "https://test.invalid/v1",
      apiKey: "test",
      model: "gpt-test",
    };

    // Build SSE chunks: tool_call.start at slot 3, then two delta
    // chunks at slot 3 with argument fragments, then a usage final
    // chunk.
    const chunks: Uint8Array[] = [
      wire.openai.toolCallStart(3, "call_sparse", "search"),
      wire.openai.toolCallArgumentsDelta(3, '{"a":'),
      wire.openai.toolCallArgumentsDelta(3, "1}"),
      wire.openai.usageChunk({ promptTokens: 5, completionTokens: 1 }),
    ];

    const deps: Dependencies = {
      fetch: streamingFetch(chunks),
      scheduler: inertScheduler,
    };
    let seq = 0;
    const events = await drain(
      runInference({
        turns: [
          {
            role: "user",
            content: [{ type: "text", text: "x" }],
            timestamp: 0,
          },
        ],
        source: openaiSource,
        nextSeq: () => seq++,
        deps,
      }),
    );

    const done = events.find((e) => e.type === "inference.done");
    if (done?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }
    const turn = done.data.turn;
    const toolCall = turn.content.find((b) => b.type === "tool_call");
    if (toolCall?.type !== "tool_call") {
      throw new Error("expected tool_call block in final turn");
    }
    expect(toolCall.id).toBe("call_sparse");
    expect(toolCall.name).toBe("search");
    expect(toolCall.arguments).toEqual({ a: 1 });
  });
});

describe("runInference — tool_use marker resolution", () => {
  test("tool_use marker at index N resolves to the finalized ContentBlock for that callId", async () => {
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "text_delta",
        text: "Calling: ",
      }),
      wire.anthropic.contentBlockStart({
        index: 1,
        kind: "tool_use",
        id: "toolu_specific",
        name: "search",
      }),
      wire.anthropic.contentBlockDelta({
        index: 1,
        kind: "input_json_delta",
        partialJson: '{"q":"x"}',
      }),
      wire.anthropic.contentBlockStop({ index: 1 }),
      wire.anthropic.messageDelta({ stopReason: "tool_use", outputTokens: 1 }),
      wire.anthropic.messageStop(),
    ];

    const events = await runWithChunks(chunks);
    const turn = finalTurn(events);
    const blockKinds = turn.content.map((b) => b.type);
    expect(blockKinds).toEqual(["text", "tool_call"]);
    const toolCall: ContentBlock | undefined = turn.content[1];
    if (toolCall?.type !== "tool_call") {
      throw new Error("expected tool_call at content[1]");
    }
    expect(toolCall.id).toBe("toolu_specific");
    expect(toolCall.name).toBe("search");
    expect(toolCall.arguments).toEqual({ q: "x" });
  });
});
