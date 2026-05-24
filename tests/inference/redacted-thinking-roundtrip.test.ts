// End-to-end coverage of the Anthropic redacted_thinking round-trip:
// SSE `content_block_start` of type `redacted_thinking` arrives →
// parser emits `inference.thinking.redacted` → harness threads it
// through and lands the RedactedThinkingBlock in the final
// `inference.done` turn → the request builder echoes the same
// opaque `data` bytes back on a follow-up turn.
//
// The opaque `data` blob must survive every stage unchanged.
// Anthropic rejects any follow-up that mutates or omits the blob
// with a 400 (or worse, with silent context corruption that taints
// downstream turns), so the round-trip is the load-bearing invariant
// the adapter exists to preserve.

import { describe, test, expect } from "bun:test";

import {
  createAnthropicAdapter,
  runInference,
  type Dependencies,
  type Scheduler,
} from "@intx/inference";
import { wire } from "@intx/inference-testing";
import type {
  AssistantTurn,
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

// Adversarial payload: includes characters that an over-eager
// normalizer would touch (newlines, padding, internal whitespace,
// escape-sensitive bytes). The byte-identical round-trip is the
// invariant — a `JSON.stringify`-of-already-stringified bug, a
// whitespace strip, or a Unicode normalization would all corrupt
// this payload.
const SYNTHETIC_REDACTED_DATA = 'Opaque\nBytes\r\n  ==\tFromAnthropic\\"AAAA==';

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

describe("runInference — Anthropic redacted_thinking round-trip", () => {
  test("emits inference.thinking.redacted and lands the block in the final turn", async () => {
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      ...wire.anthropic.redactedThinkingBlock(SYNTHETIC_REDACTED_DATA, 0),
      wire.anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 1 }),
      wire.anthropic.messageStop(),
    ];

    const deps: Dependencies = {
      fetch: streamingFetch(chunks),
      scheduler: inertScheduler,
    };
    const turns: ConversationTurn[] = [
      {
        role: "user",
        content: [
          { type: "text", text: "Tell me something the classifier hits." },
        ],
        timestamp: 0,
      },
    ];

    let seq = 0;
    const events = await drain(
      runInference({
        turns,
        source: SOURCE,
        nextSeq: () => seq++,
        deps,
      }),
    );

    // The streaming event lands with the opaque data and the source
    // index — downstream consumers (event collector, audit store) see
    // it before inference.done.
    const redactedEvents = events.filter(
      (e) => e.type === "inference.thinking.redacted",
    );
    expect(redactedEvents).toHaveLength(1);
    const redactedEv = redactedEvents[0];
    if (redactedEv?.type !== "inference.thinking.redacted") {
      throw new Error("expected inference.thinking.redacted event");
    }
    expect(redactedEv.data.redactedThinking.data).toBe(SYNTHETIC_REDACTED_DATA);
    expect(redactedEv.data.index).toBe(0);

    // The final turn carries the RedactedThinkingBlock — that's what
    // gets persisted to history and echoed back next turn.
    const doneEvent = events.find((e) => e.type === "inference.done");
    if (doneEvent?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }
    const finalTurn: AssistantTurn = doneEvent.data.turn;
    const blocksOfKind = finalTurn.content.filter(
      (b) => b.type === "redacted_thinking",
    );
    expect(blocksOfKind).toHaveLength(1);
    const finalBlock = blocksOfKind[0];
    if (finalBlock?.type !== "redacted_thinking") {
      throw new Error("expected redacted_thinking block in final turn");
    }
    expect(finalBlock.data).toBe(SYNTHETIC_REDACTED_DATA);
  });

  test("data survives the full round-trip back into a follow-up request body", async () => {
    // Drive a redacted_thinking response through the harness, take the
    // final assistant turn, and feed it back into the request builder
    // as conversation history. The opaque data must land in the
    // outbound request's messages[].content[] verbatim. This is the
    // actual invariant Anthropic checks on every follow-up turn.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      ...wire.anthropic.redactedThinkingBlock(SYNTHETIC_REDACTED_DATA, 0),
      wire.anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 1 }),
      wire.anthropic.messageStop(),
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
            content: [{ type: "text", text: "round-trip" }],
            timestamp: 0,
          },
        ],
        source: SOURCE,
        nextSeq: () => seq++,
        deps,
      }),
    );

    const doneEvent = events.find((e) => e.type === "inference.done");
    if (doneEvent?.type !== "inference.done") {
      throw new Error("expected inference.done event");
    }
    const assistantTurn: ConversationTurn = doneEvent.data.turn;

    // Build the next request with the assistant turn back in history.
    const adapter = createAnthropicAdapter();
    const req = adapter.buildRequest(
      [
        {
          role: "user",
          content: [{ type: "text", text: "first" }],
          timestamp: 0,
        },
        assistantTurn,
        {
          role: "user",
          content: [{ type: "text", text: "follow-up" }],
          timestamp: 1,
        },
      ],
      "claude-test",
      {},
    );

    // Decode the body and locate the assistant message's
    // redacted_thinking block to assert byte-exact equality on the
    // opaque data. A substring match (toContain) would still pass on
    // a buggy adapter that surrounded the data with whitespace,
    // padding, or a BOM — Anthropic rejects such mutations, so the
    // test must be just as strict.
    const parsed: unknown = JSON.parse(req.body);
    if (!isRecord(parsed)) {
      throw new Error("expected request body to be a JSON object");
    }
    const messages = parsed["messages"];
    if (!Array.isArray(messages)) {
      throw new Error("expected body.messages to be an array");
    }
    let observedData: unknown;
    for (const msg of messages) {
      if (!isRecord(msg)) continue;
      if (msg["role"] !== "assistant") continue;
      const content = msg["content"];
      if (!Array.isArray(content)) continue;
      for (const block of content) {
        if (!isRecord(block)) continue;
        if (block["type"] === "redacted_thinking") {
          observedData = block["data"];
        }
      }
    }
    expect(observedData).toBe(SYNTHETIC_REDACTED_DATA);
  });
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
