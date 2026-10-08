// End-to-end coverage of the Anthropic redacted_thinking round-trip:
// start event → inference.thinking.redacted → RedactedThinkingBlock in
// the final turn → request builder echoes the opaque `data` bytes back.
//
// The `data` blob must survive every stage unchanged; Anthropic
// rejects a mutated or omitted blob (or silently corrupts context),
// so byte-identical round-trip is the load-bearing invariant.

import { describe, test, expect } from "bun:test";

import {
  runInference,
  type Dependencies,
  type Scheduler,
} from "@intx/inference";
import {
  createAnthropicAdapter,
  createBuiltinRegistry,
} from "@intx/inference/providers";
import { wire } from "@intx/inference-testing";
import type {
  AssistantTurn,
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
  LastCycleSource,
} from "@intx/types/runtime";

const TEST_SOURCE: LastCycleSource = {
  sourceId: "test-anthropic",
  provider: "anthropic",
  model: "test-anthropic-model",
};

// Adversarial payload: newlines, padding, whitespace, and
// escape-sensitive bytes that an over-eager normalizer would touch.
const SYNTHETIC_REDACTED_DATA = 'Opaque\nBytes\r\n  ==\tFromAnthropic\\"AAAA==';

const SOURCE: InferenceSource = {
  id: "anthropic:claude-test",
  provider: "anthropic",
  baseURL: "https://test.invalid/v1",
  credentialId: "test",
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
      adapters: createBuiltinRegistry(),
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
        readMaterial: () => ({ secret: "test-secret" }),
        turns,
        source: SOURCE,
        nextSeq: () => seq++,
        deps,
      }),
    );

    // Downstream consumers see the event (data + source index) before
    // inference.done.
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
    // Feed the final assistant turn back into the request builder: the
    // opaque data must land in the outbound messages[].content[]
    // verbatim — the invariant Anthropic checks on follow-up turns.
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
      adapters: createBuiltinRegistry(),
    };

    let seq = 0;
    const events = await drain(
      runInference({
        readMaterial: () => ({ secret: "test-secret" }),
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
    const adapter = createAnthropicAdapter(TEST_SOURCE);
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

    // Byte-exact equality on the opaque data; a substring match would
    // pass on an adapter that padded or wrapped it, which Anthropic
    // rejects.
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
