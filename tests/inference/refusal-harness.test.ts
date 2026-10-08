// End-to-end refusal-event routing through the inference harness.
//
// The harness accumulates `inference.refusal.delta` events into a
// per-index BlockState and emits a `RefusalBlock` in the finalized
// turn alongside any text. Coverage: refusal-only streams, mixed
// text + refusal in arrival order, and same-index fragment
// concatenation.

import { describe, expect, test } from "bun:test";

import {
  runInference,
  type Dependencies,
  type Scheduler,
} from "@intx/inference";
import { createBuiltinRegistry } from "@intx/inference/providers";
import { wire } from "@intx/inference-testing";
import type {
  ContentBlock,
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

type RefusalBlock = Extract<ContentBlock, { type: "refusal" }>;

const SOURCE: InferenceSource = {
  id: "openai:test-model",
  provider: "openai",
  baseURL: "https://test.invalid/v1",
  credentialId: "test",
  model: "test-model",
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
    adapters: createBuiltinRegistry(),
  };
  let seq = 0;
  return drain(
    runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "Extract user fields." }],
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

describe("runInference — refusal event routing through the harness", () => {
  test("refusal-only stream surfaces refusal events and a RefusalBlock", async () => {
    const chunks: Uint8Array[] = [
      wire.openai.chunk({ refusal: "I cannot" }),
      wire.openai.chunk({ refusal: " help with" }),
      wire.openai.chunk({ refusal: " that request." }),
      wire.openai.chunk({
        usage: { promptTokens: 12, completionTokens: 6 },
      }),
    ];

    const events = await runWithChunks(chunks);

    const refusalDeltas = events.filter(
      (e) => e.type === "inference.refusal.delta",
    );
    expect(refusalDeltas).toHaveLength(3);

    const turn = finalTurn(events);
    const kinds = turn.content.map((b) => b.type);
    expect(kinds).toEqual(["refusal"]);
    const block = turn.content[0];
    if (block?.type !== "refusal") {
      throw new Error("expected refusal block at content[0]");
    }
    const refusal: RefusalBlock = block;
    expect(refusal.reason).toBe("I cannot help with that request.");
  });

  test("text + refusal stream preserves both blocks in arrival order", async () => {
    const chunks: Uint8Array[] = [
      wire.openai.chunk({ content: "Let me see..." }),
      wire.openai.chunk({ refusal: "Actually, I cannot proceed." }),
      wire.openai.chunk({
        usage: { promptTokens: 10, completionTokens: 8 },
      }),
    ];

    const events = await runWithChunks(chunks);

    const turn = finalTurn(events);
    const kinds = turn.content.map((b) => b.type);
    expect(kinds).toEqual(["text", "refusal"]);

    const textBlock = turn.content[0];
    if (textBlock?.type !== "text") {
      throw new Error("expected text block at content[0]");
    }
    expect(textBlock.text).toBe("Let me see...");

    const refusalBlock = turn.content[1];
    if (refusalBlock?.type !== "refusal") {
      throw new Error("expected refusal block at content[1]");
    }
    expect(refusalBlock.reason).toBe("Actually, I cannot proceed.");
  });

  test("multiple refusal fragments at the same index concatenate into one block", async () => {
    const chunks: Uint8Array[] = [
      wire.openai.chunk({ refusal: "I " }),
      wire.openai.chunk({ refusal: "will " }),
      wire.openai.chunk({ refusal: "not." }),
      wire.openai.chunk({
        usage: { promptTokens: 5, completionTokens: 3 },
      }),
    ];

    const events = await runWithChunks(chunks);

    const turn = finalTurn(events);
    expect(turn.content).toHaveLength(1);
    const block = turn.content[0];
    if (block?.type !== "refusal") {
      throw new Error("expected refusal block at content[0]");
    }
    expect(block.reason).toBe("I will not.");
  });
});
