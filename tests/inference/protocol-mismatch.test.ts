// End-to-end coverage of the protocol_mismatch failure path: when an
// upstream emits a chunk whose JSON is malformed or whose shape rejects
// against the adapter's arktype schema, `runInference` must surface
// the failure as an `inference.error` event with category
// `"protocol_mismatch"` carrying the offending bytes in `error.raw` —
// not silently drop the chunk and let the agent guess.
//
// The unit tests in `tests/inference/providers/{openai,anthropic}.test.ts`
// pin the throw shape at the adapter layer; the test in
// `packages/inference/src/errors.test.ts` pins `classifyStreamError`'s
// dispatch. This file pins the harness-level integration: a real
// `runInference` call against a stub fetch sees the failure category
// at the event boundary the way every downstream consumer (default
// director's reply, hub event collector, audit store) will see it.

import { describe, test, expect } from "bun:test";

import { runInference } from "@intx/inference";
import type { Dependencies, Scheduler } from "@intx/inference";
import { createBuiltinRegistry } from "@intx/inference/providers";
import type {
  InferenceEvent,
  InferenceError,
  ConversationTurn,
  InferenceSource,
} from "@intx/types/runtime";

const inertScheduler: Scheduler = {
  setTimeout: () => () => {
    /* tests do not exercise timer firing */
  },
  now: () => 0,
};

function makeTurns(): ConversationTurn[] {
  return [
    { role: "user", content: [{ type: "text", text: "hi" }], timestamp: 0 },
  ];
}

async function drain(
  stream: AsyncIterable<InferenceEvent>,
): Promise<InferenceEvent[]> {
  const out: InferenceEvent[] = [];
  for await (const event of stream) {
    out.push(event);
  }
  return out;
}

function findError(events: InferenceEvent[]): InferenceError | undefined {
  const errorEvent = events.find((e) => e.type === "inference.error");
  if (errorEvent?.type !== "inference.error") return undefined;
  return errorEvent.data.error;
}

function streamingFetch(sseBody: string): Dependencies["fetch"] {
  return () => {
    const enc = new TextEncoder();
    return Promise.resolve(
      new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(enc.encode(sseBody));
            controller.close();
          },
        }),
        { status: 200, headers: { "content-type": "text/event-stream" } },
      ),
    );
  };
}

async function runAgainst(
  provider: "openai" | "anthropic",
  sseBody: string,
): Promise<InferenceError | undefined> {
  const source: InferenceSource = {
    id: `${provider}:test-model`,
    provider,
    baseURL: "https://test.invalid/v1",
    apiKey: "test",
    model: "test-model",
  };
  const deps: Dependencies = {
    fetch: streamingFetch(sseBody),
    scheduler: inertScheduler,
    adapters: createBuiltinRegistry(),
  };
  let seq = 0;
  const events = await drain(
    runInference({
      turns: makeTurns(),
      source,
      nextSeq: () => seq++,
      deps,
    }),
  );
  return findError(events);
}

describe("runInference — protocol_mismatch surfacing", () => {
  test("openai: malformed JSON chunk produces inference.error protocol_mismatch", async () => {
    const err = await runAgainst("openai", "data: not json {\n\n");
    expect(err).toBeDefined();
    expect(err?.category).toBe("protocol_mismatch");
    expect(err?.message).toContain("malformed JSON");
    expect(err?.raw).toBe("not json {");
  });

  test("openai: schema-mismatched chunk produces inference.error protocol_mismatch", async () => {
    const malformed = '{"choices":[{"delta":{"role":42}}]}';
    const err = await runAgainst("openai", `data: ${malformed}\n\n`);
    expect(err).toBeDefined();
    expect(err?.category).toBe("protocol_mismatch");
    expect(err?.message).toContain("schema validation");
    expect(err?.raw).toEqual({ choices: [{ delta: { role: 42 } }] });
  });

  test("anthropic: malformed JSON chunk produces inference.error protocol_mismatch", async () => {
    const err = await runAgainst("anthropic", "event: x\ndata: not json {\n\n");
    expect(err).toBeDefined();
    expect(err?.category).toBe("protocol_mismatch");
    expect(err?.message).toContain("malformed JSON");
    expect(err?.raw).toBe("not json {");
  });

  test("anthropic: schema-mismatched event produces inference.error protocol_mismatch", async () => {
    const err = await runAgainst("anthropic", 'data: {"type":42}\n\n');
    expect(err).toBeDefined();
    expect(err?.category).toBe("protocol_mismatch");
    expect(err?.message).toContain("schema validation");
    expect(err?.raw).toEqual({ type: 42 });
  });
});
