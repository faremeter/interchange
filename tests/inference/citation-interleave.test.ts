// Citation interleaving in the inference harness. Indexed
// `inference.citation` events interleave into the finalized turn's
// `content[]` immediately after the block at the matching index;
// citations without an index append at the tail (legacy positional
// rule on CitationBlock).
//
// Indexed citations from the Anthropic adapter interleave at their
// source block index across two text blocks; unindexed citations from
// a stub adapter append at the end of `content[]`.

import { describe, expect, test } from "bun:test";

import {
  runInference,
  type Dependencies,
  type Scheduler,
  type AdapterRegistry,
} from "@intx/inference";
import {
  createBuiltinRegistry,
  loadAdapterRegistry,
} from "@intx/inference/providers";
import { wire } from "@intx/inference-testing";
import type {
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

const ANTHROPIC_SOURCE: InferenceSource = {
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

async function runWithChunks(
  source: InferenceSource,
  chunks: Uint8Array[],
  adapters: AdapterRegistry = createBuiltinRegistry(),
): Promise<InferenceEvent[]> {
  const deps: Dependencies = {
    fetch: streamingFetch(chunks),
    scheduler: inertScheduler,
    adapters,
  };
  let seq = 0;
  return drain(
    runInference({
      readMaterial: () => ({ secret: "test-secret" }),
      turns: [
        {
          role: "user",
          content: [{ type: "text", text: "x" }],
          timestamp: 0,
        },
      ],
      source,
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

describe("runInference — citation interleaving", () => {
  test("indexed citations from Anthropic interleave at their source block", async () => {
    // Two text blocks, each with one citation: the final content[] must
    // be [text@0, citation@0, text@1, citation@1] — neither flat-tail
    // nor sort-by-index order.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      ...wire.anthropic.textBlockWithCitations(
        "First sentence.",
        [
          {
            type: "web_search_result_location",
            cited_text: "First sentence.",
            url: "https://example.com/a",
            title: "Source A",
            encrypted_index: "EA==",
          },
        ],
        0,
      ),
      ...wire.anthropic.textBlockWithCitations(
        "Second sentence.",
        [
          {
            type: "web_search_result_location",
            cited_text: "Second sentence.",
            url: "https://example.com/b",
            title: "Source B",
            encrypted_index: "EB==",
          },
        ],
        1,
      ),
      wire.anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 2 }),
      wire.anthropic.messageStop(),
    ];

    const events = await runWithChunks(ANTHROPIC_SOURCE, chunks);
    const turn = finalTurn(events);
    const kinds = turn.content.map((b) => b.type);
    expect(kinds).toEqual(["text", "citation", "text", "citation"]);

    const text0 = turn.content[0];
    const cit0 = turn.content[1];
    const text1 = turn.content[2];
    const cit1 = turn.content[3];
    if (text0?.type !== "text") {
      throw new Error("expected text block at content[0]");
    }
    if (cit0?.type !== "citation") {
      throw new Error("expected citation block at content[1]");
    }
    if (text1?.type !== "text") {
      throw new Error("expected text block at content[2]");
    }
    if (cit1?.type !== "citation") {
      throw new Error("expected citation block at content[3]");
    }
    expect(text0.text).toBe("First sentence.");
    expect(cit0.source.uri).toBe("https://example.com/a");
    expect(text1.text).toBe("Second sentence.");
    expect(cit1.source.uri).toBe("https://example.com/b");
  });

  test("citation interleaves after text in a thinking + text + citation turn", async () => {
    // INTR-119 DoD scenario: citation interleaves directly after the
    // text it annotates — [thinking, text, citation], not tail.
    const chunks: Uint8Array[] = [
      wire.anthropic.messageStart({
        usage: { inputTokens: 5, outputTokens: 0 },
      }),
      wire.anthropic.contentBlockDelta({
        index: 0,
        kind: "thinking_delta",
        thinking: "Reasoning about the source.",
      }),
      wire.anthropic.contentBlockStop({ index: 0 }),
      ...wire.anthropic.textBlockWithCitations(
        "Annotated sentence.",
        [
          {
            type: "web_search_result_location",
            cited_text: "Annotated sentence.",
            url: "https://example.com/x",
            title: "Source X",
            encrypted_index: "EX==",
          },
        ],
        1,
      ),
      wire.anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 1 }),
      wire.anthropic.messageStop(),
    ];

    const events = await runWithChunks(ANTHROPIC_SOURCE, chunks);
    const turn = finalTurn(events);
    const kinds = turn.content.map((b) => b.type);
    expect(kinds).toEqual(["thinking", "text", "citation"]);

    const thinkingBlock = turn.content[0];
    const textBlock = turn.content[1];
    const citationBlock = turn.content[2];
    if (thinkingBlock?.type !== "thinking") {
      throw new Error("expected thinking block at content[0]");
    }
    if (textBlock?.type !== "text") {
      throw new Error("expected text block at content[1]");
    }
    if (citationBlock?.type !== "citation") {
      throw new Error("expected citation block at content[2]");
    }
    expect(thinkingBlock.thinking).toBe("Reasoning about the source.");
    expect(textBlock.text).toBe("Annotated sentence.");
    expect(citationBlock.source.uri).toBe("https://example.com/x");
  });

  test("citation indexed at a block that never emits surfaces as ProtocolMismatchError", async () => {
    // A citation indexing a block absent from the final turn would
    // otherwise be silently dropped; surface it via
    // ProtocolMismatchError, the same posture as an unmatched
    // tool_use callId.
    const providerName = `test-orphan-citation-${Math.random().toString(36).slice(2)}`;
    const make = () => ({
      buildRequest: () => ({
        url: "/stub",
        headers: { "content-type": "application/json" },
        body: "{}",
      }),
      parseJSONResponse: () => [],
      parseResponse: (sse: string): InferenceEvent[] => {
        if (sse !== "go") return [];
        return [
          {
            type: "inference.text.delta",
            seq: 0,
            data: {
              token: "Body text.",
              partial: { text: "Body text." },
              index: 0,
            },
          },
          {
            type: "inference.citation",
            seq: 0,
            data: {
              citation: {
                type: "citation",
                citedText: "Nothing matches this.",
                source: { uri: "https://example.com/orphan" },
              },
              // Index 42 points at no block in the final turn.
              index: 42,
            },
          },
          {
            type: "inference.usage",
            seq: 0,
            data: {
              usage: {
                input: 1,
                output: 2,
                cacheRead: 0,
                cacheWrite: 0,
                thinking: 0,
              },
              source: TEST_SOURCE,
            },
          },
        ];
      },
    });

    const adapters = await loadAdapterRegistry(
      [{ provider: providerName, specifier: "x", export: "make" }],
      { import: () => Promise.resolve({ make }) },
    );

    const source: InferenceSource = {
      id: `${providerName}:test-model`,
      provider: providerName,
      baseURL: "https://test.invalid",
      credentialId: "test",
      model: "test-model",
    };

    const encoder = new TextEncoder();
    const chunks: Uint8Array[] = [encoder.encode("data: go\n\n")];

    // The orphan check fires from the finalize path, so the error
    // rejects the async iterator (like the tool_use-marker-missing
    // throw) rather than surfacing as an inference.error event.
    await expect(runWithChunks(source, chunks, adapters)).rejects.toThrow(
      /no matching emitted block.*42|42.*no matching emitted block/,
    );
  });

  test("citations without an index append at the tail of content[]", async () => {
    // `index` is optional on citation events for protocols without
    // per-citation block indices; unindexed citations append at the
    // end of `content[]`. A stub adapter emits a text block and an
    // unindexed citation; a fresh provider name per run keeps runs
    // isolated.
    const providerName = `test-citations-no-index-${Math.random().toString(36).slice(2)}`;
    const make = () => ({
      buildRequest: () => ({
        url: "/stub",
        headers: { "content-type": "application/json" },
        body: "{}",
      }),
      parseJSONResponse: () => [],
      parseResponse: (sse: string): InferenceEvent[] => {
        // The stub emits a fixed sequence on the "go" payload; seq and
        // partial snapshots are placeholders the harness overwrites.
        if (sse !== "go") return [];
        return [
          {
            type: "inference.text.delta",
            seq: 0,
            data: {
              token: "Body text.",
              partial: { text: "Body text." },
              index: 0,
            },
          },
          {
            type: "inference.citation",
            seq: 0,
            data: {
              citation: {
                type: "citation",
                citedText: "Body text.",
                source: { uri: "https://example.com/unindexed" },
              },
            },
          },
          {
            type: "inference.usage",
            seq: 0,
            data: {
              usage: {
                input: 1,
                output: 2,
                cacheRead: 0,
                cacheWrite: 0,
                thinking: 0,
              },
              source: TEST_SOURCE,
            },
          },
        ];
      },
    });

    const adapters = await loadAdapterRegistry(
      [{ provider: providerName, specifier: "x", export: "make" }],
      { import: () => Promise.resolve({ make }) },
    );

    const source: InferenceSource = {
      id: `${providerName}:test-model`,
      provider: providerName,
      baseURL: "https://test.invalid",
      credentialId: "test",
      model: "test-model",
    };

    const encoder = new TextEncoder();
    const chunks: Uint8Array[] = [encoder.encode("data: go\n\n")];

    const events = await runWithChunks(source, chunks, adapters);
    const turn = finalTurn(events);
    const kinds = turn.content.map((b) => b.type);
    expect(kinds).toEqual(["text", "citation"]);
    const text = turn.content[0];
    const cit = turn.content[1];
    if (text?.type !== "text") {
      throw new Error("expected text block at content[0]");
    }
    if (cit?.type !== "citation") {
      throw new Error("expected citation block at content[1]");
    }
    expect(text.text).toBe("Body text.");
    expect(cit.source.uri).toBe("https://example.com/unindexed");
  });
});
