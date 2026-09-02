import { describe, expect, test } from "bun:test";
import { type } from "arktype";

import type {
  AssistantTurn,
  ContentBlock,
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
  LastCycleSource,
} from "@intx/types/runtime";

import { ProtocolMismatchError } from "../errors";
import {
  createDefaultScheduler,
  runInference,
  type Dependencies,
} from "../harness";
import { createBuiltinRegistry } from "../providers";
import { createAnthropicAdapter } from "./anthropic";

const TEST_SOURCE: LastCycleSource = {
  sourceId: "test-anthropic",
  provider: "anthropic",
  model: "test-anthropic-model",
};

// The parser is exercised through the public adapter rather than imported
// directly. parseResponse is intentionally not exported — the adapter
// owns the per-request `blockIndexToCallId` map and creating it through
// the same factory production uses keeps tests honest about the lifecycle.

function parse(
  adapter: ReturnType<typeof createAnthropicAdapter>,
  sse: object,
): InferenceEvent[] {
  return adapter.parseResponse(JSON.stringify(sse));
}

// Inline narrowing utilities. Each `pickFirst*` returns the strongly-typed
// variant by exhausting the failure modes — empty result or wrong type
// throw a descriptive error, leaving the body of the test to operate on
// the narrowed value without an unsafe cast.

function pickFirstTextDelta(
  events: InferenceEvent[],
): Extract<InferenceEvent, { type: "inference.text.delta" }> {
  const ev = events[0];
  if (ev === undefined) throw new Error("expected at least one event");
  if (ev.type !== "inference.text.delta") {
    throw new Error(`expected inference.text.delta, got ${ev.type}`);
  }
  return ev;
}

function pickFirstThinkingDelta(
  events: InferenceEvent[],
): Extract<InferenceEvent, { type: "inference.thinking.delta" }> {
  const ev = events[0];
  if (ev === undefined) throw new Error("expected at least one event");
  if (ev.type !== "inference.thinking.delta") {
    throw new Error(`expected inference.thinking.delta, got ${ev.type}`);
  }
  return ev;
}

function pickFirstThinkingSignature(
  events: InferenceEvent[],
): Extract<InferenceEvent, { type: "inference.block.signature" }> {
  const ev = events[0];
  if (ev === undefined) throw new Error("expected at least one event");
  if (ev.type !== "inference.block.signature") {
    throw new Error(`expected inference.block.signature, got ${ev.type}`);
  }
  return ev;
}

function pickFirstToolCallStart(
  events: InferenceEvent[],
): Extract<InferenceEvent, { type: "inference.tool_call.start" }> {
  const ev = events[0];
  if (ev === undefined) throw new Error("expected at least one event");
  if (ev.type !== "inference.tool_call.start") {
    throw new Error(`expected inference.tool_call.start, got ${ev.type}`);
  }
  return ev;
}

function pickFirstToolCallDelta(
  events: InferenceEvent[],
): Extract<InferenceEvent, { type: "inference.tool_call.delta" }> {
  const ev = events[0];
  if (ev === undefined) throw new Error("expected at least one event");
  if (ev.type !== "inference.tool_call.delta") {
    throw new Error(`expected inference.tool_call.delta, got ${ev.type}`);
  }
  return ev;
}

describe("Anthropic parser — index propagation on delta events", () => {
  test("text_delta carries the SSE block index forward as data.index", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_delta",
      index: 1,
      delta: { type: "text_delta", text: "hi" },
    });
    expect(events).toHaveLength(1);
    const ev = pickFirstTextDelta(events);
    expect(ev.data.index).toBe(1);
    expect(ev.data.token).toBe("hi");
  });

  test("thinking_delta carries the SSE block index forward as data.index", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_delta",
      index: 2,
      delta: { type: "thinking_delta", thinking: "reasoning" },
    });
    expect(events).toHaveLength(1);
    const ev = pickFirstThinkingDelta(events);
    expect(ev.data.index).toBe(2);
    expect(ev.data.token).toBe("reasoning");
  });

  test("signature_delta carries the SSE block index forward as data.index", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_delta",
      index: 3,
      delta: { type: "signature_delta", signature: "sig-abc" },
    });
    expect(events).toHaveLength(1);
    const ev = pickFirstThinkingSignature(events);
    expect(ev.data.index).toBe(3);
    expect(ev.data.signature).toBe("sig-abc");
  });

  test("tool_call.start carries the SSE block index forward as data.index", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_start",
      index: 4,
      content_block: { type: "tool_use", id: "call_xyz", name: "search" },
    });
    expect(events).toHaveLength(1);
    const ev = pickFirstToolCallStart(events);
    expect(ev.data.index).toBe(4);
    expect(ev.data.callId).toBe("call_xyz");
  });

  test("tool_call.delta carries the SSE block index forward as data.index", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    parse(adapter, {
      type: "content_block_start",
      index: 5,
      content_block: { type: "tool_use", id: "call_abc", name: "search" },
    });
    const events = parse(adapter, {
      type: "content_block_delta",
      index: 5,
      delta: { type: "input_json_delta", partial_json: '{"q":' },
    });
    expect(events).toHaveLength(1);
    const ev = pickFirstToolCallDelta(events);
    expect(ev.data.index).toBe(5);
    expect(ev.data.callId).toBe("call_abc");
    expect(ev.data.argumentFragment).toBe('{"q":');
  });
});

describe("Anthropic parser — multi-tool callId routing across indices", () => {
  // Regression target: when two tool_use blocks open at distinct indices,
  // the input_json_delta lookup must resolve to the correct callId per
  // index. Collapsing the indices into one slot would route the second
  // tool's argument fragments to the first tool's callId.
  test("input_json_deltas at distinct indices resolve to distinct callIds", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    parse(adapter, {
      type: "content_block_start",
      index: 0,
      content_block: { type: "tool_use", id: "call_first", name: "alpha" },
    });
    parse(adapter, {
      type: "content_block_start",
      index: 1,
      content_block: { type: "tool_use", id: "call_second", name: "beta" },
    });

    const firstFrag = parse(adapter, {
      type: "content_block_delta",
      index: 0,
      delta: { type: "input_json_delta", partial_json: '{"a":1}' },
    });
    const secondFrag = parse(adapter, {
      type: "content_block_delta",
      index: 1,
      delta: { type: "input_json_delta", partial_json: '{"b":2}' },
    });

    const first = pickFirstToolCallDelta(firstFrag);
    const second = pickFirstToolCallDelta(secondFrag);
    expect(first.data.callId).toBe("call_first");
    expect(first.data.index).toBe(0);
    expect(first.data.argumentFragment).toBe('{"a":1}');
    expect(second.data.callId).toBe("call_second");
    expect(second.data.index).toBe(1);
    expect(second.data.argumentFragment).toBe('{"b":2}');
  });
});

describe("Anthropic parser — required-index schema enforcement", () => {
  // Anthropic's SSE protocol guarantees `index` on every content_block_*
  // event. A missing `index` is a protocol violation, not a "default to
  // 0" situation — the parser's `blockIndexToCallId` cache is
  // load-bearing on the index being real, not synthesized.

  test("content_block_delta without index throws ProtocolMismatchError", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    let thrown: unknown;
    try {
      parse(adapter, {
        type: "content_block_delta",
        delta: { type: "text_delta", text: "hi" },
      });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ProtocolMismatchError);
    if (thrown instanceof ProtocolMismatchError) {
      expect(thrown.message).toMatch(/schema validation/);
    }
  });

  test("content_block_start without index throws ProtocolMismatchError", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    let thrown: unknown;
    try {
      parse(adapter, {
        type: "content_block_start",
        content_block: { type: "tool_use", id: "x", name: "y" },
      });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ProtocolMismatchError);
  });

  test("content_block_stop without index throws ProtocolMismatchError", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    let thrown: unknown;
    try {
      parse(adapter, { type: "content_block_stop" });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ProtocolMismatchError);
  });
});

// Happy-path redacted_thinking fixtures use a realistic opaque `data`
// blob shaped like Anthropic wire (`{ type, data }` only — no
// signature, no thinking text). Streaming captures deliver each block
// as a one-shot `content_block_start` (no thinking_delta) and may open
// multiple redacted blocks before text. This constant is test material,
// not a live re-export of a corpus fixture path.
//
// Adversarial unit cases (missing data, whitespace-preserving blobs)
// stay synthetic: the corpus does not cover those failure modes. Corpus
// smoke lives in the session parser regression, which replays every
// fixture-bearing capture through the adapter.
const CAPTURED_REDACTED_DATA =
  "EuMFCpQBCBAYAipAk6yhHbnY8TlQ7bUY2Ji/24unHQHqjdggHcUEqfwJ30Aw" +
  "0/MEN7OxckAJk+w8kg0Gb7Wa4bUBHooVkOAYSFI7TTIaY2xhdWRlLXNvbm5l" +
  "dC00LTUtMjAyNTA5Mjk4AEIIdGhpbmtpbmdaJDc0MjMxNmM4LTY3MzEtNDAx" +
  "Yy04ZmI3LWI0MWVjODAyM2NmMhIMseVAbj6HOCNWLgCXGgxw2hQLnMP7Yg2X" +
  "gO0iMM1+zDaFQSBikq5VRaynjx4TvWDT6Oi4q17HK31nb1e2VuyxWZ9Z7eqj" +
  "fdt/SudU5Cr7AzsIr6Z5Zc6fq2RyvXr6HLUBNkXZychED/tHcFDSIDQgjhJ4" +
  "vUxWMezGrtdQEsusCyYwnyRDKoZx7DpKUMPiIRksfaF+rom+wqrCSVY73qZ/" +
  "NJUEMmAVi+nnRisMgiwENaBJAKaT5fqa7x1BVybPsG+ZLNoDOze4F5sacFbz" +
  "uT3bRBod6Jo7gf4MueX5eKE7zegLIQK4frHtxeCKCBbkVjCFICasdTZmK6Fw" +
  "IP42peQjuyMLevmw+1jD860CSMOI0EUVXjsGbfMOd8Wu6J0myLPF59ca9xAz" +
  "4cZp4nUazbUz7WGJ4Zi8rOIC2Ebx1mQIvu02mRla3wphm48z9UgKhMThVn3q" +
  "5+sJPZhuQ8d5UbIM5ZJvlQ4Kho+XE+H7GjMi8iArTh2GbhXNA7y2y/uYfOQW" +
  "XRnro7oIfHJ6CpIIZWp4nQ1vHA+kRyNa4yB5JXUSuwVhTCmaFroP3AT3ydrp" +
  "OnTnMPt06DY4p+SarntDeHp5XB3n7Gf6Zmk+rnAFC+EA9nnIk/IYmeCRJaSv" +
  "fMnBoAcLAj99bZoH3K9/AKcc4M2t63j9lrcjOfg1Ozy/rvBeUH4R0uqRM55G" +
  "5aau5fmxAp1ERjs9RAdPGcqdeWqlvebTeMXTrPnWfXf2l+hQrs5f1+grwmsa" +
  "OO4UA7P9uJJj6FbOD1z4bVuya9ZrhxgB";

function pickFirstThinkingRedacted(
  events: InferenceEvent[],
): Extract<InferenceEvent, { type: "inference.thinking.redacted" }> {
  const ev = events[0];
  if (ev === undefined) throw new Error("expected at least one event");
  if (ev.type !== "inference.thinking.redacted") {
    throw new Error(`expected inference.thinking.redacted, got ${ev.type}`);
  }
  return ev;
}

describe("Anthropic parser — redacted_thinking content_block_start", () => {
  test("emits inference.thinking.redacted carrying the data and source index", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "redacted_thinking",
        data: CAPTURED_REDACTED_DATA,
      },
    });
    expect(events).toHaveLength(1);
    const ev = pickFirstThinkingRedacted(events);
    expect(ev.data.index).toBe(0);
    expect(ev.data.redactedThinking.type).toBe("redacted_thinking");
    expect(ev.data.redactedThinking.data).toBe(CAPTURED_REDACTED_DATA);
  });

  test("preserves the data verbatim — no normalization or transformation", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    // Adversarial: the corpus never delivers whitespace-laden data;
    // this unit case still pins the no-mutation invariant.
    const adversarial = "abc\n  ==\r\n\tdef==";
    const events = parse(adapter, {
      type: "content_block_start",
      index: 2,
      content_block: { type: "redacted_thinking", data: adversarial },
    });
    const ev = pickFirstThinkingRedacted(events);
    expect(ev.data.redactedThinking.data).toBe(adversarial);
  });

  test("missing `data` field throws ProtocolMismatchError naming the field", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    let thrown: unknown;
    try {
      parse(adapter, {
        type: "content_block_start",
        index: 4,
        content_block: { type: "redacted_thinking" },
      });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ProtocolMismatchError);
    if (thrown instanceof ProtocolMismatchError) {
      expect(thrown.message).toMatch(/redacted_thinking/);
      expect(thrown.message).toMatch(/data/);
      expect(thrown.message).toMatch(/index 4/);
    }
  });

  test("emits one redacted event per content_block_start when multiple appear", () => {
    // Streaming captures open several redacted_thinking blocks before
    // text (Haiku turn-1 streaming had multiple). Each start is
    // independent and carries its own data + index.
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const first = parse(adapter, {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "redacted_thinking",
        data: CAPTURED_REDACTED_DATA,
      },
    });
    const second = parse(adapter, {
      type: "content_block_start",
      index: 1,
      content_block: {
        type: "redacted_thinking",
        data: CAPTURED_REDACTED_DATA + "x",
      },
    });
    expect(first).toHaveLength(1);
    expect(second).toHaveLength(1);
    const a = pickFirstThinkingRedacted(first);
    const b = pickFirstThinkingRedacted(second);
    expect(a.data.index).toBe(0);
    expect(b.data.index).toBe(1);
    expect(a.data.redactedThinking.data).toBe(CAPTURED_REDACTED_DATA);
    expect(b.data.redactedThinking.data).toBe(CAPTURED_REDACTED_DATA + "x");
  });
});

describe("Anthropic adapter — redacted_thinking parser-to-builder round-trip", () => {
  // The parser-side wire shape and the request-builder-side wire shape
  // are tested independently elsewhere. This test closes the loop: it
  // proves that the opaque `data` blob the parser surfaces in
  // `inference.thinking.redacted` reconstructs back to a request body
  // that carries the same bytes verbatim. That round-trip is the
  // invariant Anthropic requires on every follow-up turn.
  test("data survives parse → reconstruct → buildRequest unchanged", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "redacted_thinking",
        data: CAPTURED_REDACTED_DATA,
      },
    });
    const ev = pickFirstThinkingRedacted(events);
    const reconstructed: ConversationTurn = {
      role: "assistant",
      content: [ev.data.redactedThinking],
      timestamp: 0,
    };
    const req = adapter.buildRequest([reconstructed], "claude-test", {
      maxTokens: 100,
    });
    // The structural shape of the body is asserted elsewhere — here
    // we care only that the opaque `data` survives the round-trip.
    const bodyText = req.body;
    expect(bodyText).toContain(CAPTURED_REDACTED_DATA);
    expect(bodyText).toContain(`"type":"redacted_thinking"`);
  });
});

function pickFirstCitation(
  events: InferenceEvent[],
): Extract<InferenceEvent, { type: "inference.citation" }> {
  const ev = events[0];
  if (ev === undefined) throw new Error("expected at least one event");
  if (ev.type !== "inference.citation") {
    throw new Error(`expected inference.citation, got ${ev.type}`);
  }
  return ev;
}

describe("Anthropic parser — citations_delta to inference.citation", () => {
  test("web_search_result_location maps to source.uri + title with no location", () => {
    // Anthropic's web_search citations carry url + title + cited_text +
    // encrypted_index. The encrypted_index has no echo-back target in
    // CitationBlock today and is intentionally dropped at the adapter.
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_delta",
      index: 3,
      delta: {
        type: "citations_delta",
        citation: {
          type: "web_search_result_location",
          cited_text: "Quoted span from the web search result.",
          url: "https://example.com/article",
          title: "Example Article Title",
          encrypted_index: "EncryptedIndexAAAA==",
        },
      },
    });
    const ev = pickFirstCitation(events);
    expect(ev.data.citation.type).toBe("citation");
    expect(ev.data.citation.citedText).toBe(
      "Quoted span from the web search result.",
    );
    expect(ev.data.citation.source.uri).toBe("https://example.com/article");
    expect(ev.data.citation.source.title).toBe("Example Article Title");
    expect(ev.data.citation.location).toBeUndefined();
    // textOffset is intentionally never populated at this layer.
    expect(ev.data.citation.textOffset).toBeUndefined();
    // The content_block_delta.index from the wire propagates onto the
    // emitted event so the harness can interleave the citation at the
    // matching block position in the finalized turn.
    expect(ev.data.index).toBe(3);
  });

  test("citations on two different block indices preserve their respective indices", () => {
    // Catches a regression where the parser caches the most recent
    // content_block_delta.index across subsequent citation events
    // instead of reading it fresh for each.
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = [
      ...parse(adapter, {
        type: "content_block_delta",
        index: 1,
        delta: {
          type: "citations_delta",
          citation: {
            type: "web_search_result_location",
            cited_text: "First citation.",
            url: "https://example.com/a",
            title: "A",
            encrypted_index: "EA==",
          },
        },
      }),
      ...parse(adapter, {
        type: "content_block_delta",
        index: 5,
        delta: {
          type: "citations_delta",
          citation: {
            type: "web_search_result_location",
            cited_text: "Second citation.",
            url: "https://example.com/b",
            title: "B",
            encrypted_index: "EB==",
          },
        },
      }),
    ];
    const citations = events.filter(
      (ev): ev is Extract<InferenceEvent, { type: "inference.citation" }> =>
        ev.type === "inference.citation",
    );
    expect(citations).toHaveLength(2);
    expect(citations[0]?.data.index).toBe(1);
    expect(citations[1]?.data.index).toBe(5);
  });

  test("page_location maps to location.kind=page with start/end page numbers", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_delta",
      index: 0,
      delta: {
        type: "citations_delta",
        citation: {
          type: "page_location",
          cited_text: "Excerpt from page 4.",
          document_index: 0,
          document_title: "Quarterly Report",
          start_page_number: 4,
          end_page_number: 5,
        },
      },
    });
    const ev = pickFirstCitation(events);
    expect(ev.data.citation.citedText).toBe("Excerpt from page 4.");
    expect(ev.data.citation.source.title).toBe("Quarterly Report");
    expect(ev.data.citation.source.documentRef).toEqual({ index: 0 });
    expect(ev.data.citation.location).toEqual({
      kind: "page",
      start: 4,
      end: 5,
    });
  });

  test("char_location maps to location.kind=char with character offsets", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_delta",
      index: 1,
      delta: {
        type: "citations_delta",
        citation: {
          type: "char_location",
          cited_text: "Inline quote.",
          document_index: 2,
          document_title: "Spec",
          start_char_index: 1024,
          end_char_index: 1037,
        },
      },
    });
    const ev = pickFirstCitation(events);
    expect(ev.data.citation.source.documentRef).toEqual({ index: 2 });
    expect(ev.data.citation.location).toEqual({
      kind: "char",
      start: 1024,
      end: 1037,
    });
  });

  test("content_block_location maps to location.kind=content-block", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events = parse(adapter, {
      type: "content_block_delta",
      index: 0,
      delta: {
        type: "citations_delta",
        citation: {
          type: "content_block_location",
          cited_text: "Block-indexed quote.",
          document_index: 1,
          document_title: "Structured doc",
          start_block_index: 7,
          end_block_index: 8,
        },
      },
    });
    const ev = pickFirstCitation(events);
    expect(ev.data.citation.source.documentRef).toEqual({ index: 1 });
    expect(ev.data.citation.location).toEqual({
      kind: "content-block",
      start: 7,
      end: 8,
    });
  });

  test("unrecognized citation variant throws ProtocolMismatchError naming the variant", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    let thrown: unknown;
    try {
      parse(adapter, {
        type: "content_block_delta",
        index: 0,
        delta: {
          type: "citations_delta",
          citation: {
            type: "future_unknown_location",
            cited_text: "x",
          },
        },
      });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ProtocolMismatchError);
    if (thrown instanceof ProtocolMismatchError) {
      expect(thrown.message).toMatch(/unrecognized citation variant/);
      expect(thrown.message).toMatch(/future_unknown_location/);
    }
  });

  test("citations_delta missing citation payload throws ProtocolMismatchError", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    let thrown: unknown;
    try {
      parse(adapter, {
        type: "content_block_delta",
        index: 0,
        delta: { type: "citations_delta" },
      });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ProtocolMismatchError);
    if (thrown instanceof ProtocolMismatchError) {
      expect(thrown.message).toMatch(/missing citation payload/);
    }
  });

  test("citation missing cited_text throws ProtocolMismatchError", () => {
    // CitationBlock.citedText is required ("Both providers emit it;
    // required for inspection and for fallback offset reconstruction"
    // — runtime.ts). Surfacing a missing wire field as a thrown error
    // is the load-bearing alternative to coalescing to an empty
    // string and silently emitting a content-free citation.
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    let thrown: unknown;
    try {
      parse(adapter, {
        type: "content_block_delta",
        index: 0,
        delta: {
          type: "citations_delta",
          citation: {
            type: "web_search_result_location",
            url: "https://example.com/",
            title: "Title",
          },
        },
      });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ProtocolMismatchError);
    if (thrown instanceof ProtocolMismatchError) {
      expect(thrown.message).toMatch(/cited_text/);
      expect(thrown.message).toMatch(/block 0/);
    }
  });

  test("citations interleave with text deltas preserving arrival order", () => {
    // Anthropic streams citations attached to the most recent text
    // run as `citations_delta` events interleaved with subsequent
    // `text_delta`s. Downstream consumers building citation-aware UI
    // re-attach each citation to the text region preceding it, so
    // the parser-emitted event stream must preserve the wire order
    // exactly. Feed a mixed sequence and assert the emitted events
    // come out in the same order they went in.
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const events: InferenceEvent[] = [];
    events.push(
      ...parse(adapter, {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: "First claim. " },
      }),
    );
    events.push(
      ...parse(adapter, {
        type: "content_block_delta",
        index: 0,
        delta: {
          type: "citations_delta",
          citation: {
            type: "web_search_result_location",
            cited_text: "supporting quote one",
            url: "https://example.com/one",
            title: "Source One",
          },
        },
      }),
    );
    events.push(
      ...parse(adapter, {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: "Second claim. " },
      }),
    );
    events.push(
      ...parse(adapter, {
        type: "content_block_delta",
        index: 0,
        delta: {
          type: "citations_delta",
          citation: {
            type: "web_search_result_location",
            cited_text: "supporting quote two",
            url: "https://example.com/two",
            title: "Source Two",
          },
        },
      }),
    );

    const types = events.map((e) => e.type);
    expect(types).toEqual([
      "inference.text.delta",
      "inference.citation",
      "inference.text.delta",
      "inference.citation",
    ]);
    // Verify each citation's citedText so the alternation isn't just
    // type-correct but content-accurate.
    const citations = events.filter((e) => e.type === "inference.citation");
    expect(citations).toHaveLength(2);
    const first = citations[0];
    const second = citations[1];
    if (
      first?.type !== "inference.citation" ||
      second?.type !== "inference.citation"
    ) {
      throw new Error("expected two citation events");
    }
    expect(first.data.citation.citedText).toBe("supporting quote one");
    expect(second.data.citation.citedText).toBe("supporting quote two");
  });

  test("page_location missing start_page_number throws ProtocolMismatchError", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    let thrown: unknown;
    try {
      parse(adapter, {
        type: "content_block_delta",
        index: 0,
        delta: {
          type: "citations_delta",
          citation: {
            type: "page_location",
            cited_text: "Quote",
            document_index: 0,
            end_page_number: 5,
          },
        },
      });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ProtocolMismatchError);
    if (thrown instanceof ProtocolMismatchError) {
      expect(thrown.message).toMatch(/start_page_number/);
    }
  });
});

describe("Anthropic adapter — responseFormat boundary", () => {
  const conversation: ConversationTurn[] = [
    {
      role: "user",
      content: [{ type: "text", text: "Extract structured fields." }],
      timestamp: 1000,
    },
  ];

  test("omitted responseFormat builds a request without throwing", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const req = adapter.buildRequest(conversation, "claude-sonnet-5", {});
    expect(req.url).toBe("/v1/messages");
  });

  test("responseFormat.kind=text builds a request without throwing", () => {
    // Free-form text is Anthropic's default; the option is a no-op
    // here rather than a throw so the cross-provider call site can
    // pass `{ kind: "text" }` uniformly without conditional logic.
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const req = adapter.buildRequest(conversation, "claude-sonnet-5", {
      responseFormat: { kind: "text" },
    });
    expect(req.url).toBe("/v1/messages");
  });

  test("responseFormat.kind=json throws at the marshaling boundary", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    expect(() =>
      adapter.buildRequest(conversation, "claude-sonnet-5", {
        responseFormat: { kind: "json" },
      }),
    ).toThrow(/does not support structured outputs/);
  });

  test("responseFormat.kind=json-schema throws and names the kind", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    expect(() =>
      adapter.buildRequest(conversation, "claude-sonnet-5", {
        responseFormat: {
          kind: "json-schema",
          name: "user",
          schema: { type: "object" },
        },
      }),
    ).toThrow(/json-schema/);
  });
});

describe("Anthropic adapter — tool-name codec round-trip", () => {
  const PREFIXED = "@intx/tools-posix/sidecar-bundle:run_shell";
  const ToolsBody = type({ tools: type({ name: "string" }).array() });

  function wireToolName(body: string): string {
    const parsed = ToolsBody(JSON.parse(body));
    if (parsed instanceof type.errors) {
      throw new Error(`unexpected request body shape: ${parsed.summary}`);
    }
    const name = parsed.tools[0]?.name;
    if (name === undefined) throw new Error("request body carried no tool");
    return name;
  }

  test("buildRequest encodes a package-qualified tool name to the wire charset", () => {
    const req = createAnthropicAdapter(TEST_SOURCE).buildRequest([], "claude", {
      maxTokens: 100,
      tools: [
        { name: PREFIXED, description: "run a shell command", inputSchema: {} },
      ],
    });
    expect(wireToolName(req.body)).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(req.body).not.toContain(PREFIXED);
  });

  test("a tool_use start echoing the encoded name decodes back to the prefixed name", () => {
    const adapter = createAnthropicAdapter(TEST_SOURCE);
    const req = adapter.buildRequest([], "claude", {
      maxTokens: 100,
      tools: [
        { name: PREFIXED, description: "run a shell command", inputSchema: {} },
      ],
    });
    const events = parse(adapter, {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "tool_use",
        id: "toolu_1",
        name: wireToolName(req.body),
        input: {},
      },
    });
    expect(pickFirstToolCallStart(events).data.name).toBe(PREFIXED);
  });
});

const JSON_SOURCE: InferenceSource = {
  id: "anthropic:claude-test",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "test",
  model: "claude-test",
};

// Drives a response body through the real harness accumulator and returns the
// assembled assistant turn plus every emitted event. Asserting on the decoded
// turn (rather than the raw event array) is deliberate: the accumulator
// silently drops unmodeled event types, ignores tool-call deltas with no open
// call, and swallows argument-parse failures, so only the turn reflects
// whether the parser actually decoded correctly. The content-type selects the
// harness decode path (JSON body vs SSE stream), which lets one helper drive
// both parseJSONResponse and parseResponse for cross-path parity.
async function driveTurn(
  body: string,
  contentType = "application/json",
): Promise<{
  turn: AssistantTurn | undefined;
  events: InferenceEvent[];
}> {
  const deps: Dependencies = {
    fetch: () =>
      Promise.resolve(
        new Response(body, {
          status: 200,
          headers: { "content-type": contentType },
        }),
      ),
    scheduler: createDefaultScheduler(),
    adapters: createBuiltinRegistry(),
  };
  let seq = 0;
  const events: InferenceEvent[] = [];
  for await (const ev of runInference({
    turns: [
      { role: "user", content: [{ type: "text", text: "hi" }], timestamp: 0 },
    ],
    source: JSON_SOURCE,
    nextSeq: () => ++seq,
    deps,
    readMaterial: () => ({ secret: "test-secret" }),
  })) {
    events.push(ev);
  }
  const done = events.find(
    (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
      e.type === "inference.done",
  );
  return { turn: done?.data.turn, events };
}

function blocksOfType<T extends ContentBlock["type"]>(
  turn: AssistantTurn,
  blockType: T,
): Extract<ContentBlock, { type: T }>[] {
  return turn.content.filter(
    (b): b is Extract<ContentBlock, { type: T }> => b.type === blockType,
  );
}

function requireTurn(turn: AssistantTurn | undefined): AssistantTurn {
  if (turn === undefined) throw new Error("expected an inference.done turn");
  return turn;
}

describe("createAnthropicAdapter — parseJSONResponse (non-streaming)", () => {
  test("decodes a plain-text message into a text block and usage", async () => {
    const body = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [{ type: "text", text: "The capital of France is Paris." }],
      stop_reason: "end_turn",
      usage: { input_tokens: 20, output_tokens: 10 },
    });
    const { turn, events } = await driveTurn(body);
    const t = requireTurn(turn);
    expect(blocksOfType(t, "text").map((b) => b.text)).toEqual([
      "The capital of France is Paris.",
    ]);
    const done = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    expect(done?.data.usage.input).toBe(20);
    expect(done?.data.usage.output).toBe(10);
  });

  test("decodes a tool_use block into a tool call with parsed arguments", async () => {
    const body = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [
        {
          type: "tool_use",
          id: "toolu_1",
          name: "get_weather",
          input: { location: "Boston, MA" },
        },
      ],
      stop_reason: "tool_use",
      usage: { input_tokens: 5, output_tokens: 3 },
    });
    const t = requireTurn((await driveTurn(body)).turn);
    const calls = blocksOfType(t, "tool_call");
    expect(calls).toHaveLength(1);
    const call = calls[0];
    if (call === undefined) throw new Error("expected a tool call");
    expect(call.name).toBe("get_weather");
    expect(call.id).toBe("toolu_1");
    expect(call.arguments).toEqual({ location: "Boston, MA" });
  });

  test("decodes a thinking block with its signature", async () => {
    const body = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [
        {
          type: "thinking",
          thinking: "Let me consider.",
          signature: "sig-abc",
        },
        { type: "text", text: "Answer." },
      ],
      stop_reason: "end_turn",
      usage: { input_tokens: 5, output_tokens: 3 },
    });
    const t = requireTurn((await driveTurn(body)).turn);
    const thinking = blocksOfType(t, "thinking");
    expect(thinking).toHaveLength(1);
    const block = thinking[0];
    if (block === undefined) throw new Error("expected a thinking block");
    expect(block.thinking).toBe("Let me consider.");
    expect(block.signature).toBe("sig-abc");
  });

  test("decodes a redacted_thinking block, preserving its opaque data", async () => {
    const body = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [{ type: "redacted_thinking", data: "opaque-blob" }],
      stop_reason: "end_turn",
      usage: { input_tokens: 5, output_tokens: 3 },
    });
    const t = requireTurn((await driveTurn(body)).turn);
    expect(blocksOfType(t, "redacted_thinking").map((b) => b.data)).toEqual([
      "opaque-blob",
    ]);
  });

  test("emits citations from a text block's inline citations", async () => {
    const body = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [
        {
          type: "text",
          text: "Grounded answer.",
          citations: [
            {
              type: "web_search_result_location",
              cited_text: "the source text",
              url: "https://example.com/a",
              title: "Example A",
            },
          ],
        },
      ],
      stop_reason: "end_turn",
      usage: { input_tokens: 5, output_tokens: 3 },
    });
    const t = requireTurn((await driveTurn(body)).turn);
    const citations = blocksOfType(t, "citation");
    expect(citations).toHaveLength(1);
    const citation = citations[0];
    if (citation === undefined) throw new Error("expected a citation block");
    expect(citation.citedText).toBe("the source text");
    expect(citation.source.uri).toBe("https://example.com/a");
  });

  test("ignores unmodeled server-tool blocks without failing the turn", async () => {
    const body = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [
        { type: "server_tool_use", id: "srv_1", name: "web_search", input: {} },
        { type: "text", text: "Result." },
      ],
      stop_reason: "end_turn",
      usage: { input_tokens: 5, output_tokens: 3 },
    });
    const { turn, events } = await driveTurn(body);
    const t = requireTurn(turn);
    expect(blocksOfType(t, "text").map((b) => b.text)).toEqual(["Result."]);
    expect(events.some((e) => e.type === "inference.error")).toBe(false);
  });

  test("surfaces a protocol mismatch on a non-message body", async () => {
    // A streaming SSE event object (not a whole message) must not decode as a
    // non-streaming response.
    const body = JSON.stringify({ type: "message_start", message: {} });
    const { events } = await driveTurn(body);
    const error = events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.error" }> =>
        e.type === "inference.error",
    );
    if (error === undefined) throw new Error("expected inference.error");
    expect(error.data.error.category).toBe("protocol_mismatch");
  });
});

function sse(events: object[]): string {
  return events.map((e) => `event: x\ndata: ${JSON.stringify(e)}\n\n`).join("");
}

describe("createAnthropicAdapter — streaming vs non-streaming parity", () => {
  // The whole point of parseJSONResponse is that a replayed non-streaming
  // capture decodes to the same turn its streaming sibling would. Drive one
  // logically-equivalent multi-block response through both harness decode
  // paths and assert the assembled turn and final usage are identical, so a
  // future edit that drifts one path's index/callId/usage/citation semantics
  // from the other cannot pass in isolation.
  test("a multi-block turn decodes identically through both paths", async () => {
    const jsonBody = JSON.stringify({
      type: "message",
      role: "assistant",
      model: "claude-test",
      content: [
        {
          type: "text",
          text: "Grounded.",
          citations: [
            {
              type: "web_search_result_location",
              cited_text: "src",
              url: "https://example.com/a",
              title: "A",
            },
          ],
        },
        { type: "thinking", thinking: "hmm", signature: "sig-1" },
        { type: "redacted_thinking", data: "blob" },
        {
          type: "tool_use",
          id: "toolu_1",
          name: "get_weather",
          input: { location: "Boston" },
        },
      ],
      stop_reason: "tool_use",
      usage: { input_tokens: 20, output_tokens: 10 },
    });

    const streamBody = sse([
      {
        type: "message_start",
        message: { usage: { input_tokens: 20, output_tokens: 0 } },
      },
      {
        type: "content_block_start",
        index: 0,
        content_block: { type: "text" },
      },
      {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: "Grounded." },
      },
      {
        type: "content_block_delta",
        index: 0,
        delta: {
          type: "citations_delta",
          citation: {
            type: "web_search_result_location",
            cited_text: "src",
            url: "https://example.com/a",
            title: "A",
          },
        },
      },
      { type: "content_block_stop", index: 0 },
      {
        type: "content_block_start",
        index: 1,
        content_block: { type: "thinking" },
      },
      {
        type: "content_block_delta",
        index: 1,
        delta: { type: "thinking_delta", thinking: "hmm" },
      },
      {
        type: "content_block_delta",
        index: 1,
        delta: { type: "signature_delta", signature: "sig-1" },
      },
      { type: "content_block_stop", index: 1 },
      {
        type: "content_block_start",
        index: 2,
        content_block: { type: "redacted_thinking", data: "blob" },
      },
      { type: "content_block_stop", index: 2 },
      {
        type: "content_block_start",
        index: 3,
        content_block: { type: "tool_use", id: "toolu_1", name: "get_weather" },
      },
      {
        type: "content_block_delta",
        index: 3,
        delta: {
          type: "input_json_delta",
          partial_json: '{"location":"Boston"}',
        },
      },
      { type: "content_block_stop", index: 3 },
      { type: "message_delta", usage: { output_tokens: 10 } },
      { type: "message_stop" },
    ]);

    const jsonResult = await driveTurn(jsonBody, "application/json");
    const streamResult = await driveTurn(streamBody, "text/event-stream");

    expect(jsonResult.events.some((e) => e.type === "inference.error")).toBe(
      false,
    );
    expect(streamResult.events.some((e) => e.type === "inference.error")).toBe(
      false,
    );

    const jt = requireTurn(jsonResult.turn);
    const st = requireTurn(streamResult.turn);
    expect(jt.content).toEqual(st.content);

    const jdone = jsonResult.events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    const sdone = streamResult.events.find(
      (e): e is Extract<InferenceEvent, { type: "inference.done" }> =>
        e.type === "inference.done",
    );
    expect(jdone?.data.usage).toEqual(sdone?.data.usage);
  });
});
