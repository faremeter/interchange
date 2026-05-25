import { describe, expect, test } from "bun:test";

import type { ConversationTurn, InferenceEvent } from "@intx/types/runtime";

import { ProtocolMismatchError } from "../errors";
import { createAnthropicAdapter } from "./anthropic";

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
): Extract<InferenceEvent, { type: "inference.thinking.signature" }> {
  const ev = events[0];
  if (ev === undefined) throw new Error("expected at least one event");
  if (ev.type !== "inference.thinking.signature") {
    throw new Error(`expected inference.thinking.signature, got ${ev.type}`);
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
    let thrown: unknown;
    try {
      parse(adapter, { type: "content_block_stop" });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ProtocolMismatchError);
  });
});

// All redacted_thinking test fixtures below are SYNTHETIC: derived
// from Anthropic's documented wire shape rather than from a real
// captured response. The fixture corpus carries no captured
// redacted_thinking bytes today because Anthropic's documented canary
// did not trigger the safety classifier on capture day — every
// `redacted-thinking[-streaming]` row landed in the corpus with
// `outcome: "misled"` and contains regular `thinking` blocks instead.
//
// The opaque `data` payload below mimics Anthropic's format
// (long base64-looking string) but carries no real cryptographic
// content. Round-trip tests assert the harness/adapter pass the
// bytes verbatim — the actual contents are irrelevant to the
// invariant being tested.
const SYNTHETIC_REDACTED_DATA =
  "ErUBCkYIBxgCKkABEHk1RmZpaWlsOXJxN0Z6cVB" +
  "QcjBQYS9wQUdBQUFBQUFBQUFBQUFRQUFBQUFBQU" +
  "FBQUFBQUFBQUFBQT09EhJYWXpBOXJxN0Z6cVBQc" +
  "jBQYS9wAAA=";

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
    const adapter = createAnthropicAdapter();
    const events = parse(adapter, {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "redacted_thinking",
        data: SYNTHETIC_REDACTED_DATA,
      },
    });
    expect(events).toHaveLength(1);
    const ev = pickFirstThinkingRedacted(events);
    expect(ev.data.index).toBe(0);
    expect(ev.data.redactedThinking.type).toBe("redacted_thinking");
    expect(ev.data.redactedThinking.data).toBe(SYNTHETIC_REDACTED_DATA);
  });

  test("preserves the data verbatim — no normalization or transformation", () => {
    const adapter = createAnthropicAdapter();
    // The data is an opaque blob; any mutation breaks the round-trip.
    // Use a string with characters that an over-eager normalizer would
    // touch (newlines, whitespace, base64 padding).
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
    const adapter = createAnthropicAdapter();
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
});

describe("Anthropic adapter — redacted_thinking parser-to-builder round-trip", () => {
  // The parser-side wire shape and the request-builder-side wire shape
  // are tested independently elsewhere. This test closes the loop: it
  // proves that the opaque `data` blob the parser surfaces in
  // `inference.thinking.redacted` reconstructs back to a request body
  // that carries the same bytes verbatim. That round-trip is the
  // invariant Anthropic requires on every follow-up turn.
  test("data survives parse → reconstruct → buildRequest unchanged", () => {
    const adapter = createAnthropicAdapter();
    const events = parse(adapter, {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "redacted_thinking",
        data: SYNTHETIC_REDACTED_DATA,
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
    // Use a structural extraction via JSON.parse + cast through unknown
    // because the integration-style assertion lives in the broader
    // tests/inference/providers/anthropic.test.ts and is already
    // exercised.
    const bodyText = req.body;
    expect(bodyText).toContain(SYNTHETIC_REDACTED_DATA);
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
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
    const adapter = createAnthropicAdapter();
    const req = adapter.buildRequest(conversation, "claude-sonnet-4", {});
    expect(req.url).toBe("/v1/messages");
  });

  test("responseFormat.kind=text builds a request without throwing", () => {
    // Free-form text is Anthropic's default; the option is a no-op
    // here rather than a throw so the cross-provider call site can
    // pass `{ kind: "text" }` uniformly without conditional logic.
    const adapter = createAnthropicAdapter();
    const req = adapter.buildRequest(conversation, "claude-sonnet-4", {
      responseFormat: { kind: "text" },
    });
    expect(req.url).toBe("/v1/messages");
  });

  test("responseFormat.kind=json throws at the marshaling boundary", () => {
    const adapter = createAnthropicAdapter();
    expect(() =>
      adapter.buildRequest(conversation, "claude-sonnet-4", {
        responseFormat: { kind: "json" },
      }),
    ).toThrow(/does not support structured outputs/);
  });

  test("responseFormat.kind=json-schema throws and names the kind", () => {
    const adapter = createAnthropicAdapter();
    expect(() =>
      adapter.buildRequest(conversation, "claude-sonnet-4", {
        responseFormat: {
          kind: "json-schema",
          name: "user",
          schema: { type: "object" },
        },
      }),
    ).toThrow(/json-schema/);
  });
});
