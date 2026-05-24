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
