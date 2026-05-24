import { describe, test, expect } from "bun:test";

import type { InferenceEvent, PartialMessage } from "@intx/types/runtime";

import {
  expectEvents,
  expectMediaBlock,
  expectToolCall,
  expectToolCalls,
  type MediaBlock,
} from "./matchers";

const EMPTY_PARTIAL: PartialMessage = { text: "" };

function startEvent(seq: number): InferenceEvent {
  return {
    type: "inference.start",
    seq,
    data: { model: "claude-test" },
  };
}

function textDeltaEvent(seq: number, token: string): InferenceEvent {
  return {
    type: "inference.text.delta",
    seq,
    data: { token, partial: EMPTY_PARTIAL },
  };
}

function toolCallEndEvent(
  seq: number,
  callId: string,
  name: string,
  args: Record<string, unknown>,
): InferenceEvent {
  return {
    type: "inference.tool_call.end",
    seq,
    data: { callId, name, arguments: args, partial: EMPTY_PARTIAL },
  };
}

function doneEvent(seq: number): InferenceEvent {
  return {
    type: "inference.done",
    seq,
    data: {
      turn: {
        role: "assistant",
        content: [{ type: "text", text: "" }],
        model: "claude-test",
        timestamp: 0,
      },
      usage: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      },
    },
  };
}

describe("expectEvents.toMatchSequence", () => {
  test("matches an exact sequence", () => {
    const events: InferenceEvent[] = [
      startEvent(0),
      textDeltaEvent(1, "Hello"),
      doneEvent(2),
    ];
    expectEvents(events).toMatchSequence([
      { type: "inference.start" },
      { type: "inference.text.delta", data: { token: "Hello" } },
      { type: "inference.done" },
    ]);
  });

  test("allows gaps between expected entries", () => {
    const events: InferenceEvent[] = [
      startEvent(0),
      textDeltaEvent(1, "Hello "),
      textDeltaEvent(2, "world"),
      doneEvent(3),
    ];
    expectEvents(events).toMatchSequence([
      { type: "inference.start" },
      { type: "inference.done" },
    ]);
  });

  test("matches a partial event with nested data", () => {
    const events: InferenceEvent[] = [
      textDeltaEvent(0, "Hello"),
      textDeltaEvent(1, "World"),
    ];
    expectEvents(events).toMatchSequence([
      { type: "inference.text.delta", data: { token: "World" } },
    ]);
  });

  test("throws when no matching event exists", () => {
    const events: InferenceEvent[] = [startEvent(0), doneEvent(1)];
    expect(() =>
      expectEvents(events).toMatchSequence([{ type: "inference.text.delta" }]),
    ).toThrow(/no event matching/);
  });

  test("throws when ordering does not match", () => {
    const events: InferenceEvent[] = [doneEvent(0), startEvent(1)];
    expect(() =>
      expectEvents(events).toMatchSequence([
        { type: "inference.start" },
        { type: "inference.done" },
      ]),
    ).toThrow(/no event matching/);
  });

  test("is chainable", () => {
    const events: InferenceEvent[] = [
      startEvent(0),
      textDeltaEvent(1, "a"),
      textDeltaEvent(2, "b"),
      doneEvent(3),
    ];
    expectEvents(events)
      .toMatchSequence([{ type: "inference.start" }])
      .toMatchSequence([{ type: "inference.done" }]);
  });
});

describe("expectToolCalls.toInclude", () => {
  test("matches when the named tool call appears", () => {
    const events: InferenceEvent[] = [
      toolCallEndEvent(0, "call_a", "search", { q: "x" }),
      toolCallEndEvent(1, "call_b", "calc", { a: 1, b: 2 }),
    ];
    expectToolCalls(events).toInclude({ name: "calc" });
  });

  test("matches arguments partially", () => {
    const events: InferenceEvent[] = [
      toolCallEndEvent(0, "call_a", "search", { q: "x", extra: 7 }),
    ];
    expectToolCalls(events).toInclude({
      name: "search",
      arguments: { q: "x" },
    });
  });

  test("matches callId when supplied", () => {
    const events: InferenceEvent[] = [
      toolCallEndEvent(0, "call_a", "search", { q: "x" }),
    ];
    expectToolCalls(events).toInclude({
      name: "search",
      callId: "call_a",
    });
  });

  test("throws when no matching tool call is found", () => {
    const events: InferenceEvent[] = [
      toolCallEndEvent(0, "call_a", "search", { q: "x" }),
    ];
    expect(() =>
      expectToolCalls(events).toInclude({ name: "missing" }),
    ).toThrow(/no tool call matching/);
  });

  test("throws when arguments differ", () => {
    const events: InferenceEvent[] = [
      toolCallEndEvent(0, "call_a", "search", { q: "x" }),
    ];
    expect(() =>
      expectToolCalls(events).toInclude({
        name: "search",
        arguments: { q: "y" },
      }),
    ).toThrow(/no tool call matching/);
  });
});

describe("expectToolCall(name).from(events).toHaveBeenCalledTimes", () => {
  test("counts occurrences of the named tool", () => {
    const events: InferenceEvent[] = [
      toolCallEndEvent(0, "a", "search", { q: "x" }),
      toolCallEndEvent(1, "b", "search", { q: "y" }),
      toolCallEndEvent(2, "c", "calc", { a: 1 }),
    ];
    expectToolCall("search").from(events).toHaveBeenCalledTimes(2);
    expectToolCall("calc").from(events).toHaveBeenCalledTimes(1);
    expectToolCall("missing").from(events).toHaveBeenCalledTimes(0);
  });

  test("throws on mismatched count", () => {
    const events: InferenceEvent[] = [
      toolCallEndEvent(0, "a", "search", { q: "x" }),
    ];
    expect(() =>
      expectToolCall("search").from(events).toHaveBeenCalledTimes(2),
    ).toThrow(/observed 1/);
  });

  test("is chainable", () => {
    const events: InferenceEvent[] = [
      toolCallEndEvent(0, "a", "search", { q: "x" }),
    ];
    expectToolCall("search")
      .from(events)
      .toHaveBeenCalledTimes(1)
      .toHaveBeenCalledTimes(1);
  });
});

// A tiny base64 blob (~64 chars → ~48 decoded bytes); the contents are
// realistic-looking but small, so a failed assertion that accidentally
// leaks the data is easy to spot.
const SHORT_B64 = "aGVsbG8gd29ybGQ=";
// A larger base64 string (1024 chars → 768 decoded bytes). 1024 is a
// multiple of 4 with no padding required, so the formula gives an exact
// decoded count and the fixture is well-formed.
const LONG_B64 = "A".repeat(1024);

function baseBlock(
  type: MediaBlock["type"],
  mimeType: string,
  data: string,
): MediaBlock {
  return { type, source: { kind: "base64", mimeType, data } };
}

function referenceBlock(
  type: MediaBlock["type"],
  mimeType: string,
  reference: string,
): MediaBlock {
  return { type, source: { kind: "file-reference", mimeType, reference } };
}

describe("expectMediaBlock — happy paths", () => {
  test.each(["image", "audio", "video", "document"] as const)(
    "accepts a %s base64 block matching mimeType and byte length",
    (type) => {
      const block = baseBlock(type, "application/octet-stream", SHORT_B64);
      expectMediaBlock(block, { source: "base64" })
        .toHaveMimeType("application/octet-stream")
        .toHaveByteLengthAtLeast(5)
        .toHaveByteLength(11); // "hello world" = 11 bytes
    },
  );

  test.each(["image", "audio", "video", "document"] as const)(
    "accepts a %s file-reference block matching mimeType",
    (type) => {
      const block = referenceBlock(type, "image/png", "file_abc");
      expectMediaBlock(block, { source: "file-reference" }).toHaveMimeType(
        "image/png",
      );
    },
  );

  test("is chainable across all assertions", () => {
    const block = baseBlock("image", "image/png", SHORT_B64);
    expectMediaBlock(block)
      .toHaveMimeType("image/png")
      .toHaveByteLengthAtLeast(1)
      .toHaveByteLengthAtLeast(5);
  });

  test("chain returns the same assertion object every step", () => {
    // The chain identity matters because each terminal method returns
    // `this`; if any returned a fresh assertion, callers could miss
    // state-carrying behavior in future extensions of the matcher.
    const block = baseBlock("image", "image/png", SHORT_B64);
    const a = expectMediaBlock(block);
    const b = a.toHaveMimeType("image/png");
    const c = b.toHaveByteLengthAtLeast(1);
    expect(a).toBe(b);
    expect(b).toBe(c);
  });

  test("base64ByteLength throws on input whose length is not a multiple of 4", () => {
    // "===" is three padding chars — neither a valid base64 nor a sane
    // input to the byte-length helper. Surfacing it as a thrown error
    // beats returning a negative byte count that callers then assert
    // against without realising they're operating on garbage.
    const block = baseBlock("image", "image/png", "===");
    expect(() => expectMediaBlock(block).toHaveByteLength(0)).toThrow(
      /not a multiple of 4/,
    );
  });
});

describe("expectMediaBlock — failure paths", () => {
  test("rejects on wrong source kind via opts", () => {
    const block = baseBlock("image", "image/png", SHORT_B64);
    expect(() => expectMediaBlock(block, { source: "file-reference" })).toThrow(
      /expected source=file-reference/,
    );
  });

  test("rejects on mismatched mimeType", () => {
    const block = baseBlock("image", "image/png", SHORT_B64);
    expect(() => expectMediaBlock(block).toHaveMimeType("image/jpeg")).toThrow(
      /toHaveMimeType/,
    );
  });

  test("rejects byte-length assertions on file-reference sources", () => {
    const block = referenceBlock("image", "image/png", "file_abc");
    expect(() => expectMediaBlock(block).toHaveByteLength(100)).toThrow(
      /not observable on file-reference sources/,
    );
    expect(() => expectMediaBlock(block).toHaveByteLengthAtLeast(100)).toThrow(
      /not observable on file-reference sources/,
    );
  });

  test("rejects when actual byte length is below minimum", () => {
    const block = baseBlock("audio", "audio/wav", SHORT_B64);
    expect(() =>
      expectMediaBlock(block).toHaveByteLengthAtLeast(1_000_000),
    ).toThrow(/toHaveByteLengthAtLeast/);
  });

  test("rejects when exact byte length mismatches", () => {
    const block = baseBlock("audio", "audio/wav", SHORT_B64);
    expect(() => expectMediaBlock(block).toHaveByteLength(99)).toThrow(
      /toHaveByteLength/,
    );
  });
});

describe("expectMediaBlock — failure messages never leak base64 payload", () => {
  // This is the load-bearing property of the helper. A failing assertion
  // on a megabyte-sized image must NOT dump the raw payload into test
  // logs; the elided format is what callers debug against.

  function captureError(fn: () => void): string {
    try {
      fn();
    } catch (e) {
      return e instanceof Error ? e.message : String(e);
    }
    throw new Error("expected the assertion to throw");
  }

  test("mismatched mimeType error elides the payload", () => {
    const block = baseBlock("image", "image/png", LONG_B64);
    const msg = captureError(() =>
      expectMediaBlock(block).toHaveMimeType("image/jpeg"),
    );
    expect(msg).not.toContain(LONG_B64);
    expect(msg).toContain("bytes=");
    expect(msg).toContain("source=base64");
  });

  test("byte-length-below-minimum error elides the payload", () => {
    const block = baseBlock("video", "video/mp4", LONG_B64);
    const msg = captureError(() =>
      expectMediaBlock(block).toHaveByteLengthAtLeast(10_000_000),
    );
    expect(msg).not.toContain(LONG_B64);
    expect(msg).toContain("bytes=");
  });

  test("wrong-source-kind error elides the payload", () => {
    const block = baseBlock("document", "application/pdf", LONG_B64);
    const msg = captureError(() =>
      expectMediaBlock(block, { source: "file-reference" }),
    );
    expect(msg).not.toContain(LONG_B64);
    expect(msg).toContain("source=base64");
  });
});
