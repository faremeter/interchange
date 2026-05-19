import { describe, test, expect } from "bun:test";

import type { InferenceEvent, PartialMessage } from "@intx/types/runtime";

import { expectEvents, expectToolCall, expectToolCalls } from "./matchers";

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
