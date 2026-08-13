import { describe, expect, test } from "bun:test";

import {
  TERMINAL_RUN_EVENT_TYPES,
  findAwaitingSignal,
  isTerminalRunEvents,
} from "./transforms";
import type { WorkflowRunEvent } from "./validators";

function event(
  seq: number,
  type: string,
  body: Record<string, unknown> = {},
): WorkflowRunEvent {
  return { seq, type, body };
}

describe("isTerminalRunEvents", () => {
  test("false for a live run", () => {
    expect(
      isTerminalRunEvents([event(0, "RunStarted"), event(1, "StepStarted")]),
    ).toBe(false);
  });

  test("false for an empty log", () => {
    expect(isTerminalRunEvents([])).toBe(false);
  });

  test.each([...TERMINAL_RUN_EVENT_TYPES])(
    "true once %s is committed",
    (terminalType) => {
      expect(
        isTerminalRunEvents([event(0, "RunStarted"), event(1, terminalType)]),
      ).toBe(true);
    },
  );
});

describe("findAwaitingSignal", () => {
  test("returns null when the run never awaits", () => {
    expect(
      findAwaitingSignal([event(0, "RunStarted"), event(1, "StepStarted")]),
    ).toBeNull();
  });

  test("returns the awaited signal while unresolved", () => {
    const events = [
      event(0, "RunStarted"),
      event(1, "SignalAwaited", { signalName: "approve" }),
    ];
    expect(findAwaitingSignal(events)).toEqual({
      seq: 1,
      signalName: "approve",
    });
  });

  test("a matching SignalReceived clears the await", () => {
    const events = [
      event(1, "SignalAwaited", { signalName: "approve" }),
      event(2, "SignalReceived", { signalName: "approve" }),
    ];
    expect(findAwaitingSignal(events)).toBeNull();
  });

  test("a different SignalReceived does not clear the await", () => {
    const events = [
      event(1, "SignalAwaited", { signalName: "approve" }),
      event(2, "SignalReceived", { signalName: "other" }),
    ];
    expect(findAwaitingSignal(events)).toEqual({
      seq: 1,
      signalName: "approve",
    });
  });

  test("a terminal event clears the await", () => {
    const events = [
      event(1, "SignalAwaited", { signalName: "approve" }),
      event(2, "RunCompleted"),
    ];
    expect(findAwaitingSignal(events)).toBeNull();
  });

  test("tracks the latest await when a run parks twice", () => {
    const events = [
      event(1, "SignalAwaited", { signalName: "first" }),
      event(2, "SignalReceived", { signalName: "first" }),
      event(3, "SignalAwaited", { signalName: "second" }),
    ];
    expect(findAwaitingSignal(events)).toEqual({
      seq: 3,
      signalName: "second",
    });
  });

  test("throws when SignalAwaited lacks a string signalName", () => {
    expect(() =>
      findAwaitingSignal([event(1, "SignalAwaited", { signalName: 42 })]),
    ).toThrow(/missing a string signalName/);
  });
});
