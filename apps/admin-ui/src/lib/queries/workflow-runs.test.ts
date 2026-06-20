import { describe, expect, test } from "bun:test";

import {
  findAwaitingSignal,
  isTerminalRunEvents,
  type WorkflowRunEvent,
} from "./tenants";

function event(
  seq: number,
  type: string,
  body: Record<string, unknown> = {},
): WorkflowRunEvent {
  return { seq, type, body };
}

describe("isTerminalRunEvents", () => {
  test("a run without a terminal event is not terminal", () => {
    expect(
      isTerminalRunEvents([
        event(0, "RunStarted"),
        event(1, "StepStarted", { stepId: "step1" }),
      ]),
    ).toBe(false);
  });

  test.each(["RunCompleted", "RunFailed", "RunCancelled"])(
    "a run with a %s event is terminal",
    (terminalType) => {
      expect(
        isTerminalRunEvents([event(0, "RunStarted"), event(1, terminalType)]),
      ).toBe(true);
    },
  );
});

describe("findAwaitingSignal", () => {
  test("returns null when no SignalAwaited event is present", () => {
    expect(
      findAwaitingSignal([
        event(0, "RunStarted"),
        event(1, "StepCompleted", { stepId: "step1" }),
      ]),
    ).toBeNull();
  });

  test("surfaces the awaited signal name and seq while parked", () => {
    expect(
      findAwaitingSignal([
        event(0, "RunStarted"),
        event(1, "StepStarted", { stepId: "gate" }),
        event(2, "SignalAwaited", { stepId: "gate", signalName: "approve" }),
      ]),
    ).toEqual({ seq: 2, signalName: "approve" });
  });

  test("clears the await once a matching SignalReceived arrives", () => {
    expect(
      findAwaitingSignal([
        event(0, "SignalAwaited", { signalName: "approve" }),
        event(1, "SignalReceived", { signalName: "approve" }),
      ]),
    ).toBeNull();
  });

  test("a non-matching SignalReceived does not clear the await", () => {
    expect(
      findAwaitingSignal([
        event(0, "SignalAwaited", { signalName: "approve" }),
        event(1, "SignalReceived", { signalName: "other" }),
      ]),
    ).toEqual({ seq: 0, signalName: "approve" });
  });

  test("a terminal event clears the await", () => {
    expect(
      findAwaitingSignal([
        event(0, "SignalAwaited", { signalName: "approve" }),
        event(1, "RunFailed", { error: { message: "boom" } }),
      ]),
    ).toBeNull();
  });

  test("re-arms on a later SignalAwaited after a prior one resolved", () => {
    expect(
      findAwaitingSignal([
        event(0, "SignalAwaited", { signalName: "go" }),
        event(1, "SignalReceived", { signalName: "go" }),
        event(2, "SignalAwaited", { signalName: "approve" }),
      ]),
    ).toEqual({ seq: 2, signalName: "approve" });
  });

  test("throws when a SignalAwaited event lacks a string signalName", () => {
    expect(() =>
      findAwaitingSignal([event(0, "SignalAwaited", { stepId: "gate" })]),
    ).toThrow(/missing a string signalName/);
  });
});
