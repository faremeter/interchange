import { describe, expect, test } from "bun:test";

import {
  TERMINAL_RUN_EVENT_TYPES,
  findAwaitingSignal,
  findAwaitingSignals,
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

describe("findAwaitingSignals", () => {
  test("returns two live correlations of the same name", () => {
    const events = [
      event(1, "SignalAwaited", {
        signalName: "go",
        stepId: "left",
        correlationId: "corr-a",
      }),
      event(2, "SignalAwaited", {
        signalName: "go",
        stepId: "right",
        correlationId: "corr-b",
      }),
    ];
    expect(findAwaitingSignals(events)).toEqual([
      { seq: 1, signalName: "go", stepId: "left", correlationId: "corr-a" },
      { seq: 2, signalName: "go", stepId: "right", correlationId: "corr-b" },
    ]);
  });

  test("an abandon of one correlation leaves the other", () => {
    const events = [
      event(1, "SignalAwaited", {
        signalName: "go",
        stepId: "left",
        correlationId: "corr-a",
      }),
      event(2, "SignalAwaited", {
        signalName: "go",
        stepId: "right",
        correlationId: "corr-b",
      }),
      event(3, "SignalAwaitAbandoned", {
        signalName: "go",
        correlationId: "corr-a",
      }),
    ];
    expect(findAwaitingSignals(events)).toEqual([
      { seq: 2, signalName: "go", stepId: "right", correlationId: "corr-b" },
    ]);
  });

  test("omits a reserved __signal__: park", () => {
    const events = [
      event(1, "SignalAwaited", { signalName: "__signal__:corr-1" }),
      event(2, "SignalAwaited", { signalName: "go", stepId: "gate" }),
    ];
    expect(findAwaitingSignals(events)).toEqual([
      { seq: 2, signalName: "go", stepId: "gate" },
    ]);
  });

  test("a name-only receive of go leaves the correlated go parked", () => {
    const events = [
      event(1, "SignalAwaited", { signalName: "go", stepId: "gate" }),
      event(2, "SignalAwaited", {
        signalName: "go",
        stepId: "section",
        correlationId: "corr-1",
      }),
      event(3, "SignalReceived", { signalName: "go", signalId: "sig-name" }),
    ];
    expect(findAwaitingSignals(events)).toEqual([
      {
        seq: 2,
        signalName: "go",
        stepId: "section",
        correlationId: "corr-1",
      },
    ]);
  });

  test("a name-only abandon clears the name-only park and leaves the correlation", () => {
    const events = [
      event(1, "SignalAwaited", { signalName: "go", stepId: "gate" }),
      event(2, "SignalAwaited", {
        signalName: "go",
        stepId: "section",
        correlationId: "corr-1",
      }),
      event(3, "SignalAwaitAbandoned", { signalName: "go" }),
    ];
    expect(findAwaitingSignals(events)).toEqual([
      {
        seq: 2,
        signalName: "go",
        stepId: "section",
        correlationId: "corr-1",
      },
    ]);
  });

  test("a correlated receive leaves the name-only park", () => {
    const events = [
      event(1, "SignalAwaited", { signalName: "go", stepId: "gate" }),
      event(2, "SignalAwaited", {
        signalName: "go",
        stepId: "section",
        correlationId: "corr-1",
      }),
      event(3, "SignalReceived", {
        signalName: "go",
        signalId: "sig-1",
        correlationId: "corr-1",
      }),
    ];
    expect(findAwaitingSignals(events)).toEqual([
      { seq: 1, signalName: "go", stepId: "gate" },
    ]);
  });

  test("a delivery that arrived first is not offered after the gate completes", () => {
    const early = [
      event(1, "SignalReceived", { signalName: "go", signalId: "sig-early" }),
      event(2, "SignalAwaited", { signalName: "go", stepId: "gate" }),
    ];
    const events = [
      ...early,
      event(3, "StepCompleted", { stepId: "gate" }),
      event(4, "SignalAwaited", {
        signalName: "approve",
        stepId: "later",
        correlationId: "corr-1",
      }),
    ];
    expect(findAwaitingSignals(early)).toEqual([]);
    expect(findAwaitingSignals(events)).toEqual([
      {
        seq: 4,
        signalName: "approve",
        stepId: "later",
        correlationId: "corr-1",
      },
    ]);
  });

  test("a gate that times out is not offered beside the next park", () => {
    const fired = [
      event(1, "SignalAwaited", { signalName: "go", stepId: "gate" }),
      event(2, "TimerSet", {
        timerId: "timer-1",
        stepId: "gate",
        fireAt: "2026-01-01T00:00:00.000Z",
      }),
      event(3, "TimerFired", { timerId: "timer-1" }),
    ];
    const events = [
      ...fired,
      event(4, "StepCompleted", { stepId: "gate" }),
      event(5, "SignalAwaited", {
        signalName: "approve",
        stepId: "later",
        correlationId: "corr-1",
      }),
    ];
    expect(findAwaitingSignals(fired)).toEqual([]);
    expect(findAwaitingSignals(events)).toEqual([
      {
        seq: 5,
        signalName: "approve",
        stepId: "later",
        correlationId: "corr-1",
      },
    ]);
  });

  test.each(["StepFailed", "CancelPropagated"])(
    "%s retires the gate and leaves the other park",
    (terminalStep) => {
      const events = [
        event(1, "SignalAwaited", { signalName: "go", stepId: "gate" }),
        event(2, "SignalAwaited", {
          signalName: "approve",
          stepId: "later",
          correlationId: "corr-1",
        }),
        event(3, terminalStep, { stepId: "gate" }),
      ];
      expect(findAwaitingSignals(events)).toEqual([
        {
          seq: 2,
          signalName: "approve",
          stepId: "later",
          correlationId: "corr-1",
        },
      ]);
    },
  );
});
