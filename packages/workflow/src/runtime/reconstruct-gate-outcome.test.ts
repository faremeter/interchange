// Focused unit tests for `reconstructGateOutcome` -- the reducer-FIFO replay
// that recovers how a single admitted `awaitSignal` gate left `awaiting-signal`
// on resume: which delivered signal it consumed, or that its own timer fired
// first. The resume short-circuit reaches it only on the crash-window log-read
// path, so its bindings are proven here against hand-built logs.
//
// Each log is a COMPLETE run log from seq 1; the gate step id is "gate" and the
// signal name is "go" throughout. The gate is the sole awaiter of the name in
// every log here -- the precondition the short-circuit enforces via
// `hasForeignSameNameAwaiter` before calling the replay.

import { describe, test, expect } from "bun:test";

import { applyEvent, emptyState, type WorkflowEvent } from "@intx/workflow";

// Imported directly from the runtime module: like `boundSignalForContainerAwait`
// the binding is not part of the package's public surface, so it is exercised
// via its source path rather than promoted to the barrel for a test.
import { reconstructGateOutcome } from "./run";

const at = "2026-01-01T00:00:00.000Z";
const GATE = "gate";
const NAME = "go";

function runStarted(seq: number): WorkflowEvent {
  return {
    kind: "RunStarted",
    seq,
    at,
    runId: "gate-run",
    definitionHash: "x",
    trigger: { type: "manual", payload: null },
  };
}

function started(seq: number, stepId: string): WorkflowEvent {
  return {
    kind: "StepStarted",
    seq,
    at,
    stepId,
    attempt: 1,
    input: { ref: "inline:null" },
  };
}

function awaited(seq: number, stepId: string, name = NAME): WorkflowEvent {
  return { kind: "SignalAwaited", seq, at, stepId, signalName: name };
}

function received(
  seq: number,
  signalId: string,
  payload: unknown,
  name = NAME,
): WorkflowEvent {
  return {
    kind: "SignalReceived",
    seq,
    at,
    signalName: name,
    signalId,
    payload,
  };
}

describe("reconstructGateOutcome", () => {
  test("single delivery: binds the one consumed signal (findConsumedSignal parity)", () => {
    const log: WorkflowEvent[] = [
      runStarted(1),
      started(2, GATE),
      awaited(3, GATE),
      received(4, "sig", { v: 1 }),
    ];
    const outcome = reconstructGateOutcome(log, NAME, GATE);
    // The common happy path: exactly one delivery. The retired
    // findConsumedSignal returned this same {payload, signalId} for this case
    // (its newest-observed is the only observed), so the replay is a
    // no-regression byte-for-byte here.
    expect(outcome).toEqual({
      timedOut: false,
      payload: { v: 1 },
      signalId: "sig",
    });
  });

  test("multi-delivery: binds the signal consumed while awaiting, not a later queued one", () => {
    const log: WorkflowEvent[] = [
      runStarted(1),
      started(2, GATE),
      awaited(3, GATE),
      received(4, "sig-1", { v: 1 }), // consumed while the gate is awaiting
      received(5, "sig-2", { v: 2 }), // lands after; no awaiter, so it queues
    ];
    const outcome = reconstructGateOutcome(log, NAME, GATE);
    // The gate consumed sig-1 while awaiting; sig-2 landed after and only
    // queued (the gate was no longer awaiting), so it is NOT what the gate
    // consumed. The retired findConsumedSignal returned the LAST observed for
    // the name -- sig-2 -- and would have bound the gate to {v:2}: exactly the
    // silent mis-bind this replay closes.
    expect(outcome).toEqual({
      timedOut: false,
      payload: { v: 1 },
      signalId: "sig-1",
    });
  });

  test("reducer fidelity: the bound signalId matches the reducer's actual consuming event", () => {
    const log: WorkflowEvent[] = [
      runStarted(1),
      started(2, GATE),
      awaited(3, GATE),
      received(4, "sig-1", { v: 1 }),
      received(5, "sig-2", { v: 2 }),
    ];
    // Independent oracle: fold the SAME log through the real reducer event by
    // event and capture the SignalReceived that flips the gate
    // awaiting-signal -> in-flight. That is the delivery the reducer actually
    // consumed, derived WITHOUT the replay under test -- so this asserts the
    // replay reproduces the reducer's move, not a plausible FIFO that diverges.
    let state = emptyState("fidelity");
    let reducerConsumed: string | undefined;
    for (const event of log) {
      const before = state.steps.get(GATE)?.phase;
      state = applyEvent(state, event);
      const after = state.steps.get(GATE)?.phase;
      if (
        event.kind === "SignalReceived" &&
        before === "awaiting-signal" &&
        after === "in-flight"
      ) {
        reducerConsumed = event.signalId;
      }
    }
    expect(reducerConsumed).toBe("sig-1");

    const outcome = reconstructGateOutcome(log, NAME, GATE);
    const boundId =
      outcome !== undefined && !outcome.timedOut ? outcome.signalId : undefined;
    expect(boundId).toBe(reducerConsumed);
  });
});
