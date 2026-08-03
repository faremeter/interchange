// Focused unit tests for `boundSignalForContainerAwait` -- the reducer-FIFO
// replay that binds a delivered signal to the container `signal-relay` await
// that consumed it. This is the correctness crux of the signal-relay pass: the
// runtime driver reaches it only on the pre-consume and race-landed log-read
// paths, so its corrections (signalId dedup, FIFO-oldest, abandon-retire) are
// proven here against hand-built logs rather than through the driver.
//
// Each log is a COMPLETE run log from seq 1 (the helper's precondition); the
// container step id is "section" throughout.

import { describe, test, expect } from "bun:test";

import type { WorkflowEvent } from "@intx/workflow";

// Imported directly from the runtime module: the binding is intentionally not
// part of the package's public surface, so it is exercised via its source path
// rather than promoted to the barrel for a test.
import { boundSignalForContainerAwait } from "./run";

const at = "2026-01-01T00:00:00.000Z";
const SECTION = "section";
const N = "go";

function runStarted(seq: number): WorkflowEvent {
  return {
    kind: "RunStarted",
    seq,
    at,
    runId: "sec",
    definitionHash: "x",
    trigger: { type: "manual", payload: null },
  };
}

function awaited(seq: number, name = N, stepId = SECTION): WorkflowEvent {
  return {
    kind: "SignalAwaited",
    seq,
    at,
    stepId,
    signalName: name,
    parkKind: "signal-relay",
  };
}

function received(
  seq: number,
  signalId: string,
  payload: unknown,
  name = N,
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

function abandoned(seq: number, name = N, stepId = SECTION): WorkflowEvent {
  return { kind: "SignalAwaitAbandoned", seq, at, stepId, signalName: name };
}

describe("boundSignalForContainerAwait", () => {
  test("pre-consume: binds the signal queued before the await (seq < awaitSeq)", () => {
    const log: WorkflowEvent[] = [
      runStarted(1),
      received(2, "s1", { v: 1 }),
      awaited(3),
    ];
    // The await at seq 3 consumes the queue head committed at seq 2.
    expect(boundSignalForContainerAwait(log, N, SECTION, 3)).toEqual({
      payload: { v: 1 },
      signalId: "s1",
    });
  });

  test("FIFO multi-queue: binds the OLDEST queued signal, not the newest", () => {
    const log: WorkflowEvent[] = [
      runStarted(1),
      received(2, "s1", { v: 1 }),
      received(3, "s2", { v: 2 }),
      awaited(4),
    ];
    // The reducer consumes the queue HEAD (oldest). A newest-observed heuristic would
    // return the newest ("s2") -- the exact divergence this replay avoids.
    const bound = boundSignalForContainerAwait(log, N, SECTION, 4);
    expect(bound?.signalId).toBe("s1");
    expect(bound?.payload).toEqual({ v: 1 });
  });

  test("dedup: a redelivered SignalReceived (same id) does not double-advance the FIFO", () => {
    const log: WorkflowEvent[] = [
      runStarted(1),
      awaited(2), // await #1
      received(3, "s1", { v: 1 }), // pairs with #1
      received(4, "s1", { v: 1 }), // redelivery of the SAME id -- skipped
      awaited(5), // await #2
      received(6, "s2", { v: 2 }), // pairs with #2
    ];
    // With dedup, await #2 binds s2. Without it, the redelivery would queue and
    // await #2 would wrongly bind the duplicate.
    expect(boundSignalForContainerAwait(log, N, SECTION, 5)).toEqual({
      payload: { v: 2 },
      signalId: "s2",
    });
    expect(boundSignalForContainerAwait(log, N, SECTION, 2)).toEqual({
      payload: { v: 1 },
      signalId: "s1",
    });
  });

  test("abandon-rebind: a signal after an abandoned await binds the NEXT await, not the retired one", () => {
    const log: WorkflowEvent[] = [
      runStarted(1),
      awaited(2), // await #1
      abandoned(3), // #1 retired without consuming
      awaited(4), // await #2
      received(5, "s1", { v: 1 }),
    ];
    // The reducer drops the abandoned awaiter, so the signal binds await #2.
    // Without abandon-retire, the replay would pair it with the retired #1.
    expect(boundSignalForContainerAwait(log, N, SECTION, 4)).toEqual({
      payload: { v: 1 },
      signalId: "s1",
    });
    expect(boundSignalForContainerAwait(log, N, SECTION, 2)).toBeUndefined();
  });

  test("race-landed: binds the earliest signal committed after the await", () => {
    const log: WorkflowEvent[] = [
      runStarted(1),
      awaited(2),
      received(3, "s1", { v: 1 }),
      received(4, "s2", { v: 2 }),
    ];
    // The await at seq 2 binds the earliest later signal (s1); s2 queues for a
    // future await.
    const bound = boundSignalForContainerAwait(log, N, SECTION, 2);
    expect(bound?.signalId).toBe("s1");
  });

  test("scoping: ignores awaits/signals for a different name or a different step", () => {
    const log: WorkflowEvent[] = [
      runStarted(1),
      received(2, "other", { v: 9 }, "not-go"), // different name
      awaited(3, N, "other-section"), // different step
      awaited(4), // our await
      received(5, "s1", { v: 1 }),
    ];
    expect(boundSignalForContainerAwait(log, N, SECTION, 4)).toEqual({
      payload: { v: 1 },
      signalId: "s1",
    });
  });

  test("precondition: a windowed suffix (not from seq 1) throws", () => {
    const log: WorkflowEvent[] = [awaited(5), received(6, "s1", { v: 1 })];
    expect(() => boundSignalForContainerAwait(log, N, SECTION, 5)).toThrow(
      /full run log from seq 1/,
    );
  });

  test("no signal for the await: returns undefined", () => {
    const log: WorkflowEvent[] = [runStarted(1), awaited(2)];
    expect(boundSignalForContainerAwait(log, N, SECTION, 2)).toBeUndefined();
  });
});
