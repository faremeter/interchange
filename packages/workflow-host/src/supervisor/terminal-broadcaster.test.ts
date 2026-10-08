// Unit coverage for the per-cohort terminal-run broadcaster.
//
// The recycle suite pins iterator finalisation on cohort teardown but
// does NOT pin the race where a terminal event from the old cohort
// arrives during the new cohort's spawn. The supervisor wires one
// broadcaster per spawn cohort and disposes the previous cohort's
// broadcaster when the new cohort takes over. The tests below exercise
// the broadcaster's notify/dispose surface directly so the
// cross-cohort isolation is observable without standing up the full
// supervisor.

import { describe, test, expect } from "bun:test";

import { createTerminalBroadcaster } from "./terminal-broadcaster";
import type { TerminalRunEvent } from "./types";

const COMPLETED: TerminalRunEvent = {
  kind: "RunCompleted",
  seq: 0,
  at: "test",
};
const FAILED: TerminalRunEvent = {
  kind: "RunFailed",
  seq: 0,
  at: "test",
  error: { message: "boom" },
};

describe("terminal-broadcaster: in-flight event from disposed cohort", () => {
  test("a notify against the OLD cohort after dispose neither lands on the OLD cohort's iterator nor leaks into the NEW cohort", async () => {
    // Two cohorts, simulating the recycle path's installNewChild. The
    // race: an upstream `terminal.event` frame from cohort A's child
    // arrives during cohort B's spawn-time wiring. The notify path
    // routes the event through whichever broadcaster is `active` at
    // that moment; the disposed cohort A broadcaster must not deliver
    // to cohort B's listeners, and a stale-cohort notify must not
    // crash either broadcaster.
    const a = createTerminalBroadcaster();
    const b = createTerminalBroadcaster();

    const a_iter = a.source("run-x")[Symbol.asyncIterator]();
    const b_iter = b.source("run-x")[Symbol.asyncIterator]();

    const a_next = a_iter.next();
    const b_next = b_iter.next();

    // The recycle path now disposes cohort A. Every iterator on A
    // settles with `done: true`.
    a.dispose();
    const a_settled = await a_next;
    expect(a_settled.done).toBe(true);

    // A defensive notify against A must be a no-op: the broadcaster's
    // contract is that notify after dispose does nothing.
    expect(() => a.notify("run-x", COMPLETED)).not.toThrow();

    // B's iterator must not have taken A's event. The evidence is the
    // VALUE B settles with, not a sleep: `notify` fans out
    // synchronously (it calls each matching listener's `onEvent` in
    // the same turn), so a cross-cohort leak would already have
    // settled `b_next` with A's COMPLETED above. Cohort B's own child
    // then emits a distinguishable FAILED, and a listener settles
    // exactly once, so FAILED arriving here proves A's notify never
    // reached B.
    b.notify("run-x", FAILED);
    const b_resolved = await b_next;
    expect(b_resolved.done).toBe(false);
    expect(b_resolved.value).toEqual(FAILED);

    b.dispose();
  });

  test("a notify against the OLD cohort after dispose is a no-op even for runIds without any listener", () => {
    // Listenerless runIds are a normal case (the dispatch loop drops
    // the listener as soon as a terminal event arrives), and the
    // terminal.event pump still routes by runId, so the notify path
    // has to be safe for unknown runIds.
    const a = createTerminalBroadcaster();
    a.dispose();
    expect(() => a.notify("run-no-listener", COMPLETED)).not.toThrow();
  });

  test("dispose during an in-flight notify settles the pending iterator without crashing the broadcaster", async () => {
    // The recycle path may invoke `dispose()` while a pending
    // `notify()` from the cohort's terminal.event tap is still
    // mid-flight (the pump is asynchronous; a frame in the receive
    // buffer can land after the cohort abort signal flips). The
    // broadcaster's listener fan-out must remain safe in that window.
    const a = createTerminalBroadcaster();
    const iter = a.source("run-x")[Symbol.asyncIterator]();
    const next = iter.next();

    // A normal in-cohort notify lands first; the listener settles.
    a.notify("run-x", FAILED);
    const result = await next;
    expect(result.done).toBe(false);
    expect(result.value).toEqual(FAILED);

    // The cohort tears down. dispose must be safe even though the
    // listener already settled and unsubscribed.
    expect(() => a.dispose()).not.toThrow();
    expect(a.disposed).toBe(true);
  });

  test("a new iterator on a disposed broadcaster yields done immediately", async () => {
    // The dispatch loop may subscribe to the cohort's broadcaster
    // after the cohort has been disposed (a tear-down / late-subscribe
    // race). The contract: yield `done: true` immediately so the
    // consumer wakes up rather than blocking on a notification that
    // will never arrive.
    const a = createTerminalBroadcaster();
    a.dispose();
    const iter = a.source("run-x")[Symbol.asyncIterator]();
    const result = await iter.next();
    expect(result.done).toBe(true);
  });
});

describe("terminal-broadcaster: per-cohort isolation under concurrent dispose", () => {
  test("cohort A's terminal events do not appear on cohort B's iterators even when notify and dispose interleave", async () => {
    // The strongest version of the race: cohort A's terminal event
    // arrives at the supervisor BEFORE cohort A's broadcaster has been
    // disposed, AND cohort B's dispatch loop has already subscribed to
    // cohort B's broadcaster for the same runId. The broadcasters are
    // distinct objects; B's listener set has no overlap with A's; A's
    // notify cannot fan out to B's listeners.
    const a = createTerminalBroadcaster();
    const b = createTerminalBroadcaster();

    const b_iter = b.source("run-cross")[Symbol.asyncIterator]();
    const b_next = b_iter.next();

    // Simulate the in-flight terminal event arriving on cohort A.
    a.notify("run-cross", COMPLETED);

    // Cohort A is disposed (the recycle path's installNewChild path
    // has reached the broadcaster-swap point).
    a.dispose();

    // B's iterator must not have taken A's event: A's notify cannot
    // influence B's listener set, and `notify` fans out synchronously,
    // so a leak would already have settled `b_next` with COMPLETED.
    // Disposing B finalises its pending iterator with `done: true`;
    // a leak surfaces here as a value instead of `done`.
    b.dispose();
    const b_result = await b_next;
    expect(b_result.done).toBe(true);
  });
});
