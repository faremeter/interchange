import { describe, test, expect } from "bun:test";

import type { SignalCorrelationRegisterFrame } from "@intx/types/sidecar";
import { waitUntil } from "@intx/types/testing";

import { createRegisterAcker } from "./register-acker";

function frameFor(correlationId: string): SignalCorrelationRegisterFrame {
  return {
    type: "signal.correlation.register",
    correlationId,
    runId: "run-1",
    anchorRunId: "dep-1",
    agentAddress: "addr-1",
    kind: "approval",
    snapshot: {
      name: "charge_card",
      description: "Charge the card",
      inputSchema: { type: "object" },
      arguments: {},
    },
  };
}

describe("register acker", () => {
  test("sends once and stops retrying once acked", async () => {
    const sends: string[] = [];
    const acker = createRegisterAcker({
      sendFrame: (f) => sends.push(f.correlationId),
      isOpen: () => true,
      timeoutMs: 10,
      maxAttempts: 3,
    });

    acker.send(frameFor("c1"));
    expect(sends).toEqual(["c1"]);
    expect(acker.handleAck("c1")).toBe(true);

    // A second ack finding nothing pending is the settled state itself, and
    // the watchdog handler no-ops for a correlation with no pending entry --
    // so no retry can fire later. The pause this replaces could only report
    // that none had fired yet.
    expect(acker.handleAck("c1")).toBe(false);
    expect(sends).toEqual(["c1"]);
  });

  test("retries on the watchdog up to the attempt cap, then gives up", async () => {
    const sends: string[] = [];
    // The acker consults `isOpen` once per watchdog fire and from nowhere
    // else, so counting the calls counts the fires. The fire that brings the
    // count to three is the one that finds the budget spent: it drops the
    // pending entry and, unlike the resend branch, arms no replacement
    // timer. A fourth fire therefore has nothing to fire from, at any load,
    // so the count cannot overshoot the cap -- and the ack below reads that
    // settled state back.
    let watchdogFires = 0;
    const acker = createRegisterAcker({
      sendFrame: (f) => sends.push(f.correlationId),
      isOpen: () => {
        watchdogFires += 1;
        return true;
      },
      timeoutMs: 10,
      maxAttempts: 3,
    });

    acker.send(frameFor("c1"));
    await waitUntil(() => watchdogFires >= 3);

    // Three sends total: the initial plus two watchdog retries.
    expect(sends).toEqual(["c1", "c1", "c1"]);
    // After giving up, a late ack finds nothing pending.
    expect(acker.handleAck("c1")).toBe(false);
  });

  test("abandons a pending retry the moment the link is not open", async () => {
    const sends: string[] = [];
    let open = true;
    let watchdogFires = 0;
    const acker = createRegisterAcker({
      sendFrame: (f) => sends.push(f.correlationId),
      isOpen: () => {
        watchdogFires += 1;
        return open;
      },
      timeoutMs: 10,
      maxAttempts: 5,
    });

    acker.send(frameFor("c1"));
    open = false;
    // The watchdog reads `isOpen` as its first act, so one fire is the whole
    // decision: it either resent or abandoned before that call returned.
    await waitUntil(() => watchdogFires >= 1);

    // No resend fired onto the closed link, and the entry was dropped.
    expect(sends).toEqual(["c1"]);
    expect(acker.handleAck("c1")).toBe(false);
  });

  test("cancelAll drops every pending retry so none fire", async () => {
    const sends: string[] = [];
    const acker = createRegisterAcker({
      sendFrame: (f) => sends.push(f.correlationId),
      isOpen: () => true,
      timeoutMs: 10,
      maxAttempts: 5,
    });

    acker.send(frameFor("c1"));
    acker.send(frameFor("c2"));
    acker.cancelAll();

    // Only the two initial sends. Both entries are gone -- the acks below
    // report that -- and the watchdog handler no-ops without an entry, so
    // there is nothing left that could send a third time.
    expect(sends).toEqual(["c1", "c2"]);
    expect(acker.handleAck("c1")).toBe(false);
    expect(acker.handleAck("c2")).toBe(false);
  });

  test("a second send for a pending correlationId collapses to one entry", async () => {
    const sends: string[] = [];
    const acker = createRegisterAcker({
      sendFrame: (f) => sends.push(f.correlationId),
      isOpen: () => true,
      timeoutMs: 20,
      maxAttempts: 5,
    });

    acker.send(frameFor("c1"));
    // A concurrent re-emit for the same correlation: refreshes the one entry
    // and resets its watchdog rather than arming a second. Nothing has to
    // elapse between the two sends -- the pause this replaces had to land
    // inside the watchdog window, and a slow machine landed past it and let
    // the first watchdog fire.
    acker.send(frameFor("c1"));

    // One ack settles the single pending entry; there is no second entry left.
    expect(acker.handleAck("c1")).toBe(true);
    expect(acker.handleAck("c1")).toBe(false);

    // Exactly the two explicit sends fired -- the first watchdog was cancelled
    // by the second send, and the ack settled the entry before it could fire.
    expect(sends).toEqual(["c1", "c1"]);
  });
});
