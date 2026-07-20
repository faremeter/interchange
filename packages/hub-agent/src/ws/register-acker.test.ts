import { describe, test, expect } from "bun:test";

import type { SignalCorrelationRegisterFrame } from "@intx/types/sidecar";

import { createRegisterAcker } from "./register-acker";

function frameFor(correlationId: string): SignalCorrelationRegisterFrame {
  return {
    type: "signal.correlation.register",
    correlationId,
    runId: "run-1",
    deploymentId: "dep-1",
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

const tick = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

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

    await tick(40);
    // No retry fired after the ack settled the pending entry.
    expect(sends).toEqual(["c1"]);
  });

  test("retries on the watchdog up to the attempt cap, then gives up", async () => {
    const sends: string[] = [];
    const acker = createRegisterAcker({
      sendFrame: (f) => sends.push(f.correlationId),
      isOpen: () => true,
      timeoutMs: 10,
      maxAttempts: 3,
    });

    acker.send(frameFor("c1"));
    await tick(60);

    // Three sends total: the initial plus two watchdog retries.
    expect(sends).toEqual(["c1", "c1", "c1"]);
    // After giving up, a late ack finds nothing pending.
    expect(acker.handleAck("c1")).toBe(false);
  });

  test("abandons a pending retry the moment the link is not open", async () => {
    const sends: string[] = [];
    let open = true;
    const acker = createRegisterAcker({
      sendFrame: (f) => sends.push(f.correlationId),
      isOpen: () => open,
      timeoutMs: 10,
      maxAttempts: 5,
    });

    acker.send(frameFor("c1"));
    open = false;
    await tick(40);

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
    await tick(40);

    // Only the two initial sends; both watchdogs were cleared.
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
    await tick(5);
    // A concurrent re-emit for the same correlation: refreshes the one entry
    // and resets its watchdog rather than arming a second.
    acker.send(frameFor("c1"));

    // One ack settles the single pending entry; there is no second entry left.
    expect(acker.handleAck("c1")).toBe(true);
    expect(acker.handleAck("c1")).toBe(false);

    await tick(60);
    // Exactly the two explicit sends fired -- the first watchdog was cancelled
    // by the second send, and the ack settled the entry before it could fire.
    expect(sends).toEqual(["c1", "c1"]);
  });
});
