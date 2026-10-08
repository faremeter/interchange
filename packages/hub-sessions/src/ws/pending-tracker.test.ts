import { describe, expect, test } from "bun:test";

import {
  PendingTracker,
  type ScheduleTimeout,
  type WsHandle,
} from "./pending-tracker";

/**
 * A timer the test drives: every arming is recorded with its delay for the
 * test to fire or assert was disarmed, so a timeout is exercised by firing
 * it rather than waiting out its delay.
 */
function createManualTimer(): {
  schedule: ScheduleTimeout;
  armed: { ms: number; fire: () => void; disarmed: boolean }[];
} {
  const armed: { ms: number; fire: () => void; disarmed: boolean }[] = [];
  const schedule: ScheduleTimeout = (handler, ms) => {
    const entry = { ms, fire: handler, disarmed: false };
    armed.push(entry);
    return () => {
      entry.disarmed = true;
    };
  };
  return { schedule, armed };
}

function onlyArmed(
  armed: { ms: number; fire: () => void; disarmed: boolean }[],
) {
  const [entry, ...rest] = armed;
  if (entry === undefined) throw new Error("no timeout was armed");
  if (rest.length > 0) throw new Error("more than one timeout was armed");
  return entry;
}

function createMockWs(): WsHandle & { sent: string[] } {
  return {
    sent: [],
    send(data: string) {
      this.sent.push(data);
    },
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    close() {},
  };
}

describe("PendingTracker", () => {
  test("resolve settles the pending promise and clears its timer", () => {
    const timer = createManualTimer();
    const tracker = new PendingTracker<string, number>(timer.schedule);
    const ws = createMockWs();
    const state: { settled: "pending" | number } = { settled: "pending" };
    tracker.register(
      "k1",
      ws,
      {
        timeoutMs: 10_000,
        timeoutMessage: "timed out",
        resolve(value) {
          state.settled = value;
        },
        reject() {
          state.settled = "pending";
        },
      },
      undefined,
    );
    expect(tracker.has("k1")).toBe(true);
    expect(tracker.resolve("k1", 42)).toBe(true);
    expect(state.settled).toBe(42);
    expect(tracker.has("k1")).toBe(false);
    // Settling again is a no-op.
    expect(tracker.resolve("k1", 43)).toBe(false);
    expect(state.settled).toBe(42);
    // The timeout is disarmed, which is what the test name claims; waiting
    // could not establish it, since the timeout is ten seconds out.
    expect(onlyArmed(timer.armed).disarmed).toBe(true);
  });

  test("reject settles the pending promise with the error string", () => {
    const tracker = new PendingTracker<string>();
    const ws = createMockWs();
    const state: { error: string | null } = { error: null };
    tracker.register(
      "k1",
      ws,
      {
        timeoutMs: 10_000,
        timeoutMessage: "timed out",
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        resolve() {},
        reject(reason) {
          state.error = reason;
        },
      },
      undefined,
    );
    expect(tracker.reject("k1", "nope")).toBe(true);
    expect(state.error).toBe("nope");
    expect(tracker.reject("k1", "again")).toBe(false);
    expect(state.error).toBe("nope");
  });

  test("timeout rejects with the timeout message and drops the entry", () => {
    const timer = createManualTimer();
    const tracker = new PendingTracker<string>(timer.schedule);
    const ws = createMockWs();
    const state: { error: string | null } = { error: null };
    tracker.register(
      "k1",
      ws,
      {
        timeoutMs: 5,
        timeoutMessage: "k1 timed out",
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        resolve() {},
        reject(reason) {
          state.error = reason;
        },
      },
      undefined,
    );
    const pending = onlyArmed(timer.armed);
    expect(pending.ms).toBe(5);
    pending.fire();
    expect(state.error).toBe("k1 timed out");
    expect(tracker.has("k1")).toBe(false);
    expect(tracker.reject("k1", "late")).toBe(false);
  });

  test("timeout routes through the same reject closure as a frame error", () => {
    const timer = createManualTimer();
    const tracker = new PendingTracker<string>(timer.schedule);
    const ws = createMockWs();
    const rejections: string[] = [];
    tracker.register(
      "k1",
      ws,
      {
        timeoutMs: 5,
        timeoutMessage: "k1 timed out",
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        resolve() {},
        reject(reason) {
          rejections.push(reason);
        },
      },
      undefined,
    );
    onlyArmed(timer.armed).fire();
    expect(rejections).toEqual(["k1 timed out"]);
  });

  test("delete drops the entry without settling and disarms the timer", () => {
    const timer = createManualTimer();
    const tracker = new PendingTracker<string>(timer.schedule);
    const ws = createMockWs();
    let settled = false;
    tracker.register(
      "k1",
      ws,
      {
        timeoutMs: 5,
        timeoutMessage: "k1 timed out",
        resolve() {
          settled = true;
        },
        reject() {
          settled = true;
        },
      },
      undefined,
    );
    tracker.delete("k1");
    expect(tracker.has("k1")).toBe(false);
    // Disarmed, so there is no later fire to wait for. Firing it anyway must
    // still not settle: delete drops the entry, so the handler finds nothing.
    expect(onlyArmed(timer.armed).disarmed).toBe(true);
    onlyArmed(timer.armed).fire();
    expect(settled).toBe(false);
  });

  test("rejectAllForWs rejects only the entries owned by that ws", () => {
    const tracker = new PendingTracker<string>();
    const ws1 = createMockWs();
    const ws2 = createMockWs();
    const rejected: string[] = [];
    const register = (ws: WsHandle, key: string) =>
      tracker.register(
        key,
        ws,
        {
          timeoutMs: 10_000,
          timeoutMessage: "timed out",
          // eslint-disable-next-line @typescript-eslint/no-empty-function
          resolve() {},
          reject(reason) {
            rejected.push(`${key}:${reason}`);
          },
        },
        undefined,
      );
    register(ws1, "a");
    register(ws1, "b");
    register(ws2, "c");
    tracker.rejectAllForWs(ws1, "gone");
    expect(rejected.sort()).toEqual(["a:gone", "b:gone"]);
    expect(tracker.has("a")).toBe(false);
    expect(tracker.has("b")).toBe(false);
    expect(tracker.has("c")).toBe(true);
    // A second sweep over the same ws is a no-op.
    tracker.rejectAllForWs(ws1, "gone");
    expect(rejected).toHaveLength(2);
  });

  test("a stale timeout leaves a later entry under the same key alone", () => {
    const timer = createManualTimer();
    const tracker = new PendingTracker<string, number>(timer.schedule);
    const ws = createMockWs();
    const settlements: string[] = [];
    const register = (label: string) =>
      tracker.register(
        "k1",
        ws,
        {
          timeoutMs: 10_000,
          timeoutMessage: `${label} timed out`,
          resolve() {
            settlements.push(`${label} resolved`);
          },
          reject(reason) {
            settlements.push(`${label} rejected: ${reason}`);
          },
        },
        undefined,
      );

    register("first");
    expect(tracker.resolve("k1", 1)).toBe(true);
    expect(settlements).toEqual(["first resolved"]);

    // Keys are reused by design: `pendingDeploys`/`pendingUndeploys` are
    // keyed by agent address, so a second round-trip to the same agent
    // registers under the settled first one's key.
    register("second");

    const [stale, ...rest] = timer.armed;
    if (stale === undefined) throw new Error("no timeout was armed");
    expect(rest).toHaveLength(1);
    // The first entry's timeout was disarmed, and this fires it anyway -- the
    // canceller only records the disarm, the case the entry-identity check in
    // the handler exists for.
    expect(stale.disarmed).toBe(true);
    stale.fire();

    // The first entry is settled, so its per-site cleanup must not run again.
    expect(settlements).toEqual(["first resolved"]);
    // The second entry owns the key now, so the stale fire must not drop it.
    expect(tracker.has("k1")).toBe(true);
    expect(tracker.resolve("k1", 2)).toBe(true);
    expect(settlements).toEqual(["first resolved", "second resolved"]);
  });

  test("meta is carried on the entry for settle-time ownership checks", () => {
    type PackMeta = { agentAddress: string; repoId: string };
    const tracker = new PendingTracker<string, void, PackMeta>();
    const ws = createMockWs();
    tracker.register(
      "transfer-1",
      ws,
      {
        timeoutMs: 10_000,
        timeoutMessage: "timed out",
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        resolve() {},
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        reject() {},
      },
      { agentAddress: "a@t", repoId: "repo-1" },
    );
    expect(tracker.get("transfer-1")?.meta).toEqual({
      agentAddress: "a@t",
      repoId: "repo-1",
    });
  });
});
