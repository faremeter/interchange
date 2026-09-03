import { describe, expect, test } from "bun:test";

import { PendingTracker, type WsHandle } from "./pending-tracker";

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
  test("resolve settles the pending promise and clears its timer", async () => {
    const tracker = new PendingTracker<string, number>();
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
    // The armed timer must not fire after settle.
    await new Promise((resolve) => setTimeout(resolve, 15));
    expect(state.settled).toBe(42);
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

  test("timeout rejects with the timeout message and drops the entry", async () => {
    const tracker = new PendingTracker<string>();
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
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(state.error).toBe("k1 timed out");
    expect(tracker.has("k1")).toBe(false);
    expect(tracker.reject("k1", "late")).toBe(false);
  });

  test("timeout routes through the same reject closure as a frame error", async () => {
    const tracker = new PendingTracker<string>();
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
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(rejections).toEqual(["k1 timed out"]);
  });

  test("delete drops the entry without settling and disarms the timer", async () => {
    const tracker = new PendingTracker<string>();
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
    await new Promise((resolve) => setTimeout(resolve, 25));
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
