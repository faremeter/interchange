import { describe, test, expect } from "bun:test";

import { createSendQueue, SendQueueFullError } from "./send-queue";

/**
 * Tests use `string` as the result type so the queue's generic R parameter
 * holds a real value (it cannot be `void` under
 * `@typescript-eslint/no-invalid-void-type`). The actual values are
 * arbitrary; tests assert ordering and lifecycle, not result content.
 */

describe("createSendQueue", () => {
  test("starts the first enqueued item immediately", () => {
    const started: string[] = [];
    const q = createSendQueue<string, string>({
      maxDepth: 4,
      start: (item) => {
        started.push(item);
      },
    });

    void q.enqueue("a");
    expect(started).toEqual(["a"]);
  });

  test("queues subsequent items and starts each on resolveActive", async () => {
    const started: string[] = [];
    const q = createSendQueue<string, string>({
      maxDepth: 4,
      start: (item) => {
        started.push(item);
      },
    });

    const p1 = q.enqueue("a");
    const p2 = q.enqueue("b");
    expect(started).toEqual(["a"]);

    q.resolveActive("r1");
    expect(await p1).toBe("r1");
    expect(started).toEqual(["a", "b"]);

    q.resolveActive("r2");
    expect(await p2).toBe("r2");
  });

  test("throws SendQueueFullError synchronously at capacity", () => {
    const started: number[] = [];
    const q = createSendQueue<number, string>({
      maxDepth: 2,
      start: (item) => {
        started.push(item);
      },
    });

    void q.enqueue(1);
    void q.enqueue(2);
    expect(() => q.enqueue(3)).toThrow(SendQueueFullError);
  });

  test("rejects pre-aborted signal without enqueuing", async () => {
    const ctl = new AbortController();
    ctl.abort();

    const started: number[] = [];
    const q = createSendQueue<number, string>({
      maxDepth: 4,
      start: (item) => {
        started.push(item);
      },
    });

    await expect(q.enqueue(1, ctl.signal)).rejects.toBeDefined();
    expect(started).toEqual([]);
    expect(q.depth).toBe(0);
  });

  test("removes a queued item whose signal fires before processing", async () => {
    const started: number[] = [];
    const ctl = new AbortController();
    const q = createSendQueue<number, string>({
      maxDepth: 4,
      start: (item) => {
        started.push(item);
      },
    });

    const p1 = q.enqueue(1);
    const p2 = q.enqueue(2, ctl.signal);
    const p3 = q.enqueue(3);

    expect(started).toEqual([1]);
    ctl.abort();
    await expect(p2).rejects.toBeDefined();

    q.resolveActive("r1");
    expect(await p1).toBe("r1");
    expect(started).toEqual([1, 3]);

    q.resolveActive("r3");
    expect(await p3).toBe("r3");
  });

  test("settles caller on in-flight abort but waits for consumer to advance", async () => {
    const started: number[] = [];
    const ctl = new AbortController();
    const q = createSendQueue<number, string>({
      maxDepth: 4,
      start: (item) => {
        started.push(item);
      },
    });

    const p1 = q.enqueue(1, ctl.signal);
    const p2 = q.enqueue(2);
    expect(started).toEqual([1]);

    ctl.abort();
    await expect(p1).rejects.toBeDefined();
    // Active slot is still held until the consumer reports the cycle done.
    expect(started).toEqual([1]);

    q.resolveActive("late-r1");
    expect(started).toEqual([1, 2]);

    q.resolveActive("r2");
    expect(await p2).toBe("r2");
  });

  test("late resolveActive after abort is a no-op for the caller", async () => {
    const ctl = new AbortController();
    const started: number[] = [];
    const q = createSendQueue<number, string>({
      maxDepth: 4,
      start: (item) => {
        started.push(item);
      },
    });

    const p = q.enqueue(1, ctl.signal);
    ctl.abort();
    await expect(p).rejects.toBeDefined();

    q.resolveActive("late");
    expect(q.depth).toBe(0);
  });

  test("drain rejects active and pending jobs with the given reason", async () => {
    const started: number[] = [];
    const q = createSendQueue<number, string>({
      maxDepth: 4,
      start: (item) => {
        started.push(item);
      },
    });

    const p1 = q.enqueue(1);
    const p2 = q.enqueue(2);
    const reason = new Error("closed");

    q.drain(reason);
    await expect(p1).rejects.toBe(reason);
    await expect(p2).rejects.toBe(reason);
    expect(q.depth).toBe(0);
  });

  test("depth reflects active + pending", () => {
    const started: number[] = [];
    const q = createSendQueue<number, string>({
      maxDepth: 4,
      start: (item) => {
        started.push(item);
      },
    });

    expect(q.depth).toBe(0);
    void q.enqueue(1);
    expect(q.depth).toBe(1);
    void q.enqueue(2);
    expect(q.depth).toBe(2);

    q.resolveActive("r1");
    expect(q.depth).toBe(1);
    q.resolveActive("r2");
    expect(q.depth).toBe(0);
  });

  test("capacity check counts the abandoned active slot", async () => {
    const ctl = new AbortController();
    const started: number[] = [];
    const q = createSendQueue<number, string>({
      maxDepth: 2,
      start: (item) => {
        started.push(item);
      },
    });

    const p1 = q.enqueue(1, ctl.signal);
    void q.enqueue(2);
    ctl.abort();
    await expect(p1).rejects.toBeDefined();

    // Active slot still occupied (consumer has not advanced). Adding a
    // third would exceed maxDepth=2.
    expect(() => q.enqueue(3)).toThrow(SendQueueFullError);
  });
});
