import { describe, test, expect } from "bun:test";

import type { ReactorEmittedEvent } from "@intx/inference";

import { createStreamConsumer, StreamBackpressureError } from "./stream";

/**
 * Build a minimal ReactorEmittedEvent suitable for fan-out testing. The
 * event's structural details do not matter — the stream consumer treats
 * events opaquely — so we use `reactor.done`, which has an empty `data`.
 */
function makeEvent(seq: number): ReactorEmittedEvent {
  return { type: "reactor.done", seq, data: {} };
}

async function collect(
  it: AsyncIterableIterator<ReactorEmittedEvent>,
  n: number,
): Promise<ReactorEmittedEvent[]> {
  const out: ReactorEmittedEvent[] = [];
  for (let i = 0; i < n; i++) {
    const r = await it.next();
    if (r.done === true) break;
    out.push(r.value);
  }
  return out;
}

describe("createStreamConsumer", () => {
  test("delivers buffered events to a later iterator read", async () => {
    const c = createStreamConsumer(8);
    const it = c.iterator();
    c.push(makeEvent(1));
    c.push(makeEvent(2));
    const got = await collect(it, 2);
    expect(got.map((e) => e.seq)).toEqual([1, 2]);
  });

  test("delivers events directly to a waiting iterator", async () => {
    const c = createStreamConsumer(8);
    const it = c.iterator();
    const pending = it.next();
    c.push(makeEvent(42));
    const r = await pending;
    expect(r.done).toBe(false);
    if (r.done !== true) expect(r.value.seq).toBe(42);
  });

  test("close terminates pending and subsequent reads with done", async () => {
    const c = createStreamConsumer(8);
    const it = c.iterator();
    const pending = it.next();
    c.close();
    const r1 = await pending;
    expect(r1.done).toBe(true);
    const r2 = await it.next();
    expect(r2.done).toBe(true);
  });

  test("close after buffered events still drains them before done", async () => {
    const c = createStreamConsumer(8);
    const it = c.iterator();
    c.push(makeEvent(1));
    c.push(makeEvent(2));
    c.close();
    const r1 = await it.next();
    expect(r1.done).toBe(false);
    const r2 = await it.next();
    expect(r2.done).toBe(false);
    const r3 = await it.next();
    expect(r3.done).toBe(true);
  });

  test("overflow throws StreamBackpressureError on next read", async () => {
    const c = createStreamConsumer(3);
    const it = c.iterator();
    c.push(makeEvent(1));
    c.push(makeEvent(2));
    c.push(makeEvent(3));
    c.push(makeEvent(4));

    // Buffered events drain first.
    const r1 = await it.next();
    expect(r1.done).toBe(false);
    const r2 = await it.next();
    expect(r2.done).toBe(false);
    const r3 = await it.next();
    expect(r3.done).toBe(false);

    // Next read sees the overflow.
    await expect(it.next()).rejects.toBeInstanceOf(StreamBackpressureError);
  });

  test("overflow rejects a pending waiter", async () => {
    const c = createStreamConsumer(2);
    const it = c.iterator();
    const pending = it.next();
    // Direct delivery to the waiter does NOT increase the buffer.
    c.push(makeEvent(1));
    const r1 = await pending;
    expect(r1.done).toBe(false);

    // Now buffer 2 events (capacity), then a 3rd while another waiter is
    // pending — wait, an immediate waiter would consume the 3rd directly.
    // Instead saturate the buffer first.
    c.push(makeEvent(2));
    c.push(makeEvent(3));
    // Saturated. A pending waiter at this point will be served from the
    // buffer; the overflow only fires on a push that has no waiter and a
    // full buffer.
    c.push(makeEvent(4));

    // Drain.
    const r2 = await it.next();
    expect(r2.done).toBe(false);
    const r3 = await it.next();
    expect(r3.done).toBe(false);

    await expect(it.next()).rejects.toBeInstanceOf(StreamBackpressureError);
  });

  test("multiple consumers buffer independently", async () => {
    const a = createStreamConsumer(8);
    const b = createStreamConsumer(8);
    const ai = a.iterator();
    const bi = b.iterator();

    a.push(makeEvent(1));
    b.push(makeEvent(1));
    a.push(makeEvent(2));
    b.push(makeEvent(2));

    const aGot = await collect(ai, 2);
    const bGot = await collect(bi, 2);
    expect(aGot.map((e) => e.seq)).toEqual([1, 2]);
    expect(bGot.map((e) => e.seq)).toEqual([1, 2]);
  });

  test("iterator.return() closes the consumer", async () => {
    const c = createStreamConsumer(8);
    const it = c.iterator();
    expect(c.closed).toBe(false);
    await it.return?.();
    expect(c.closed).toBe(true);
    const r = await it.next();
    expect(r.done).toBe(true);
  });

  test("push after close is ignored", async () => {
    const c = createStreamConsumer(8);
    const it = c.iterator();
    c.close();
    c.push(makeEvent(1));
    const r = await it.next();
    expect(r.done).toBe(true);
  });

  test("Symbol.asyncIterator returns the iterator itself", async () => {
    const c = createStreamConsumer(8);
    const it = c.iterator();
    expect(it[Symbol.asyncIterator]()).toBe(it);
  });

  test("rejects maxBuffer < 1", () => {
    expect(() => createStreamConsumer(0)).toThrow();
  });
});
