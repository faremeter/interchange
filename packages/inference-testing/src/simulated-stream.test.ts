import { describe, test, expect } from "bun:test";

import { createClock } from "./clock";
import { createSimulatedStream, toStreamId } from "./simulated-stream";

const utf8 = (s: string): Uint8Array => new TextEncoder().encode(s);

async function drain(body: ReadableStream<Uint8Array>): Promise<string> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let out = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value !== undefined) out += decoder.decode(value, { stream: true });
  }
  out += decoder.decode();
  return out;
}

describe("createSimulatedStream", () => {
  test("each call yields its own controller (no shared state)", async () => {
    const clock = createClock();
    let terminatedA = false;
    let terminatedB = false;
    const a = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        terminatedA = true;
      },
    });
    const b = createSimulatedStream({
      clock,
      streamId: toStreamId(1),
      onTerminate: () => {
        terminatedB = true;
      },
    });

    a.stream.enqueueAt(10, utf8("a"));
    b.stream.enqueueAt(10, utf8("b"));
    a.stream.closeAt(20);
    b.stream.closeAt(30);

    await clock.advanceTo(50);

    expect(await drain(a.stream.body)).toBe("a");
    expect(await drain(b.stream.body)).toBe("b");
    expect(terminatedA).toBe(true);
    expect(terminatedB).toBe(true);
  });

  test("enqueue uses clock.now() as the schedule time", async () => {
    const clock = createClock();
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        return;
      },
    });

    // Schedule a side-effecting checkpoint at t=20 that calls `enqueue`.
    // Since `enqueue` uses `clock.now()`, this should land at t=20.
    clock.schedule(20, () => {
      stream.enqueue(utf8("at-20"));
    });
    stream.closeAt(30);

    await clock.advanceTo(50);
    expect(await drain(stream.body)).toBe("at-20");
  });

  test("enqueue ordering at the same virtualMs follows monotonic seq", async () => {
    const clock = createClock();
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        return;
      },
    });

    stream.enqueueAt(10, utf8("first"));
    stream.enqueueAt(10, utf8("second"));
    stream.enqueueAt(10, utf8("third"));
    stream.closeAt(10);

    await clock.advanceTo(20);
    expect(await drain(stream.body)).toBe("firstsecondthird");
  });

  test("errorAt rejects the reader", async () => {
    const clock = createClock();
    let terminated = false;
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        terminated = true;
      },
    });

    const failure = new Error("simulated stream failure");
    stream.errorAt(10, failure);
    await clock.advanceTo(20);

    const reader = stream.body.getReader();
    expect(reader.read()).rejects.toThrow("simulated stream failure");
    expect(terminated).toBe(true);
  });

  test("enqueueAt rejects non-Uint8Array bytes", () => {
    const clock = createClock();
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        return;
      },
    });
    expect(() => {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- deliberately passing wrong type to exercise the guard
      stream.enqueueAt(10, "not bytes" as unknown as Uint8Array);
    }).toThrow(/Uint8Array/);
  });

  test("toStreamId rejects negative or non-integer ids", () => {
    expect(() => toStreamId(-1)).toThrow(/non-negative integer/);
    expect(() => toStreamId(1.5)).toThrow(/non-negative integer/);
  });

  test("a multi-layer async-generator consumer attached before advanceTo settles within a single advance", async () => {
    // Pins the harness contract that `drainMicrotasks` keeps flushing
    // until consumer microtask chains have actually settled. The
    // consumer chain is an async generator yielding each chunk into a
    // `for await` loop — mirroring the depth of the production
    // `parseSSE -> runInference -> reactor` composition.
    //
    // The drain must traverse several microtask rounds per fired
    // callback (reader.read resolves -> the inner async generator's
    // yield resolves the outer for-await's promise -> the outer
    // for-await body runs and accumulates -> the consumer's
    // `for await` runs). A drain implementation that exits on the
    // first quiet iteration leaves the consumer mid-chain, so the
    // snapshot taken when `errorAt` fires sees fewer than 3 chunks.
    const clock = createClock();
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        return;
      },
    });

    stream.enqueueAt(10, utf8("first"));
    stream.enqueueAt(20, utf8("second"));
    stream.enqueueAt(30, utf8("third"));

    const observed: string[] = [];
    let snapshotAtError: readonly string[] = [];

    async function* readChunks(): AsyncIterable<string> {
      const reader = stream.body.getReader();
      const decoder = new TextDecoder();
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) return;
          if (value !== undefined) {
            yield decoder.decode(value, { stream: true });
          }
        }
      } finally {
        reader.releaseLock();
      }
    }

    async function* accumulate(): AsyncIterable<string> {
      for await (const chunk of readChunks()) {
        observed.push(chunk);
        yield chunk;
      }
    }

    const consumer = (async () => {
      try {
        for await (const _ of accumulate()) {
          // Drive the chain; the snapshot is captured at the error.
        }
      } catch {
        snapshotAtError = [...observed];
      }
    })();

    // Schedule a stream error AFTER all three chunks. Within a single
    // `advanceTo` the consumer chain must traverse three layers per
    // chunk (reader.read -> readChunks yield -> accumulate yield ->
    // outer for-await) before the error fires; if the drain bails
    // early, `snapshotAtError` will be shorter than 3.
    stream.errorAt(40, new Error("drain-probe error"));

    await clock.advanceTo(50);
    await consumer;

    expect(snapshotAtError).toEqual(["first", "second", "third"]);
  });
});

describe("SimulatedStream.enqueueAll", () => {
  test("schedules chunks at monotonically-increasing virtual times and auto-closes", async () => {
    const clock = createClock();
    let terminated = false;
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        terminated = true;
      },
    });

    stream.enqueueAll([utf8("a"), utf8("b"), utf8("c")], { startAt: 10 });

    await clock.advanceTo(20);
    expect(await drain(stream.body)).toBe("abc");
    expect(terminated).toBe(true);
  });

  test("respects an explicit stepMs gap between chunks", async () => {
    const clock = createClock();
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        return;
      },
    });

    const observed: { at: number; chunk: string }[] = [];
    const decoder = new TextDecoder();

    const consumer = (async () => {
      const reader = stream.body.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        if (value !== undefined) {
          observed.push({ at: clock.now(), chunk: decoder.decode(value) });
        }
      }
    })();

    stream.enqueueAll([utf8("a"), utf8("b"), utf8("c")], {
      startAt: 10,
      stepMs: 5,
    });

    await clock.advanceTo(30);
    await consumer;

    expect(observed.map((e) => e.chunk).join("")).toBe("abc");
    expect(observed.map((e) => e.at)).toEqual([10, 15, 20]);
  });

  test("autoClose: false leaves the stream open for additional enqueues", async () => {
    const clock = createClock();
    let terminated = false;
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        terminated = true;
      },
    });

    stream.enqueueAll([utf8("a"), utf8("b")], {
      startAt: 10,
      autoClose: false,
    });
    stream.enqueueAt(15, utf8("c"));
    stream.closeAt(20);

    await clock.advanceTo(30);
    expect(await drain(stream.body)).toBe("abc");
    expect(terminated).toBe(true);
  });

  test("empty chunks array is a no-op and does NOT auto-close", async () => {
    const clock = createClock();
    let terminated = false;
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        terminated = true;
      },
    });

    stream.enqueueAll([], { startAt: 10 });

    // Verify the stream remains open by advancing the clock and then
    // closing it manually; if `enqueueAll` had auto-closed, the manual
    // closeAt below would throw "closeAt after terminal state".
    await clock.advanceTo(20);
    expect(terminated).toBe(false);
    stream.closeAt(25);
    await clock.advanceTo(30);
    expect(terminated).toBe(true);
    expect(await drain(stream.body)).toBe("");
  });

  test("rejects non-finite or negative stepMs", () => {
    const clock = createClock();
    const { stream } = createSimulatedStream({
      clock,
      streamId: toStreamId(0),
      onTerminate: () => {
        return;
      },
    });
    expect(() =>
      stream.enqueueAll([utf8("a")], { startAt: 10, stepMs: -1 }),
    ).toThrow(/non-negative finite/);
    expect(() =>
      stream.enqueueAll([utf8("a")], { startAt: 10, stepMs: Infinity }),
    ).toThrow(/non-negative finite/);
  });
});
