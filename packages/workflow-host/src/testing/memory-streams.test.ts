import { describe, test, expect } from "bun:test";

import {
  createMemoryFrameStream,
  createMemoryNdjsonStream,
} from "./memory-streams";

describe("createMemoryNdjsonStream", () => {
  test("a second read() is refused", () => {
    const stream = createMemoryNdjsonStream();
    stream.reader.read();
    // Both channels are single-consumer, and the buffer is drained by
    // whoever iterates. A spawner double that hands the same streams to a
    // respawned child re-reads them, and the second iteration returns off the
    // closed stream instead of receiving the new child's frames -- which
    // reads as a child that died on its handshake.
    expect(() => stream.reader.read()).toThrow(/second read/);
  });

  test("nextWrite resolves for the write and for an injection", async () => {
    const stream = createMemoryNdjsonStream();
    const wrote = stream.nextWrite();
    await stream.writer.write("from-production\n");
    await wrote;

    const injected = stream.nextWrite();
    stream.inject("from-the-test");
    await injected;
    expect(stream.flushed()).toEqual(["from-production", "from-the-test"]);
  });

  test("close releases a caller waiting on a write that cannot come", async () => {
    const stream = createMemoryNdjsonStream();
    const waiting = stream.nextWrite();
    stream.close();
    // Otherwise the wait outlives the test, parked on a stream that will
    // never take another line.
    await waiting;
  });

  test("awaitReadCount is satisfied by a read that already happened", async () => {
    const stream = createMemoryNdjsonStream();
    stream.inject("one");
    const iterator = stream.reader.read();
    await iterator.next();
    // The count advances when the caller asks for the NEXT line, which is
    // what proves it is done with the first. Closing first so that request
    // ends the iteration rather than parking on a line that cannot come.
    stream.close();
    await iterator.next();
    expect(stream.readCount()).toBe(1);
    // Level-triggered: the read completed before this call, and an edge wait
    // on the next read would hang here. That is the shape the stale-frame
    // test needs, because the pump usually reads an injected frame before
    // the test can ask to be told about it.
    await stream.awaitReadCount(1);
  });

  test("awaitReadCount waits for the consumer body, not just the dequeue", async () => {
    const stream = createMemoryNdjsonStream();
    const processed: string[] = [];

    const consumer = (async () => {
      for await (const line of stream.reader.read()) {
        processed.push(line);
      }
    })();

    stream.inject("frame-1");
    await stream.awaitReadCount(1);

    // The negative assertion this counter exists for -- that the consumer
    // IGNORED an injected frame -- is only worth making once the consumer has
    // run its body on it. Counting the dequeue instead resolves the wait from
    // inside the generator, queueing the waiter AHEAD of the consumer, so
    // `processed` would still be empty here for the wrong reason.
    expect(processed).toEqual(["frame-1"]);

    stream.close();
    await consumer;
  });

  test("awaitReadCount still trails the consumer body through a nested generator", async () => {
    const stream = createMemoryNdjsonStream();
    const processed: string[] = [];

    // The shape every production caller has: nothing iterates the reader
    // directly, they iterate `receiveControlChannel`, which is itself a
    // generator over `reader.read()`. The count has to keep meaning "the
    // outermost body ran" across that hop, not "the middle generator
    // dequeued".
    async function* decode(
      lines: AsyncIterableIterator<string>,
    ): AsyncIterableIterator<string> {
      for await (const line of lines) yield line.toUpperCase();
    }

    const consumer = (async () => {
      for await (const decoded of decode(stream.reader.read())) {
        processed.push(decoded);
      }
    })();

    stream.inject("frame-1");
    await stream.awaitReadCount(1);
    expect(processed).toEqual(["FRAME-1"]);

    stream.close();
    await consumer;
  });

  test("awaitReadCount advances for a line a nested generator DROPS", async () => {
    const stream = createMemoryNdjsonStream();
    const processed: string[] = [];

    async function* dropEmpty(
      lines: AsyncIterableIterator<string>,
    ): AsyncIterableIterator<string> {
      for await (const line of lines) {
        if (line === "") continue;
        yield line;
      }
    }

    const consumer = (async () => {
      for await (const kept of dropEmpty(stream.reader.read())) {
        processed.push(kept);
      }
    })();

    stream.inject("");
    // A dropped line never reaches the outermost body, so the count has to
    // come from the intermediate generator asking for the next one. This is
    // the case the negative assertion rests on: the barrier has to hold for a
    // frame the consumer ignored, which is the only kind it is ever used on.
    await stream.awaitReadCount(1);
    expect(processed).toEqual([]);

    stream.close();
    await consumer;
  });

  test("flushed shows only what the consumer has not taken", async () => {
    const stream = createMemoryNdjsonStream();
    const consumer = (async () => {
      for await (const _line of stream.reader.read()) {
        // drain
      }
    })();

    stream.inject("a");
    stream.inject("b");
    await stream.awaitReadCount(2);

    // `flushed` slices the same array the reader shifts from, so it is the
    // unconsumed remainder rather than an arrival log. `waitForUpstreamPayload`
    // re-reads it on every pass, which is why those waiters only work on a
    // direction nothing iterates.
    expect(stream.flushed()).toEqual([]);

    stream.close();
    await consumer;
  });
});

describe("createMemoryFrameStream", () => {
  test("inject supplies the frame terminator and injectRaw does not", () => {
    const stream = createMemoryFrameStream();
    stream.inject(new TextEncoder().encode("framed"));
    stream.injectRaw(new TextEncoder().encode("raw"));
    const [framed, raw] = stream.flushed();
    // The divergence that made consolidating these worth doing: one copy
    // appended the newline the event channel delimits on, another pushed
    // bytes verbatim, and the same call meant two different things.
    expect(framed?.at(-1)).toBe(0x0a);
    expect(raw?.at(-1)).not.toBe(0x0a);
  });

  test("a second read() is refused", () => {
    const stream = createMemoryFrameStream();
    stream.reader.read();
    expect(() => stream.reader.read()).toThrow(/second read/);
  });
});
