// In-memory stand-ins for the two IPC transports, for tests that drive a
// supervisor or a workflow-process child without spawning one.
//
// Seventeen copies of these two factories accumulated across this package,
// `apps/sidecar`, and the deploy tests, in nine variants apiece. The
// divergences were almost entirely cosmetic, but the copies disagreed on one
// thing that matters: whether `inject` appends the newline terminator that
// delimits a wire frame.
//
// The two doubles answer that differently, because the two channels carry the
// terminator differently. The event channel is byte-oriented and its
// terminator is part of the payload, so `createMemoryFrameStream` separates
// three roles and a caller says which one it means:
//
//   `writer`    what production writes through; bytes go in verbatim
//   `inject`    one complete frame arriving on the wire, terminator supplied
//   `injectRaw` bytes verbatim, for a deliberately malformed wire
//
// The control channel is line-oriented: one NDJSON line is one frame, and the
// terminator is the line break the buffer does not store. So
// `createMemoryNdjsonStream` has no third role -- `writer.write` and `inject`
// are the same function -- and it strips one trailing newline, so a caller
// that spells the terminator and one that omits it buffer the same line. A
// malformed control frame is spelled as a malformed line, which `inject`
// passes through unchanged apart from that newline, so there is no separate
// raw role for a caller to reach for.
//
// A reader supports ONE `read()` iteration. Both channels are single-consumer
// by construction -- the supervisor starts one pump per stream per child --
// and the buffer is drained by whoever iterates, so a second iteration would
// silently steal frames from the first. `read()` enforces that rather than
// trusting it, because the way it gets violated is invisible: a spawner
// double that hands the same streams to a respawned child re-reads them, and
// the second iteration returns immediately off the closed stream instead of
// receiving the new child's frames. The child then looks like it died on its
// handshake, which is indistinguishable from the failure such a test is
// usually written to examine.

import type { NdjsonReader, NdjsonWriter } from "../ipc/control-channel";
import type { FrameReader, FrameWriter } from "../ipc/event-channel";

/**
 * Observers of the buffer growing, distinct from the reader's own waiter: a
 * test watches for a line arriving so it can re-read `flushed` when there is
 * something new, rather than on a timer.
 */
type WriteObservers = { list: (() => void)[] };

function announce(observers: WriteObservers): void {
  const waiting = observers.list;
  observers.list = [];
  for (const observer of waiting) observer();
}

/**
 * Resolves on the next item to reach the buffer, from either direction.
 *
 * Edge-triggered: it does not see an item already buffered. A caller reads
 * `flushed`, arms this, and re-checks in a loop, with the read and the arm in
 * ONE synchronous block -- an `await` between them is what loses an item.
 * Which of the two comes first inside that block decides nothing, because
 * nothing can land between two synchronous statements. Both orders are in use
 * here (`waitForUpstreamPayload` arms first, the park-notify waits in
 * `apps/sidecar` read first) and both are safe for that reason.
 */
function nextWriteOf(observers: WriteObservers): Promise<void> {
  return new Promise<void>((resolve) => {
    observers.list.push(resolve);
  });
}

export type MemoryNdjsonStream = {
  writer: NdjsonWriter;
  reader: NdjsonReader;
  inject(line: string): void;
  flushed(): readonly string[];
  nextWrite(): Promise<void>;
  /** How many lines the consumer has taken off the buffer and finished with. */
  readCount(): number;
  /**
   * Resolves once the consumer has finished with at least `count` lines.
   *
   * The write report says a line arrived; this says the pump reading the
   * stream is done with it. A test that injects a frame the consumer is
   * supposed to IGNORE needs the second one: without it, the assertion that
   * nothing happened can run before the consumer has even looked.
   *
   * "Finished with" is the consumer asking for the next line, which a
   * `for await` loop does only after its body returns -- so the guarantee is
   * exactly as strong as the loop body is. A body that dispatches work with
   * `void` and returns has finished in this sense while its handler is still
   * in flight, and a consumer that stops iterating never asks again, so its
   * last line stays uncounted.
   *
   * Level-triggered on a count rather than edge-triggered on the next read,
   * so a caller that arms it after the read already happened is not left
   * waiting for another one.
   */
  awaitReadCount(count: number): Promise<void>;
  close(): void;
};

export function createMemoryNdjsonStream(): MemoryNdjsonStream {
  const buffer: string[] = [];
  const observers: WriteObservers = { list: [] };
  const readObservers: WriteObservers = { list: [] };
  let reads = 0;
  let waiter: (() => void) | null = null;
  let done = false;
  let reading = false;

  function wake(): void {
    const w = waiter;
    waiter = null;
    if (w) w();
  }

  function push(line: string): void {
    buffer.push(line.replace(/\n$/, ""));
    wake();
    announce(observers);
  }

  const reader: NdjsonReader = {
    read(): AsyncIterableIterator<string> {
      if (reading) {
        throw new Error(
          "createMemoryNdjsonStream: a second read() on one stream; hand each spawn its own streams, as a real spawn does",
        );
      }
      reading = true;
      return (async function* () {
        let handedOut = false;
        for (;;) {
          if (handedOut) {
            // Counted here, on re-entry, rather than beside the `yield`. A
            // `for await` loop calls `next()` only after its body has run to
            // completion, so re-entry is the point at which the previous line
            // is finished with. Counting at the `yield` instead resolves a
            // waiter whose continuation is queued AHEAD of the consumer's, so
            // the one assertion the count exists for -- that a frame was
            // ignored -- would run before the consumer had looked at it.
            handedOut = false;
            reads += 1;
            announce(readObservers);
          }
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("buffer shift returned undefined");
            }
            handedOut = true;
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };

  return {
    writer: { write: push },
    reader,
    inject: push,
    flushed: () => buffer.slice(),
    nextWrite: () => nextWriteOf(observers),
    readCount: () => reads,
    async awaitReadCount(count: number) {
      for (;;) {
        // Re-checked on every pass: level-triggered on the count, so a read
        // that happened before this call resolves it. An edge wait on the next
        // read deadlocks here, because the consumer usually reads an injected
        // frame before a test can ask to be told about it.
        const read = nextWriteOf(readObservers);
        if (reads >= count) return;
        await read;
      }
    },
    close() {
      done = true;
      wake();
      // A closed stream takes nothing further, so release anyone still
      // watching rather than leaving them parked on a write that cannot come.
      announce(observers);
      announce(readObservers);
    },
  };
}

export type MemoryFrameStream = {
  writer: FrameWriter;
  reader: FrameReader;
  inject(bytes: Uint8Array): void;
  injectRaw(bytes: Uint8Array): void;
  flushed(): readonly Uint8Array[];
  nextWrite(): Promise<void>;
  close(): void;
};

export function createMemoryFrameStream(): MemoryFrameStream {
  const buffer: Uint8Array[] = [];
  const observers: WriteObservers = { list: [] };
  let waiter: (() => void) | null = null;
  let done = false;
  let reading = false;

  function wake(): void {
    const w = waiter;
    waiter = null;
    if (w) w();
  }

  function pushRaw(bytes: Uint8Array): void {
    buffer.push(bytes);
    wake();
    announce(observers);
  }

  const reader: FrameReader = {
    read(): AsyncIterableIterator<Uint8Array> {
      if (reading) {
        throw new Error(
          "createMemoryFrameStream: a second read() on one stream; hand each spawn its own streams, as a real spawn does",
        );
      }
      reading = true;
      return (async function* () {
        for (;;) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("frame buffer shift returned undefined");
            }
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };

  return {
    writer: { write: pushRaw },
    reader,
    inject(bytes: Uint8Array) {
      // The event channel is newline-delimited: its sender terminates every
      // frame, and the receiver splits on that terminator. A caller passing
      // envelope bytes is describing one frame, so the terminator belongs
      // here rather than at each call site.
      const framed = new Uint8Array(bytes.length + 1);
      framed.set(bytes, 0);
      framed[bytes.length] = 0x0a;
      pushRaw(framed);
    },
    injectRaw: pushRaw,
    flushed: () => buffer.slice(),
    nextWrite: () => nextWriteOf(observers),
    close() {
      done = true;
      wake();
      announce(observers);
    },
  };
}
