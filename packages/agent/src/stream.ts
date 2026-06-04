// Bounded per-consumer fan-out for the agent's reactor event stream.
//
// Each call to `agent.stream()` creates a fresh `StreamConsumer`. The
// agent feeds every reactor event to every consumer; consumers buffer
// independently. If a consumer falls more than `maxBuffer` events behind
// it is poisoned with `StreamBackpressureError` and its iterator throws
// on the next read — the consumer is removed but other consumers keep
// running.
//
// Loud failure matches the defensive-coding rule: silently dropping
// events would hide consumer bugs, and unbounded buffering would let a
// stalled consumer balloon the agent's memory. The cap is configurable
// via `streamBufferMax` on `BaseEnv`.

import type { ReactorEmittedEvent } from "@intx/inference";

export class StreamBackpressureError extends Error {
  readonly maxBuffer: number;

  constructor(maxBuffer: number) {
    super(`stream consumer fell more than ${String(maxBuffer)} events behind`);
    this.name = "StreamBackpressureError";
    this.maxBuffer = maxBuffer;
  }
}

type Waiter = {
  resolve: (value: IteratorResult<ReactorEmittedEvent>) => void;
  reject: (reason: unknown) => void;
};

export type StreamConsumer = {
  /** Deliver an event to this consumer's buffer. */
  push(event: ReactorEmittedEvent): void;
  /** Cleanly terminate the iterator with `done: true`. */
  close(): void;
  /** True once close() or an overflow has poisoned the consumer. */
  readonly closed: boolean;
  /** Iterator handed back to the caller of `stream()`. */
  iterator(): AsyncIterableIterator<ReactorEmittedEvent>;
};

export function createStreamConsumer(maxBuffer: number): StreamConsumer {
  if (maxBuffer < 1) {
    throw new Error(`streamBufferMax must be >= 1, got ${String(maxBuffer)}`);
  }

  const buffer: ReactorEmittedEvent[] = [];
  const waiters: Waiter[] = [];
  let overflow: StreamBackpressureError | undefined;
  let done = false;

  function settleOverflowedWaiters(err: StreamBackpressureError): void {
    while (waiters.length > 0) {
      const w = waiters.shift();
      if (w === undefined) return;
      w.reject(err);
    }
  }

  function settleDoneWaiters(): void {
    while (waiters.length > 0) {
      const w = waiters.shift();
      if (w === undefined) return;
      w.resolve({ value: undefined, done: true });
    }
  }

  function push(event: ReactorEmittedEvent): void {
    if (done || overflow !== undefined) return;

    if (waiters.length > 0) {
      const w = waiters.shift();
      if (w === undefined) return;
      w.resolve({ value: event, done: false });
      return;
    }

    if (buffer.length >= maxBuffer) {
      overflow = new StreamBackpressureError(maxBuffer);
      settleOverflowedWaiters(overflow);
      return;
    }

    buffer.push(event);
  }

  function close(): void {
    if (done) return;
    done = true;
    settleDoneWaiters();
  }

  function nextResult(): Promise<IteratorResult<ReactorEmittedEvent>> {
    if (overflow !== undefined) {
      // Drain any buffered events before throwing so the caller sees
      // every event up to the overflow point.
      if (buffer.length > 0) {
        const ev = buffer.shift();
        if (ev !== undefined) {
          return Promise.resolve({ value: ev, done: false });
        }
      }
      return Promise.reject(overflow);
    }
    if (buffer.length > 0) {
      const ev = buffer.shift();
      if (ev !== undefined) {
        return Promise.resolve({ value: ev, done: false });
      }
    }
    if (done) {
      return Promise.resolve({ value: undefined, done: true });
    }
    return new Promise((resolve, reject) => {
      waiters.push({ resolve, reject });
    });
  }

  function iterator(): AsyncIterableIterator<ReactorEmittedEvent> {
    const it: AsyncIterableIterator<ReactorEmittedEvent> = {
      next: nextResult,
      async return() {
        close();
        return { value: undefined, done: true };
      },
      [Symbol.asyncIterator]() {
        return it;
      },
    };
    return it;
  }

  return {
    push,
    close,
    get closed() {
      return done || overflow !== undefined;
    },
    iterator,
  };
}
