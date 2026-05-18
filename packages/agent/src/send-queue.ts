// FIFO queue for serializing send() calls against a single reactor.
//
// The agent processes one reactor cycle at a time, so concurrent send()
// callers are queued. Each queued item carries the caller's resolve/reject
// hooks and an optional AbortSignal:
//
//   - If the signal is already aborted when enqueue() is called the queue
//     rejects synchronously without enqueueing.
//   - If the signal fires while the item is still queued the item is
//     removed and rejected.
//   - If the signal fires while the item is active the caller-facing
//     promise rejects immediately, but the reactor cycle continues in the
//     background. The queue does not start the next item until the consumer
//     reports the cycle done via resolveActive/rejectActive. This keeps the
//     queue ordered against actual reactor cycles — two send() promises
//     cannot interleave at the reactor level.
//
// Queue depth (active + pending) is bounded by `maxDepth`; exceeding it
// throws `SendQueueFullError` synchronously from enqueue() so a buggy
// caller flooding sends fails loud instead of silently buffering.

export class SendQueueFullError extends Error {
  readonly maxDepth: number;

  constructor(maxDepth: number) {
    super(`send queue is full (max depth ${String(maxDepth)})`);
    this.name = "SendQueueFullError";
    this.maxDepth = maxDepth;
  }
}

type Job<T, R> = {
  item: T;
  signal?: AbortSignal;
  abortHandler?: () => void;
  resolve: (value: R) => void;
  reject: (reason: unknown) => void;
  /**
   * True once the caller-facing promise has been settled (resolve or
   * reject). Subsequent settles are no-ops. The active slot may remain
   * occupied after a settle when the caller aborted mid-cycle — the queue
   * waits for the consumer's resolveActive/rejectActive before pumping the
   * next item.
   */
  settled: boolean;
};

export type SendQueueOptions<T> = {
  maxDepth: number;
  /**
   * Called when a job moves from pending to active. The consumer drives
   * the underlying work and must eventually call `resolveActive` or
   * `rejectActive` exactly once.
   */
  start: (item: T) => void;
};

export type SendQueue<T, R> = {
  enqueue(item: T, signal?: AbortSignal): Promise<R>;
  /** Mark the active job complete with success and pump the next. */
  resolveActive(value: R): void;
  /** Mark the active job complete with failure and pump the next. */
  rejectActive(reason: unknown): void;
  /** Reject the active job (if any) and every pending job with `reason`. */
  drain(reason: unknown): void;
  /** Current pending count (queued + active). */
  readonly depth: number;
};

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("aborted", "AbortError");
}

export function createSendQueue<T, R>(
  opts: SendQueueOptions<T>,
): SendQueue<T, R> {
  const pending: Job<T, R>[] = [];
  let active: Job<T, R> | null = null;

  function settle(
    job: Job<T, R>,
    kind: "resolve" | "reject",
    value: unknown,
  ): void {
    if (job.settled) return;
    job.settled = true;
    if (job.abortHandler !== undefined && job.signal !== undefined) {
      job.signal.removeEventListener("abort", job.abortHandler);
    }
    if (kind === "resolve") {
      // The queue's value type is checked at enqueue / resolveActive; the
      // generic narrowing here is safe by construction.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- generic resolve value
      job.resolve(value as R);
    } else {
      job.reject(value);
    }
  }

  function pump(): void {
    while (active === null && pending.length > 0) {
      const next = pending.shift();
      if (next === undefined) return;
      if (next.signal?.aborted === true) {
        settle(next, "reject", abortReason(next.signal));
        continue;
      }
      active = next;
      opts.start(next.item);
      return;
    }
  }

  function enqueue(item: T, signal?: AbortSignal): Promise<R> {
    if (signal?.aborted === true) {
      return Promise.reject(abortReason(signal));
    }

    const depth = pending.length + (active !== null ? 1 : 0);
    if (depth >= opts.maxDepth) {
      throw new SendQueueFullError(opts.maxDepth);
    }

    let resolve!: (value: R) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<R>((res, rej) => {
      resolve = res;
      reject = rej;
    });

    const job: Job<T, R> = {
      item,
      ...(signal !== undefined ? { signal } : {}),
      resolve,
      reject,
      settled: false,
    };

    if (signal !== undefined) {
      const handler = (): void => {
        const reason = abortReason(signal);
        if (active === job) {
          // In flight: settle the caller now; the consumer will eventually
          // call resolveActive/rejectActive which becomes a no-op and
          // advances the queue.
          settle(job, "reject", reason);
        } else {
          const idx = pending.indexOf(job);
          if (idx >= 0) pending.splice(idx, 1);
          settle(job, "reject", reason);
        }
      };
      signal.addEventListener("abort", handler, { once: true });
      job.abortHandler = handler;
    }

    pending.push(job);
    pump();
    return promise;
  }

  function resolveActive(value: R): void {
    if (active === null) return;
    const job = active;
    active = null;
    settle(job, "resolve", value);
    pump();
  }

  function rejectActive(reason: unknown): void {
    if (active === null) return;
    const job = active;
    active = null;
    settle(job, "reject", reason);
    pump();
  }

  function drain(reason: unknown): void {
    const drained: Job<T, R>[] = [];
    if (active !== null) {
      drained.push(active);
      active = null;
    }
    drained.push(...pending);
    pending.length = 0;
    for (const job of drained) {
      settle(job, "reject", reason);
    }
  }

  return {
    enqueue,
    resolveActive,
    rejectActive,
    drain,
    get depth() {
      return pending.length + (active !== null ? 1 : 0);
    },
  };
}
