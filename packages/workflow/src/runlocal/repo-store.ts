// In-memory event log keyed by run id.

import type { RepoStore, SubscribeOpts } from "../runtime/env";
import type { WorkflowEvent } from "../state-machine/index";

const DEFAULT_BUFFER_LIMIT = 1024;

type Subscriber = {
  bufferLimit: number;
  buffer: { seq: number; event: WorkflowEvent }[];
  closed: boolean;
  error: Error | null;
  /** Resolver for an awaiting `next()` call. */
  waiter:
    | ((value: IteratorResult<{ seq: number; event: WorkflowEvent }>) => void)
    | null;
};

export function createInMemoryRepoStore(): RepoStore {
  const logs = new Map<string, WorkflowEvent[]>();
  // One subscriber set per runId. The set is keyed on runId, not on
  // subscription lifetime; a fresh subscription on a runId whose
  // previous subscribers have all unsubscribed creates a new set.
  const subscribers = new Map<string, Set<Subscriber>>();

  function notify(
    runId: string,
    entry: { seq: number; event: WorkflowEvent },
  ): void {
    const set = subscribers.get(runId);
    if (set === undefined) return;
    for (const sub of set) {
      if (sub.closed) continue;
      if (sub.buffer.length >= sub.bufferLimit) {
        // Crash on overrun rather than silently dropping. Dropping an
        // event corrupts the consumer's view of the log; the
        // workflow-runtime treats observability loss as an invariant
        // violation.
        sub.error = new Error(
          `repo_store_subscribe_buffer_overrun: runId=${runId} limit=${String(sub.bufferLimit)}`,
        );
        sub.closed = true;
        if (sub.waiter !== null) {
          const w = sub.waiter;
          sub.waiter = null;
          w({ value: undefined, done: true });
        }
        continue;
      }
      sub.buffer.push(entry);
      if (sub.waiter !== null) {
        const w = sub.waiter;
        sub.waiter = null;
        const head = sub.buffer.shift();
        if (head === undefined) {
          w({ value: undefined, done: true });
        } else {
          w({ value: head, done: false });
        }
      }
    }
  }

  return {
    async read(runId) {
      return logs.get(runId) ?? [];
    },
    async append(runId, event) {
      appendOne(logs, notify, runId, event);
    },
    async appendBatch(runId, events) {
      // One logical commit: validate-and-apply every event in seq
      // order, so a batch with a same-seq idempotent re-seed at its
      // head and fresh events after it lands coherently. The in-memory
      // store has no separate "commit" boundary to coalesce, so the
      // batch is simply each event applied in order; correctness
      // (monotonicity, append-only) is identical to N single appends.
      for (const event of events) {
        appendOne(logs, notify, runId, event);
      }
    },
    subscribe(runId, opts) {
      return createSubscription(runId, opts, logs, subscribers);
    },
  };
}

function createSubscription(
  runId: string,
  opts: SubscribeOpts,
  logs: Map<string, WorkflowEvent[]>,
  subscribers: Map<string, Set<Subscriber>>,
): AsyncIterableIterator<{ seq: number; event: WorkflowEvent }> {
  const bufferLimit = opts.bufferLimit ?? DEFAULT_BUFFER_LIMIT;
  if (!Number.isInteger(bufferLimit) || bufferLimit <= 0) {
    throw new Error(
      `repo_store_subscribe_buffer_limit_invalid: ${String(opts.bufferLimit)}`,
    );
  }

  const sub: Subscriber = {
    bufferLimit,
    buffer: [],
    closed: false,
    error: null,
    waiter: null,
  };

  let set = subscribers.get(runId);
  if (set === undefined) {
    set = new Set();
    subscribers.set(runId, set);
  }
  set.add(sub);

  const remove = (): void => {
    const current = subscribers.get(runId);
    if (current === undefined) return;
    current.delete(sub);
    if (current.size === 0) subscribers.delete(runId);
  };

  const closeNow = (): void => {
    sub.closed = true;
    remove();
    if (sub.waiter !== null) {
      const w = sub.waiter;
      sub.waiter = null;
      w({ value: undefined, done: true });
    }
  };

  if (opts.signal.aborted) {
    closeNow();
  } else {
    opts.signal.addEventListener("abort", closeNow, { once: true });
  }

  // Replay historical events when `from: { seq }`. Replay is staged
  // through the buffer so buffer-limit bookkeeping is uniform across
  // replay and live paths. When `from: "head"`, the iterator starts
  // empty and surfaces only events committed after subscription.
  const log = logs.get(runId) ?? [];
  if (opts.from !== "head") {
    const fromSeq = opts.from.seq;
    for (const event of log) {
      if (event.seq < fromSeq) continue;
      if (sub.buffer.length >= sub.bufferLimit) {
        sub.error = new Error(
          `repo_store_subscribe_buffer_overrun: runId=${runId} limit=${String(sub.bufferLimit)} during replay`,
        );
        sub.closed = true;
        break;
      }
      sub.buffer.push({ seq: event.seq, event });
    }
  }

  const iterator: AsyncIterableIterator<{
    seq: number;
    event: WorkflowEvent;
  }> = {
    [Symbol.asyncIterator]() {
      return iterator;
    },
    async next() {
      if (sub.error !== null) {
        const err = sub.error;
        sub.error = null;
        closeNow();
        throw err;
      }
      if (sub.buffer.length > 0) {
        const head = sub.buffer.shift();
        if (head === undefined) return { value: undefined, done: true };
        return { value: head, done: false };
      }
      if (sub.closed) return { value: undefined, done: true };
      return new Promise<IteratorResult<{ seq: number; event: WorkflowEvent }>>(
        (resolve) => {
          sub.waiter = resolve;
        },
      );
    },
    async return() {
      closeNow();
      return { value: undefined, done: true };
    },
  };
  return iterator;
}

function appendOne(
  logs: Map<string, WorkflowEvent[]>,
  notify: (runId: string, entry: { seq: number; event: WorkflowEvent }) => void,
  runId: string,
  event: WorkflowEvent,
): void {
  let log = logs.get(runId);
  if (!log) {
    log = [];
    logs.set(runId, log);
  }
  const last = log[log.length - 1];
  if (last && last.seq >= event.seq) {
    // A same-seq append is an idempotent re-seed only when the
    // payload is structurally identical (the resume seam relies
    // on this behavior). A same-seq append whose kind or content
    // differs would silently drop a real event and corrupt the
    // log; reject it with a diagnostic that names both sides.
    // Non-monotonic appends with a lower seq are also rejected.
    if (last.seq === event.seq) {
      if (!eventsEqual(last, event)) {
        throw new Error(
          `same-seq conflict at ${runId} seq ${String(event.seq)}: store holds ${last.kind}, append carries ${event.kind}; payloads do not match`,
        );
      }
      return;
    }
    throw new Error(
      `non-monotonic append to ${runId}: last seq ${String(last.seq)}, event seq ${String(event.seq)}`,
    );
  }
  log.push(event);
  notify(runId, { seq: event.seq, event });
}

/**
 * Structural equality check for two events at the same seq. The
 * canonical-JSON comparison is good enough for the in-memory store --
 * events are plain JSON-serializable objects by the state-machine
 * contract -- and treats key ordering and undefined fields as
 * insignificant.
 */
function eventsEqual(a: WorkflowEvent, b: WorkflowEvent): boolean {
  return canonicalize(a) === canonicalize(b);
}

function canonicalize(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? "null";
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalize).join(",")}]`;
  }
  const entries = Object.entries(value)
    .filter(([, v]) => v !== undefined)
    .sort(([l], [r]) => (l < r ? -1 : l > r ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalize(v)}`).join(",")}}`;
}
