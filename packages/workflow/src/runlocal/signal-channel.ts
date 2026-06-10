// In-process FIFO single-consumer signal channel.
//
// Mirrors the state machine's signal semantics: a `SignalReceived`
// before any `SignalAwaited` is queued under the signal name and
// consumed by the next awaiter for that name. Per-signal dedup by
// `signalId` is enforced by the state machine; this layer only
// tracks the dispatch queue.

import type { SignalChannel } from "../runtime/env";

interface Awaiter {
  resolve: (value: { payload: unknown; signalId: string }) => void;
  reject: (cause: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface QueuedDelivery {
  payload: unknown;
  signalId: string;
}

export interface InMemorySignalChannelOptions {
  newId?: () => string;
}

export function createInMemorySignalChannel(
  opts: InMemorySignalChannelOptions = {},
): SignalChannel {
  const newId = opts.newId ?? defaultNewId;
  const awaiters = new Map<string, Awaiter[]>();
  const queued = new Map<string, QueuedDelivery[]>();

  return {
    async deliver(name, payload, signalId) {
      const id = signalId ?? newId();
      const queueAwaiters = awaiters.get(name);
      if (queueAwaiters && queueAwaiters.length > 0) {
        const next = queueAwaiters.shift();
        if (queueAwaiters.length === 0) awaiters.delete(name);
        if (next) {
          if (next.signal && next.onAbort) {
            next.signal.removeEventListener("abort", next.onAbort);
          }
          next.resolve({ payload, signalId: id });
          return;
        }
      }
      let q = queued.get(name);
      if (!q) {
        q = [];
        queued.set(name, q);
      }
      q.push({ payload, signalId: id });
    },
    async awaitNext(name, signal) {
      const q = queued.get(name);
      if (q && q.length > 0) {
        const next = q.shift();
        if (q.length === 0) queued.delete(name);
        if (next) {
          return next;
        }
      }
      return new Promise<{ payload: unknown; signalId: string }>(
        (resolve, reject) => {
          const awaiter: Awaiter = {
            resolve,
            reject,
            ...(signal !== undefined ? { signal } : {}),
          };
          if (signal !== undefined) {
            if (signal.aborted) {
              reject(new Error("aborted"));
              return;
            }
            const onAbort = (): void => {
              const list = awaiters.get(name);
              if (list) {
                const idx = list.indexOf(awaiter);
                if (idx >= 0) list.splice(idx, 1);
                if (list.length === 0) awaiters.delete(name);
              }
              reject(new Error("aborted"));
            };
            awaiter.onAbort = onAbort;
            signal.addEventListener("abort", onAbort, { once: true });
          }
          let list = awaiters.get(name);
          if (!list) {
            list = [];
            awaiters.set(name, list);
          }
          list.push(awaiter);
        },
      );
    },
  };
}

function defaultNewId(): string {
  return `sig-${Math.random().toString(36).slice(2, 10)}`;
}
