// In-process signal channel.
//
// A delivery pairs with the earliest awaiter of the same author name
// whose correlationId is equal (`undefined` with `undefined`). A
// correlated delivery does not wake a name-only awaiter, and a
// name-only delivery does not wake a correlated one. An unmatched
// delivery stays queued under the author name until such an awaiter
// arrives. Per-signal dedup by `signalId` is the state machine's
// job; this layer only tracks the dispatch queue.

import type { SignalChannel } from "../runtime/env";

interface Awaiter {
  resolve: (value: {
    payload: unknown;
    signalId: string;
    correlationId?: string;
  }) => void;
  reject: (cause: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  correlationId?: string;
}

interface QueuedDelivery {
  payload: unknown;
  signalId: string;
  correlationId?: string;
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
    async deliver(name, payload, signalId, correlationId) {
      const id = signalId ?? newId();
      const delivery: QueuedDelivery = {
        payload,
        signalId: id,
        ...(correlationId !== undefined ? { correlationId } : {}),
      };
      const queueAwaiters = awaiters.get(name);
      const match =
        queueAwaiters?.findIndex(
          (awaiter) => awaiter.correlationId === correlationId,
        ) ?? -1;
      if (queueAwaiters !== undefined && match >= 0) {
        const next = queueAwaiters[match];
        queueAwaiters.splice(match, 1);
        if (queueAwaiters.length === 0) awaiters.delete(name);
        if (next !== undefined) {
          if (next.signal && next.onAbort) {
            next.signal.removeEventListener("abort", next.onAbort);
          }
          next.resolve(delivery);
          return;
        }
      }
      let q = queued.get(name);
      if (!q) {
        q = [];
        queued.set(name, q);
      }
      q.push(delivery);
    },
    async awaitNext(name, signal, correlationId) {
      const q = queued.get(name);
      const queuedMatch =
        q?.findIndex((item) => item.correlationId === correlationId) ?? -1;
      if (q !== undefined && queuedMatch >= 0) {
        const next = q[queuedMatch];
        q.splice(queuedMatch, 1);
        if (q.length === 0) queued.delete(name);
        if (next !== undefined) {
          return next;
        }
      }
      return new Promise<{
        payload: unknown;
        signalId: string;
        correlationId?: string;
      }>((resolve, reject) => {
        const awaiter: Awaiter = {
          resolve,
          reject,
          ...(signal !== undefined ? { signal } : {}),
          ...(correlationId !== undefined ? { correlationId } : {}),
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
      });
    },
  };
}

function defaultNewId(): string {
  return `sig-${Math.random().toString(36).slice(2, 10)}`;
}
