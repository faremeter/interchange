// Per-cohort terminal-run event broadcaster.
//
// The supervisor's workflow-process child commits each run's terminal
// event (`RunCompleted`, `RunFailed`, `RunCancelled`) through its own
// substrate handle and mirrors the event over the control IPC as a
// `terminal.event` upstream frame. The supervisor's `pumpUpstreamControl`
// calls `notify()` on this broadcaster when such a frame lands; the
// dispatch loop and any armed drainTimeout accumulator subscribe via
// `source(runId)` to await the matching terminal event without
// re-reading the workflow-run substrate from the supervisor's address
// space.
//
// One broadcaster instance per spawn cohort. The supervisor mints a
// fresh broadcaster inside `spawn()` and inside the recycle path's
// `installNewChild`; the previous cohort's broadcaster is disposed,
// which finalises every minted iterator with `done: true` (the
// disposed-side iterator settles its pending `next()` resolver
// directly so the consumer wakes up even if its own cohort-abort race
// has not landed yet).

import type { TerminalEventSource, TerminalRunEvent } from "./types";

type Listener = {
  onEvent: (event: TerminalRunEvent) => void;
  onDispose: () => void;
};

/**
 * Per-cohort terminal-run broadcaster. Lifetime matches the supervisor
 * spawn cohort's `terminalCohortAbort`; `dispose()` is invoked when the
 * cohort is torn down (shutdown or recycle's `installNewChild`).
 */
export interface TerminalBroadcaster {
  /**
   * Fan a terminal-run event out to every active subscriber listening
   * for the supplied `runId`. The broadcaster buffers one event per
   * subscriber if the consumer's first `next()` has not yet been
   * awaited so a notification that lands between `subscribe` and the
   * first `next()` is still delivered. Notifications for runIds with
   * no listeners are dropped (the dispatch loop and the drain
   * accumulator both arm before the corresponding trigger.fire /
   * drain mail is forwarded to the child).
   */
  notify(runId: string, event: TerminalRunEvent): void;
  /**
   * `TerminalEventSource`-shaped accessor for consumers. Each call
   * yields an `AsyncIterable<TerminalRunEvent>` scoped to one `runId`.
   * The iterator yields the first terminal event the broadcaster fans
   * out for that runId (or `done: true` if the broadcaster is disposed
   * first) and ends.
   */
  readonly source: TerminalEventSource;
  /**
   * Finalise every minted iterator with `done: true` and drop every
   * listener. Called on cohort teardown (shutdown / recycle).
   */
  dispose(): void;
  /**
   * Whether `dispose()` has been called. Post-dispose, `source(...)`
   * still returns an iterable whose first `next()` immediately yields
   * `{done: true}` -- callers do not need to guard against the cohort
   * being torn down before they subscribe.
   */
  readonly disposed: boolean;
}

/**
 * Construct a fresh per-cohort terminal-run broadcaster. The fan-out is
 * single-shot per listener: a notification settles every matching
 * listener exactly once, dropping the listener afterwards.
 */
export function createTerminalBroadcaster(): TerminalBroadcaster {
  const listenersByRunId = new Map<string, Set<Listener>>();
  let disposed = false;

  function addListener(runId: string, listener: Listener): () => void {
    let set = listenersByRunId.get(runId);
    if (set === undefined) {
      set = new Set();
      listenersByRunId.set(runId, set);
    }
    set.add(listener);
    return () => {
      const current = listenersByRunId.get(runId);
      if (current === undefined) return;
      current.delete(listener);
      if (current.size === 0) listenersByRunId.delete(runId);
    };
  }

  const source: TerminalEventSource = (runId) => ({
    [Symbol.asyncIterator](): AsyncIterator<TerminalRunEvent> {
      let resolved = false;
      let unsubscribe: (() => void) | null = null;
      let pending: TerminalRunEvent | null = null;
      let resolveNext:
        | ((result: IteratorResult<TerminalRunEvent>) => void)
        | null = null;

      function deliverEvent(event: TerminalRunEvent): void {
        if (resolved) return;
        if (resolveNext !== null) {
          const resolver = resolveNext;
          resolveNext = null;
          resolved = true;
          if (unsubscribe !== null) {
            unsubscribe();
            unsubscribe = null;
          }
          resolver({ value: event, done: false });
          return;
        }
        pending = event;
      }

      function deliverDispose(): void {
        if (resolved) return;
        if (resolveNext !== null) {
          const resolver = resolveNext;
          resolveNext = null;
          resolved = true;
          if (unsubscribe !== null) {
            unsubscribe();
            unsubscribe = null;
          }
          resolver({ value: undefined, done: true });
        }
        // No pending resolver: leave `resolved` false and rely on
        // the next `next()` call to observe `disposed === true` and
        // return `{done: true}`.
      }

      if (disposed) {
        resolved = true;
      } else {
        unsubscribe = addListener(runId, {
          onEvent: deliverEvent,
          onDispose: deliverDispose,
        });
      }

      return {
        next(): Promise<IteratorResult<TerminalRunEvent>> {
          if (resolved) {
            return Promise.resolve({ value: undefined, done: true });
          }
          if (pending !== null) {
            const event = pending;
            pending = null;
            resolved = true;
            if (unsubscribe !== null) {
              unsubscribe();
              unsubscribe = null;
            }
            return Promise.resolve({ value: event, done: false });
          }
          if (disposed) {
            resolved = true;
            if (unsubscribe !== null) {
              unsubscribe();
              unsubscribe = null;
            }
            return Promise.resolve({ value: undefined, done: true });
          }
          return new Promise<IteratorResult<TerminalRunEvent>>((resolve) => {
            resolveNext = resolve;
          });
        },
        return(): Promise<IteratorResult<TerminalRunEvent>> {
          resolved = true;
          if (unsubscribe !== null) {
            unsubscribe();
            unsubscribe = null;
          }
          if (resolveNext !== null) {
            const resolver = resolveNext;
            resolveNext = null;
            resolver({ value: undefined, done: true });
          }
          return Promise.resolve({ value: undefined, done: true });
        },
      };
    },
  });

  return {
    notify(runId, event): void {
      if (disposed) return;
      const set = listenersByRunId.get(runId);
      if (set === undefined) return;
      const snapshot = [...set];
      for (const listener of snapshot) {
        listener.onEvent(event);
      }
    },
    source,
    dispose(): void {
      if (disposed) return;
      disposed = true;
      const snapshot: Listener[] = [];
      for (const set of listenersByRunId.values()) {
        for (const listener of set) snapshot.push(listener);
      }
      listenersByRunId.clear();
      for (const listener of snapshot) {
        listener.onDispose();
      }
    },
    get disposed() {
      return disposed;
    },
  };
}
