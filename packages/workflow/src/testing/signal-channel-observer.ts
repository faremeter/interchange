// A `SignalChannel` that reports when the run parks on it.
//
// `waitForEvent` covers a FRESH park, because `parkOnSignal` commits a
// `SignalAwaited` and flushes it before waiting. It cannot cover a RE-PARK: a
// run resuming from a durable log finds its step already `awaiting-signal`,
// so `parkOnSignal` re-adopts the seed's `SignalAwaited` and skips the
// re-emit. Nothing new reaches the log, and a log waiter resolves off the
// seed -- before the run has re-parked -- which makes every "no second
// SignalAwaited was minted" assertion read pre-crash state and pass for the
// wrong reason.
//
// The one observable the re-park does produce is the channel registration
// itself: `parkOnSignal`'s tail calls `awaitNext(name)`. This wraps a channel
// to count those calls per signal name, so a test can wait for the park it
// cares about.
//
// Counting, rather than reporting the next call, is the load-bearing choice.
// A test does not control whether the run re-parks before or after it asks to
// be told, so an edge-triggered "resolve on the next `awaitNext`" deadlocks
// on exactly the interleaving that is most common -- the run parks first. The
// count is level-triggered: asking for a count already reached returns.
// `readCount`/`awaitReadCount` in `@intx/workflow-host/testing` is the same
// pair for the same reason.

import { createInMemorySignalChannel } from "../runlocal/signal-channel";
import type { SignalChannel } from "../runtime/env";

export type ObservedSignalChannel = SignalChannel & {
  /** How many times the run has called `awaitNext` for this name so far. */
  awaitedCount(name: string): number;
  /**
   * Resolve once `awaitNext` has been called for `name` at least `count`
   * times, including when it already has been.
   */
  awaitAwaitedCount(name: string, count: number): Promise<void>;
};

/**
 * Wrap a fresh in-memory signal channel so a test can await the run parking
 * on it. `deliver` and `awaitNext` behave exactly as the unwrapped channel
 * does; the observation is additive.
 */
export function createObservedSignalChannel(): ObservedSignalChannel {
  const inner = createInMemorySignalChannel();
  const counts = new Map<string, number>();
  let waiters: (() => void)[] = [];

  function announce(): void {
    // Clearing the list is what bounds it. Every pass of the wait loop below
    // registers a resolver, and the pass that returns leaves its own resolver
    // behind unsettled; clearing here is what discards those rather than
    // accumulating one per park for the life of the channel. Measured over 50
    // parks with one waiter: 1 entry with the clear, 51 without.
    //
    // Clearing BEFORE the wake rather than after decides neither of those. It
    // is not what makes a re-arming waiter wait for the next park instead of
    // the current one, and it is not what bounds the list either. Waking a
    // settled resolver is a no-op, and the loop re-registers only after this
    // function has finished iterating, so both orders behave identically and
    // both hold the list at 1 -- a mutant that wakes the live list and clears
    // afterwards passes every test in the sibling test file.
    const waking = waiters;
    waiters = [];
    for (const waiter of waking) waiter();
  }

  function countOf(name: string): number {
    const seen = counts.get(name);
    // A name with no entry is one the run has not awaited, which is zero. The
    // map records observations; it is not a source of truth that can fail.
    return seen === undefined ? 0 : seen;
  }

  return {
    deliver(name, payload, signalId) {
      return inner.deliver(name, payload, signalId);
    },
    awaitNext(name, signal) {
      counts.set(name, countOf(name) + 1);
      announce();
      return inner.awaitNext(name, signal);
    },
    awaitedCount: countOf,
    async awaitAwaitedCount(name, count) {
      if (!Number.isInteger(count) || count < 1) {
        throw new Error(
          `awaitAwaitedCount: count must be a positive integer, got ${String(count)}`,
        );
      }
      for (;;) {
        // Re-checking on every pass is the invariant: it is what lets a park
        // that already happened satisfy the wait. Drop the check and await
        // the next report instead, and the "satisfied by a park that happened
        // BEFORE the call" test hangs -- which is the failure mode this shape
        // exists to avoid.
        //
        // The ORDER of the registration and the check is not the invariant,
        // and no comment should claim it is. The executor runs synchronously,
        // so no `awaitNext` can land between them in either arrangement;
        // swapping the two lines passes every test in the sibling test file.
        const awaited = new Promise<void>((resolve) => {
          waiters.push(resolve);
        });
        if (countOf(name) >= count) return;
        await awaited;
      }
    },
  };
}
