// Waiters that resolve on a run's committed events.
//
// The runtime interface documents tailing `repoStore.subscribe` as how a
// caller awaits a commit, and these are that pattern for tests: a test that
// needs a run to have reached some state waits for the event proving it
// rather than for an interval long enough to assume it.
//
// Subscribing from seq 0 replays what is already committed before delivering
// live events, so an event that landed before the call counts exactly as a
// later one does. Neither waiter carries a deadline: an event that never
// arrives is caught by the lane timeout.

import type { RepoStore } from "../runtime/env";
import type { WorkflowEvent } from "../state-machine/index";

/**
 * Resolve with the `count`-th event `match` accepts, counting from the start
 * of the log.
 *
 * Note it is the count-th match, not the most recent one once `count` exist.
 * The two differ whenever more than `count` matching events are already
 * committed when the call is made.
 */
export async function waitForNthEvent(
  repoStore: RepoStore,
  runId: string,
  match: (event: WorkflowEvent) => boolean,
  count = 1,
): Promise<WorkflowEvent> {
  if (!Number.isInteger(count) || count < 1) {
    throw new Error(`waitForNthEvent: count must be a positive integer`);
  }
  const abort = new AbortController();
  let seen = 0;
  try {
    for await (const { event } of repoStore.subscribe(runId, {
      signal: abort.signal,
      from: { seq: 0 },
    })) {
      if (!match(event)) continue;
      seen += 1;
      if (seen >= count) return event;
    }
  } finally {
    // Ends the subscription whether the match arrived or the loop was left by
    // a throw, so the store is not left holding a subscriber for this waiter.
    //
    // Reads as redundant against an in-memory store and is not: `for await`
    // exit calls the iterator's `return()`, and an implementation that honours
    // it has already closed the subscription -- but honouring `return()` is not
    // required of an iterator, and `RepoStore` is an interface (production
    // wraps the substrate's `subscribeKind`). This line is what ends the
    // subscription for an implementation that does not.
    abort.abort();
  }
  throw new Error(
    `log for ${runId} ended after ${String(seen)} of ${String(count)} matching event(s)`,
  );
}

/** Resolve once the run's log holds an event `match` accepts. */
export async function waitForEvent(
  repoStore: RepoStore,
  runId: string,
  match: (event: WorkflowEvent) => boolean,
): Promise<WorkflowEvent> {
  return waitForNthEvent(repoStore, runId, match, 1);
}
