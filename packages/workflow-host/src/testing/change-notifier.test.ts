import { describe, test, expect } from "bun:test";

import { createChangeNotifier } from "./change-notifier";

describe("createChangeNotifier", () => {
  test("until returns without waiting when the predicate already holds", async () => {
    const changes = createChangeNotifier();
    // No notify will ever arrive, so only the initial check can resolve this.
    // A bare wait on the next notify hangs here, which is the failure mode
    // that makes a report-based wait deadlock when the state it wants arrived
    // before the wait did.
    await changes.until(() => true);
  });

  test("until resolves on the notify that satisfies the predicate", async () => {
    const changes = createChangeNotifier();
    let state = 0;
    const waited = changes.until(() => state >= 2);
    let settled = false;
    void waited.then(() => {
      settled = true;
    });

    state = 1;
    changes.notify();
    await Promise.resolve();
    expect(settled).toBe(false);

    state = 2;
    changes.notify();
    await waited;
  });

  test("a change that happened before the wait still resolves it", async () => {
    const changes = createChangeNotifier();
    let state = 0;
    // The load-bearing property, and the one an edge-triggered wait lacks:
    // the predicate is re-checked rather than waited on, so state that
    // arrived before the wait was created satisfies it. The notify below is
    // consumed by nobody; only the re-check can resolve this.
    state = 5;
    changes.notify();
    await changes.until(() => state >= 5);
  });

  test("a waiter re-arming in its own continuation waits for the next change", async () => {
    const changes = createChangeNotifier();
    let ticks = 0;
    const seen: number[] = [];
    // Signalled by the chained waiter itself, so this test waits on its
    // progress rather than on a count of microtask turns -- which is the
    // same bet on timing the notifier exists to remove.
    const firstWaitDone = Promise.withResolvers<boolean>();
    const chained = (async () => {
      await changes.until(() => ticks >= 1);
      seen.push(ticks);
      firstWaitDone.resolve(true);
      // Re-arm from inside the continuation of the first wait. `notify`
      // empties the waiter list before waking any of it, so this lands in a
      // fresh list and waits for the NEXT notify rather than being woken by
      // the one still being delivered.
      await changes.until(() => ticks >= 2);
      seen.push(ticks);
    })();

    ticks = 1;
    changes.notify();
    await firstWaitDone.promise;
    // The re-armed wait is outstanding: its predicate is unsatisfied, and the
    // notify that released the first wait must not have released it too.
    expect(seen).toEqual([1]);

    ticks = 2;
    changes.notify();
    await chained;
    expect(seen).toEqual([1, 2]);
  });

  test("every waiter is woken, not just the most recent", async () => {
    const changes = createChangeNotifier();
    let ready = false;
    // A single-slot resolver -- the shape this replaced in two fixtures --
    // would strand the first of these two waits, because arming the second
    // overwrote the first's resolve.
    const first = changes.until(() => ready);
    const second = changes.until(() => ready);
    ready = true;
    changes.notify();
    await Promise.all([first, second]);
  });
});
