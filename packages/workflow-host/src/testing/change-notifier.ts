// Level-triggered wait over a mutable test double.
//
// A test that needs a double to have reached some state -- two entries
// consumed, an entry moved to processing -- has no event to await, because
// the state lives in a map the double mutates in place. The usual stand-in is
// a predicate re-evaluated on a timer, which makes the tick interval and the
// deadline part of whether the test passes.
//
// This inverts it: the double reports that it changed, and the waiter
// re-evaluates the predicate only then. The predicate is still a predicate --
// there is no event to name when the subject is "the map now looks like this"
// -- but nothing is re-read on a schedule and no duration decides the
// outcome.
//
// `until` evaluates the predicate on entry to every pass of its loop, so a
// state the double already reached resolves it and a caller never has to know
// whether it armed the wait in time. Only `notify` is an edge: one with no
// waiter registered is dropped, which is why the double must call it after
// every mutation rather than only when it thinks someone is watching.
//
// It is deliberately NOT a consolidation of the doubles themselves. The five
// inbox doubles in this package are genuinely different implementations
// rather than cosmetic variants, and a merged superset could not be shown to
// preserve what each test relies on. Sharing the waiting is the part that was
// worth sharing.

export type ChangeNotifier = {
  /** Call from the double after any mutation a waiter might care about. */
  notify(): void;
  /**
   * Resolve once `predicate` holds. Evaluated immediately, then again after
   * each `notify`. Carries no deadline: a predicate that never holds is
   * caught by the lane timeout, per "Synchronizing on State, Not Time" in
   * CONVENTIONS.md.
   */
  until(predicate: () => boolean): Promise<void>;
};

export function createChangeNotifier(): ChangeNotifier {
  let waiters: (() => void)[] = [];
  return {
    notify() {
      // Clearing the list is what bounds it. Every pass of the wait loop
      // below registers a resolver, and the pass that returns leaves its own
      // resolver behind unsettled; without the clear those accumulate for the
      // life of the notifier. Measured over 50 notifies with one waiter: 1
      // entry with the clear, 51 without.
      //
      // Clearing BEFORE the wake rather than after is not what makes a
      // re-arming waiter wait for the next change instead of this one. Waking
      // a settled resolver is a no-op, and the loop re-registers only after
      // `await changed` resumes, which is a microtask boundary -- by then this
      // function has finished iterating. A mutant that wakes the live list and
      // clears afterwards passes every test that uses this helper.
      const waking = waiters;
      waiters = [];
      for (const waiter of waking) waiter();
    },
    async until(predicate) {
      for (;;) {
        // Re-checked on every pass, which is what makes this usable when the
        // change already happened before the wait was created. The registering
        // executor runs synchronously, so no notify can land between the check
        // and the registration in either order -- the loop is the invariant,
        // not the order of those two lines.
        const changed = new Promise<void>((resolve) => {
          waiters.push(resolve);
        });
        if (predicate()) return;
        await changed;
      }
    },
  };
}
