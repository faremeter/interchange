// Level-triggered wait over a mutable test double: the double calls `notify`
// after each mutation and the waiter re-evaluates the predicate only then, so
// no tick interval or deadline decides whether a test passes. `until`
// evaluates on entry to every pass of its loop, so a state already reached
// resolves it; a `notify` with no waiter registered is dropped, so the double
// must notify after every mutation. This shares the waiting only -- the inbox
// doubles themselves are genuinely different implementations and stay
// separate.

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
      // Clearing the list bounds it: every wait pass leaves one resolver
      // behind, and without the clear they accumulate for the life of the
      // notifier. Clearing before the wake is what lets a re-arming waiter
      // wait for the next change: waking a settled resolver is a no-op, and
      // the loop re-registers only after `await changed` resumes.
      const waking = waiters;
      waiters = [];
      for (const waiter of waking) waiter();
    },
    async until(predicate) {
      for (;;) {
        // Re-checked on every pass, so a change that already happened before
        // the wait resolves it. The registering executor runs synchronously,
        // so no notify can land between the check and the registration in
        // either order.
        const changed = new Promise<void>((resolve) => {
          waiters.push(resolve);
        });
        if (predicate()) return;
        await changed;
      }
    },
  };
}
