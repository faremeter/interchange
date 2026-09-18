// Waiting for a condition without betting on how long it takes.
//
// The shape this replaces is `await new Promise((r) => setTimeout(r, 50))`
// followed by an assertion: a duration picked to be longer than the work,
// which on a loaded machine is not, so the assertion reads a state that had
// not arrived yet. The interval decides whether the run passes.
//
// `waitUntil` removes the decision. It re-checks after yielding the event
// loop and carries no deadline, so a slow machine makes it take longer and
// never makes it fail; a condition that never holds is caught by the lane
// timeout, which is where CONVENTIONS.md puts the failsafe for a hang.
//
// It is second-best on purpose. CONVENTIONS.md asks for the SIGNAL -- the
// emitted event, the reported mutation -- and where a double can report what
// it did, that is better than this: it wakes exactly once, on the thing the
// test is actually waiting for, instead of spinning. Prefer a report. Reach
// for this where the state under test has no reporter and giving it one would
// mean reshaping production code for a test's convenience.

/**
 * Resolve once `predicate` returns true, re-checking after each event-loop
 * turn. No deadline: see the module comment.
 */
export async function waitUntil(predicate: () => boolean): Promise<void> {
  while (!predicate()) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
  }
}
