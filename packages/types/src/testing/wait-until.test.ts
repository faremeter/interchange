import { describe, test, expect } from "bun:test";

import { waitUntil } from "./wait-until";

describe("waitUntil", () => {
  test("returns without a timer turn when the condition already holds", async () => {
    let armedTimerRan = false;
    const armed = setTimeout(() => {
      armedTimerRan = true;
    }, 0);
    await waitUntil(() => true);
    clearTimeout(armed);
    // The only yield in `waitUntil` is its loop body's `setTimeout`, which a
    // predicate holding on entry never reaches -- so the returned promise
    // settles on the microtask queue, and microtasks drain before any timer
    // callback runs. Had it yielded, its own `setTimeout` would sit behind the
    // one armed above and this flag would be set by the time the await
    // resumed. The `0` is an ordering probe, not a duration: no delay makes a
    // timer callback overtake the microtask queue.
    expect(armedTimerRan).toBe(false);
  });

  test("returns once the condition becomes true", async () => {
    let ready = false;
    setTimeout(() => {
      ready = true;
    }, 5);
    await waitUntil(() => ready);
    expect(ready).toBe(true);
  });

  test("outlasts a condition that takes many turns", async () => {
    // No deadline, so the only thing a slow machine changes is how many
    // turns this takes. A fixed pause in its place is what decides a run:
    // too short on a loaded worker and the caller reads a state that has
    // not arrived.
    let turns = 0;
    await waitUntil(() => {
      turns += 1;
      return turns > 50;
    });
    expect(turns).toBeGreaterThan(50);
  });
});
