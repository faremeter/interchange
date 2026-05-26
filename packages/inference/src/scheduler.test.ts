// Tests for the `Scheduler` time source. The harness reads
// `scheduler.now()` to time deltas across multiple operations within a
// single call; the contract that matters is monotonicity and the same
// time domain as `setTimeout`. We do not assert specific values — the
// production default uses `performance.now()` which is sub-ms and
// floating-point.

import { describe, test, expect } from "bun:test";

import { createDefaultScheduler } from "./harness";

describe("createDefaultScheduler", () => {
  test("now() returns a finite number", () => {
    const scheduler = createDefaultScheduler();
    const value = scheduler.now();
    expect(Number.isFinite(value)).toBe(true);
  });

  test("now() is monotonic: a second read is not less than the first", () => {
    const scheduler = createDefaultScheduler();
    const a = scheduler.now();
    const b = scheduler.now();
    expect(b).toBeGreaterThanOrEqual(a);
  });

  test("now() advances across a setTimeout that fires", async () => {
    const scheduler = createDefaultScheduler();
    const before = scheduler.now();
    await new Promise<void>((resolve) => {
      scheduler.setTimeout(() => {
        resolve();
      }, 5);
    });
    const after = scheduler.now();
    // Real wall-clock time elapsed across the await must be reflected
    // in `now()` — the time domain of `setTimeout` and `now()` is the
    // contract that lets the retry wrapper compute `elapsedMs` from
    // two `now()` reads sandwiching a `setTimeout`-driven delay.
    expect(after - before).toBeGreaterThanOrEqual(5);
  });
});
