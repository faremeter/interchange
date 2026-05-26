// Tests for the `Scheduler` exposed by `setupHarness` via `deps.scheduler`.
// Both modes (`enableInferenceTimers: false` and `true`) report `now()`
// from the harness's virtual clock so the harness's authoritative time
// source agrees with whatever a consumer reads through the injected
// Scheduler. The inert-mode scheduler additionally guarantees that
// `setTimeout` is a no-op — only `now()` follows the clock.

import { describe, test, expect } from "bun:test";

import { setupHarness } from "@intx/inference-testing";
import type { Scheduler } from "@intx/inference";

// `Dependencies.scheduler` is structurally optional, but `setupHarness`
// always populates it; pulling it out through a tiny helper keeps the
// tests focused on the behaviour, not the optional-chain ceremony.
function getScheduler(harness: { deps: { scheduler?: Scheduler } }): Scheduler {
  const scheduler = harness.deps.scheduler;
  if (scheduler === undefined) {
    throw new Error("setupHarness did not populate deps.scheduler");
  }
  return scheduler;
}

describe("inference-testing Scheduler.now() (inert mode)", () => {
  test("starts at the virtual clock's current value", () => {
    const harness = setupHarness();
    try {
      expect(getScheduler(harness).now()).toBe(harness.clock.now());
    } finally {
      harness.dispose();
    }
  });

  test("tracks virtual time advanced by harness.advanceTo()", async () => {
    const harness = setupHarness();
    try {
      const scheduler = getScheduler(harness);
      const before = scheduler.now();
      await harness.advanceTo(250);
      const after = scheduler.now();
      expect(after - before).toBe(250);
      expect(after).toBe(harness.clock.now());
    } finally {
      harness.dispose();
    }
  });

  test("setTimeout remains a no-op even though now() tracks the clock", async () => {
    // The asymmetry is deliberate: production timers are suppressed,
    // but a test that advances the clock for unrelated reasons still
    // sees `now()` move. The callback handed to `setTimeout` must
    // not fire even after the clock advances well past `delayMs`.
    const harness = setupHarness();
    try {
      const scheduler = getScheduler(harness);
      let fired = false;
      scheduler.setTimeout(() => {
        fired = true;
      }, 10);
      await harness.advanceTo(1000);
      expect(fired).toBe(false);
    } finally {
      harness.dispose();
    }
  });
});

describe("inference-testing Scheduler.now() (enableInferenceTimers)", () => {
  test("tracks virtual time and fires setTimeout at virtualMs delayMs later", async () => {
    const harness = setupHarness({ enableInferenceTimers: true });
    try {
      const scheduler = getScheduler(harness);
      expect(scheduler.now()).toBe(harness.clock.now());
      const firings: number[] = [];
      scheduler.setTimeout(() => {
        firings.push(harness.clock.now());
      }, 100);
      await harness.advanceTo(100);
      expect(firings).toEqual([100]);
      expect(scheduler.now()).toBe(100);
    } finally {
      harness.dispose();
    }
  });
});
