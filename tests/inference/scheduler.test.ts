// Tests for the `Scheduler` exposed by `setupHarness` via `deps.scheduler`.
// Both modes (`enableInferenceTimers: false` and `true`) report `now()`
// from the harness's virtual clock so the harness's authoritative time
// source agrees with whatever a consumer reads through the injected
// Scheduler. The inert-mode scheduler additionally guarantees that
// `setTimeout` is a no-op — only `now()` follows the clock.

import { describe, test, expect } from "bun:test";

import { setupHarness } from "@intx/inference-testing";

describe("inference-testing Scheduler.now() (inert mode)", () => {
  test("starts at the virtual clock's current value", () => {
    const harness = setupHarness();
    try {
      expect(harness.deps.scheduler.now()).toBe(harness.clock.now());
    } finally {
      harness.dispose();
    }
  });

  test("tracks virtual time advanced by harness.advanceTo()", async () => {
    const harness = setupHarness();
    try {
      const before = harness.deps.scheduler.now();
      await harness.advanceTo(250);
      const after = harness.deps.scheduler.now();
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
      let fired = false;
      harness.deps.scheduler.setTimeout(() => {
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
      expect(harness.deps.scheduler.now()).toBe(harness.clock.now());
      const firings: number[] = [];
      harness.deps.scheduler.setTimeout(() => {
        firings.push(harness.clock.now());
      }, 100);
      await harness.advanceTo(100);
      expect(firings).toEqual([100]);
      expect(harness.deps.scheduler.now()).toBe(100);
    } finally {
      harness.dispose();
    }
  });
});
