// Regression test for the stability-window contract of the clock's
// `microtaskBudget`. The drain runs while activity advances AND for an
// internal stability window (16 microtask waves) after activity
// stabilizes; a consumer chain that keeps bumping activity for many
// waves per fired callback is what the budget exists to gate.
//
// The synthetic workload here is a stand-in for the failure mode a
// future `parseSSE` refactor could introduce: a consumer that takes
// many activity-emitting microtask waves per fired chunk. The test
// asserts that
//
//   - a budget tight enough to expose the chain throws
//     `ClockOverrunError`, and
//   - the default `microtaskBudget=256` has enough headroom to absorb
//     the same chain cleanly.
//
// Without the stability window the first stable flush ended the drain
// and the leftover consumer waves landed outside `clock.run()`'s
// accounting, so the budget knob did not actually gate consumer-chain
// bloat. This test pins the corrected contract.

import { describe, test, expect } from "bun:test";

import { ClockOverrunError, createClock } from "./clock";

// Build a workload that, once fired, drives the activity counter
// forward for `waves` sequential microtask waves. Each wave settles via
// a `queueMicrotask`-resolved promise — the same construction
// `drainMicrotasks` uses for its own pump — so chain progress maps 1:1
// to drain iterations. `firedCallback` plays the role of the SSE chunk
// delivery; the awaited chain models a consumer that reads, processes,
// signals, and loops.
function scheduleActivityChain(
  clock: ReturnType<typeof createClock>,
  waves: number,
): Promise<void> {
  // The `done` promise resolves once the chain finishes so the test can
  // join on settlement (and surface any rejection deterministically).
  let resolveDone!: () => void;
  let rejectDone!: (err: unknown) => void;
  const done = new Promise<void>((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  clock.schedule(1, function firedCallback() {
    // Mirror `SimulatedStream.enqueueAt`: a fired chunk bumps activity
    // once at delivery. The consumer chain below adds further bumps as
    // it drives microtask waves through its body.
    clock.notifyActivity();
    const chain = async (): Promise<void> => {
      for (let i = 0; i < waves; i++) {
        await new Promise<void>((resolve) => {
          queueMicrotask(resolve);
        });
        clock.notifyActivity();
      }
    };
    chain().then(resolveDone, rejectDone);
  });
  return done;
}

describe("microtask budget stability window", () => {
  test("a tight budget surfaces ClockOverrunError when a consumer chain inflates past it", async () => {
    // With STABILITY_WINDOW=16, a budget of 32 gives at most 16
    // activity-bumping waves of headroom (the remaining 16 iterations
    // are the stability window itself). A chain of 30 waves overruns
    // that budget.
    const clock = createClock();
    const chainDone = scheduleActivityChain(clock, 30);
    let caught: unknown = null;
    try {
      await clock.run({ microtaskBudget: 32 });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClockOverrunError);
    // Detach the chain's settlement so it does not pollute later tests
    // in this file. The clock has already errored; we only need to
    // observe the rejection so bun:test does not warn about an
    // unhandled promise.
    chainDone.catch(() => undefined);
  });

  test("the default microtaskBudget=256 absorbs the same consumer chain cleanly", async () => {
    // 30 active waves + 16 stability iterations = 46 total per drain.
    // The default budget of 256 has comfortable headroom.
    const clock = createClock();
    const chainDone = scheduleActivityChain(clock, 30);
    await clock.run();
    await chainDone;
    expect(clock.now()).toBeGreaterThanOrEqual(1);
  });
});
