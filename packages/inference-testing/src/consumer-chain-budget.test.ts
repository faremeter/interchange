// Regression test for the stability-window contract of the clock's
// `microtaskBudget`: the drain runs while activity advances AND for an
// internal stability window (16 microtask waves) after activity
// stabilizes. A consumer chain that keeps bumping activity for many
// waves per fired callback is what the budget exists to gate.
//
// The synthetic workload models the failure mode a future `parseSSE`
// refactor could introduce — a consumer taking many activity-emitting
// microtask waves per fired chunk — and asserts that a tight budget
// throws `ClockOverrunError` while the default `microtaskBudget=256`
// absorbs the same chain. Without the stability window the leftover
// consumer waves landed outside `clock.run()`'s accounting, so the
// budget knob did not actually gate consumer-chain bloat.

import { describe, test, expect } from "bun:test";

import { ClockOverrunError, createClock } from "./clock";

// Build a workload that, once fired, drives the activity counter
// forward for `waves` sequential microtask waves. Each wave settles via
// a `queueMicrotask`-resolved promise — the same construction
// `drainMicrotasks` uses — so chain progress maps 1:1 to drain
// iterations. `firedCallback` plays the SSE chunk delivery; the awaited
// chain models a consumer that reads, processes, signals, and loops.
function scheduleActivityChain(
  clock: ReturnType<typeof createClock>,
  waves: number,
): Promise<void> {
  // The `done` promise lets the test join on chain settlement (and
  // surface any rejection deterministically).
  let resolveDone!: () => void;
  let rejectDone!: (err: unknown) => void;
  const done = new Promise<void>((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  clock.schedule(1, function firedCallback() {
    // Mirror `SimulatedStream.enqueueAt`: a fired chunk bumps activity
    // once at delivery; the consumer chain below adds further bumps.
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
    // Observe the chain's rejection so bun:test does not warn about an
    // unhandled promise.
    chainDone.catch(() => undefined);
  });

  test("the default microtaskBudget=256 absorbs the same consumer chain cleanly", async () => {
    // 30 active waves + 16 stability iterations = 46 total per drain;
    // the default budget of 256 has comfortable headroom.
    const clock = createClock();
    const chainDone = scheduleActivityChain(clock, 30);
    await clock.run();
    await chainDone;
    expect(clock.now()).toBeGreaterThanOrEqual(1);
  });
});
