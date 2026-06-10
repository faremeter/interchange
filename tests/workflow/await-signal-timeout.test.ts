// awaitSignal timeout enforcement.
//
// The `awaitSignal` primitive's `timeout` is committed to the log as
// `SignalAwaited.timeoutAt`. Without runtime enforcement the log
// records a deadline the runtime never honors -- a workflow whose
// signal never arrives hangs forever. The runtime arms the
// scheduler against the deadline; expiry aborts the channel wait and
// the safe-runner commits a StepFailed.

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";

import { awaitSignal, defineWorkflow, runLocal } from "@intx/workflow";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

describe("awaitSignal timeout", () => {
  test("fires the timeout when the signal does not arrive", async () => {
    void makeAgent;
    const def = defineWorkflow({
      id: "wait-timeout",
      trigger: { type: "manual" },
      steps: {
        wait: awaitSignal({ name: "approve", timeout: 30 }),
      },
    });
    const result = await runLocal(def).complete;
    expect(result.terminalStatus).toBe("failed");
    const stepFailed = result.events.find(
      (e) => e.kind === "StepFailed" && e.stepId === "wait",
    );
    expect(stepFailed).toBeDefined();
    // The timeout must commit paired TimerSet + TimerFired so the
    // log carries the deadline as a first-class event. A production
    // scheduler reading the log at startup for unfired timers
    // depends on TimerSet being present.
    const timerSet = result.events.find(
      (e) => e.kind === "TimerSet" && e.stepId === "wait",
    );
    expect(timerSet).toBeDefined();
    const timerSetId =
      timerSet?.kind === "TimerSet" ? timerSet.timerId : undefined;
    const timerFired = result.events.find(
      (e) => e.kind === "TimerFired" && e.timerId === timerSetId,
    );
    expect(timerFired).toBeDefined();
  });

  test("signal arriving before the timeout commits no TimerFired", async () => {
    void makeAgent;
    const def = defineWorkflow({
      id: "wait-signal-first",
      trigger: { type: "manual" },
      steps: {
        wait: awaitSignal({ name: "approve", timeout: 5000 }),
      },
    });
    const run = runLocal(def);
    await run.signal("approve", { ok: true });
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    const timerSet = result.events.find(
      (e) => e.kind === "TimerSet" && e.stepId === "wait",
    );
    expect(timerSet).toBeDefined();
    const timerFired = result.events.find((e) => e.kind === "TimerFired");
    // No TimerFired -- the scheduler's disposer cancelled the timer.
    expect(timerFired).toBeUndefined();
  });
});
