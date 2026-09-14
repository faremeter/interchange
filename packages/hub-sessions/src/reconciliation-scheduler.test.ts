import { describe, expect, test } from "bun:test";

import { createReconciliationScheduler } from "./reconciliation-scheduler";

describe("reconciliation scheduling", () => {
  test("defaults to eight slots and replaces finished work without a batch barrier", async () => {
    const completions = Array.from({ length: 10 }, () =>
      Promise.withResolvers<boolean>(),
    );
    const firstWave = Promise.withResolvers<boolean>();
    const ninthStarted = Promise.withResolvers<boolean>();
    let started = 0;
    const scheduler = createReconciliationScheduler({
      name: "allocation test",
      reconcileNext() {
        const completion = completions[started];
        started += 1;
        if (started === 8) firstWave.resolve(true);
        if (started === 9) ninthStarted.resolve(true);
        return completion?.promise ?? Promise.resolve(false);
      },
    });
    scheduler.start();
    try {
      await firstWave.promise;
      scheduler.wake();
      scheduler.wake();
      expect(started).toBe(8);

      completions[1]?.resolve(true);
      await ninthStarted.promise;
      expect(started).toBe(9);
    } finally {
      scheduler.stop();
      for (const completion of completions) completion.resolve(false);
    }
  });

  test("polls newly queued work while another operation is still pending", async () => {
    const held = Promise.withResolvers<boolean>();
    const idleSlot = Promise.withResolvers<boolean>();
    const newWorkStarted = Promise.withResolvers<boolean>();
    let first = true;
    let newWork = false;
    const scheduler = createReconciliationScheduler({
      name: "allocation test",
      concurrency: 2,
      intervalMs: 5,
      reconcileNext() {
        if (first) {
          first = false;
          return held.promise;
        }
        if (newWork) {
          newWork = false;
          newWorkStarted.resolve(true);
        } else {
          idleSlot.resolve(true);
        }
        return Promise.resolve(false);
      },
    });
    scheduler.start();
    try {
      await idleSlot.promise;
      newWork = true;
      await newWorkStarted.promise;
    } finally {
      scheduler.stop();
      held.resolve(false);
    }
  });

  test("continues polling after a reconciliation throws", async () => {
    const nextPoll = Promise.withResolvers<boolean>();
    let calls = 0;
    const scheduler = createReconciliationScheduler({
      name: "failure test",
      concurrency: 1,
      intervalMs: 5,
      reconcileNext() {
        calls += 1;
        if (calls === 1) throw new Error("temporary claim failure");
        nextPoll.resolve(true);
        return Promise.resolve(false);
      },
    });
    scheduler.start();
    try {
      await nextPoll.promise;
      expect(calls).toBeGreaterThanOrEqual(2);
    } finally {
      scheduler.stop();
    }
  });
});
