// The window between the pre-park durable flush and the bridge that carries
// the outer abort into the park.
//
// An in-process child runs with its parent's signal as its local abort, so a
// parent teardown landing in this window is reachable in production.

import { describe, test, expect } from "bun:test";

import { awaitSignal, defineWorkflow, runtimeRun } from "@intx/workflow";

import {
  abortOnDurableEvent,
  buildAbortWindowEnv,
  settlesWithin,
} from "./abort-window.test-helpers";

const gateWorkflow = defineWorkflow({
  id: "gate-only",
  trigger: { type: "manual" },
  steps: { g: awaitSignal({ name: "approve" }) },
});

// Far enough out that the timer cannot fire during the run, so an observed
// timeout outcome would mean the park misreported rather than raced.
const UNREACHABLE_TIMEOUT_MS = 60_000;

const timedGateWorkflow = defineWorkflow({
  id: "timed-gate-only",
  trigger: { type: "manual" },
  steps: {
    g: awaitSignal({ name: "approve", timeout: UNREACHABLE_TIMEOUT_MS }),
  },
});

describe("an abort landing during the pre-park flush", () => {
  test("tears the run down instead of parking on a signal nothing sends", async () => {
    const teardown = new AbortController();
    const env = buildAbortWindowEnv(gateWorkflow, {
      repoStore: abortOnDurableEvent(teardown, "SignalAwaited"),
    });

    const run = runtimeRun(gateWorkflow, env, {
      runId: "run-flush-window",
      localAbort: teardown.signal,
    });

    expect(await settlesWithin(run.complete, 2000)).toBe("settled");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
  });

  // A timed gate races the signal against a scheduler timer, so the abort has
  // to unwind two legs rather than one. Here the failure mode is not a hang
  // but a wrong outcome: a park that reports a timeout routes the gate onward
  // through `onTimeout` instead of tearing the run down.
  test("fails a timed gate as aborted rather than reporting a timeout", async () => {
    const teardown = new AbortController();
    const runId = "run-timed-flush-window";
    const repoStore = abortOnDurableEvent(teardown, "SignalAwaited");
    const env = buildAbortWindowEnv(timedGateWorkflow, { repoStore });

    const run = runtimeRun(timedGateWorkflow, env, {
      runId,
      localAbort: teardown.signal,
    });

    expect(await settlesWithin(run.complete, 2000)).toBe("settled");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");

    const events = await repoStore.read(runId);
    const kinds = events.map((e) => e.kind);
    expect(kinds).toContain("TimerSet");
    expect(kinds).not.toContain("TimerFired");

    const failed = events.find((e) => e.kind === "StepFailed");
    expect(failed?.kind === "StepFailed" ? failed.error.message : "").toContain(
      "abort",
    );
  });
});
