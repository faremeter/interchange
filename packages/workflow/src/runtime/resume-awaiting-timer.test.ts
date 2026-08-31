// Resume contract for a bare `sleep`. Two shapes resume:
//
//   1. The tail is `TimerSet` (the timer has not fired): the sleep is
//      `awaiting-timer`. A run re-driving the durable log re-offers it
//      (`isResumableSleepStep`); runSleep re-adopts the unfired durable timer
//      -- honouring its persisted `fireAt` rather than restarting the clock --
//      skips the already-emitted StepStarted, and RE-PARKS on `waitForTimer`,
//      so the scheduler-committed `TimerFired` resolves it. No second
//      `TimerSet` is minted.
//   2. The tail already carries the sleep's `TimerFired`, so the reduction
//      moved the step `in-flight` (the crash-after-TimerFired-before-
//      `StepCompleted` window). `TimerFired` is the only mover off
//      `awaiting-timer` for a sleep, so runSleep completes it with `null`
//      without re-parking and with no outcome to reconstruct.
//
// A retrying `step`/`action` also parks in `awaiting-timer` during backoff,
// but its residual is NOT a sleep and stays `RuntimeResumeUnsupportedError`;
// the kind-exact predicate keeps the two apart.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  RuntimeResumeUnsupportedError,
  sleep,
  step,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const sleepDuration = defineWorkflow({
  id: "sleep-resume-duration",
  trigger: { type: "manual" },
  steps: { w: sleep({ duration: 60_000 }) },
});

// An absolute-deadline sleep set far in the future. The re-park resume must
// honour the persisted `fireAt` (seeded near-now), not recompute from `until`;
// a recompute would sleep for the full hour and hang the test.
const sleepUntil = defineWorkflow({
  id: "sleep-resume-until",
  trigger: { type: "manual" },
  steps: {
    w: sleep({ until: new Date(Date.now() + 3_600_000).toISOString() }),
  },
});

// A short duration so an UNCRASHED run reaches its timer quickly, letting the
// organic test capture the runtime's own emitted in-flight window.
const sleepShort = defineWorkflow({
  id: "sleep-resume-short",
  trigger: { type: "manual" },
  steps: { w: sleep({ duration: 20 }) },
});

function retryAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
}

// A retry-eligible agent step: seeded through a first-attempt failure into a
// backoff timer, its residual reduces to `awaiting-timer` -- the phase a sleep
// shares but the kind-exact predicate must NOT admit.
const retryStep = defineWorkflow({
  id: "sleep-resume-retry-step",
  trigger: { type: "manual" },
  steps: {
    s: step({
      agent: retryAgent("s"),
      retry: { maxAttempts: 3, initialBackoffMs: 200 },
    }),
  },
});

function buildEnv(
  def: WorkflowDefinition,
  opts: { invokeStep?: StepInvoker } = {},
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  return {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: opts.invokeStep ?? (async () => ({ output: null })),
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
  };
}

const at = new Date().toISOString();

// A `fireAt` a short hop in the future: the resumed run re-arms it and the
// in-memory scheduler fires it within the test window.
function soon(): string {
  return new Date(Date.now() + 30).toISOString();
}

function runStartedSeed(runId: string): WorkflowEvent {
  return {
    kind: "RunStarted",
    seq: 1,
    at,
    runId,
    definitionHash: "x",
    trigger: { type: "manual", payload: undefined },
  };
}

describe("resume awaiting timer", () => {
  test("resumes a seed whose tail is TimerSet (timer unfired): re-adopts the timer, re-parks, and the fired timer drives it to completion", async () => {
    const runId = "run-timer";
    const env = buildEnv(sleepDuration);
    const seed: WorkflowEvent[] = [
      runStartedSeed(runId),
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "w",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "TimerSet",
        seq: 3,
        at,
        timerId: "timer-1",
        fireAt: soon(),
        stepId: "w",
      },
    ];

    const result = await runtimeRun(sleepDuration, env, {
      runId,
      resumeFromEvents: seed,
    }).complete;
    expect(result.terminalStatus).toBe("completed");

    const types = result.events.map((e) => e.kind);
    // The durable StepStarted/TimerSet are re-adopted, not re-emitted: no
    // second TimerSet is minted (the no-double-count guarantee).
    expect(types.filter((t) => t === "StepStarted").length).toBe(1);
    expect(types.filter((t) => t === "TimerSet").length).toBe(1);
    expect(types.filter((t) => t === "TimerFired").length).toBe(1);
    expect(types.filter((t) => t === "StepCompleted").length).toBe(1);
    expect(types).toContain("RunCompleted");
  });

  test("resumes a seed that already carries the TimerFired (sleep in-flight), completing it without re-parking", async () => {
    const runId = "run-fired";
    const env = buildEnv(sleepDuration);
    // The timer fired durably before the crash: TimerFired after TimerSet moves
    // the sleep to `in-flight`. No StepCompleted{w} yet -- the crash-after-
    // TimerFired-before-StepCompleted window.
    const seed: WorkflowEvent[] = [
      runStartedSeed(runId),
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "w",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "TimerSet",
        seq: 3,
        at,
        timerId: "timer-1",
        fireAt: at,
        stepId: "w",
      },
      { kind: "TimerFired", seq: 4, at, timerId: "timer-1" },
    ];

    const result = await runtimeRun(sleepDuration, env, {
      runId,
      resumeFromEvents: seed,
    }).complete;
    expect(result.terminalStatus).toBe("completed");

    const types = result.events.map((e) => e.kind);
    // No second timer is armed on the short-circuit path.
    expect(types.filter((t) => t === "TimerSet").length).toBe(1);
    expect(types.filter((t) => t === "TimerFired").length).toBe(1);
    expect(types.filter((t) => t === "StepCompleted").length).toBe(1);
    expect(types).toContain("RunCompleted");
  });

  test("an until-based sleep re-adopts the persisted fireAt rather than recomputing from until", async () => {
    const runId = "run-until";
    const env = buildEnv(sleepUntil);
    // `until` is an hour out, but the seeded timer's `fireAt` is near-now. The
    // resume path re-adopts the persisted `fireAt`; completing promptly proves
    // it did not recompute the deadline from `until`.
    const seed: WorkflowEvent[] = [
      runStartedSeed(runId),
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "w",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "TimerSet",
        seq: 3,
        at,
        timerId: "timer-1",
        fireAt: soon(),
        stepId: "w",
      },
    ];

    const result = await runtimeRun(sleepUntil, env, {
      runId,
      resumeFromEvents: seed,
    }).complete;
    expect(result.terminalStatus).toBe("completed");

    const types = result.events.map((e) => e.kind);
    expect(types.filter((t) => t === "TimerSet").length).toBe(1);
    expect(types.filter((t) => t === "TimerFired").length).toBe(1);
    expect(types.filter((t) => t === "StepCompleted").length).toBe(1);
  });

  test("a retry-backoff step left awaiting-timer is refused (not a sleep, so not resumable)", async () => {
    const runId = "run-retry";
    const env = buildEnv(retryStep, {
      invokeStep: async () => {
        throw new Error("agent must not be re-invoked on a refused resume");
      },
    });
    // A first-attempt failure scheduled a backoff timer, leaving step `s` in
    // `awaiting-timer` with attempt 2 -- the same phase a parked sleep holds,
    // but `s` is an agent `step`, not a `sleep`. `isResumableSleepStep`'s exact
    // kind guard must exclude it so the residual stays unsupported.
    const seed: WorkflowEvent[] = [
      runStartedSeed(runId),
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "s",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "StepFailed",
        seq: 3,
        at,
        stepId: "s",
        attempt: 1,
        error: { message: "attempt 1 failed" },
        retriesExhausted: false,
      },
      {
        kind: "TimerSet",
        seq: 4,
        at,
        timerId: "t1",
        fireAt: at,
        stepId: "s",
      },
      {
        kind: "AttemptScheduled",
        seq: 5,
        at,
        stepId: "s",
        nextAttempt: 2,
        timerId: "t1",
        fireAt: at,
      },
    ];

    await expect(
      runtimeRun(retryStep, env, { runId, resumeFromEvents: seed }).complete,
    ).rejects.toBeInstanceOf(RuntimeResumeUnsupportedError);
  });

  test("resumes an in-flight window captured from the runtime's OWN emitted log, not a hand-authored seed", async () => {
    // The hand-authored seeds above leave the emitter unproven: the resume path
    // assumes the runtime emits a StepStarted/TimerSet/TimerFired shape it can
    // recover from. Here a REAL run drives the sleep to completion, we slice its
    // durably-emitted log at the crash-after-TimerFired-before-StepCompleted
    // window, and resume from that slice -- so the emitter and the resume path
    // are proven against each other end to end.
    const liveRunId = "run-organic";
    const liveEnv = buildEnv(sleepShort);
    const live = await runtimeRun(sleepShort, liveEnv, {
      runId: liveRunId,
      triggerPayload: null,
    }).complete;
    expect(live.terminalStatus).toBe("completed");

    const emitted = await liveEnv.repoStore.read(liveRunId);
    const completedIdx = emitted.findIndex(
      (e) => e.kind === "StepCompleted" && e.stepId === "w",
    );
    expect(completedIdx).toBeGreaterThan(-1);
    const inFlightWindow = emitted.slice(0, completedIdx);

    // The captured window is the emitter's own output: a StepStarted, its
    // TimerSet, and the fired TimerFired, but no StepCompleted.
    const windowKinds = inFlightWindow.map((e) => e.kind);
    expect(windowKinds.filter((k) => k === "TimerSet").length).toBe(1);
    expect(windowKinds.filter((k) => k === "TimerFired").length).toBe(1);
    expect(windowKinds.filter((k) => k === "StepCompleted").length).toBe(0);

    const resumeEnv = buildEnv(sleepShort);
    const resumed = await runtimeRun(sleepShort, resumeEnv, {
      runId: liveRunId,
      resumeFromEvents: inFlightWindow,
    }).complete;
    expect(resumed.terminalStatus).toBe("completed");
    expect(
      resumed.events.some(
        (e) => e.kind === "StepCompleted" && e.stepId === "w",
      ),
    ).toBe(true);
    // The resume did not mint a second timer.
    const resumedTypes = resumed.events.map((e) => e.kind);
    expect(resumedTypes.filter((t) => t === "TimerSet").length).toBe(1);
    expect(resumedTypes.filter((t) => t === "TimerFired").length).toBe(1);
  });
});
