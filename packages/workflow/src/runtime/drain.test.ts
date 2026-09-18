// Drain controller and four-observation-point tests.
//
// The runtime body observes drain at exactly four sites: main loop
// entry, retry-between-attempts inside runStep, waitForTimer, and
// runAwaitSignal. Each site reads `shouldAbortForDrain(drain, stepId)`
// and aborts the step's local controller when the drain signal has
// fired AND the step's declared `drainBehavior` is `"cancel"`. A
// `"wait"`-behavior step ignores drain.
//
// These tests construct a custom env so the test can flip a
// controllable drain controller mid-flight and assert behavior at
// each observation point.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  awaitSignal,
  defineWorkflow,
  onTrigger,
  sleep,
  step,
  type WorkflowDefinition,
} from "../definition/index";
import { createInMemoryBlobSubstrate } from "../runlocal/blob-substrate";
import { createInMemoryRepoStore } from "../runlocal/repo-store";
import { createInMemoryScheduler } from "../runlocal/scheduler";
import { commit } from "./commit-chain";
import { waitForEvent } from "@intx/workflow/testing";
import { createInMemorySignalChannel } from "../runlocal/signal-channel";
import type {
  RepoStore,
  Scheduler,
  StepInvoker,
  WorkflowRuntimeEnv,
} from "./env";
import { runtimeRun } from "./run";
import {
  createNoopDrainController,
  resolveDrainBehavior,
  shouldAbortForDrain,
  type DrainController,
} from "./drain";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

function createControllableDrain(
  definition: WorkflowDefinition,
): DrainController & {
  trigger: () => void;
  observed: () => Promise<string>;
} {
  const controller = new AbortController();
  // `shouldAbortForDrain` returns early while the signal is unfired, so it
  // reaches `behaviorFor` only after the drain has fired. Every call here is
  // therefore an observation of a fired drain at one of the four points, which
  // is the event a test needs to wait for -- including the wait-mode case,
  // where the runtime observes the drain and deliberately does nothing, so
  // no event reaches the log to wait on instead.
  const waiters: ((stepId: string) => void)[] = [];
  return {
    signal: controller.signal,
    behaviorFor(stepId) {
      // Take the whole waiter list before resolving any of it: a waiter that
      // arms a fresh `observed()` in its own continuation must wait for the
      // NEXT observation, not be woken by this one.
      for (const wake of waiters.splice(0)) wake(stepId);
      return resolveDrainBehavior(definition, stepId);
    },
    trigger() {
      controller.abort();
    },
    /** Resolves with the step id at the next observation of a fired drain. */
    observed() {
      return new Promise<string>((resolve) => {
        waiters.push(resolve);
      });
    },
  };
}

/**
 * A scheduler that records what the runtime arms and fires nothing by itself.
 *
 * The two tests below whose subject is what happens *while* a run sits in a
 * timer wait cannot use the real scheduler: with it, the run leaves the wait
 * once the duration elapses, so whether the drain lands inside the wait is
 * decided by how quickly the test reaches the trigger. Holding the timer
 * unfired holds the run at the observation point for as long as the test
 * needs, and the test fires the timer itself when it wants the run to
 * proceed.
 */
function createManualScheduler(): {
  factory: (repoStore: RepoStore, clock: () => Date) => Scheduler;
  fireAll: () => Promise<void>;
  armedCount: () => number;
  nextArmed: () => Promise<void>;
} {
  const armed: { runId: string; timerId: string; disposed: boolean }[] = [];
  let armWaiters: (() => void)[] = [];
  let store: RepoStore | undefined;
  let now: (() => Date) | undefined;
  return {
    factory(repoStore, clock) {
      store = repoStore;
      now = clock;
      return {
        scheduleIn(runId, timerId) {
          const entry = { runId, timerId, disposed: false };
          armed.push(entry);
          const waiters = armWaiters;
          armWaiters = [];
          for (const waiter of waiters) waiter();
          return () => {
            entry.disposed = true;
          };
        },
      };
    },
    armedCount: () => armed.filter((a) => !a.disposed).length,
    /**
     * Resolve once the runtime has armed a timer, so a caller knows the run
     * is inside the wait rather than merely past the marker that precedes
     * it. The timer is armed after the wait's drain entry check, so a drain
     * triggered once this resolves takes the mid-wait path.
     */
    nextArmed(): Promise<void> {
      // Arm before checking, so a timer armed in between is not missed.
      const changed = new Promise<void>((resolve) => {
        armWaiters.push(resolve);
      });
      if (armed.some((a) => !a.disposed)) return Promise.resolve();
      return changed;
    },
    async fireAll() {
      if (store === undefined || now === undefined) {
        throw new Error("the scheduler factory was never invoked");
      }
      for (const entry of armed) {
        if (entry.disposed) continue;
        entry.disposed = true;
        await commit({ repoStore: store }, entry.runId, {
          kind: "TimerFired",
          seq: 0,
          at: now().toISOString(),
          timerId: entry.timerId,
        });
      }
    },
  };
}

function buildEnv(
  _definition: WorkflowDefinition,
  invokeStep: StepInvoker,
  drain: DrainController,
  opts: {
    scheduler?: (repoStore: RepoStore, clock: () => Date) => Scheduler;
  } = {},
): WorkflowRuntimeEnv {
  const clock = () => new Date();
  const repoStore = createInMemoryRepoStore();
  return {
    repoStore,
    scheduler:
      opts.scheduler === undefined
        ? createInMemoryScheduler({ repoStore, clock })
        : opts.scheduler(repoStore, clock),
    signalChannel: createInMemorySignalChannel(),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain,
  };
}

describe("DrainController shape", () => {
  test("createNoopDrainController's signal never fires", () => {
    const def = defineWorkflow({
      id: "noop",
      trigger: { type: "manual" },
      steps: { s: step({ agent: makeAgent("a") }) },
    });
    const drain = createNoopDrainController(def);
    expect(drain.signal.aborted).toBe(false);
  });

  test("resolveDrainBehavior returns each primitive's declared behavior", () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "behaviors",
      trigger: { type: "manual" },
      steps: {
        cancelStep: step({ agent: a, drainBehavior: "cancel" }),
        waitStep: step({ agent: a, drainBehavior: "wait" }),
        waitSignal: awaitSignal({ name: "go" }),
        cancelSignal: awaitSignal({ name: "x", drainBehavior: "cancel" }),
        sleepStep: sleep({ duration: 10 }),
      },
    });
    expect(resolveDrainBehavior(def, "cancelStep")).toBe("cancel");
    expect(resolveDrainBehavior(def, "waitStep")).toBe("wait");
    expect(resolveDrainBehavior(def, "waitSignal")).toBe("wait");
    expect(resolveDrainBehavior(def, "cancelSignal")).toBe("cancel");
    expect(resolveDrainBehavior(def, "sleepStep")).toBe("cancel");
  });

  test("an onTrigger section drains as wait by default, honoring an explicit cancel", () => {
    const body = defineWorkflow({
      id: "section-body",
      trigger: { type: "manual" },
      steps: { s: step({ agent: makeAgent("a") }) },
    });
    const def = defineWorkflow({
      id: "onTrigger-drain",
      trigger: { type: "manual" },
      steps: {
        live: onTrigger({ on: { type: "mail", to: "x@y.example" }, body }),
        cancelable: onTrigger({
          on: { type: "mail", to: "z@y.example" },
          body,
          drainBehavior: "cancel",
        }),
      },
    });
    expect(resolveDrainBehavior(def, "live")).toBe("wait");
    expect(resolveDrainBehavior(def, "cancelable")).toBe("cancel");
  });

  test("shouldAbortForDrain returns false when signal not aborted", () => {
    const def = defineWorkflow({
      id: "noop",
      trigger: { type: "manual" },
      steps: { s: step({ agent: makeAgent("a") }) },
    });
    const drain = createNoopDrainController(def);
    expect(shouldAbortForDrain(drain, "s")).toBe(false);
  });

  test("shouldAbortForDrain gates on behaviorFor when signal is aborted", () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "mix",
      trigger: { type: "manual" },
      steps: {
        cancelStep: step({ agent: a, drainBehavior: "cancel" }),
        waitSignal: awaitSignal({ name: "go" }),
      },
    });
    const drain = createControllableDrain(def);
    drain.trigger();
    expect(shouldAbortForDrain(drain, "cancelStep")).toBe(true);
    expect(shouldAbortForDrain(drain, "waitSignal")).toBe(false);
  });

  test("map-inner step id resolves to the inner step's behavior", () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "map-default",
      trigger: { type: "manual" },
      steps: {
        m: {
          kind: "map",
          id: "",
          over: { from: "trigger.payload" },
          step: step({ agent: a, drainBehavior: "cancel" }),
        },
      },
    });
    expect(resolveDrainBehavior(def, "m[0]")).toBe("cancel");
  });

  test("long-lived step default resolves to wait", () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "long-lived-defaults",
      trigger: { type: "manual" },
      steps: {
        multi: step({ agent: a, triggers: 5 }),
        unbounded: step({ agent: a, triggers: "unbounded" }),
        batch: step({ agent: a, triggers: 1 }),
        defaultBatch: step({ agent: a }),
      },
    });
    expect(resolveDrainBehavior(def, "multi")).toBe("wait");
    expect(resolveDrainBehavior(def, "unbounded")).toBe("wait");
    expect(resolveDrainBehavior(def, "batch")).toBe("cancel");
    expect(resolveDrainBehavior(def, "defaultBatch")).toBe("cancel");
  });

  test("explicit drainBehavior overrides the trigger-budget default at runtime", () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "explicit-override",
      trigger: { type: "manual" },
      steps: {
        multiCancel: step({
          agent: a,
          triggers: 5,
          drainBehavior: "cancel",
        }),
        batchWait: step({
          agent: a,
          triggers: 1,
          drainBehavior: "wait",
        }),
      },
    });
    expect(resolveDrainBehavior(def, "multiCancel")).toBe("cancel");
    expect(resolveDrainBehavior(def, "batchWait")).toBe("wait");
  });

  test("map-inner step with long-lived budget resolves to wait", () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "map-long-lived",
      trigger: { type: "manual" },
      steps: {
        m: {
          kind: "map",
          id: "",
          over: { from: "trigger.payload" },
          step: step({ agent: a, triggers: 3 }),
        },
      },
    });
    expect(resolveDrainBehavior(def, "m[0]")).toBe("wait");
  });
});

describe("observation point #1: main loop entry", () => {
  test("drain fired mid-flight aborts a long-running cancel-mode step", async () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "mainloop",
      trigger: { type: "manual" },
      steps: {
        s: step({ agent: a, drainBehavior: "cancel" }),
      },
    });
    const drain = createControllableDrain(def);
    const invoked = Promise.withResolvers<boolean>();
    let stepAborted = false;
    const invokeStep: StepInvoker = ({ signal }) =>
      new Promise((_resolve, reject) => {
        invoked.resolve(true);
        if (signal.aborted) {
          stepAborted = true;
          reject(new Error("aborted before start"));
          return;
        }
        signal.addEventListener("abort", () => {
          stepAborted = true;
          reject(new Error("aborted"));
        });
      });
    const env = buildEnv(def, invokeStep, drain);
    const run = runtimeRun(def, env);
    // The invoker resolves this as its first act, so awaiting it is the
    // runner landing on invokeStep -- the state the drain must interrupt.
    // It also subsumes the `stepInvoked` flag this replaces: reaching the
    // next line is the proof the flag used to assert.
    await invoked.promise;
    drain.trigger();
    const result = await run.complete;
    expect(stepAborted).toBe(true);
    // The step fails because invokeStep rejects; the runtime commits
    // StepFailed and the run terminates as failed (no CancelRequested
    // was issued -- the supervisor's drainTimeout escalation lives
    // outside this layer).
    expect(result.terminalStatus).toBe("failed");
  });

  test("drain fired mid-flight does NOT abort a wait-mode awaitSignal step", async () => {
    const def = defineWorkflow({
      id: "wait-await",
      trigger: { type: "manual" },
      steps: {
        s: awaitSignal({ name: "go" }),
      },
    });
    const drain = createControllableDrain(def);
    const env = buildEnv(def, async () => ({ output: null }), drain);
    const runId = "wait-await-run";
    const run = runtimeRun(def, env, { runId });
    // The park is committed before the runtime reads drain, so awaiting the
    // marker puts the run at the observation point under test.
    await waitForEvent(env.repoStore, runId, (e) => e.kind === "SignalAwaited");
    // Arm before triggering: the observation can land in the same turn as
    // the abort, and a latch armed afterwards would miss it.
    const seen = drain.observed();
    drain.trigger();
    // A fixed pause cannot establish that the run is still in flight: it
    // guarantees a minimum only, and under load it overshoots into whatever
    // happens next. Awaiting the observation establishes that the runtime
    // read the fired drain -- no more than that, since the latch resolves on
    // any behaviorFor call and this discards the step id it returns. That it
    // is THIS step's observation, and that the run is still parked, comes
    // from the definition under test: an awaitSignal cannot advance until the
    // deliver below, so the parked runAwaitSignal site is the only thing that
    // can be asking.
    await seen;
    await env.signalChannel.deliver("go", null);
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
  });
});

describe("observation point #2: retry-between-attempts in runStep", () => {
  test("drain fired during the retry backoff aborts before the next attempt", async () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "retry-drain",
      trigger: { type: "manual" },
      steps: {
        s: step({
          agent: a,
          drainBehavior: "cancel",
          retry: { maxAttempts: 3, initialBackoffMs: 200 },
        }),
      },
    });
    const drain = createControllableDrain(def);
    let attempts = 0;
    const invokeStep: StepInvoker = async () => {
      attempts += 1;
      throw new Error("attempt fails");
    };
    const timers = createManualScheduler();
    const env = buildEnv(def, invokeStep, drain, { scheduler: timers.factory });
    const runId = "retry-drain-run";
    const run = runtimeRun(def, env, { runId });
    // The retry path commits TimerSet then AttemptScheduled and only then
    // enters waitForTimer, so the second marker is the boundary this test
    // wants: the first attempt has failed and the backoff is armed.
    //
    // Reaching the marker is not enough on its own. With the real scheduler
    // the run leaves the wait when the declared backoff elapses, so whether
    // the drain arrives during the backoff is decided by whether the test
    // gets there first -- a worker slower than the backoff sees the second
    // attempt launch and fails on a count. The scheduler here fires nothing,
    // so the run stays in the wait until this test acts.
    await waitForEvent(
      env.repoStore,
      runId,
      (e) => e.kind === "AttemptScheduled",
    );
    // The marker precedes the wait; the arming is inside it. Waiting for the
    // arm is what puts the run at observation point #2 rather than merely
    // near it.
    await timers.nextArmed();
    expect(attempts).toBe(1);
    const seen = drain.observed();
    drain.trigger();
    await seen;
    const result = await run.complete;
    // The retry was inside `waitForTimer` when drain fired; the
    // observation aborts before launching the next invokeStep.
    expect(attempts).toBe(1);
    expect(result.terminalStatus).toBe("failed");
  });
});

describe("observation point #3: waitForTimer", () => {
  test("drain fired during a sleep (cancel-mode default) aborts the sleep", async () => {
    const def = defineWorkflow({
      id: "sleep-drain",
      trigger: { type: "manual" },
      steps: {
        s: sleep({ duration: 60_000 }),
      },
    });
    const drain = createControllableDrain(def);
    const env = buildEnv(def, async () => ({ output: null }), drain);
    const runId = "sleep-drain-run";
    const run = runtimeRun(def, env, { runId });
    // TimerSet is committed on the way into waitForTimer, which is the
    // observation point this test exercises.
    await waitForEvent(env.repoStore, runId, (e) => e.kind === "TimerSet");
    drain.trigger();
    // No race against a 100ms rejection. The sleep is 60 seconds, so a drain
    // that fails to abort it does not finish early enough to fool this
    // assertion -- it never finishes, and the lane timeout says so. The
    // rejection could only ever fire on a runner too busy to abort in time.
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
  });

  test("drain ignored by wait-mode sleep step", async () => {
    const def = defineWorkflow({
      id: "wait-sleep",
      trigger: { type: "manual" },
      steps: {
        s: sleep({ duration: 30, drainBehavior: "wait" }),
      },
    });
    const drain = createControllableDrain(def);
    const timers = createManualScheduler();
    const env = buildEnv(def, async () => ({ output: null }), drain, {
      scheduler: timers.factory,
    });
    const runId = "wait-sleep-run";
    const run = runtimeRun(def, env, { runId });
    await waitForEvent(env.repoStore, runId, (e) => e.kind === "TimerSet");
    // The step's declared 30ms would otherwise decide this test: with the
    // real scheduler a run that reaches the trigger late has already slept,
    // completed, and satisfied the assertion below without a drain ever
    // being read. Nothing fires here until this test fires it, so the run is
    // still in the sleep when the drain lands.
    await timers.nextArmed();
    const seen = drain.observed();
    drain.trigger();
    await seen;
    // Observed against a wait-mode step and ignored: the run is still
    // parked, so it can only finish once the timer this test controls fires.
    expect(timers.armedCount()).toBe(1);
    await timers.fireAll();
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
  });
});

describe("observation point #4: runAwaitSignal entry", () => {
  test("drain fired before await aborts a cancel-mode awaitSignal step", async () => {
    const def = defineWorkflow({
      id: "await-cancel",
      trigger: { type: "manual" },
      steps: {
        s: awaitSignal({ name: "go", drainBehavior: "cancel" }),
      },
    });
    const drain = createControllableDrain(def);
    const env = buildEnv(def, async () => ({ output: null }), drain);
    const runId = "await-cancel-run";
    const run = runtimeRun(def, env, { runId });
    await waitForEvent(env.repoStore, runId, (e) => e.kind === "SignalAwaited");
    drain.trigger();
    // The signal is never delivered, so a drain that fails to abort leaves
    // the run parked forever rather than completing -- the lane timeout is
    // the failsafe, and the 100ms rejection this replaces could only fire
    // on a runner too busy to abort within it.
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
  });

  test("drain ignored by wait-mode (default) awaitSignal step", async () => {
    const def = defineWorkflow({
      id: "await-wait",
      trigger: { type: "manual" },
      steps: {
        s: awaitSignal({ name: "go" }),
      },
    });
    const drain = createControllableDrain(def);
    const env = buildEnv(def, async () => ({ output: null }), drain);
    const runId = "await-wait-run";
    const run = runtimeRun(def, env, { runId });
    await waitForEvent(env.repoStore, runId, (e) => e.kind === "SignalAwaited");
    const seen = drain.observed();
    drain.trigger();
    await seen;
    await env.signalChannel.deliver("go", null);
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
  });
});
