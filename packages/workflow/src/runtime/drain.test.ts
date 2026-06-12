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
  sleep,
  step,
  type WorkflowDefinition,
} from "../definition/index";
import { createInMemoryBlobSubstrate } from "../runlocal/blob-substrate";
import { createInMemoryRepoStore } from "../runlocal/repo-store";
import { createInMemoryScheduler } from "../runlocal/scheduler";
import { createInMemorySignalChannel } from "../runlocal/signal-channel";
import type { StepInvoker, WorkflowRuntimeEnv } from "./env";
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
): DrainController & { trigger: () => void } {
  const controller = new AbortController();
  return {
    signal: controller.signal,
    behaviorFor(stepId) {
      return resolveDrainBehavior(definition, stepId);
    },
    trigger() {
      controller.abort();
    },
  };
}

function buildEnv(
  _definition: WorkflowDefinition,
  invokeStep: StepInvoker,
  drain: DrainController,
): WorkflowRuntimeEnv {
  const clock = () => new Date();
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
    let stepInvoked = false;
    let stepAborted = false;
    const invokeStep: StepInvoker = ({ signal }) =>
      new Promise((_resolve, reject) => {
        stepInvoked = true;
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
    // Let StepStarted commit and the runner land on invokeStep.
    await new Promise<void>((r) => setTimeout(r, 10));
    expect(stepInvoked).toBe(true);
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
    const run = runtimeRun(def, env);
    await new Promise<void>((r) => setTimeout(r, 10));
    drain.trigger();
    // Wait briefly to confirm the run is still in flight (the
    // awaitSignal is wait-mode and ignores drain).
    await new Promise<void>((r) => setTimeout(r, 20));
    // Now deliver the signal so the run can complete normally.
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
    const env = buildEnv(def, invokeStep, drain);
    const run = runtimeRun(def, env);
    // Wait for first attempt to commit StepFailed and enter retry
    // backoff (200ms initialBackoffMs leaves plenty of room).
    await new Promise<void>((r) => setTimeout(r, 50));
    expect(attempts).toBe(1);
    drain.trigger();
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
    const run = runtimeRun(def, env);
    await new Promise<void>((r) => setTimeout(r, 10));
    drain.trigger();
    const result = await Promise.race([
      run.complete,
      new Promise<{ terminalStatus: string }>((_, reject) =>
        setTimeout(
          () => reject(new Error("sleep did not abort within 100ms of drain")),
          100,
        ),
      ),
    ]);
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
    const env = buildEnv(def, async () => ({ output: null }), drain);
    const run = runtimeRun(def, env);
    await new Promise<void>((r) => setTimeout(r, 5));
    drain.trigger();
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
    const run = runtimeRun(def, env);
    await new Promise<void>((r) => setTimeout(r, 10));
    drain.trigger();
    const result = await Promise.race([
      run.complete,
      new Promise<{ terminalStatus: string }>((_, reject) =>
        setTimeout(
          () =>
            reject(
              new Error("awaitSignal did not abort within 100ms of drain"),
            ),
          100,
        ),
      ),
    ]);
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
    const run = runtimeRun(def, env);
    await new Promise<void>((r) => setTimeout(r, 10));
    drain.trigger();
    await new Promise<void>((r) => setTimeout(r, 20));
    await env.signalChannel.deliver("go", null);
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
  });
});
