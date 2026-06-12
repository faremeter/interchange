// Cancellation log invariants.
//
// The runtime body's responsibility for the cancel cascade lives in
// `runtime/run.ts`. The state machine documents in `state-machine/
// resume.ts` that a `CancelRequested` without a matching
// `RunCancelled` requires the runtime to emit a `CancelPropagated`
// for every non-terminal step and a `ChildCancelRequested` for every
// tracked child whose `cancelRequested` flag is still false. Both
// tests below assert the runtime upholds the responsibility against
// the persisted log -- the canonical source of truth a resuming
// runtime would consult.

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";

import {
  childWorkflow,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runLocal,
  resumeFromLog,
  runtimeRun,
  step,
  type SpawnChildWorkflow,
  type StepInvoker,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { createDefaultDirectorRegistry } from "@intx/agent";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

describe("childWorkflow terminal-status propagation", () => {
  test("propagates a failed child to a StepFailed on the parent's spawn step", async () => {
    const parent = defineWorkflow({
      id: "parent-w",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definitionRef: "child-w" }) },
    });
    const spawnChild: SpawnChildWorkflow = async () => ({
      terminalStatus: "failed",
    });
    const clock = () => new Date();
    const repoStore = createInMemoryRepoStore();
    const env: WorkflowRuntimeEnv = {
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
      invokeStep: async ({ input }) => ({ output: input }),
      spawnChild,
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(parent),
    };
    const result = await runtimeRun(parent, env).complete;
    expect(result.terminalStatus).toBe("failed");
    const stepFailed = result.events.find(
      (e) => e.kind === "StepFailed" && e.stepId === "spawn",
    );
    expect(stepFailed).toBeDefined();
  });

  test("propagates a cancelled child to a StepFailed on the parent's spawn step", async () => {
    const parent = defineWorkflow({
      id: "parent-w",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definitionRef: "child-w" }) },
    });
    const spawnChild: SpawnChildWorkflow = async () => ({
      terminalStatus: "cancelled",
    });
    const clock = () => new Date();
    const repoStore = createInMemoryRepoStore();
    const env: WorkflowRuntimeEnv = {
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
      invokeStep: async ({ input }) => ({ output: input }),
      spawnChild,
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(parent),
    };
    const result = await runtimeRun(parent, env).complete;
    expect(result.terminalStatus).toBe("failed");
    const stepFailed = result.events.find(
      (e) => e.kind === "StepFailed" && e.stepId === "spawn",
    );
    expect(stepFailed).toBeDefined();
  });
});

describe("cancellation log invariants", () => {
  test("emits a terminal RunCancelled even when a step completes during the cancel race", async () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "race-w",
      trigger: { type: "manual" },
      steps: { a: step({ agent: a }) },
    });

    // Hold the step open with a deferred resolver so the test can
    // sequence cancel() before the step's commit lands.
    let resolveStep!: (value: { output: unknown }) => void;
    const invokeStep: StepInvoker = () =>
      new Promise((resolve) => {
        resolveStep = resolve;
      });

    const run = runLocal(def, { invokeStep });
    // Wait a tick for StepStarted to commit and the runner to land
    // on env.invokeStep.
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
    await run.cancel("self", "race the step's StepCompleted");
    // Release the step so it commits StepCompleted; the runtime's
    // post-loop branch must still emit RunCancelled.
    resolveStep({ output: null });
    const result = await run.complete;
    expect(result.terminalStatus).toBe("cancelled");

    const lastKind = result.events[result.events.length - 1]?.kind;
    expect(["RunCancelled"]).toContain(lastKind);
    const replay = resumeFromLog(result.runId, result.events);
    expect(replay.phase).toBe("cancelled");
  });

  test("emits ChildCancelRequested for every live child on parent cancel", async () => {
    // Drive the spawn callback through a stub `runtimeRun` env so
    // the test can hold the spawn open until cancel arrives,
    // independently of what runLocal's default childResolver wires.
    const parent = defineWorkflow({
      id: "parent-w",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definitionRef: "child-w" }) },
    });

    let resolveSpawn!: (value: {
      terminalStatus: "completed" | "failed" | "cancelled";
    }) => void;
    let signalSpawnStarted!: () => void;
    const spawnStarted = new Promise<void>((resolve) => {
      signalSpawnStarted = resolve;
    });
    const spawnChild: SpawnChildWorkflow = ({ signal }) => {
      signalSpawnStarted();
      const settled = new Promise<{
        terminalStatus: "completed" | "failed" | "cancelled";
      }>((resolve) => {
        resolveSpawn = resolve;
      });
      // When the parent aborts, settle the spawn as cancelled so
      // the spawn step's runner returns and the main loop drains.
      signal.addEventListener("abort", () => {
        resolveSpawn({ terminalStatus: "cancelled" });
      });
      return settled;
    };

    const clock = () => new Date();
    const repoStore = createInMemoryRepoStore();
    const env: WorkflowRuntimeEnv = {
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
      invokeStep: async ({ input }) => ({ output: input }),
      spawnChild,
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(parent),
    };

    const run = runtimeRun(parent, env);
    const startedOrTimeout = await Promise.race([
      spawnStarted.then(() => "started" as const),
      new Promise<"timeout">((resolve) => {
        setTimeout(() => resolve("timeout"), 2000);
      }),
    ]);
    if (startedOrTimeout === "timeout") {
      throw new Error("spawn callback never invoked within 2s");
    }
    await run.cancel("self", "test");
    // The abort listener on the spawn signal resolves the spawn
    // promise as cancelled, letting the parent's main loop drain.
    void resolveSpawn;
    const result = await run.complete;
    expect(result.terminalStatus).toBe("cancelled");
    const childCancelCount = result.events.filter(
      (e) => e.kind === "ChildCancelRequested",
    ).length;
    expect(childCancelCount).toBeGreaterThanOrEqual(1);
  });
});
