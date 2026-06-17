// Resume contract: a seed log whose tail is `SignalAwaited` (without
// the matching `SignalReceived`) cannot be resumed by the in-process
// runtime body; the signal channel cannot be rehydrated and the
// awaiting primitive cannot be re-armed. Such a seed log must surface
// `RuntimeResumeUnsupportedError` rather than stall with an opaque
// "no schedulable primitives" message.
//
// Hosts that need to recover an awaiting-signal step on resume own
// the re-arming surface (crash and redeploy, supervisor signal
// re-injection, etc); the runtime's job is to refuse honestly.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import {
  awaitSignal,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  RuntimeResumeUnsupportedError,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

describe("resume awaiting signal", () => {
  test("rejects with RuntimeResumeUnsupportedError when the seed log leaves a step in awaiting-signal", async () => {
    const def = defineWorkflow({
      id: "wait-resume",
      trigger: { type: "manual" },
      steps: { w: awaitSignal({ name: "go" }) },
    });
    const at = new Date().toISOString();
    const seed: WorkflowEvent[] = [
      {
        kind: "RunStarted",
        seq: 1,
        at,
        runId: "run-test",
        definitionHash: "x",
        trigger: { type: "manual", payload: undefined },
      },
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "w",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "SignalAwaited",
        seq: 3,
        at,
        stepId: "w",
        signalName: "go",
      },
    ];
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
      invokeStep: async () => ({ output: null }),
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
    };
    await expect(
      runtimeRun(def, env, {
        runId: "run-test",
        resumeFromEvents: seed,
      }).complete,
    ).rejects.toBeInstanceOf(RuntimeResumeUnsupportedError);
  });
});
