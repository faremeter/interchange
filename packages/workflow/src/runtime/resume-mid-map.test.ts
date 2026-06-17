// Resume contract: mid-map seed logs are unsupported and must surface
// the limitation as `RuntimeResumeUnsupportedError` rather than
// stalling with an opaque "no schedulable primitives" message.
//
// The runtime's v1 resume path supports complete-or-cancelled seed
// logs and seed logs aligned on step boundaries. A seed log that
// stops mid-map (one inner item completed, the outer `map` step still
// in-flight) has no way to re-arm without rebuilding the runMap inner
// state; the runtime body declines and the host decides how to recover.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  map,
  runtimeRun,
  RuntimeResumeUnsupportedError,
  step,
  type StepInvoker,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: id,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

describe("resume mid-map", () => {
  test("a seed log that stops after one inner item rejects with RuntimeResumeUnsupportedError", async () => {
    const def = defineWorkflow({
      id: "midmap-resume",
      trigger: { type: "manual" },
      steps: {
        m: map({
          over: { literal: [{ x: 0 }, { x: 1 }] },
          step: step({ agent: makeAgent("inner") }),
        }),
      },
    });
    const invokeStep: StepInvoker = async ({ input }) => ({
      output: { processed: input },
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
      invokeStep,
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
    };
    const result1 = await runtimeRun(def, env).complete;
    const trimmed: WorkflowEvent[] = [];
    for (const e of result1.events) {
      trimmed.push(e);
      if (
        e.kind === "StepCompleted" &&
        (e as { stepId: string }).stepId === "m[0]"
      ) {
        break;
      }
    }
    const repoStore2 = createInMemoryRepoStore();
    const env2: WorkflowRuntimeEnv = {
      ...env,
      repoStore: repoStore2,
      blobs: env.blobs,
      scheduler: createInMemoryScheduler({ repoStore: repoStore2, clock }),
      signalChannel: createInMemorySignalChannel(),
    };
    await expect(
      runtimeRun(def, env2, {
        runId: result1.runId,
        resumeFromEvents: trimmed,
      }).complete,
    ).rejects.toBeInstanceOf(RuntimeResumeUnsupportedError);
  });
});
