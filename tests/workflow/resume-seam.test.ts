// Resume-from-log seam test: drive a workflow to a known state, snapshot the
// event log, then run a fresh runtime against the same log and assert it
// resumes to the same state and runs to completion.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runLocal,
  runtimeRun,
  step,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

describe("resume-from-log seam", () => {
  test("runs the log through to completion on a fresh runtime", async () => {
    // Phase 1: drive a 2-step workflow to completion and capture its log.
    const a = makeAgent("a");
    const b = makeAgent("b");
    const def = defineWorkflow({
      id: "resume-w",
      trigger: { type: "manual" },
      steps: {
        first: step({ agent: a }),
        second: step({ agent: b, after: ["first"] }),
      },
    });

    const invokeStep: StepInvoker = async ({ agent, input }) => {
      return { output: { agent: agent.id, input } };
    };

    const run1 = runLocal(def, {
      authorize: allowAll,
      triggerPayload: { initial: true },
      hasUpstreamSignalResolver: true,
      invokeStep,
    });
    const result1 = await run1.complete;
    expect(result1.terminalStatus).toBe("completed");
    const log = result1.events;

    // Phase 2: a fresh runtime resumes the same log without re-running steps --
    // its invokeStep throws if a step is invoked.
    const invokeStepNever: StepInvoker = async () => {
      throw new Error("must not invoke a step during resume");
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
      invokeStep: invokeStepNever,
      spawnChild: async () => ({
        childRunId: "n/a",
        terminalStatus: "completed",
      }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
      hasUpstreamSignalResolver: true,
    };

    const run2 = runtimeRun(def, env, {
      runId: result1.runId,
      resumeFromEvents: log,
    });
    const result2 = await run2.complete;
    expect(result2.terminalStatus).toBe("completed");
    expect(result2.runId).toBe(result1.runId);
  });

  test("targeted error when a seed log references blobs against a fresh ephemeral substrate", async () => {
    const a = makeAgent("a");
    const def = defineWorkflow({
      id: "spill-resume",
      trigger: { type: "manual" },
      steps: { big: step({ agent: a }) },
    });
    // Cap inline at a tiny size to force the step's output to spill to a blob.
    const originating = createInMemoryBlobSubstrate({ inlineMaxBytes: 4 });
    const clock = () => new Date();
    const repoStore = createInMemoryRepoStore();
    const env: WorkflowRuntimeEnv = {
      repoStore,
      scheduler: createInMemoryScheduler({ repoStore, clock }),
      signalChannel: createInMemorySignalChannel(),
      blobs: originating,
      directors: createDefaultDirectorRegistry(),
      authorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      invokeStep: async () => ({
        output: { large: "x".repeat(64) },
      }),
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
      hasUpstreamSignalResolver: true,
    };
    const result1 = await runtimeRun(def, env).complete;
    expect(result1.terminalStatus).toBe("completed");
    const hasBlobRef = result1.events.some(
      (e) => e.kind === "StepCompleted" && e.output.ref.startsWith("blob:"),
    );
    expect(hasBlobRef).toBe(true);

    // Resume against a fresh ephemeral substrate -- must fail with the targeted
    // error rather than crash on `unknown blob ref`.
    const freshEnv: WorkflowRuntimeEnv = {
      ...env,
      repoStore: createInMemoryRepoStore(),
      blobs: createInMemoryBlobSubstrate({ inlineMaxBytes: 4 }),
    };
    await expect(
      runtimeRun(def, freshEnv, {
        runId: result1.runId,
        resumeFromEvents: result1.events,
      }).complete,
    ).rejects.toThrow(/resume requires the BlobSubstrate/);
  });
});
