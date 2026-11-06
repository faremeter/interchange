// A host learns from `started` that the trigger it delivered is durable, so a
// run stopped after that point keeps its trigger.

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
  step,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { createTerminalChildHandle } from "./suspendable-child-handle";

const definition = defineWorkflow({
  id: "started",
  trigger: { type: "manual" },
  steps: {
    only: step({
      agent: defineAgent({
        id: "only",
        systemPrompt: "s",
        tools: [],
        capabilities: [],
        inference: { sources: [{ provider: "anthropic", model: "m" }] },
      }),
    }),
  },
});

function createEnv(invoke: () => Promise<void>) {
  const clock = (): Date => new Date();
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
    invokeStep: async () => {
      await invoke();
      return { output: null };
    },
    spawnChild: async () => createTerminalChildHandle("completed"),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(definition),
    hasUpstreamSignalResolver: true,
  };
  return { env, repoStore };
}

describe("a run's started promise", () => {
  test("settles once RunStarted is durable, before the run completes", async () => {
    const finish = Promise.withResolvers<undefined>();
    const { env, repoStore } = createEnv(() => finish.promise);

    const run = runtimeRun(definition, env, {
      runId: "r",
      consumedMessageId: "mail-1",
      triggerPayload: null,
    });
    expect(await run.started).toBe(true);
    const log = await repoStore.read("r");
    expect(log[0]).toMatchObject({
      kind: "RunStarted",
      consumedMessageId: "mail-1",
    });

    finish.resolve(undefined);
    expect((await run.complete).terminalStatus).toBe("completed");
  });

  test("resolves false for a run that settles without a durable RunStarted", async () => {
    const { env } = createEnv(async () => undefined);
    env.repoStore = {
      ...env.repoStore,
      appendBatch: () => Promise.reject(new Error("the disk is gone")),
    };
    const run = runtimeRun(definition, env, {
      runId: "r",
      consumedMessageId: "mail-1",
      triggerPayload: null,
    });
    expect(await run.started).toBe(false);
    await run.complete.catch(() => undefined);
  });

  test("settles for a resumed run whose log already holds RunStarted", async () => {
    const first = createEnv(async () => undefined);
    const done = runtimeRun(definition, first.env, {
      runId: "r",
      triggerPayload: null,
    });
    await done.complete;
    const log = await first.repoStore.read("r");

    const finish = Promise.withResolvers<undefined>();
    const second = createEnv(() => finish.promise);
    const resumed = runtimeRun(definition, second.env, {
      runId: "r",
      resumeFromEvents: log.filter((event) => event.kind === "RunStarted"),
    });
    expect(await resumed.started).toBe(true);
    finish.resolve(undefined);
    await resumed.complete;
  });

  test("answers false for a run resumed from a log that has already ended", async () => {
    const first = createEnv(async () => undefined);
    const done = runtimeRun(definition, first.env, {
      runId: "r",
      triggerPayload: null,
    });
    await done.complete;
    const log = await first.repoStore.read("r");

    const second = createEnv(async () => undefined);
    const resumed = runtimeRun(definition, second.env, {
      runId: "r",
      resumeFromEvents: log,
    });
    expect(await resumed.started).toBe(false);
    await resumed.complete;
  });
});
