import { describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import { defineWorkflow, step } from "../definition/index";
import { createInMemoryBlobSubstrate } from "../runlocal/blob-substrate";
import { createInMemoryRepoStore } from "../runlocal/repo-store";
import { createInMemoryScheduler } from "../runlocal/scheduler";
import { createInMemorySignalChannel } from "../runlocal/signal-channel";
import { flushChain } from "./commit-chain";
import { createNoopDrainController } from "./drain";
import type { RepoStore, WorkflowRuntimeEnv } from "./env";
import { runtimeRun } from "./run";

const definition = defineWorkflow({
  id: "apply-committed-cancellation",
  trigger: { type: "manual" },
  steps: {
    park: step({
      agent: defineAgent({
        id: "parked",
        systemPrompt: "you are parked",
        tools: [],
        capabilities: [],
        inference: { sources: [{ provider: "fake", model: "fake" }] },
      }),
    }),
  },
});

/** Start a run parked in its only step, with its store's reads failable. */
async function startParkedRun() {
  const inner = createInMemoryRepoStore();
  let failNextRead = false;
  const repoStore: RepoStore = {
    async read(runId) {
      if (failNextRead) {
        failNextRead = false;
        throw new Error("substrate read failed");
      }
      return inner.read(runId);
    },
    append: inner.append.bind(inner),
    appendBatch: inner.appendBatch.bind(inner),
    subscribe: inner.subscribe.bind(inner),
  };
  const parked = Promise.withResolvers<undefined>();
  const clock = () => new Date();
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
    invokeStep: async ({ signal }) => {
      parked.resolve(undefined);
      await new Promise((resolve) =>
        signal.addEventListener("abort", resolve, { once: true }),
      );
      throw signal.reason;
    },
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(definition),
    hasUpstreamSignalResolver: true,
  };
  const run = runtimeRun(definition, env);
  await parked.promise;

  // Commit the request outside the runtime, as a supervisor does.
  await flushChain(env, run.runId);
  const events = await inner.read(run.runId);
  await inner.append(run.runId, {
    kind: "CancelRequested",
    seq: (events.at(-1)?.seq ?? 0) + 1,
    at: clock().toISOString(),
    origin: "supervisor-operator",
    reason: "Lifetime expired",
  });

  return {
    run,
    failNextRead: () => {
      failNextRead = true;
    },
  };
}

describe("applyCommittedCancellation", () => {
  test("cancels the run without requesting cancellation again", async () => {
    const { run } = await startParkedRun();

    await run.applyCommittedCancellation();

    const result = await run.complete;
    expect(result.terminalStatus).toBe("cancelled");
    expect(
      result.events.filter((event) => event.kind === "CancelRequested"),
    ).toHaveLength(1);
  });

  test("stops a parked run even when reading its state fails", async () => {
    const { run, failNextRead } = await startParkedRun();

    failNextRead();
    await expect(run.applyCommittedCancellation()).rejects.toThrow(
      "substrate read failed",
    );

    const result = await run.complete;
    expect(result.terminalStatus).toBe("cancelled");
  });
});
