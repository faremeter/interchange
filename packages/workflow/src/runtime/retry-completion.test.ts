import { expect, test } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import {
  action,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  step,
  type RepoStore,
  type Scheduler,
  type WorkflowEvent,
} from "@intx/workflow";

import { bufferedEvents } from "./commit-chain";

test("a sibling's durable completion cannot settle a run between failure and retry", async () => {
  const definition = defineWorkflow({
    id: "retry-completion",
    trigger: { type: "manual" },
    steps: {
      retrying: step({
        agent: defineAgent({
          id: "retrying",
          systemPrompt: "retrying",
          tools: [],
          capabilities: [],
          inference: { sources: [{ provider: "fake", model: "fake" }] },
        }),
        retry: { maxAttempts: 2, initialBackoffMs: 0 },
      }),
      sibling: action({ handler: "sibling" }),
    },
  });
  const inner = createInMemoryRepoStore();
  const releaseSibling = Promise.withResolvers<undefined>();
  const releaseRetryRead = Promise.withResolvers<undefined>();
  const batches: (readonly WorkflowEvent[])[] = [];
  let retryReadHeld = false;
  let loopReadOvertookRetry = false;
  const repoStore: RepoStore = {
    ...inner,
    async read(runId) {
      const events = [...(await inner.read(runId))];
      if (bufferedEvents(runId).some((event) => event.kind === "StepFailed")) {
        releaseSibling.resolve(undefined);
      }
      if (
        events.some(
          (event) =>
            event.kind === "StepCompleted" && event.stepId === "sibling",
        ) &&
        !events.some((event) => event.kind === "AttemptScheduled")
      ) {
        // A disk read in the retry writer can finish after the main loop's
        // read of the same commit. Keep the writer behind that terminal check.
        if (!retryReadHeld) {
          retryReadHeld = true;
          await releaseRetryRead.promise;
        } else {
          loopReadOvertookRetry = true;
        }
      }
      return events;
    },
    async appendBatch(runId, events) {
      await inner.appendBatch(runId, events);
      batches.push([...events]);
      if (events.some((event) => event.kind === "StepFailed")) {
        releaseSibling.resolve(undefined);
      }
    },
  };
  const clock = () => {
    // Constructing the terminal event proves the loop consumed its snapshot.
    // Release the writer without making the test depend on elapsed time.
    if (loopReadOvertookRetry) releaseRetryRead.resolve(undefined);
    return new Date();
  };
  const timers: (() => void)[] = [];
  let finished = false;
  const innerScheduler = createInMemoryScheduler({ repoStore, clock });
  const scheduler: Scheduler = {
    scheduleIn(...args) {
      const cancel = innerScheduler.scheduleIn(...args);
      timers.push(cancel);
      if (finished) cancel();
      return cancel;
    },
  };
  let attempts = 0;
  const run = runtimeRun(
    definition,
    {
      repoStore,
      scheduler,
      signalChannel: createInMemorySignalChannel(),
      blobs: createInMemoryBlobSubstrate(),
      directors: createDefaultDirectorRegistry(),
      authorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      invokeStep: async () => {
        attempts += 1;
        if (attempts === 1) throw new Error("temporary failure");
        return { output: "retried" };
      },
      invokeAction: async () => {
        await releaseSibling.promise;
        return { output: "sibling" };
      },
      spawnChild: async () => {
        throw new Error("unexpected child workflow");
      },
      clock,
      newId: (prefix) => `${prefix}-${crypto.randomUUID()}`,
      drain: createNoopDrainController(definition),
      hasUpstreamSignalResolver: true,
    },
    { runId: "retry-completion" },
  );
  try {
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(attempts).toBe(2);
    expect(result.outputs).toEqual({ retrying: "retried", sibling: "sibling" });

    const retryBatch = batches.find((events) =>
      events.some((event) => event.kind === "StepFailed"),
    );
    expect(retryBatch?.map((event) => event.kind)).toEqual(
      expect.arrayContaining(["StepFailed", "TimerSet", "AttemptScheduled"]),
    );
  } finally {
    finished = true;
    releaseRetryRead.resolve(undefined);
    for (const cancel of timers) cancel();
  }
});
