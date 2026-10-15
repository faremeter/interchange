import { expect, test } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";
import {
  action,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  type RepoStore,
} from "@intx/workflow";

test.each([
  {
    scenario: "dependent",
    name: "a sibling's completion cannot start a dependent before its input is published",
  },
  {
    scenario: "terminal",
    name: "a run publishes every output when its last completion acknowledgement is delayed",
  },
  {
    scenario: "cancel",
    name: "cancellation during the last completion acknowledgement still settles the run",
  },
])("$name", async ({ scenario }) => {
  const definition = defineWorkflow({
    id: `completion-output-${scenario}`,
    trigger: { type: "manual" },
    steps: {
      sibling: action({ handler: "sibling" }),
      producer: action({ handler: "producer" }),
      ...(scenario === "dependent"
        ? {
            consumer: action({
              handler: "consumer",
              after: ["producer"],
              input: { from: "steps.producer.output" },
            }),
          }
        : {}),
    },
  });
  const inner = createInMemoryRepoStore();
  const releaseAcknowledgement = Promise.withResolvers<undefined>();
  const cancellation = Promise.withResolvers<undefined>();
  let cancellationRequested = false;
  let acknowledgementHeld = false;
  let readBeforeAcknowledgement = false;
  const repoStore: RepoStore = {
    ...inner,
    async appendBatch(runId, events) {
      await inner.appendBatch(runId, events);
      if (
        events.some(
          (event) =>
            event.kind === "RunCompleted" || event.kind === "RunCancelled",
        )
      ) {
        expect(acknowledgementHeld).toBe(false);
      }
      if (
        events.some(
          (event) =>
            event.kind === "StepCompleted" && event.stepId === "producer",
        )
      ) {
        // The supervisor's commit is visible on disk before the child's
        // write acknowledgement arrives. A sibling wakes the scheduler here.
        acknowledgementHeld = true;
        await releaseAcknowledgement.promise;
        acknowledgementHeld = false;
      }
    },
    async read(runId) {
      const events = await inner.read(runId);
      if (acknowledgementHeld) {
        readBeforeAcknowledgement = true;
        if (scenario === "cancel" && !cancellationRequested) {
          cancellationRequested = true;
          void run
            .cancel("self", "cancel while completion is acknowledged")
            .then(() => cancellation.resolve(undefined), cancellation.reject);
        }
        releaseAcknowledgement.resolve(undefined);
      }
      return events;
    },
  };
  const clock = () => new Date();
  const inputs: unknown[] = [];
  const run = runtimeRun(
    definition,
    {
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
      invokeAction: async (request) => {
        if (request.handler === "consumer") inputs.push(request.input);
        return {
          output: request.handler === "producer" ? { value: "ready" } : null,
        };
      },
      spawnChild: async () => {
        throw new Error("unexpected child workflow");
      },
      clock,
      newId: (prefix) => `${prefix}-${crypto.randomUUID()}`,
      drain: createNoopDrainController(definition),
      hasUpstreamSignalResolver: true,
    },
    { runId: `completion-output-${scenario}` },
  );
  const [result] = await Promise.all([
    run.complete,
    scenario === "cancel" ? cancellation.promise : Promise.resolve(),
  ]);

  expect(readBeforeAcknowledgement).toBe(true);
  expect(result.terminalStatus).toBe(
    scenario === "cancel" ? "cancelled" : "completed",
  );
  expect(result.outputs).toEqual({
    sibling: null,
    producer: { value: "ready" },
    ...(scenario === "dependent" ? { consumer: null } : {}),
  });
  expect(inputs).toEqual(scenario === "dependent" ? [{ value: "ready" }] : []);
});
