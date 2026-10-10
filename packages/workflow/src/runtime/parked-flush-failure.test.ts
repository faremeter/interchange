// A sibling can buffer StepCompleted after the gate has parked. The host
// flushes that buffer before a signal or a timer takes a sequence number.
// When the flush throws, the batch is gone and the parked step is still
// waiting, so the run has to reject with the flush error. The durable
// prefix stays as the store left it: no StepCompleted and no terminal
// event.

import { describe, expect, test } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import {
  action,
  awaitSignal,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  sleep,
  type ActionInvoker,
  type RepoStore,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

import { whenRunNextBuffers, withRunCommitBarrier } from "./commit-chain";

const parkedSignal = defineWorkflow({
  id: "parked-signal-flush",
  trigger: { type: "manual" },
  steps: {
    gate: awaitSignal({ name: "approve" }),
    sibling: action({ handler: "sibling" }),
  },
});

const parkedTimer = defineWorkflow({
  id: "parked-timer-flush",
  trigger: { type: "manual" },
  steps: {
    nap: sleep({ duration: 60_000 }),
    sibling: action({ handler: "sibling" }),
  },
});

function whenKindAppended(
  inner: RepoStore,
  kind: WorkflowEvent["kind"],
): { repoStore: RepoStore; happened: Promise<undefined> } {
  const happened = Promise.withResolvers<undefined>();
  let seen = false;
  function note(events: readonly WorkflowEvent[]): void {
    if (seen) return;
    if (!events.some((event) => event.kind === kind)) return;
    seen = true;
    happened.resolve(undefined);
  }
  return {
    happened: happened.promise,
    repoStore: {
      ...inner,
      async append(runId, event) {
        await inner.append(runId, event);
        note([event]);
      },
      async appendBatch(runId, events) {
        await inner.appendBatch(runId, events);
        note(events);
      },
    },
  };
}

function rejectStepCompleted(inner: RepoStore, boom: Error): RepoStore {
  return {
    ...inner,
    async append(runId, event) {
      if (event.kind === "StepCompleted") throw boom;
      await inner.append(runId, event);
    },
    async appendBatch(runId, events) {
      if (events.some((event) => event.kind === "StepCompleted")) throw boom;
      await inner.appendBatch(runId, events);
    },
  };
}

function buildEnv(
  def: WorkflowDefinition,
  repoStore: RepoStore,
  invokeAction: ActionInvoker,
  scheduler: WorkflowRuntimeEnv["scheduler"],
): WorkflowRuntimeEnv {
  return {
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
    invokeStep: async () => ({ output: null }),
    invokeAction,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock: () => new Date(0),
    newId: (prefix) => `${prefix}-parked-flush`,
    drain: createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
  };
}

async function flushWhileParked(
  def: WorkflowDefinition,
  parkKind: "SignalAwaited" | "TimerSet",
  runId: string,
  scheduler: (repoStore: RepoStore) => WorkflowRuntimeEnv["scheduler"],
  write: (repoStore: RepoStore) => Promise<void>,
  boom: Error,
): Promise<void> {
  const inner = createInMemoryRepoStore();
  const parked = whenKindAppended(inner, parkKind);
  const repoStore = rejectStepCompleted(parked.repoStore, boom);
  const buffered = Promise.withResolvers<undefined>();
  const invokeAction: ActionInvoker = async () => {
    await parked.happened;
    buffered.resolve(whenRunNextBuffers(runId).then(() => undefined));
    return { output: { done: true } };
  };
  const complete = runtimeRun(
    def,
    buildEnv(def, repoStore, invokeAction, scheduler(repoStore)),
    { runId },
  ).complete;
  const outcome = complete.then(
    () => undefined,
    (cause: unknown) => cause,
  );
  await buffered.promise;
  await expect(write(repoStore)).rejects.toBe(boom);
  await expect(outcome).resolves.toBe(boom);
  const durable = await inner.read(runId);
  expect(durable.some((event) => event.kind === "StepCompleted")).toBe(false);
  expect(durable.some((event) => event.kind === "RunFailed")).toBe(false);
  expect(durable.some((event) => event.kind === "RunCancelled")).toBe(false);
  expect(
    durable.some(
      (event) => event.kind === "SignalReceived" || event.kind === "TimerFired",
    ),
  ).toBe(false);
}

describe("external flush failure while a run is parked", () => {
  test("a failed signal flush rejects the parked run", async () => {
    const boom = new Error("signal append failed");
    const runId = "run-parked-signal-flush";
    const clock = (): Date => new Date(0);
    await flushWhileParked(
      parkedSignal,
      "SignalAwaited",
      runId,
      (repoStore) => createInMemoryScheduler({ repoStore, clock }),
      (repoStore) =>
        withRunCommitBarrier({ repoStore }, runId, async () => {
          await repoStore.append(runId, {
            kind: "SignalReceived",
            seq: 0,
            at: clock().toISOString(),
            signalName: "approve",
            signalId: "sig-parked",
            payload: { ok: true },
          });
        }),
      boom,
    );
  });

  test("a failed timer flush rejects the parked run", async () => {
    const boom = new Error("timer append failed");
    const runId = "run-parked-timer-flush";
    const clock = (): Date => new Date(0);
    await flushWhileParked(
      parkedTimer,
      "TimerSet",
      runId,
      () => ({
        scheduleIn() {
          return () => undefined;
        },
      }),
      (repoStore) =>
        withRunCommitBarrier({ repoStore }, runId, async () => {
          await repoStore.append(runId, {
            kind: "TimerFired",
            seq: 0,
            at: clock().toISOString(),
            timerId: "timer-parked",
          });
        }),
      boom,
    );
  });
});
