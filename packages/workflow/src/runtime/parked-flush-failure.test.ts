// A sibling child workflow can buffer its completion after the gate has
// parked; an agent step's or an action's completion is written durably and
// never waits there. The host flushes that buffer before a signal or a
// timer takes a sequence number. When the flush throws, the batch is gone
// and the parked step is still waiting, so the run has to reject with the
// flush error. The durable prefix stays as the store left it: no sibling
// completion and no terminal event.

import { describe, expect, test } from "bun:test";

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
  sleep,
  type Primitive,
  type RepoStore,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

import { whenRunNextBuffers, withRunCommitBarrier } from "./commit-chain";
import { createTerminalChildHandle } from "./suspendable-child-handle";

const childSibling: Primitive = {
  kind: "childWorkflow",
  id: "",
  definition: { ref: "child-ref" },
  drainBehavior: "cancel",
};

const parkedSignal = defineWorkflow({
  id: "parked-signal-flush",
  trigger: { type: "manual" },
  steps: {
    gate: awaitSignal({ name: "approve" }),
    sibling: childSibling,
  },
});

const parkedTimer = defineWorkflow({
  id: "parked-timer-flush",
  trigger: { type: "manual" },
  steps: {
    nap: sleep({ duration: 60_000 }),
    sibling: childSibling,
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

function isSiblingCompletion(event: WorkflowEvent): boolean {
  return event.kind === "ChildCompleted" || event.kind === "StepCompleted";
}

function rejectSiblingCompletion(inner: RepoStore, boom: Error): RepoStore {
  return {
    ...inner,
    async append(runId, event) {
      if (isSiblingCompletion(event)) throw boom;
      await inner.append(runId, event);
    },
    async appendBatch(runId, events) {
      if (events.some(isSiblingCompletion)) throw boom;
      await inner.appendBatch(runId, events);
    },
  };
}

function buildEnv(
  def: WorkflowDefinition,
  repoStore: RepoStore,
  spawnChild: WorkflowRuntimeEnv["spawnChild"],
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
    spawnChild,
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
  const repoStore = rejectSiblingCompletion(parked.repoStore, boom);
  const buffered = Promise.withResolvers<undefined>();
  const spawnChild: WorkflowRuntimeEnv["spawnChild"] = async () => {
    await parked.happened;
    buffered.resolve(whenRunNextBuffers(runId).then(() => undefined));
    return createTerminalChildHandle("completed");
  };
  const complete = runtimeRun(
    def,
    buildEnv(def, repoStore, spawnChild, scheduler(repoStore)),
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
  expect(durable.some(isSiblingCompletion)).toBe(false);
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
