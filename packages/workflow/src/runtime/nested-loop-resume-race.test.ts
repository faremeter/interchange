// A signal handed down by an outer loop while a resuming nested loop plans its
// relays. The nested loop reads its state and its log; the hand-down lands in
// the log, so it must not land where the state sees it and the plan does not.
import { expect, test } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";
import {
  action,
  awaitSignal,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createLoopIterationHandle,
  createNoopDrainController,
  defineWorkflow,
  enumerateInlineLoopBodies,
  loop,
  loopBodyRunId,
  runtimeRun,
  type LoopFn,
  type RepoStore,
  type SignalChannel,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { waitForNthEvent } from "@intx/workflow/testing";

import { withRunCommitBarrier } from "./commit-chain";

const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return () => false;
  if (ref === "next") return () => 0;
  throw new Error(ref);
};

const leaf = defineWorkflow({
  id: "leaf",
  trigger: { type: "manual" },
  steps: { hold: awaitSignal({ name: "go" }) },
});
const middle = defineWorkflow({
  id: "middle",
  trigger: { type: "manual" },
  steps: {
    inner: loop({
      body: leaf,
      while: "cont",
      carry: "next",
      input: { literal: 0 },
      maxIterations: 2,
      onExhausted: "innerEscalate",
    }),
    innerEscalate: action({ handler: "escalate", after: ["inner"] }),
  },
});
const top: WorkflowDefinition = defineWorkflow({
  id: "top",
  trigger: { type: "manual" },
  steps: {
    outer: loop({
      body: middle,
      while: "cont",
      carry: "next",
      input: { literal: 0 },
      maxIterations: 2,
      onExhausted: "escalate",
    }),
    escalate: action({ handler: "escalate", after: ["outer"] }),
  },
});

// Like the host channel: a delivery lands in the run's own log, and an awaiter
// sees only what lands after it subscribed.
function logWritingChannel(repoStore: RepoStore, runId: string): SignalChannel {
  const awaiters: {
    name: string;
    wake: () => void;
    resolve: (delivery: { payload: unknown; signalId: string }) => void;
  }[] = [];
  return {
    async deliver(name, payload, signalId) {
      const id = signalId ?? `signal-${runId}`;
      let landed = false;
      await withRunCommitBarrier({ repoStore }, runId, async () => {
        const log = await repoStore.read(runId);
        if (log.some((e) => e.kind === "SignalReceived" && e.signalId === id)) {
          return;
        }
        await repoStore.append(runId, {
          kind: "SignalReceived",
          seq: (log.at(-1)?.seq ?? 0) + 1,
          at: new Date().toISOString(),
          signalName: name,
          signalId: id,
          payload,
        });
        landed = true;
      });
      if (landed) {
        const index = awaiters.findIndex((a) => a.name === name);
        if (index >= 0) {
          const [awaiter] = awaiters.splice(index, 1);
          awaiter?.wake();
          awaiter?.resolve({ payload, signalId: id });
        }
      }
    },
    awaitNext(name, onWake, signal) {
      if (signal?.aborted === true) {
        onWake();
        return Promise.reject(new Error("aborted"));
      }
      return new Promise((resolve, reject) => {
        const entry = { name, wake: onWake, resolve };
        awaiters.push(entry);
        signal?.addEventListener(
          "abort",
          () => {
            const index = awaiters.indexOf(entry);
            if (index >= 0) awaiters.splice(index, 1);
            onWake();
            reject(new Error("aborted"));
          },
          { once: true },
        );
      });
    },
  };
}

function buildEnv(
  repoStore: RepoStore,
  channelFor: (runId: string) => SignalChannel,
  rootRunId: string,
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: channelFor(rootRunId),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: () => {
      throw new Error("no agents here");
    },
    invokeAction: async () => ({ output: 1 }),
    spawnChild: () => {
      throw new Error("no child workflows here");
    },
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(top),
    hasUpstreamSignalResolver: true,
    loopFns,
  };
  const bodies = new Map<string, WorkflowDefinition>();
  const collect = (definition: WorkflowDefinition): void => {
    for (const body of enumerateInlineLoopBodies(definition)) {
      bodies.set(body.ref, body.definition);
      collect(body.definition);
    }
  };
  collect(top);
  env.spawnLoopIteration = async (args) => {
    const definition = bodies.get(args.definitionRef);
    if (definition === undefined) throw new Error(args.definitionRef);
    return createLoopIterationHandle(env, {
      definition,
      childRunId: args.childRunId,
      input: args.input,
      depth: args.depth,
      maxChildSpawnDepth: args.maxChildSpawnDepth,
      ...(args.resumeFromEvents !== undefined
        ? { resumeFromEvents: args.resumeFromEvents }
        : {}),
      signal: args.signal,
      signalChannel: channelFor(args.childRunId),
    });
  };
  return env;
}

function channelsFor(
  make: (runId: string) => SignalChannel,
): (runId: string) => SignalChannel {
  const channels = new Map<string, SignalChannel>();
  return (runId) => {
    let channel = channels.get(runId);
    if (channel === undefined) {
      channel = make(runId);
      channels.set(runId, channel);
    }
    return channel;
  };
}

const RUN_ID = "nested-race";
const MIDDLE_RUN_ID = loopBodyRunId(RUN_ID, "outer", 0);
const LEAF_RUN_ID = loopBodyRunId(MIDDLE_RUN_ID, "inner", 0);

async function parkedLogs(): Promise<Map<string, WorkflowEvent[]>> {
  const store = createInMemoryRepoStore();
  const channelFor = channelsFor(() => createInMemorySignalChannel());
  const abort = new AbortController();
  const run = runtimeRun(top, buildEnv(store, channelFor, RUN_ID), {
    runId: RUN_ID,
    localAbort: abort.signal,
  });
  await waitForNthEvent(
    store,
    RUN_ID,
    (e) => e.kind === "SignalAwaited" && e.parkKind === "signal-relay",
    1,
  );
  const logs = new Map<string, WorkflowEvent[]>();
  for (const id of [RUN_ID, MIDDLE_RUN_ID, LEAF_RUN_ID]) {
    logs.set(id, [...(await store.read(id))]);
  }
  abort.abort();
  await run.complete;
  return logs;
}

// `runLoop`'s own read of the log, not one by a helper it calls.
function isRunLoopRead(): boolean {
  const frame = (new Error().stack ?? "")
    .split("\n")
    .find((line) => line.includes("runtime/run.ts"));
  return frame?.includes("runLoop") === true;
}

test.each([
  { landsAfter: "the log read", snapshotFirst: true },
  { landsAfter: "the state read", snapshotFirst: false },
])(
  "a signal handed down to a resuming nested loop right after $landsAfter still reaches its body",
  async ({ snapshotFirst }) => {
    const logs = await parkedLogs();
    const inner = createInMemoryRepoStore();
    for (const id of [MIDDLE_RUN_ID, LEAF_RUN_ID]) {
      for (const event of logs.get(id) ?? []) await inner.append(id, event);
    }

    const handedDown = Promise.withResolvers<undefined>();
    const loopRead = Promise.withResolvers<undefined>();
    let held = false;
    const store: RepoStore = {
      ...inner,
      read: async (id) => {
        const holds = id === MIDDLE_RUN_ID && !held && isRunLoopRead();
        if (!holds) return inner.read(id);
        held = true;
        loopRead.resolve(undefined);
        if (snapshotFirst) {
          const snapshot = [...(await inner.read(id))];
          await handedDown.promise;
          return snapshot;
        }
        await handedDown.promise;
        return inner.read(id);
      },
    };
    const channelFor = channelsFor((id) => {
      const channel = logWritingChannel(store, id);
      if (id !== MIDDLE_RUN_ID) return channel;
      return {
        ...channel,
        deliver: async (...args) => {
          await channel.deliver(...args);
          handedDown.resolve(undefined);
        },
      };
    });

    const env = buildEnv(store, channelFor, RUN_ID);
    const awaiter = Promise.withResolvers<undefined>();
    env.onSignalAwaiter = () => {
      awaiter.resolve(undefined);
    };
    const run = runtimeRun(top, env, {
      runId: RUN_ID,
      resumeFromEvents: logs.get(RUN_ID) ?? [],
    });
    await loopRead.promise;
    await awaiter.promise;
    await channelFor(RUN_ID).deliver("go", { ok: true }, "signal-1");

    expect((await run.complete).terminalStatus).toBe("completed");
  },
);
