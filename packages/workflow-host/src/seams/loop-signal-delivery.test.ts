// One delivery of a loop-body signal is one blob on the parent log and one
// blob on the iteration log, whether it lands after the relay wait is
// subscribed or while that wait is still buffered.
//
// Both channels are host channels with `readState: () => emptyState(runId)`,
// the production reader. An in-memory channel never appends, so it hides the
// second blob this file exists to count.

import { afterAll, beforeAll, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { createDefaultDirectorRegistry } from "@intx/agent";
import { generateKeyPair } from "@intx/crypto";
import { createRepoStore } from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
  RepoId,
  ValidatePushResult,
} from "@intx/hub-sessions";
import type { KeyPair } from "@intx/types/runtime";
import {
  createTerminalChildHandle,
  action,
  awaitSignal,
  createInMemoryBlobSubstrate,
  createInMemoryScheduler,
  createNoopDrainController,
  defineWorkflow,
  emptyState,
  enumerateInlineLoopBodies,
  loop,
  loopBodyRunId,
  type ActionInvoker,
  type LoopFn,
  type SignalChannel,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import {
  bufferedEvents,
  createLoopIterationHandle,
  dropChain,
  runtimeRun,
  whenRunNextBuffers,
} from "@intx/workflow/runtime";

import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import {
  createWorkflowHostSignalChannel,
  type SignalChannelHandle,
} from "./signal-channel";

const REF = "refs/heads/main";
const PAYLOAD = { done: true };
const SIGNAL_ID = "sig-go";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const dir of tempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

const allowAll: AuthorizeFn = () => ({ allowed: true });
const principal: Principal = { kind: "test" };

function permissiveHandler(directoryPrefix: string): KindHandler {
  return {
    kind: "agent-state",
    directoryPrefix,
    validatePush(): ValidatePushResult {
      return { ok: true };
    },
    onRefUpdated() {
      /* no-op */
    },
  };
}

const parentDef = defineWorkflow({
  id: "loop-signal-parent",
  trigger: { type: "manual" },
  steps: {
    rework: loop({
      body: defineWorkflow({
        id: "loop-signal-body",
        trigger: { type: "manual" },
        steps: { hold: awaitSignal({ name: "go" }) },
      }),
      while: "cont",
      carry: "thread",
      maxIterations: 3,
      onExhausted: "escalate",
    }),
    escalate: action({ handler: "escalate", after: ["rework"] }),
  },
});

const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return () => false;
  if (ref === "thread") return () => null;
  throw new Error(`unknown loop fn ${ref}`);
};

type SignalReceivedEvent = Extract<WorkflowEvent, { kind: "SignalReceived" }>;

function receivedWithId(
  events: readonly WorkflowEvent[],
  signalId: string,
): SignalReceivedEvent[] {
  return events.filter(
    (event): event is SignalReceivedEvent =>
      event.kind === "SignalReceived" && event.signalId === signalId,
  );
}

function loopOutcome(outputs: Record<string, unknown>): string {
  const rework = outputs.rework;
  if (
    typeof rework === "object" &&
    rework !== null &&
    "outcome" in rework &&
    typeof rework.outcome === "string"
  ) {
    return rework.outcome;
  }
  throw new Error("loop output missing outcome");
}

// `awaitNext` subscribes `from: "head"` and this host's `readState` is
// empty, so a delivery that lands after the relay blob is durable but
// before that subscription misses the blob and the run never resumes.
// The gate opens from inside `awaitNext`, after the subscriber is
// registered and after the runtime has flushed the relay wait.
function deliverOnceSubscribed(inner: SignalChannelHandle): {
  channel: SignalChannel;
  subscribed: Promise<void>;
} {
  let resolveSubscribed: () => void = () => undefined;
  const subscribed = new Promise<void>((resolve) => {
    resolveSubscribed = resolve;
  });
  const channel: SignalChannel = {
    deliver: (name, payload, signalId) =>
      inner.deliver(name, payload, signalId),
    async awaitNext(name, signal) {
      const pending = inner.awaitNext(name, signal);
      if (name === "go") resolveSubscribed();
      return pending;
    },
  };
  return { channel, subscribed };
}

async function runLoopDelivery(args: {
  prefix: string;
  parentChannel: (inner: SignalChannelHandle) => SignalChannel;
  onStart: (parent: SignalChannelHandle, runId: string) => void;
}): Promise<{
  terminalStatus: string;
  outcome: string;
  escalated: number;
  parentSignals: SignalReceivedEvent[];
  iterationSignals: SignalReceivedEvent[];
}> {
  const dataDir = await makeTempDir(args.prefix);
  const substrate = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "agent-state": permissiveHandler(args.prefix) },
    authorize: allowAll,
  });
  const repoId: RepoId = { kind: "agent-state", id: args.prefix };
  const runtimeStore = createWorkflowRunRepoStore({
    substrate,
    repoId,
    principal,
    ref: REF,
  });
  const runId = `${args.prefix}-run`;
  const channels: SignalChannelHandle[] = [];
  const hostChannel = (channelRunId: string): SignalChannelHandle => {
    const channel = createWorkflowHostSignalChannel({
      repoStore: substrate,
      runtimeStore,
      principal,
      repoId,
      ref: REF,
      runId: channelRunId,
      readState: () => emptyState(channelRunId),
      newId: () => "generated-signal",
      clock: () => new Date("2026-01-01T00:00:00.000Z"),
    });
    channels.push(channel);
    return channel;
  };

  const parent = hostChannel(runId);
  const escalated: string[] = [];
  const invokeAction: ActionInvoker = async ({ handler }) => {
    if (handler === "escalate") {
      escalated.push(handler);
      return { output: "escalated" };
    }
    throw new Error(`unknown handler ${handler}`);
  };
  const bodies = new Map(
    enumerateInlineLoopBodies(parentDef).map((body) => [
      body.ref,
      body.definition,
    ]),
  );
  const clock = (): Date => new Date("2026-01-01T00:00:00.000Z");
  const env: WorkflowRuntimeEnv = {
    repoStore: runtimeStore,
    scheduler: createInMemoryScheduler({ repoStore: runtimeStore, clock }),
    signalChannel: args.parentChannel(parent),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: () => {
      throw new Error("loop signal delivery: invokeStep must not be called");
    },
    invokeAction,
    spawnChild: async () => createTerminalChildHandle("completed"),
    clock,
    newId: (prefix) => `${prefix}-id`,
    drain: createNoopDrainController(parentDef),
    hasUpstreamSignalResolver: true,
    loopFns,
  };
  env.spawnLoopIteration = async ({
    definitionRef,
    childRunId,
    input,
    depth,
    maxChildSpawnDepth,
    signal,
    resumeFromEvents,
  }) => {
    const definition = bodies.get(definitionRef);
    if (definition === undefined) {
      throw new Error(
        `loop iteration ${definitionRef} has no lifted body definition`,
      );
    }
    const channel = hostChannel(childRunId);
    return createLoopIterationHandle(env, {
      definition,
      childRunId,
      input,
      depth,
      maxChildSpawnDepth,
      ...(resumeFromEvents !== undefined ? { resumeFromEvents } : {}),
      signal,
      signalChannel: channel,
      cleanup: () => channel.stop(),
    });
  };

  try {
    args.onStart(parent, runId);
    const run = runtimeRun(parentDef, env, { runId });
    const result = await run.complete;
    const iterationId = loopBodyRunId(runId, "rework", 0);
    const parentLog = await runtimeStore.read(runId);
    const iterationLog = await runtimeStore.read(iterationId);
    return {
      terminalStatus: result.terminalStatus,
      outcome: loopOutcome(result.outputs),
      escalated: escalated.length,
      parentSignals: receivedWithId(parentLog, SIGNAL_ID),
      iterationSignals: receivedWithId(iterationLog, SIGNAL_ID),
    };
  } finally {
    dropChain(runId);
    dropChain(loopBodyRunId(runId, "rework", 0));
    await Promise.all(channels.map((channel) => channel.stop()));
  }
}

function expectOneDelivery(result: {
  terminalStatus: string;
  outcome: string;
  escalated: number;
  parentSignals: SignalReceivedEvent[];
  iterationSignals: SignalReceivedEvent[];
}): void {
  expect(result.terminalStatus).toBe("completed");
  expect(result.outcome).toBe("converged");
  expect(result.escalated).toBe(0);
  expect(result.parentSignals).toHaveLength(1);
  expect(result.iterationSignals).toHaveLength(1);
  expect(result.parentSignals[0]?.payload).toEqual(PAYLOAD);
  expect(result.iterationSignals[0]?.payload).toEqual(PAYLOAD);
}

test(
  "a signal delivered after the relay wait is subscribed is recorded once",
  async () => {
    let subscribed: Promise<void> = Promise.resolve();
    const result = await runLoopDelivery({
      prefix: "loop-sig-after",
      parentChannel: (inner) => {
        const wrapped = deliverOnceSubscribed(inner);
        subscribed = wrapped.subscribed;
        return wrapped.channel;
      },
      onStart: (parent) => {
        void subscribed.then(() => {
          void parent.deliver("go", PAYLOAD, SIGNAL_ID);
        });
      },
    });
    expectOneDelivery(result);
  },
  { timeout: 20000 },
);

test(
  "a signal delivered while the relay wait is buffered is recorded once",
  async () => {
    let delivered = false;
    const result = await runLoopDelivery({
      prefix: "loop-sig-race",
      parentChannel: (inner) => inner,
      onStart: (parent, runId) => {
        const arm = (): void => {
          void whenRunNextBuffers(runId).then(() => {
            if (delivered) return;
            const waiting = bufferedEvents(runId).some(
              (event) =>
                event.kind === "SignalAwaited" &&
                event.parkKind === "signal-relay" &&
                event.signalName === "go" &&
                event.stepId === "rework",
            );
            if (!waiting) {
              arm();
              return;
            }
            // Do not await `deliver` here. The barrier waits on this
            // commit link, and this wake is that link.
            delivered = true;
            void parent.deliver("go", PAYLOAD, SIGNAL_ID);
          });
        };
        arm();
      },
    });
    expect(delivered).toBe(true);
    expectOneDelivery(result);
  },
  { timeout: 20000 },
);
