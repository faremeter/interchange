// A child reports quiet only when nothing works and no committed input is on
// its way to a run. Whoever ends a wait must count the waiter as working
// before it stops counting itself, or a quiet check can land in between and
// stop a child that is about to act. Each test drives one way a wait ends and
// looks, at the first moment the process looks quiet, at what the woken run
// has done by then.
//
// The runs use the child's own stack: the proxy store over a real repo store,
// the host signal channel, and the runtime store adapter, so a subscription
// counts a commit as on its way until its consumer is done with it, as in a
// live child.

import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  expect,
  test,
} from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
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
  action,
  awaitSignal,
  childWorkflow,
  createInMemoryBlobSubstrate,
  createNoopDrainController,
  createSuspendableChildHandle,
  defineWorkflow,
  emptyState,
  resumeFromLog,
  rewriteInlineChildWorkflowBodies,
  runtimeRun,
  sleep,
  step,
  type RepoStore as RuntimeRepoStore,
  type RuntimeWorkflowRun,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import {
  beginDelivery,
  bufferedEvents,
  isQuiet,
  isWorking,
  whenQuiet,
  withRunCommitBarrier,
} from "@intx/workflow/runtime";

import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import {
  createWorkflowHostSignalChannel,
  type SignalChannelHandle,
} from "../seams/signal-channel";

import { createProxyWorkflowRunRepoStore } from "./proxy-repo-store";

const REF = "refs/heads/main";
const AT = "2026-01-01T00:00:00.000Z";
const principal: Principal = { kind: "test" };
const allowAll: AuthorizeFn = () => ({ allowed: true });

const tempDirs: string[] = [];
let signingKey: KeyPair;
const teardowns: (() => Promise<void>)[] = [];

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

// The counts are process-wide. Work an earlier test file left running in
// this worker would keep `whenQuiet` from ever resolving.
beforeEach(() => {
  if (!isQuiet()) {
    throw new Error("work or a delivery from an earlier test is still running");
  }
});

afterEach(async () => {
  for (const teardown of teardowns.splice(0).reverse()) await teardown();
});

afterAll(async () => {
  for (const dir of tempDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

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

type Timer = { runId: string; timerId: string };

// The adapter closes the store tail a few microtasks after its consumer
// returns. Closing it at once ends that delivery as early as it can end, so
// a handoff cannot lean on those microtasks.
function closingOnReturn(
  store: RuntimeRepoStore,
  runId: string,
  opts: Parameters<RuntimeRepoStore["subscribe"]>[1],
): ReturnType<RuntimeRepoStore["subscribe"]> {
  const done = new AbortController();
  const inner = store.subscribe(runId, {
    ...opts,
    signal: AbortSignal.any([opts.signal, done.signal]),
  });
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next: () => inner.next(),
    async return() {
      done.abort();
      return (await inner.return?.()) ?? { done: true, value: undefined };
    },
  };
}

async function childStack(
  prefix: string,
  definition: WorkflowDefinition,
  invokeStep: StepInvoker = () => {
    throw new Error("quiet handoff: no agent step runs here");
  },
) {
  const dataDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dataDir);
  const bare = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "agent-state": permissiveHandler(prefix) },
    authorize: allowAll,
  });
  const repoId: RepoId = { kind: "agent-state", id: prefix };
  const proxy = createProxyWorkflowRunRepoStore({
    bareStore: bare,
    bridge: {
      async submit(req) {
        const result = await bare.writeTreePreservingPrefix(
          principal,
          repoId,
          req.ref,
          {
            preservePrefix: req.preservePrefix,
            message: req.message,
            merge: req.merge,
          },
        );
        return { commitSha: result.commitSha };
      },
      handleMergeRequest: () => undefined,
      handleWriteResponse: () => undefined,
      cancelAll: () => undefined,
      pendingCount: 0,
    },
    workflowRunRepoId: repoId,
  });
  const adapter = createWorkflowRunRepoStore({
    substrate: proxy,
    repoId,
    principal,
    ref: REF,
  });
  // What each run has made durable, readable at once.
  const durable = new Map<string, WorkflowEvent[]>();
  const record = (runId: string, events: readonly WorkflowEvent[]): void => {
    durable.set(runId, [...(durable.get(runId) ?? []), ...events]);
  };
  const runtimeStore: RuntimeRepoStore = {
    read: (runId) => adapter.read(runId),
    subscribe: (runId, opts) => closingOnReturn(adapter, runId, opts),
    async append(runId, event) {
      await adapter.append(runId, event);
      record(runId, [event]);
    },
    async appendBatch(runId, events) {
      await adapter.appendBatch(runId, events);
      record(runId, events);
    },
  };

  const channels: SignalChannelHandle[] = [];
  const hostChannel = (runId: string): SignalChannelHandle => {
    const channel = createWorkflowHostSignalChannel({
      repoStore: proxy,
      runtimeStore,
      principal,
      repoId,
      ref: REF,
      runId,
      readState: () => emptyState(runId),
      newId: () => "generated-signal",
      clock: () => new Date(AT),
    });
    channels.push(channel);
    return channel;
  };

  const timers: Timer[] = [];
  const armed: (() => void)[] = [];
  const rewrite = rewriteInlineChildWorkflowBodies(definition);
  const bodies = new Map(rewrite.bodies.map((b) => [b.ref, b.definition]));

  const envFor = (
    runId: string,
    runDefinition: WorkflowDefinition,
  ): WorkflowRuntimeEnv => ({
    repoStore: runtimeStore,
    scheduler: {
      scheduleIn(timerRunId, timerId) {
        timers.push({ runId: timerRunId, timerId });
        for (const wake of armed.splice(0)) wake();
        return () => undefined;
      },
    },
    signalChannel: hostChannel(runId),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep,
    invokeAction: async () => ({ output: "done" }),
    async spawnChild(spawn) {
      const body = bodies.get(spawn.definitionRef);
      if (body === undefined) {
        throw new Error(`no child workflow ${spawn.definitionRef}`);
      }
      return createSuspendableChildHandle(envFor(spawn.childRunId, body), {
        definition: body,
        childRunId: spawn.childRunId,
        input: spawn.input,
        depth: spawn.depth,
        maxChildSpawnDepth: spawn.maxChildSpawnDepth,
        ...(spawn.resumeFromEvents !== undefined
          ? { resumeFromEvents: spawn.resumeFromEvents }
          : {}),
        signal: spawn.signal,
        surfaceInputPark: true,
      });
    },
    clock: () => new Date(AT),
    newId: (idPrefix) => `${idPrefix}-id`,
    drain: createNoopDrainController(runDefinition),
    hasUpstreamSignalResolver: true,
  });

  const stops: (() => Promise<void>)[] = [];
  const stop = async (): Promise<void> => {
    for (const stopRun of stops.splice(0).reverse()) await stopRun();
  };
  teardowns.push(stop);

  const start = (runId: string): RuntimeWorkflowRun => {
    const teardown = new AbortController();
    const run = runtimeRun(rewrite.workflow, envFor(runId, rewrite.workflow), {
      runId,
      triggerPayload: null,
      localAbort: teardown.signal,
    });
    stops.push(async () => {
      teardown.abort();
      await run.complete.catch(() => undefined);
      for (const channel of channels.splice(0)) await channel.stop();
    });
    return run;
  };

  // Events a run has committed or buffered so far.
  const events = (runId: string): WorkflowEvent[] => [
    ...(durable.get(runId) ?? []),
    ...bufferedEvents(runId),
  ];

  const nextTimer = (): Promise<Timer> =>
    new Promise((resolve) => {
      const take = (): void => {
        const timer = timers.shift();
        if (timer === undefined) armed.push(take);
        else resolve(timer);
      };
      take();
    });

  // Fire a timer as the host scheduler does: the commit is input on its way
  // to the run from the moment the timer leaves the queue. `seen` reads the
  // first quiet moment after the fire began.
  async function fireTimer<T>(timer: Timer, look: () => T): Promise<T> {
    const endDelivery = beginDelivery();
    const seen = whenQuiet().then(look);
    try {
      await withRunCommitBarrier(
        { repoStore: runtimeStore },
        timer.runId,
        async () => {
          const state = resumeFromLog(
            timer.runId,
            await adapter.read(timer.runId),
          );
          await runtimeStore.append(timer.runId, {
            kind: "TimerFired",
            seq: state.lastSeq + 1,
            at: AT,
            timerId: timer.timerId,
          });
        },
      );
    } finally {
      endDelivery();
    }
    return seen;
  }

  // Deliver a signal as the child's control loop does.
  async function deliverSignal<T>(
    runId: string,
    name: string,
    look: () => T,
  ): Promise<T> {
    const endDelivery = beginDelivery();
    const seen = whenQuiet().then(look);
    try {
      const transient = createWorkflowHostSignalChannel({
        repoStore: proxy,
        runtimeStore,
        principal,
        repoId,
        ref: REF,
        runId,
        readState: () => emptyState(runId),
        newId: () => "delivered-signal",
        clock: () => new Date(AT),
      });
      await transient.deliver(name, { approved: true }, `sig-${name}`);
      await transient.stop();
    } finally {
      endDelivery();
    }
    return seen;
  }

  // A log written as if an earlier child had left it.
  const seed = (runId: string, log: readonly WorkflowEvent[]): Promise<void> =>
    runtimeStore.appendBatch(runId, log);

  return {
    start,
    stop,
    events,
    durable: (runId: string) => durable.get(runId) ?? [],
    seed,
    nextTimer,
    fireTimer,
    deliverSignal,
  };
}

function kinds(events: readonly WorkflowEvent[]): string[] {
  return events.map((event) => event.kind);
}

function awaitedBy(events: readonly WorkflowEvent[], stepId: string): boolean {
  return events.some(
    (event) => event.kind === "SignalAwaited" && event.stepId === stepId,
  );
}

// A fresh child does not continue a step in retry backoff, so the child may
// not stop there.
test("a step waiting out a retry backoff counts as working", async () => {
  const definition = defineWorkflow({
    id: "quiet-backoff",
    trigger: { type: "manual" },
    steps: {
      flaky: step({
        agent: defineAgent({
          id: "flaky",
          systemPrompt: "s",
          tools: [],
          capabilities: [],
          inference: { sources: [{ provider: "anthropic", model: "mock" }] },
        }),
        input: { literal: null },
        retry: { maxAttempts: 2, initialBackoffMs: 60_000 },
      }),
    },
  });
  let attempts = 0;
  const stack = await childStack("quiet-backoff-", definition, async () => {
    attempts += 1;
    if (attempts === 1) throw new Error("the first attempt fails");
    return { output: "done" };
  });
  stack.start("run-backoff");
  const timer = await stack.nextTimer();
  expect(isWorking()).toBe(true);

  const seen = await stack.fireTimer(timer, () =>
    kinds(stack.events("run-backoff")),
  );
  expect(attempts).toBe(2);
  expect(seen).toContain("RunCompleted");
});

test("a step woken by its timer counts as working before the timer's delivery ends", async () => {
  const definition = defineWorkflow({
    id: "quiet-timer",
    trigger: { type: "manual" },
    steps: { nap: sleep({ duration: 60_000 }) },
  });
  const stack = await childStack("quiet-timer-", definition);
  stack.start("run-timer");
  const timer = await stack.nextTimer();
  await whenQuiet();

  const seen = await stack.fireTimer(timer, () =>
    kinds(stack.events("run-timer")),
  );
  expect(seen).toContain("RunCompleted");
});

test("a run woken by a signal counts as working before the signal's delivery ends", async () => {
  const definition = defineWorkflow({
    id: "quiet-signal",
    trigger: { type: "manual" },
    steps: { gate: awaitSignal({ name: "go" }) },
  });
  const stack = await childStack("quiet-signal-", definition);
  stack.start("run-signal");
  await whenQuiet();
  expect(awaitedBy(stack.events("run-signal"), "gate")).toBe(true);

  const seen = await stack.deliverSignal("run-signal", "go", () =>
    kinds(stack.events("run-signal")),
  );
  expect(seen).toContain("RunCompleted");
});

// The nested run reads its log between the park and going idle, so the
// parent wakes first even without the park's handoff. What can fail here is
// the parent going idle before it records the relay.
test("a parent relays its nested run's park before the child looks quiet", async () => {
  const definition = defineWorkflow({
    id: "quiet-nested-park",
    trigger: { type: "manual" },
    steps: {
      spawn: childWorkflow({
        definition: defineWorkflow({
          id: "quiet-nested-park-body",
          trigger: { type: "manual" },
          steps: { inner: awaitSignal({ name: "inner" }) },
        }),
      }),
    },
  });
  const stack = await childStack("quiet-nested-park-", definition);
  stack.start("run-nested-park");
  const relayed = await whenQuiet().then(() =>
    awaitedBy(stack.events("run-nested-park"), "spawn"),
  );
  expect(relayed).toBe(true);
});

test("a parent counts as working once its nested run ends, before the nested run stops counting", async () => {
  const definition = defineWorkflow({
    id: "quiet-nested-end",
    trigger: { type: "manual" },
    steps: {
      spawn: childWorkflow({
        definition: defineWorkflow({
          id: "quiet-nested-end-body",
          trigger: { type: "manual" },
          steps: { work: action({ handler: "work" }) },
        }),
      }),
    },
  });
  const stack = await childStack("quiet-nested-end-", definition);
  stack.start("run-nested-end");
  const seen = await whenQuiet().then(() =>
    kinds(stack.events("run-nested-end")),
  );
  expect(seen).toContain("RunCompleted");
});

test("a parent holding a nested gate's signal counts as working once the resumed gate re-arms", async () => {
  const definition = defineWorkflow({
    id: "quiet-rearm",
    trigger: { type: "manual" },
    steps: {
      spawn: childWorkflow({
        definition: defineWorkflow({
          id: "quiet-rearm-body",
          trigger: { type: "manual" },
          steps: { inner: awaitSignal({ name: "inner" }) },
        }),
      }),
    },
  });
  // A child left the parent relaying the nested gate.
  const before = await childStack("quiet-rearm-before-", definition);
  before.start("run-rearm");
  await whenQuiet();
  const parentLog = before.durable("run-rearm");
  const spawned = parentLog.find((event) => event.kind === "ChildSpawned");
  const relay = parentLog.find(
    (event) => event.kind === "SignalAwaited" && event.stepId === "spawn",
  );
  if (spawned?.kind !== "ChildSpawned" || relay?.kind !== "SignalAwaited") {
    throw new Error("the parent did not relay the nested gate");
  }
  const childLog = before.durable(spawned.childRunId);
  // Runs share the commit chain by run id, so the first child goes first.
  await before.stop();

  // The parent took the signal; the child never got it before it stopped.
  const after = await childStack("quiet-rearm-after-", definition);
  await after.seed(spawned.childRunId, childLog);
  await after.seed("run-rearm", [
    ...parentLog,
    {
      kind: "SignalReceived",
      seq: (parentLog.at(-1)?.seq ?? 0) + 1,
      at: AT,
      signalName: relay.signalName,
      signalId: "sig-held",
      payload: { approved: true },
    },
  ]);
  after.start("run-rearm");
  // The channel writes the delivered signal itself, so the child's own
  // events show that its gate took it.
  const child = await whenQuiet().then(() =>
    kinds(after.events(spawned.childRunId)),
  );
  expect(child).toContain("StepCompleted");
});

// The timer that releases the gate wakes the nested run too, and it works
// through to its end, so the release leaves no gap. What can fail here is
// the parent's wait for that end once it has abandoned the relay.
test("a parent that abandons a released nested relay counts as working once the nested run ends", async () => {
  const definition = defineWorkflow({
    id: "quiet-release",
    trigger: { type: "manual" },
    steps: {
      spawn: childWorkflow({
        definition: defineWorkflow({
          id: "quiet-release-body",
          trigger: { type: "manual" },
          steps: { inner: awaitSignal({ name: "inner", timeout: 60_000 }) },
        }),
      }),
    },
  });
  const stack = await childStack("quiet-release-", definition);
  stack.start("run-release");
  const timer = await stack.nextTimer();
  await whenQuiet();
  expect(awaitedBy(stack.events("run-release"), "spawn")).toBe(true);

  const seen = await stack.fireTimer(timer, () =>
    kinds(stack.events("run-release")),
  );
  expect(seen).toContain("ChildCompleted");
});

test("a run loop counts as working once a step ends, before the step stops counting", async () => {
  const definition = defineWorkflow({
    id: "quiet-loop",
    trigger: { type: "manual" },
    steps: {
      first: action({ handler: "first" }),
      gate: awaitSignal({ name: "go", after: ["first"] }),
    },
  });
  const stack = await childStack("quiet-loop-", definition);
  stack.start("run-loop");
  const parked = await whenQuiet().then(() =>
    awaitedBy(stack.events("run-loop"), "gate"),
  );
  expect(parked).toBe(true);
});

test("a gate whose timeout aborts its signal wait counts as working before the timer's delivery ends", async () => {
  const definition = defineWorkflow({
    id: "quiet-timeout",
    trigger: { type: "manual" },
    steps: { gate: awaitSignal({ name: "go", timeout: 60_000 }) },
  });
  const stack = await childStack("quiet-timeout-", definition);
  stack.start("run-timeout");
  const timer = await stack.nextTimer();
  await whenQuiet();

  const seen = await stack.fireTimer(timer, () =>
    kinds(stack.events("run-timeout")),
  );
  expect(
    seen.some((kind) => kind === "RunFailed" || kind === "RunCompleted"),
  ).toBe(true);
});
