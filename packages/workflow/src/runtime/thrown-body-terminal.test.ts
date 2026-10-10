// A body that throws while folding its log still has to leave a terminal
// event. The collision here is an outside `SignalReceived` written at the
// durable tip while a sibling `StepCompleted` is sitting in the pending
// buffer. The fold throws `non-monotonic sequence`, and the run's
// `complete` promise fulfills failed with that message on the `RunFailed`.
//
// The injection is the third read whose durable log ends in
// `SignalAwaited`. The first is the reload before the sibling is numbered.
// The second is the read inside that numbering, before the sibling is
// pushed; injecting on either lets the sibling take the next sequence and
// the run stays parked. The in-memory store returns its live log array, so
// the read copies the log before appending the injected event.

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
  type ActionInvoker,
  type Primitive,
  type RepoStore,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const collided = defineWorkflow({
  id: "collided-signal",
  trigger: { type: "manual" },
  steps: {
    gate: awaitSignal({ name: "approve" }),
    sibling: action({ handler: "sibling" }),
  },
});

const gateOnly = defineWorkflow({
  id: "gate-only",
  trigger: { type: "manual" },
  steps: { gate: awaitSignal({ name: "approve" }) },
});

function whenKindAppended(
  inner: RepoStore,
  kind: WorkflowEvent["kind"],
): { repoStore: RepoStore; happened: Promise<void> } {
  let release!: () => void;
  const happened = new Promise<void>((resolve) => {
    release = resolve;
  });
  let seen = false;
  function note(events: readonly WorkflowEvent[]): void {
    if (seen) return;
    if (!events.some((event) => event.kind === kind)) return;
    seen = true;
    release();
  }
  return {
    happened,
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

function buildEnv(
  def: WorkflowDefinition,
  repoStore: RepoStore,
  signalChannel: ReturnType<typeof createInMemorySignalChannel>,
  invokeAction: ActionInvoker,
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date(0);
  return {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel,
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
    clock,
    newId: (prefix) => `${prefix}-thrown-body`,
    drain: createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
  };
}

describe("a thrown run body", () => {
  test("records RunFailed when a buffered sibling collides with an outside signal", async () => {
    const inner = createInMemoryRepoStore();
    const parked = whenKindAppended(inner, "SignalAwaited");
    let awaitedReads = 0;
    const repoStore: RepoStore = {
      ...parked.repoStore,
      async read(runId) {
        const events = [...(await inner.read(runId))];
        const last = events.at(-1);
        if (last?.kind !== "SignalAwaited") return events;
        awaitedReads += 1;
        if (awaitedReads !== 3) return events;
        const injected: WorkflowEvent = {
          kind: "SignalReceived",
          seq: last.seq + 1,
          at: new Date(0).toISOString(),
          signalName: "approve",
          signalId: "sig-injected",
          payload: { ok: true },
        };
        await inner.append(runId, injected);
        return [...events, injected];
      },
    };
    const invokeAction: ActionInvoker = async ({ handler }) => {
      if (handler === "sibling") {
        await parked.happened;
        return { output: { done: true } };
      }
      return { output: null };
    };
    const runId = "run-thrown-body";
    const result = await runtimeRun(
      collided,
      buildEnv(
        collided,
        repoStore,
        createInMemorySignalChannel(),
        invokeAction,
      ),
      { runId },
    ).complete;

    expect(result.terminalStatus).toBe("failed");
    const last = result.events.at(-1);
    expect(last?.kind).toBe("RunFailed");
    if (last?.kind !== "RunFailed") {
      throw new Error("last event is not RunFailed");
    }
    expect(last.error.message).toContain("non-monotonic sequence");
    const durable = await inner.read(runId);
    expect(durable.at(-1)?.kind).toBe("RunFailed");
  });

  test("an empty buffer still completes when the in-memory channel delivers", async () => {
    const inner = createInMemoryRepoStore();
    const parked = whenKindAppended(inner, "SignalAwaited");
    const channel = createInMemorySignalChannel();
    const runId = "run-empty-buffer-signal";
    const complete = runtimeRun(
      gateOnly,
      buildEnv(gateOnly, parked.repoStore, channel, async () => ({
        output: null,
      })),
      { runId },
    ).complete;
    await parked.happened;
    await channel.deliver("approve", { ok: true }, "sig-empty");
    const result = await complete;
    expect(result.terminalStatus).toBe("completed");
    expect(result.events.at(-1)?.kind).toBe("RunCompleted");
  });

  test("an unreadable step output still fulfills after the terminal append", async () => {
    const inner = createInMemoryRepoStore();
    const runId = "run-unreadable-output";
    const at = new Date(0).toISOString();
    await inner.append(runId, {
      kind: "RunStarted",
      seq: 1,
      at,
      runId,
      definitionHash: "x",
      trigger: { type: "manual", payload: undefined },
    });
    await inner.append(runId, {
      kind: "StepStarted",
      seq: 2,
      at,
      stepId: "work",
      attempt: 1,
      input: { ref: "inline:null" },
    });
    await inner.append(runId, {
      kind: "StepCompleted",
      seq: 3,
      at,
      stepId: "work",
      attempt: 1,
      output: { ref: "blob:missing" },
    });
    const def = defineWorkflow({
      id: "unreadable-output",
      trigger: { type: "manual" },
      steps: { work: action({ handler: "work" }) },
    });
    const env = buildEnv(
      def,
      inner,
      createInMemorySignalChannel(),
      async () => ({ output: null }),
    );
    env.blobs = {
      ...env.blobs,
      async resolveRef() {
        throw new Error("blob unreadable");
      },
    };

    const result = await runtimeRun(def, env, { runId }).complete;

    expect(result.terminalStatus).toBe("failed");
    expect(result.events.at(-1)?.kind).toBe("RunFailed");
    const durable = await inner.read(runId);
    expect(durable.at(-1)?.kind).toBe("RunFailed");
  });

  test("an append that lands and then throws still fulfills when the output is unreadable", async () => {
    const inner = createInMemoryRepoStore();
    const runId = "run-unreadable-output-after-throw";
    const at = new Date(0).toISOString();
    await inner.append(runId, {
      kind: "RunStarted",
      seq: 1,
      at,
      runId,
      definitionHash: "x",
      trigger: { type: "manual", payload: undefined },
    });
    await inner.append(runId, {
      kind: "StepStarted",
      seq: 2,
      at,
      stepId: "work",
      attempt: 1,
      input: { ref: "inline:null" },
    });
    await inner.append(runId, {
      kind: "StepCompleted",
      seq: 3,
      at,
      stepId: "work",
      attempt: 1,
      output: { ref: "blob:missing" },
    });
    const repoStore: RepoStore = {
      ...inner,
      async appendBatch(id, events) {
        await inner.appendBatch(id, events);
        if (events.some((event) => event.kind === "RunFailed")) {
          throw new Error("append reported failure after the write");
        }
      },
    };
    const def = defineWorkflow({
      id: "unreadable-output-after-throw",
      trigger: { type: "manual" },
      steps: { work: action({ handler: "work" }) },
    });
    const env = buildEnv(
      def,
      repoStore,
      createInMemorySignalChannel(),
      async () => ({ output: null }),
    );
    env.blobs = {
      ...env.blobs,
      async resolveRef() {
        throw new Error("blob unreadable");
      },
    };

    const result = await runtimeRun(def, env, { runId }).complete;

    expect(result.terminalStatus).toBe("failed");
    const durable = await inner.read(runId);
    expect(durable.at(-1)?.kind).toBe("RunFailed");
  });

  test("rejects with the flush error when the park append fails", async () => {
    const inner = createInMemoryRepoStore();
    const boom = new Error("append failed");
    // The sibling's first flush records RunStarted. The gate's park
    // append then fails, and the gate is absent from the durable log,
    // so the drive loop would schedule it again.
    const siblingParked = Promise.withResolvers<undefined>();
    const repoStore: RepoStore = {
      ...inner,
      async appendBatch(runId, events) {
        if (events.some((event) => event.kind === "SignalAwaited")) {
          siblingParked.resolve(undefined);
          throw boom;
        }
        await inner.appendBatch(runId, events);
      },
    };
    const invokeAction: ActionInvoker = async ({ handler }) => {
      if (handler === "sibling") {
        await siblingParked.promise;
        return { output: { done: true } };
      }
      return { output: null };
    };
    const runId = "run-failed-park-flush";
    const complete = runtimeRun(
      collided,
      buildEnv(
        collided,
        repoStore,
        createInMemorySignalChannel(),
        invokeAction,
      ),
      { runId },
    ).complete;

    await expect(complete).rejects.toBe(boom);
    const durable = await inner.read(runId);
    expect(durable.some((event) => event.kind === "RunFailed")).toBe(false);
    expect(durable.some((event) => event.kind === "SignalAwaited")).toBe(false);
  });

  test("records RunCancelled when a cancel lands on the RunFailed append", async () => {
    const inner = createInMemoryRepoStore();
    const parked = whenKindAppended(inner, "SignalAwaited");
    let awaitedReads = 0;
    const repoStore: RepoStore = {
      ...parked.repoStore,
      async read(runId) {
        const events = [...(await inner.read(runId))];
        const last = events.at(-1);
        if (last?.kind !== "SignalAwaited") return events;
        awaitedReads += 1;
        if (awaitedReads !== 3) return events;
        const injected: WorkflowEvent = {
          kind: "SignalReceived",
          seq: last.seq + 1,
          at: new Date(0).toISOString(),
          signalName: "approve",
          signalId: "sig-cancel-race",
          payload: { ok: true },
        };
        await inner.append(runId, injected);
        return [...events, injected];
      },
      async appendBatch(runId, events) {
        const failed = events.find((event) => event.kind === "RunFailed");
        if (failed !== undefined) {
          await inner.append(runId, {
            kind: "CancelRequested",
            seq: failed.seq,
            at: failed.at,
            origin: "supervisor-drain",
            reason: "drain timeout",
          });
          throw new Error("RunFailed append lost to cancel");
        }
        await parked.repoStore.appendBatch(runId, events);
      },
    };
    const invokeAction: ActionInvoker = async ({ handler }) => {
      if (handler === "sibling") {
        await parked.happened;
        return { output: { done: true } };
      }
      return { output: null };
    };
    const runId = "run-cancel-during-run-failed";
    const result = await runtimeRun(
      collided,
      buildEnv(
        collided,
        repoStore,
        createInMemorySignalChannel(),
        invokeAction,
      ),
      { runId },
    ).complete;

    expect(result.terminalStatus).toBe("cancelled");
    const durable = await inner.read(runId);
    expect(durable.at(-1)?.kind).toBe("RunCancelled");
    expect(durable.some((event) => event.kind === "RunFailed")).toBe(false);
  });

  test("a child flush failure still records the parent's terminal event", async () => {
    const inner = createInMemoryRepoStore();
    const boom = new Error("child append failed");
    const parentRunId = "run-parent-of-failed-child";
    const repoStore: RepoStore = {
      ...inner,
      async appendBatch(runId, events) {
        if (runId !== parentRunId) throw boom;
        await inner.appendBatch(runId, events);
      },
    };
    const childBody = defineWorkflow({
      id: "child-body",
      trigger: { type: "manual" },
      steps: { work: action({ handler: "work" }) },
    });
    const spawn: Primitive = {
      kind: "childWorkflow",
      id: "",
      definition: { ref: "child-ref" },
      drainBehavior: "cancel",
    };
    const parent = defineWorkflow({
      id: "parent-of-failed-child",
      trigger: { type: "manual" },
      steps: { spawn },
    });
    const invokeAction: ActionInvoker = async () => ({ output: null });
    const env = buildEnv(
      parent,
      repoStore,
      createInMemorySignalChannel(),
      invokeAction,
    );
    env.newId = (prefix) => `${prefix}-nested-flush`;
    env.spawnChild = async (args) => {
      const childEnv = buildEnv(
        childBody,
        repoStore,
        createInMemorySignalChannel(),
        invokeAction,
      );
      const result = await runtimeRun(childBody, childEnv, {
        runId: args.childRunId,
        triggerPayload: args.input,
        depth: args.depth,
        maxChildSpawnDepth: args.maxChildSpawnDepth,
      }).complete;
      return { terminalStatus: result.terminalStatus };
    };

    const result = await runtimeRun(parent, env, { runId: parentRunId })
      .complete;

    expect(result.terminalStatus).toBe("failed");
    const durable = await inner.read(parentRunId);
    expect(durable.some((event) => event.kind === "ChildCompleted")).toBe(true);
    expect(durable.at(-1)?.kind).toBe("RunFailed");
  });
});
