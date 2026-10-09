// Loop iteration failure policy.
//
// Absent `onIterationFailure` fails the loop, the same as today. "tolerate"
// records a genuine iteration failure and continues with the same input,
// without calling while or carry and without a scoped StepCompleted. A
// cancelled iteration, or a failure that is an abort teardown, fails the loop
// without selecting the onExhausted arm. Replay skips a tolerated failure so the
// resume token stays on the later park, and it throws on a terminal iteration
// the policy will not continue before any spawn.

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
  createSpawnLoopIteration,
  defineWorkflow,
  enumerateInlineLoopBodies,
  inlineBodyRef,
  loop,
  loopBodyRunId,
  runLocal,
  runtimeRun,
  scopedStepId,
  type ActionHandler,
  type ActionInvoker,
  type BlobSubstrate,
  type LoopFn,
  type RepoStore,
  type SignalChannel,
  type WorkflowAuthorizeFn,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { waitForEvent, waitForNthEvent } from "@intx/workflow/testing";

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const body = defineWorkflow({
  id: "bump-body",
  trigger: { type: "manual" },
  steps: {
    bump: action({ handler: "bump", input: { from: "trigger.payload" } }),
  },
});

function callsOf(whileCalls: { n: number }, carryCalls: { n: number }) {
  const loopFns = (ref: string): LoopFn => {
    if (ref === "cont") {
      return () => {
        whileCalls.n += 1;
        return true;
      };
    }
    if (ref === "next") {
      return () => {
        carryCalls.n += 1;
        return 7;
      };
    }
    throw new Error(`unknown loop fn ${ref}`);
  };
  return loopFns;
}

function parentOf(
  maxIterations: number,
  onIterationFailure?: "end" | "tolerate",
): WorkflowDefinition {
  return defineWorkflow({
    id: "loop-iteration-failure",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body,
        while: "cont",
        carry: "next",
        input: { literal: 0 },
        maxIterations,
        onExhausted: "escalate",
        ...(onIterationFailure !== undefined ? { onIterationFailure } : {}),
      }),
      downstream: action({ handler: "downstream", after: ["rework"] }),
      escalate: action({ handler: "escalate", after: ["rework"] }),
    },
  });
}

function resolver(bump: ActionHandler): (ref: string) => ActionHandler {
  return (ref) => {
    if (ref === "bump") return bump;
    if (ref === "downstream") return async () => "down";
    if (ref === "escalate") return async () => "escalated";
    throw new Error(`unknown handler ${ref}`);
  };
}

function scopedStepCompleted(
  events: readonly WorkflowEvent[],
  index: number,
): boolean {
  const stepId = scopedStepId("rework", index);
  return events.some(
    (event) => event.kind === "StepCompleted" && event.stepId === stepId,
  );
}

describe("loop onIterationFailure", () => {
  test("an absent policy fails the loop on the first failed iteration", async () => {
    const whileCalls = { n: 0 };
    const carryCalls = { n: 0 };
    const result = await runLocal(parentOf(2), {
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
      actionResolver: resolver(async () => {
        throw new Error("boom");
      }),
      loopFns: callsOf(whileCalls, carryCalls),
    }).complete;

    expect(result.terminalStatus).toBe("failed");
    expect(whileCalls.n).toBe(0);
    expect(carryCalls.n).toBe(0);
    // A thrown iteration does not route. Both arms stay reachable, which is
    // the engine's "a failed dependency is resolved" scheduling. Routing
    // would have pruned downstream.
    expect(result.outputs.escalate).toBe("escalated");
    expect(result.outputs.downstream).toBe("down");
    expect(scopedStepCompleted(result.events, 0)).toBe(false);
  });

  test("every tolerated iteration exhausts with final null", async () => {
    const whileCalls = { n: 0 };
    const carryCalls = { n: 0 };
    const result = await runLocal(parentOf(2, "tolerate"), {
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
      actionResolver: resolver(async () => {
        throw new Error("boom");
      }),
      loopFns: callsOf(whileCalls, carryCalls),
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(whileCalls.n).toBe(0);
    expect(carryCalls.n).toBe(0);
    expect(result.outputs.rework).toEqual({
      outcome: "exhausted",
      iterations: 2,
      carry: 0,
      final: null,
    });
    expect(result.outputs.escalate).toBe("escalated");
    expect("downstream" in result.outputs).toBe(false);
    expect(scopedStepCompleted(result.events, 0)).toBe(false);
    expect(scopedStepCompleted(result.events, 1)).toBe(false);
  });

  test("a success followed by a tolerated failure keeps the success as final", async () => {
    const whileCalls = { n: 0 };
    const carryCalls = { n: 0 };
    let bumps = 0;
    const result = await runLocal(parentOf(2, "tolerate"), {
      authorize: allowAll,
      hasUpstreamSignalResolver: true,
      actionResolver: resolver(async () => {
        bumps += 1;
        if (bumps === 1) return { bumped: true };
        throw new Error("boom");
      }),
      loopFns: callsOf(whileCalls, carryCalls),
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    // while and carry run for the completed attempt only. carry's return
    // value is the input the failed attempt was fed.
    expect(whileCalls.n).toBe(1);
    expect(carryCalls.n).toBe(1);
    expect(result.outputs.rework).toEqual({
      outcome: "exhausted",
      iterations: 2,
      carry: 7,
      final: { bump: { bumped: true } },
    });
    expect(result.outputs.escalate).toBe("escalated");
    expect(scopedStepCompleted(result.events, 0)).toBe(true);
    expect(scopedStepCompleted(result.events, 1)).toBe(false);
  });
});

const holdBody = defineWorkflow({
  id: "hold-body",
  trigger: { type: "manual" },
  steps: { hold: awaitSignal({ name: "go" }) },
});

function runtimeParent(
  inner: WorkflowDefinition,
  maxIterations: number,
): WorkflowDefinition {
  return defineWorkflow({
    id: "tolerate-runtime-parent",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body: inner,
        while: "cont",
        carry: "next",
        input: { literal: 0 },
        maxIterations,
        onExhausted: "escalate",
        onIterationFailure: "tolerate",
      }),
      downstream: action({ handler: "downstream", after: ["rework"] }),
      escalate: action({ handler: "escalate", after: ["rework"] }),
    },
  });
}

function buildEnv(args: {
  parentDef: WorkflowDefinition;
  repoStore: RepoStore;
  blobs: BlobSubstrate;
  signalChannel: SignalChannel;
  invokeAction: ActionInvoker;
  loopFns: (ref: string) => LoopFn;
}): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const env: WorkflowRuntimeEnv = {
    repoStore: args.repoStore,
    scheduler: createInMemoryScheduler({ repoStore: args.repoStore, clock }),
    signalChannel: args.signalChannel,
    blobs: args.blobs,
    directors: createDefaultDirectorRegistry(),
    authorize: allowAll,
    invokeStep: () => {
      throw new Error("loop iteration-failure test: invokeStep unused");
    },
    invokeAction: args.invokeAction,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(args.parentDef),
    hasUpstreamSignalResolver: true,
    loopFns: args.loopFns,
  };
  const loopBodies = new Map(
    enumerateInlineLoopBodies(args.parentDef).map((entry) => [
      entry.ref,
      entry.definition,
    ]),
  );
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  return env;
}

function actionInvoker(onceRuns: { n: number }): ActionInvoker {
  return async ({ handler }) => {
    if (handler === "once") {
      onceRuns.n += 1;
      if (onceRuns.n === 1) throw new Error("boom");
      return { output: { ok: true } };
    }
    if (handler === "escalate") return { output: "escalated" };
    if (handler === "downstream") return { output: "down" };
    throw new Error(`unknown handler ${handler}`);
  };
}

async function copyLog(
  repoStore: RepoStore,
  runId: string,
  events: readonly WorkflowEvent[],
): Promise<void> {
  for (const event of events) {
    await repoStore.append(runId, event);
  }
}

describe("loop onIterationFailure resume and abort", () => {
  test("a cancelled iteration fails the loop without another spawn", async () => {
    await expectTerminalReplay("cancelled");
  });

  test("an abort-teardown iteration fails the loop without another spawn", async () => {
    await expectTerminalReplay("aborted");
  });

  test("replay of only tolerated failures exhausts without driving them again", async () => {
    const runId = "tolerate-replay";
    const def = runtimeParent(body, 2);
    const whileCalls = { n: 0 };
    const carryCalls = { n: 0 };
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, toleratedFailureLog(runId));
    let spawns = 0;
    const env = buildEnv({
      parentDef: def,
      repoStore,
      blobs: createInMemoryBlobSubstrate(),
      signalChannel: createInMemorySignalChannel(),
      invokeAction: actionInvoker({ n: 0 }),
      loopFns: callsOf(whileCalls, carryCalls),
    });
    const realSpawn = env.spawnLoopIteration;
    if (realSpawn === undefined) throw new Error("spawn missing");
    env.spawnLoopIteration = async (input) => {
      spawns += 1;
      return realSpawn(input);
    };

    const result = await runtimeRun(def, env, { runId }).complete;

    expect(spawns).toBe(0);
    expect(whileCalls.n).toBe(0);
    expect(carryCalls.n).toBe(0);
    expect(result.terminalStatus).toBe("completed");
    expect(result.outputs.rework).toEqual({
      outcome: "exhausted",
      iterations: 2,
      carry: 0,
      final: null,
    });
    expect(result.outputs.escalate).toBe("escalated");
    expect("downstream" in result.outputs).toBe(false);
  });

  test("cancel during a tolerated iteration ends the loop", async () => {
    const runId = "tolerate-cancel";
    const def = runtimeParent(holdBody, 1);
    const repoStore = createInMemoryRepoStore();
    const env = buildEnv({
      parentDef: def,
      repoStore,
      blobs: createInMemoryBlobSubstrate(),
      signalChannel: createInMemorySignalChannel(),
      invokeAction: actionInvoker({ n: 0 }),
      loopFns: callsOf({ n: 0 }, { n: 0 }),
    });
    const run = runtimeRun(def, env, { runId });
    await waitForEvent(
      repoStore,
      runId,
      (event) =>
        event.kind === "SignalAwaited" && event.parkKind === "signal-relay",
    );
    await run.cancel("supervisor-operator", "operator teardown");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("cancelled");
    const log = await repoStore.read(runId);
    expect(log.filter((event) => event.kind === "ChildSpawned").length).toBe(1);
    expect(
      log.some(
        (event) => event.kind === "StepStarted" && event.stepId === "escalate",
      ),
    ).toBe(false);
  });

  test("resume re-links the later park and does not drive the tolerated failure", async () => {
    const runId = "tolerate-park";
    const def = runtimeParent(holdBody, 3);
    const whileCalls = { n: 0 };
    const carryCalls = { n: 0 };
    const loopFns = (ref: string): LoopFn => {
      if (ref === "cont") {
        return () => {
          whileCalls.n += 1;
          return false;
        };
      }
      if (ref === "next") {
        return () => {
          carryCalls.n += 1;
          return 7;
        };
      }
      throw new Error(`unknown loop fn ${ref}`);
    };
    const blobs = createInMemoryBlobSubstrate();
    const repoStore1 = createInMemoryRepoStore();
    const env1 = buildEnv({
      parentDef: def,
      repoStore: repoStore1,
      blobs,
      signalChannel: createInMemorySignalChannel(),
      invokeAction: actionInvoker({ n: 0 }),
      loopFns,
    });
    // The first drive is a genuine iteration failure. The second is the real
    // body, which parks. A body step after a failed action would still run,
    // so the failure is this drive's terminal rather than a later step.
    const realSpawn1 = env1.spawnLoopIteration;
    if (realSpawn1 === undefined) throw new Error("spawn missing");
    let firstDrives = 0;
    env1.spawnLoopIteration = async (input) => {
      firstDrives += 1;
      if (firstDrives === 1) {
        return {
          next: async () => ({ kind: "terminal", terminalStatus: "failed" }),
          resume: async () => undefined,
          deliverSignal: async () => undefined,
        };
      }
      return realSpawn1(input);
    };
    void runtimeRun(def, env1, { runId }).complete;
    await waitForNthEvent(
      repoStore1,
      runId,
      (event) =>
        event.kind === "SignalAwaited" && event.parkKind === "signal-relay",
      1,
    );
    expect(firstDrives).toBe(2);
    expect(whileCalls.n).toBe(0);
    expect(carryCalls.n).toBe(0);

    const failedChild = loopBodyRunId(runId, "rework", 0);
    const parkedChild = loopBodyRunId(runId, "rework", 1);
    const parentLog = await repoStore1.read(runId);
    const failedLog = await repoStore1.read(failedChild);
    const parkedLog = await repoStore1.read(parkedChild);
    expect(
      parentLog.filter(
        (event) =>
          event.kind === "SignalAwaited" && event.parkKind === "signal-relay",
      ).length,
    ).toBe(1);

    const repoStore2 = createInMemoryRepoStore();
    await copyLog(repoStore2, runId, parentLog);
    await copyLog(repoStore2, failedChild, failedLog);
    await copyLog(repoStore2, parkedChild, parkedLog);
    const onceRuns2 = { n: 0 };
    const driven: string[] = [];
    const env2 = buildEnv({
      parentDef: def,
      repoStore: repoStore2,
      blobs,
      signalChannel: createInMemorySignalChannel(),
      invokeAction: actionInvoker(onceRuns2),
      loopFns,
    });
    const realSpawn = env2.spawnLoopIteration;
    if (realSpawn === undefined) throw new Error("spawn missing");
    env2.spawnLoopIteration = async (input) => {
      driven.push(input.childRunId);
      return realSpawn(input);
    };
    const run2 = runtimeRun(def, env2, { runId });
    await run2.signal("go", { done: true }, "sig-1");
    const result = await run2.complete;

    expect(driven).toEqual([parkedChild]);
    expect(onceRuns2.n).toBe(0);
    expect(whileCalls.n).toBe(1);
    expect(carryCalls.n).toBe(0);
    expect(result.terminalStatus).toBe("completed");
    expect(result.outputs.rework).toEqual({
      outcome: "converged",
      iterations: 2,
      carry: 0,
      final: { hold: { done: true } },
    });
    const resumed = await repoStore2.read(runId);
    expect(
      resumed.filter(
        (event) =>
          event.kind === "SignalAwaited" && event.parkKind === "signal-relay",
      ).length,
    ).toBe(1);
  });
});

async function expectTerminalReplay(
  kind: "cancelled" | "aborted",
): Promise<void> {
  const runId = `terminal-${kind}`;
  const def = runtimeParent(holdBody, 2);
  const repoStore = createInMemoryRepoStore();
  await repoStore.appendBatch(runId, terminalIterationLog(runId, kind));
  let spawns = 0;
  const env = buildEnv({
    parentDef: def,
    repoStore,
    blobs: createInMemoryBlobSubstrate(),
    signalChannel: createInMemorySignalChannel(),
    invokeAction: actionInvoker({ n: 0 }),
    loopFns: callsOf({ n: 0 }, { n: 0 }),
  });
  const realSpawn = env.spawnLoopIteration;
  if (realSpawn === undefined) throw new Error("spawn missing");
  env.spawnLoopIteration = async (input) => {
    spawns += 1;
    return realSpawn(input);
  };

  const result = await runtimeRun(def, env, { runId }).complete;

  expect(result.terminalStatus).toBe("failed");
  expect(spawns).toBe(0);
  // Not routed to onExhausted: that would prune downstream. The throw leaves
  // both arms reachable.
  expect(result.outputs.escalate).toBe("escalated");
  expect(result.outputs.downstream).toBe("down");
  const log = await repoStore.read(runId);
  const failed = log.find(
    (event) => event.kind === "StepFailed" && event.stepId === "rework",
  );
  const message = failed?.kind === "StepFailed" ? failed.error.message : "";
  expect(message).toMatch(
    kind === "cancelled" ? /ended cancelled/ : /ended failed/,
  );
}

function terminalIterationLog(
  runId: string,
  kind: "cancelled" | "aborted",
): WorkflowEvent[] {
  const at = new Date(0).toISOString();
  const childRunId = loopBodyRunId(runId, "rework", 0);
  return [
    {
      kind: "RunStarted",
      seq: 1,
      at,
      runId,
      definitionHash: "x",
      trigger: { type: "manual", payload: null },
    },
    {
      kind: "StepStarted",
      seq: 2,
      at,
      stepId: "rework",
      attempt: 1,
      input: { ref: "inline:null" },
    },
    {
      kind: "StepStarted",
      seq: 3,
      at,
      stepId: scopedStepId("rework", 0),
      attempt: 1,
      input: { ref: "inline:null" },
    },
    {
      kind: "ChildSpawned",
      seq: 4,
      at,
      stepId: "rework",
      childRunId,
      childDefinitionRef: inlineBodyRef("tolerate-runtime-parent", "rework"),
    },
    {
      kind: "ChildCompleted",
      seq: 5,
      at,
      childRunId,
      terminalStatus: kind === "cancelled" ? "cancelled" : "failed",
      ...(kind === "aborted" ? { abortedTeardown: true } : {}),
    },
  ];
}

function toleratedFailureLog(runId: string): WorkflowEvent[] {
  const at = new Date(0).toISOString();
  const first = loopBodyRunId(runId, "rework", 0);
  const second = loopBodyRunId(runId, "rework", 1);
  const spawned = (seq: number, childRunId: string): WorkflowEvent => ({
    kind: "ChildSpawned",
    seq,
    at,
    stepId: "rework",
    childRunId,
    childDefinitionRef: inlineBodyRef("tolerate-runtime-parent", "rework"),
  });
  const failed = (seq: number, childRunId: string): WorkflowEvent => ({
    kind: "ChildCompleted",
    seq,
    at,
    childRunId,
    terminalStatus: "failed",
  });
  return [
    {
      kind: "RunStarted",
      seq: 1,
      at,
      runId,
      definitionHash: "x",
      trigger: { type: "manual", payload: null },
    },
    {
      kind: "StepStarted",
      seq: 2,
      at,
      stepId: "rework",
      attempt: 1,
      input: { ref: "inline:null" },
    },
    spawned(3, first),
    failed(4, first),
    spawned(5, second),
    failed(6, second),
  ];
}
