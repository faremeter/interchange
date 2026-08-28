// Property: an action in a loop body runs AT MOST ONCE across a mid-action
// crash on a CONSISTENT store -- even with a fresh (lost) in-memory effect
// ledger. This is the load-bearing fact behind the deployed host's in-memory
// ledger: the ledger is never consulted across a crash because the handler is
// never re-invoked.
//
// `runAction` flushes the action's `StepStarted` durably before invoking the
// handler, and the loop body runs as a child run over the SHARED store, so a
// faithful crash between the effect and the action's `StepCompleted` leaves the
// child (`rework__0`) log NON-empty and non-terminal. On resume, the iteration
// re-spawns through the suspendable-loop seam; the body's `runtimeRun` adopts
// that durable child log, sees the action `in-flight` (durable `StepStarted`,
// no `StepCompleted`), and settles it a terminal `StepFailed` via
// `isCrashedInvocationStep` (the at-most-once refusal) rather than re-invoking
// the handler. The iteration fails and the run settles `failed`.
//
// Contrast `loop-resume.test.ts`, which models an INCONSISTENT store (it drops
// the child log while keeping the parent) and therefore exercises the
// ledger-dedup re-run path production never takes.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import {
  action,
  createEffectContext,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  createSpawnLoopIteration,
  defineWorkflow,
  enumerateInlineLoopBodies,
  loop,
  runtimeRun,
  type ActionInvoker,
  type EffectLedger,
  type LoopFn,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const body = defineWorkflow({
  id: "body",
  trigger: { type: "manual" },
  steps: {
    count: action({
      handler: "echo",
      input: { from: "trigger.payload" },
      effect: { requires: ["fs:write"] },
    }),
  },
});

const parentWorkflow = defineWorkflow({
  id: "loop-parent",
  trigger: { type: "manual" },
  steps: {
    rework: loop({
      body,
      while: "cont",
      carry: "next",
      input: { literal: 0 },
      maxIterations: 5,
      onExhausted: "escalate",
    }),
    escalate: action({ handler: "noop", after: ["rework"] }),
  },
});

const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return (_o, c) => (typeof c === "number" ? c : 0) < 2;
  if (ref === "next") return (_o, c) => (typeof c === "number" ? c : 0) + 1;
  throw new Error(`unknown loop fn ${ref}`);
};

function inMemoryLedger(): EffectLedger {
  const store = new Map<string, { output: unknown }>();
  return {
    async lookup(effectKey) {
      return store.get(effectKey);
    },
    async record(effectKey, output) {
      store.set(effectKey, { output });
    },
  };
}

const authorize: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});
const invokeStep: StepInvoker = async () => ({ output: null });
const clock = () => new Date();

// Build an env whose action invoker performs an effect through the ledger and
// counts real effect executions + handler invocations. `onInvoke` fires at the
// first line of every invocation (before the effect), for capturing the
// mid-action crash snapshot.
function buildEnv(args: {
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
  blobs: ReturnType<typeof createInMemoryBlobSubstrate>;
  effects: EffectLedger;
  counters: { echoInvocations: number; effectRuns: number };
  onInvoke?: () => Promise<void>;
}): WorkflowRuntimeEnv {
  const invokeAction: ActionInvoker = async ({
    handler,
    input,
    requires,
    authzContext,
  }) => {
    // A non-echo handler (the loop's onExhausted `escalate`, which runs after a
    // failed loop) is not the property under test; count only the loop-body echo.
    if (handler !== "echo") return { output: `ran:${handler}` };
    args.counters.echoInvocations += 1;
    if (args.onInvoke !== undefined) await args.onInvoke();
    const ctx = createEffectContext({
      authorize,
      effects: args.effects,
      requires,
      authzContext,
      input,
    });
    await ctx.perform({
      effectId: "touch",
      capability: "fs:write",
      run: async () => {
        args.counters.effectRuns += 1;
        return null;
      },
    });
    return { output: input };
  };
  const env: WorkflowRuntimeEnv = {
    repoStore: args.repoStore,
    scheduler: createInMemoryScheduler({ repoStore: args.repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs: args.blobs,
    directors: createDefaultDirectorRegistry(),
    authorize,
    invokeStep,
    invokeAction,
    effects: args.effects,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(parentWorkflow),
    loopFns,
  };
  const loopBodies = new Map(
    enumerateInlineLoopBodies(parentWorkflow).map((b) => [b.ref, b.definition]),
  );
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  return env;
}

describe("loop-body action crash resume (consistent store)", () => {
  test("a mid-action crash fails the iteration loud without re-invoking the handler", async () => {
    const blobs = createInMemoryBlobSubstrate();
    const repoStore1 = createInMemoryRepoStore();
    const counters1 = { echoInvocations: 0, effectRuns: 0 };

    // Capture the SHARED durable state at the first iteration's effect instant:
    // the parent log (through the loop's ChildSpawned for rework__0) and the
    // child rework__0 log (through the action's flushed StepStarted). This is
    // exactly what a power loss between the effect and StepCompleted leaves on a
    // consistent store.
    let parentSnapshot: readonly WorkflowEvent[] = [];
    let childSnapshot: readonly WorkflowEvent[] = [];
    let captured = false;
    const env1 = buildEnv({
      repoStore: repoStore1,
      blobs,
      effects: inMemoryLedger(),
      counters: counters1,
      onInvoke: async () => {
        if (captured) return;
        captured = true;
        parentSnapshot = [...(await repoStore1.read("loop-parent-run"))];
        childSnapshot = [...(await repoStore1.read("rework__0"))];
      },
    });

    await runtimeRun(parentWorkflow, env1, {
      runId: "loop-parent-run",
    }).complete;

    // The barrier makes both snapshots durable at invoke time: the parent
    // carries the loop's ChildSpawned for rework__0, and the child carries the
    // action's StepStarted with no StepCompleted.
    expect(
      parentSnapshot.some(
        (e) => e.kind === "ChildSpawned" && e.childRunId === "rework__0",
      ),
    ).toBe(true);
    expect(
      childSnapshot.some(
        (e) => e.kind === "StepStarted" && e.stepId === "count",
      ),
    ).toBe(true);
    expect(childSnapshot.some((e) => e.kind === "StepCompleted")).toBe(false);

    // Resume against a FRESH store seeded with BOTH logs (a consistent store
    // keeps the child's flushed StepStarted) and a FRESH ledger (as if the
    // in-memory ledger were lost on the crash).
    const repoStore2 = createInMemoryRepoStore();
    await repoStore2.appendBatch("rework__0", [...childSnapshot]);
    const counters2 = { echoInvocations: 0, effectRuns: 0 };
    const env2 = buildEnv({
      repoStore: repoStore2,
      blobs,
      effects: inMemoryLedger(),
      counters: counters2,
    });

    const result2 = await runtimeRun(parentWorkflow, env2, {
      runId: "loop-parent-run",
      resumeFromEvents: [...parentSnapshot],
    }).complete;

    // The iteration failed loud; the handler was NOT re-invoked and no effect
    // ran on resume, despite the fresh ledger. This is why the deployed ledger
    // can be in-memory.
    expect(result2.terminalStatus).toBe("failed");
    expect(counters2.echoInvocations).toBe(0);
    expect(counters2.effectRuns).toBe(0);
  });
});
