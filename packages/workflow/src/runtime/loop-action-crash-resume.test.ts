// Property: resume never calls a crashed loop-body action's handler again
// on a CONSISTENT store -- even with a fresh (lost) in-memory effect ledger.
// This is the load-bearing fact behind the deployed host's in-memory ledger:
// no resume needs a record written before the crash, because the crashed
// step is not re-invoked.
//
// `runAction` flushes the action's `StepStarted` durably before invoking the
// handler, and the loop body runs as a child run over the SHARED store, so a
// faithful crash between the effect and the action's `StepCompleted` leaves the
// child (`loop-parent-run__rework__0`) log NON-empty and non-terminal. On
// resume, the iteration re-spawns through the suspendable-loop seam; the body's
// `runtimeRun` adopts that durable child log, sees the action `in-flight`
// (durable `StepStarted`, no `StepCompleted`), and settles it a terminal
// `StepFailed` via `isCrashedInvocationStep` (the at-most-once refusal) rather
// than re-invoking the handler. The iteration fails and the run settles
// `failed`.
//
// Under `onIterationFailure: "tolerate"` resume settles the action the same
// way, but the loop then runs the body again with the same input in a new body
// run, so the effect repeats. The second test pins that documented exception.
//
// Contrast `loop-resume.test.ts`, which models an INCONSISTENT store (it drops
// the child log while keeping the parent) and therefore exercises the
// ledger-dedup re-run path production never takes.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import {
  action,
  createEffectContext,
  unavailableCredentialCapability,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  createSpawnLoopIteration,
  defineWorkflow,
  enumerateInlineLoopBodies,
  loop,
  loopBodyRunId,
  runtimeRun,
  type ActionInvoker,
  type EffectLedger,
  type LoopFn,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { createTerminalChildHandle } from "./suspendable-child-handle";

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

function parentWorkflowWith(opts: { tolerate: boolean }) {
  return defineWorkflow({
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
        ...(opts.tolerate ? { onIterationFailure: "tolerate" as const } : {}),
      }),
      escalate: action({ handler: "noop", after: ["rework"] }),
    },
  });
}

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
// counts real effect executions + handler invocations. `onEffect` fires inside
// `perform`'s `run`, right after the effect and before `perform` records it, for
// capturing the mid-action crash snapshot.
function buildEnv(args: {
  definition: WorkflowDefinition;
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
  blobs: ReturnType<typeof createInMemoryBlobSubstrate>;
  effects: EffectLedger;
  counters: { echoInvocations: number; effectRuns: number };
  onEffect?: () => Promise<void>;
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
    const ctx = createEffectContext({
      handler,
      authorize,
      effects: args.effects,
      requires,
      authzContext,
      input,
      credentials: unavailableCredentialCapability(handler),
    });
    await ctx.perform({
      effectId: "touch",
      capability: "fs:write",
      run: async () => {
        args.counters.effectRuns += 1;
        if (args.onEffect !== undefined) await args.onEffect();
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
    spawnChild: async () => createTerminalChildHandle("completed"),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(args.definition),
    hasUpstreamSignalResolver: true,
    loopFns,
  };
  const loopBodies = new Map(
    enumerateInlineLoopBodies(args.definition).map((b) => [
      b.ref,
      b.definition,
    ]),
  );
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  return env;
}

// Run `definition` and capture the SHARED durable state right after the first
// iteration's effect: the parent log (through the loop's ChildSpawned for
// rework__0) and the child rework__0 log (through the action's flushed
// StepStarted). This is exactly what a process crash between the effect and
// StepCompleted leaves on a consistent store. Then resume against a FRESH store
// seeded with BOTH logs (a consistent store keeps the child's flushed
// StepStarted) and a FRESH ledger (as if the in-memory ledger were lost on the
// crash).
async function crashAfterFirstEffectAndResume(
  definition: WorkflowDefinition,
  opts: { shareLedger: boolean },
) {
  const firstLedger = inMemoryLedger();
  const blobs = createInMemoryBlobSubstrate();
  const repoStore1 = createInMemoryRepoStore();
  const counters1 = { echoInvocations: 0, effectRuns: 0 };
  let parentSnapshot: readonly WorkflowEvent[] = [];
  let childSnapshot: readonly WorkflowEvent[] = [];
  let effectRunsAtCapture = 0;
  let captured = false;
  const env1 = buildEnv({
    definition,
    repoStore: repoStore1,
    blobs,
    effects: firstLedger,
    counters: counters1,
    onEffect: async () => {
      if (captured) return;
      captured = true;
      effectRunsAtCapture = counters1.effectRuns;
      parentSnapshot = [...(await repoStore1.read("loop-parent-run"))];
      childSnapshot = [
        ...(await repoStore1.read(
          loopBodyRunId("loop-parent-run", "rework", 0),
        )),
      ];
    },
  });

  await runtimeRun(definition, env1, {
    runId: "loop-parent-run",
  }).complete;

  const repoStore2 = createInMemoryRepoStore();
  await repoStore2.appendBatch(loopBodyRunId("loop-parent-run", "rework", 0), [
    ...childSnapshot,
  ]);
  const counters2 = { echoInvocations: 0, effectRuns: 0 };
  const env2 = buildEnv({
    definition,
    repoStore: repoStore2,
    blobs,
    effects: opts.shareLedger ? firstLedger : inMemoryLedger(),
    counters: counters2,
  });
  const result2 = await runtimeRun(definition, env2, {
    runId: "loop-parent-run",
    resumeFromEvents: [...parentSnapshot],
  }).complete;
  const crashedChildLog = await repoStore2.read(
    loopBodyRunId("loop-parent-run", "rework", 0),
  );

  return {
    effectRunsAtCapture,
    firstRunEffects: counters1.effectRuns,
    parentSnapshot,
    childSnapshot,
    crashedChildLog,
    result2,
    counters2,
  };
}

describe("loop-body action crash resume (consistent store)", () => {
  test("a mid-action crash fails the iteration loud without re-invoking the handler", async () => {
    const {
      effectRunsAtCapture,
      parentSnapshot,
      childSnapshot,
      result2,
      counters2,
    } = await crashAfterFirstEffectAndResume(
      parentWorkflowWith({ tolerate: false }),
      { shareLedger: false },
    );

    // The capture follows the first effect.
    expect(effectRunsAtCapture).toBe(1);

    // The barrier makes the action's StepStarted durable before the handler
    // runs, so the snapshots taken after the effect carry the loop's
    // ChildSpawned for rework__0 in the parent and the action's StepStarted
    // with no StepCompleted in the child.
    expect(
      parentSnapshot.some(
        (e) =>
          e.kind === "ChildSpawned" &&
          e.childRunId === loopBodyRunId("loop-parent-run", "rework", 0),
      ),
    ).toBe(true);
    expect(
      childSnapshot.some(
        (e) => e.kind === "StepStarted" && e.stepId === "count",
      ),
    ).toBe(true);
    expect(childSnapshot.some((e) => e.kind === "StepCompleted")).toBe(false);

    // The iteration failed loud; the handler was NOT re-invoked and no effect
    // ran on resume, despite the fresh ledger. This is why the deployed ledger
    // can be in-memory.
    expect(result2.terminalStatus).toBe("failed");
    expect(counters2.echoInvocations).toBe(0);
    expect(counters2.effectRuns).toBe(0);
  });

  test("a tolerate loop runs the crashed iteration's body again and repeats its effect", async () => {
    const {
      effectRunsAtCapture,
      firstRunEffects,
      crashedChildLog,
      result2,
      counters2,
    } = await crashAfterFirstEffectAndResume(
      parentWorkflowWith({ tolerate: true }),
      { shareLedger: true },
    );

    expect(effectRunsAtCapture).toBe(1);
    // The first run went on past the capture and recorded the effect of each
    // of its three iterations in the ledger the resume shares.
    expect(firstRunEffects).toBe(3);
    expect(
      crashedChildLog
        .filter((e) => e.kind === "StepFailed" && e.stepId === "count")
        .map((e) => (e.kind === "StepFailed" ? e.error.code : undefined)),
    ).toEqual(["crash-mid-invocation"]);

    // Resume still settles the crashed action failed without calling its
    // handler again, but the loop tolerates the failed iteration and runs the
    // body again with the same input in a new body run. The resume shares the
    // first run's ledger, which holds a record for every iteration that run
    // finished, iteration 0 included. Each re-run still misses, because the
    // ledger key holds the body run id and the retry runs under the next one:
    // the effect repeats, and each later iteration runs once.
    expect(result2.terminalStatus).toBe("completed");
    expect(counters2.echoInvocations).toBe(3);
    expect(counters2.effectRuns).toBe(3);
  });
});
