// Probe: does a resumed loop container publish the SAME
// `{ outcome, iterations, carry, final }` record a straight-through run
// publishes, at every crash boundary where `runLoop` is re-entered?
//
// The committed boundary tests assert routing and effect counts only; none
// of them read the container's own output. `final` is derived from two
// different sources on the two paths (the replay reads the scoped
// StepCompleted from the log, the drive reads `hydrateChildOutputs` off the
// child run), so agreement is an assumption, not a given.

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
  loopBodyRunId,
  runtimeRun,
  type ActionInvoker,
  type EffectLedger,
  type LoopFn,
  type RunResult,
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

const parentConverge = defineWorkflow({
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
    consolidate: action({ handler: "consolidate", after: ["rework"] }),
    escalate: action({ handler: "escalate", after: ["rework"] }),
  },
});

const parentExhaust = defineWorkflow({
  id: "loop-parent-exhaust",
  trigger: { type: "manual" },
  steps: {
    rework: loop({
      body,
      while: "always",
      carry: "next",
      input: { literal: 0 },
      maxIterations: 3,
      onExhausted: "escalate",
    }),
    consolidate: action({ handler: "consolidate", after: ["rework"] }),
    escalate: action({ handler: "escalate", after: ["rework"] }),
  },
});

function countOf(childOutput: unknown): number {
  if (
    typeof childOutput === "object" &&
    childOutput !== null &&
    "count" in childOutput
  ) {
    const count = childOutput.count;
    if (typeof count === "number") return count;
  }
  throw new Error("iteration output missing numeric count");
}

const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return (childOutput) => countOf(childOutput) < 2;
  if (ref === "always") return () => true;
  if (ref === "next")
    return (_c, currentInput) =>
      typeof currentInput === "number" ? currentInput + 1 : 0;
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

function buildEnv(
  def: ReturnType<typeof defineWorkflow>,
  repoStore: ReturnType<typeof createInMemoryRepoStore>,
  blobs: ReturnType<typeof createInMemoryBlobSubstrate>,
  effects: EffectLedger,
): WorkflowRuntimeEnv {
  const clock = () => new Date();
  const authorize: WorkflowAuthorizeFn = async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
  const invokeStep: StepInvoker = async () => ({ output: null });
  const invokeAction: ActionInvoker = async ({
    handler,
    input,
    requires,
    authzContext,
  }) => {
    if (handler !== "echo") return { output: `ran:${handler}` };
    const ctx = createEffectContext({
      authorize,
      effects,
      requires,
      authzContext,
      input,
    });
    await ctx.perform({
      effectId: "touch",
      capability: "fs:write",
      run: async () => null,
    });
    return { output: input };
  };
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs,
    directors: createDefaultDirectorRegistry(),
    authorize,
    invokeStep,
    invokeAction,
    effects,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
    loopFns,
  };
  const loopBodies = new Map(
    enumerateInlineLoopBodies(def).map((b) => [b.ref, b.definition]),
  );
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  return env;
}

type TruncPred = (
  e: WorkflowEvent,
  seen: WorkflowEvent[],
  runId: string,
) => boolean;

async function runThenResume(
  def: ReturnType<typeof defineWorkflow>,
  truncateAfter: TruncPred,
): Promise<{ first: RunResult; second: RunResult }> {
  const blobs = createInMemoryBlobSubstrate();
  const effects = inMemoryLedger();

  const env1 = buildEnv(def, createInMemoryRepoStore(), blobs, effects);
  const first = await runtimeRun(def, env1).complete;
  expect(first.terminalStatus).toBe("completed");

  const trimmed: WorkflowEvent[] = [];
  const seen: WorkflowEvent[] = [];
  let cut = false;
  for (const e of first.events) {
    trimmed.push(e);
    seen.push(e);
    if (truncateAfter(e, seen, first.runId)) {
      cut = true;
      break;
    }
  }
  if (!cut) throw new Error("truncation predicate never matched");
  if (trimmed.length === first.events.length) {
    throw new Error("truncation kept the whole log");
  }

  const env2 = buildEnv(def, createInMemoryRepoStore(), blobs, effects);
  const second = await runtimeRun(def, env2, {
    runId: first.runId,
    resumeFromEvents: trimmed,
  }).complete;
  return { first, second };
}

const boundaries: {
  name: string;
  def: ReturnType<typeof defineWorkflow>;
  pred: TruncPred;
}[] = [
  {
    name: "converge: container StepStarted",
    def: parentConverge,
    pred: (e) => e.kind === "StepStarted" && e.stepId === "rework",
  },
  {
    name: "converge: iteration 0 StepCompleted",
    def: parentConverge,
    pred: (e) => e.kind === "StepCompleted" && e.stepId === "rework[0]",
  },
  {
    name: "converge: iteration 1 StepCompleted",
    def: parentConverge,
    pred: (e) => e.kind === "StepCompleted" && e.stepId === "rework[1]",
  },
  {
    name: "converge: last iteration StepCompleted (pre-routing)",
    def: parentConverge,
    pred: (e) => e.kind === "StepCompleted" && e.stepId === "rework[2]",
  },
  {
    name: "converge: iteration 0 ChildCompleted (pre scoped StepCompleted)",
    def: parentConverge,
    pred: (e, _s, runId) =>
      e.kind === "ChildCompleted" &&
      e.childRunId === loopBodyRunId(runId, "rework", 0),
  },
  {
    name: "converge: post-routing window (pruned escalate sentinel durable)",
    def: parentConverge,
    pred: (e) => e.kind === "StepCompleted" && e.stepId === "escalate",
  },
  {
    name: "exhaust: container StepStarted",
    def: parentExhaust,
    pred: (e) => e.kind === "StepStarted" && e.stepId === "rework",
  },
  {
    name: "exhaust: iteration 1 StepCompleted",
    def: parentExhaust,
    pred: (e) => e.kind === "StepCompleted" && e.stepId === "rework[1]",
  },
  {
    name: "exhaust: last iteration StepCompleted (pre-routing)",
    def: parentExhaust,
    pred: (e) => e.kind === "StepCompleted" && e.stepId === "rework[2]",
  },
];

describe("loop container output across resume boundaries", () => {
  for (const b of boundaries) {
    test(b.name, async () => {
      const { first, second } = await runThenResume(b.def, b.pred);
      expect(second.terminalStatus).toBe("completed");
      // Pin the straight-through record as populated before comparing. The
      // comparison below is a deep equality, so two absent or empty records
      // would satisfy it while proving nothing about the seam under test.
      expect(first.outputs.rework).toMatchObject({
        outcome: expect.any(String),
        iterations: expect.any(Number),
      });
      expect(second.outputs.rework).toEqual(first.outputs.rework);
    });
  }
});
