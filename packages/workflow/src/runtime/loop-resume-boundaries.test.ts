// Temp critique test: exercise loop-resume truncation boundaries the
// committed loop-resume.test.ts does not: crash at container-start,
// clean between-iteration boundary, exhausted path, and the
// post-routing window (skip sentinels durable, container StepCompleted
// not yet landed).

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
  defineWorkflow,
  loop,
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

import { createLoopIteration } from "../runlocal/loop-iteration";

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

// while: count < 2 (converge) ; carry: +1
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

// while always true -> exhausts at maxIterations=3
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
  effectRuns: { n: number },
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
      run: async () => {
        effectRuns.n += 1;
        return null;
      },
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
    loopFns,
  };
  env.runLoopIteration = createLoopIteration(env);
  return env;
}

type TruncPred = (e: WorkflowEvent, seen: WorkflowEvent[]) => boolean;

async function runThenResume(
  def: ReturnType<typeof defineWorkflow>,
  truncateAfter: TruncPred,
): Promise<{ effectRuns: number; result2: RunResult }> {
  const blobs = createInMemoryBlobSubstrate();
  const effects = inMemoryLedger();
  const effectRuns = { n: 0 };

  const repoStore1 = createInMemoryRepoStore();
  const env1 = buildEnv(def, repoStore1, blobs, effects, effectRuns);
  const result1 = await runtimeRun(def, env1).complete;
  expect(result1.terminalStatus).toBe("completed");

  const trimmed: WorkflowEvent[] = [];
  const seen: WorkflowEvent[] = [];
  for (const e of result1.events) {
    trimmed.push(e);
    seen.push(e);
    if (truncateAfter(e, seen)) break;
  }

  const repoStore2 = createInMemoryRepoStore();
  const env2 = buildEnv(def, repoStore2, blobs, effects, effectRuns);
  const result2 = await runtimeRun(def, env2, {
    runId: result1.runId,
    resumeFromEvents: trimmed,
  }).complete;

  return { effectRuns: effectRuns.n, result2 };
}

describe("loop resume boundaries (critique)", () => {
  test("crash right after container StepStarted (no iteration yet)", async () => {
    const { effectRuns, result2 } = await runThenResume(
      parentConverge,
      (e) => e.kind === "StepStarted" && e.stepId === "rework",
    );
    expect(result2.terminalStatus).toBe("completed");
    expect(result2.outputs.consolidate).toBe("ran:consolidate");
    expect("escalate" in result2.outputs).toBe(false);
    // whole loop re-driven from scratch; ledger holds to 3.
    expect(effectRuns).toBe(3);
  });

  test("crash at clean between-iteration boundary (iter0 StepCompleted)", async () => {
    const { effectRuns, result2 } = await runThenResume(
      parentConverge,
      (e) => e.kind === "StepCompleted" && e.stepId === "rework[0]",
    );
    expect(result2.terminalStatus).toBe("completed");
    expect(result2.outputs.consolidate).toBe("ran:consolidate");
    expect(effectRuns).toBe(3);
  });

  test("crash mid-iteration on the exhausted path", async () => {
    const { effectRuns, result2 } = await runThenResume(
      parentExhaust,
      (e) => e.kind === "ChildSpawned" && e.childRunId === "rework__1",
    );
    expect(result2.terminalStatus).toBe("completed");
    expect(result2.outputs.escalate).toBe("ran:escalate");
    expect("consolidate" in result2.outputs).toBe(false);
    expect(effectRuns).toBe(3);
  });

  test("crash after final iteration StepCompleted but before routing (exhaust)", async () => {
    // maxIterations=3 -> iterations 0,1,2. Truncate right after the last
    // iteration's StepCompleted; the replay must detect exhaustion from
    // the log alone, then route + complete the container.
    const { effectRuns, result2 } = await runThenResume(
      parentExhaust,
      (e) => e.kind === "StepCompleted" && e.stepId === "rework[2]",
    );
    expect(result2.terminalStatus).toBe("completed");
    expect(result2.outputs.escalate).toBe("ran:escalate");
    expect("consolidate" in result2.outputs).toBe(false);
    expect(effectRuns).toBe(3);
  });

  test("crash in the post-routing window (skip sentinels durable, container StepCompleted not yet)", async () => {
    // Truncate right after the pruned-branch (escalate) skip sentinel's
    // StepCompleted but before the container rework's StepCompleted.
    const { effectRuns, result2 } = await runThenResume(
      parentConverge,
      (e) => e.kind === "StepCompleted" && e.stepId === "escalate",
    );
    expect(result2.terminalStatus).toBe("completed");
    expect(result2.outputs.consolidate).toBe("ran:consolidate");
    // escalate was pruned; its seeded skip-sentinel output is hydrated,
    // but it was never re-run as an action (no fresh effect).
    expect(result2.outputs.escalate).toEqual({
      skipped: true,
      loopId: "rework",
      outcome: "converged",
    });
    expect(effectRuns).toBe(3);
  });
});
