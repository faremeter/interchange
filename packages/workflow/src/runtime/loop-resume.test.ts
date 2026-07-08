// Loop resume: a mid-loop crash resumes to exactly-once. The resumed
// run re-derives its cursor from the log, re-enters the in-flight
// iteration without re-emitting its durable events, and the shared
// effect ledger deduplicates the re-driven iterations' effects.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

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
  map,
  runtimeRun,
  step,
  type ActionInvoker,
  type EffectLedger,
  type LoopFn,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

import { isResumableInFlightLoopStep } from "./dag";
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

// Build an env over caller-supplied shared substrates (so the ledger and
// blobs survive a crash + resume) and a counter of real effect executions.
function buildEnv(
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
    const output = await ctx.perform({
      effectId: "touch",
      capability: "fs:write",
      run: async () => {
        effectRuns.n += 1;
        return null;
      },
    });
    void output;
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
    drain: createNoopDrainController(parentWorkflow),
    loopFns,
  };
  env.runLoopIteration = createLoopIteration(env);
  return env;
}

describe("loop resume", () => {
  test("a mid-iteration crash resumes to exactly-once", async () => {
    const blobs = createInMemoryBlobSubstrate();
    const effects = inMemoryLedger();
    const effectRuns = { n: 0 };

    const repoStore1 = createInMemoryRepoStore();
    const env1 = buildEnv(repoStore1, blobs, effects, effectRuns);
    const result1 = await runtimeRun(parentWorkflow, env1).complete;
    expect(result1.terminalStatus).toBe("completed");
    // Converges after 3 iterations (count 0,1,2), one effect each.
    expect(effectRuns.n).toBe(3);

    // Simulate a crash mid-iteration 1: truncate the durable parent log
    // right after iteration 1's ChildSpawned flush (child spawned, not
    // yet complete) -- exactly where a mid-child crash leaves it.
    const trimmed: WorkflowEvent[] = [];
    for (const e of result1.events) {
      trimmed.push(e);
      if (e.kind === "ChildSpawned" && e.childRunId === "rework__1") break;
    }

    // Resume: fresh repoStore, SHARED blobs + effects ledger.
    const repoStore2 = createInMemoryRepoStore();
    const env2 = buildEnv(repoStore2, blobs, effects, effectRuns);
    const result2 = await runtimeRun(parentWorkflow, env2, {
      runId: result1.runId,
      resumeFromEvents: trimmed,
    }).complete;

    expect(result2.terminalStatus).toBe("completed");
    // Converged, so the normal dependent ran and onExhausted was pruned.
    expect(result2.outputs.consolidate).toBe("ran:consolidate");
    expect("escalate" in result2.outputs).toBe(false);
    // The resumed run re-drove iterations 1 and 2, but the shared ledger
    // held every effect to exactly one execution across both runs.
    expect(effectRuns.n).toBe(3);
  });
});

describe("isResumableInFlightLoopStep", () => {
  const def = defineWorkflow({
    id: "predicate",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body,
        while: "cont",
        carry: "next",
        input: { literal: 0 },
        maxIterations: 3,
        onExhausted: "esc",
      }),
      fan: map({
        over: { literal: [1, 2] },
        step: step({
          agent: defineAgent({
            id: "inner",
            systemPrompt: "inner",
            tools: [],
            capabilities: [],
            inference: { sources: [{ provider: "fake", model: "fake" }] },
          }),
        }),
      }),
      esc: action({ handler: "e", after: ["rework"] }),
    },
  });

  test("exempts an in-flight loop container and its synthetic iteration", () => {
    expect(isResumableInFlightLoopStep(def, "rework", "in-flight")).toBe(true);
    expect(isResumableInFlightLoopStep(def, "rework[2]", "in-flight")).toBe(
      true,
    );
    // Multi-digit iteration index strips correctly.
    expect(isResumableInFlightLoopStep(def, "rework[12]", "in-flight")).toBe(
      true,
    );
  });

  test("rejects non-loop steps, map ids, malformed ids, and terminal phases", () => {
    expect(isResumableInFlightLoopStep(def, "esc", "in-flight")).toBe(false);
    expect(isResumableInFlightLoopStep(def, "esc[0]", "in-flight")).toBe(false);
    // A map container and its synthetic inner id keep rejecting.
    expect(isResumableInFlightLoopStep(def, "fan", "in-flight")).toBe(false);
    expect(isResumableInFlightLoopStep(def, "fan[3]", "in-flight")).toBe(false);
    // Malformed brackets do not strip to a loop container.
    expect(isResumableInFlightLoopStep(def, "rework[]", "in-flight")).toBe(
      false,
    );
    expect(isResumableInFlightLoopStep(def, "rework[x]", "in-flight")).toBe(
      false,
    );
    expect(isResumableInFlightLoopStep(def, "unknown", "in-flight")).toBe(
      false,
    );
    expect(isResumableInFlightLoopStep(def, "rework", "completed")).toBe(false);
    expect(
      isResumableInFlightLoopStep(def, "rework[2]", "awaiting-signal"),
    ).toBe(false);
  });
});
