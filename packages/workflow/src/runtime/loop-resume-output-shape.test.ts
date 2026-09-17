// The loop container's output shape across a crash boundary.
//
// A loop's output rides inline in its persisted `StepCompleted`. On resume,
// `executeRunBody` hydrates `stepOutputs` from the log and `nextSchedulable`
// never re-offers a `completed` loop container, so a run that crashed AFTER
// the loop settled feeds its downstream steps the PERSISTED record verbatim
// and never recomputes it. That window -- settled, crashed, resumed on a
// later build of the runtime -- is the one place where the loop's output
// shape is frozen and unrecomputable, and it is what makes any change to
// that shape a compatibility question.
//
// The two tests below pin both halves of the answer. A log written before a
// key was added resumes green as long as no step reads the new key, because
// a step reading it cannot exist in a run authored before the key did. A log
// missing a key a step DOES read fails the resumed run outright, which is
// why a key can only ever be added to this record, never renamed or removed.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import {
  action,
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
  type WorkflowDefinition,
  type WorkflowAuthorizeFn,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const body = defineWorkflow({
  id: "bump-body",
  trigger: { type: "manual" },
  steps: {
    bump: action({ handler: "bump", input: { from: "trigger.payload" } }),
  },
});

function buildWorkflow(downstreamPath: string) {
  return defineWorkflow({
    id: "loop-resume-output-shape",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body,
        while: "cont",
        carry: "next",
        input: { literal: 0 },
        maxIterations: 10,
        onExhausted: "escalate",
      }),
      downstream: action({
        handler: "downstream",
        after: ["rework"],
        input: { from: downstreamPath },
      }),
      escalate: action({ handler: "escalate", after: ["rework"] }),
    },
  });
}

function bumpOf(childOutput: unknown): number {
  if (
    typeof childOutput === "object" &&
    childOutput !== null &&
    "bump" in childOutput &&
    typeof childOutput.bump === "number"
  ) {
    return childOutput.bump;
  }
  throw new Error("iteration output missing numeric bump");
}

const loopFns = (ref: string): LoopFn => {
  if (ref === "cont") return (childOutput) => bumpOf(childOutput) < 3;
  if (ref === "next") return (childOutput) => bumpOf(childOutput);
  throw new Error(`unknown loop fn ${ref}`);
};

const seen: { downstream?: unknown } = {};

/** The substrate both the crashed run and its resume share. */
function createSubstrate() {
  const store = new Map<string, { output: unknown }>();
  const effects: EffectLedger = {
    async lookup(effectKey) {
      return store.get(effectKey);
    },
    async record(effectKey, output) {
      store.set(effectKey, { output });
    },
  };
  return { blobs: createInMemoryBlobSubstrate(), effects };
}

/**
 * One process's view of the run. `bumpCalls` counts loop-body iterations
 * this process drove, which is how a resume proves it did NOT re-enter
 * `runLoop`: a replayed loop would re-invoke the body handler.
 */
function buildEnv(
  definition: WorkflowDefinition,
  repoStore: ReturnType<typeof createInMemoryRepoStore>,
  substrate: ReturnType<typeof createSubstrate>,
): { env: WorkflowRuntimeEnv; bumpCalls: () => number } {
  let bumpCalls = 0;
  const clock = () => new Date();
  const authorize: WorkflowAuthorizeFn = async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
  const invokeStep: StepInvoker = async () => ({ output: null });
  const invokeAction: ActionInvoker = async ({ handler, input }) => {
    if (handler === "bump") {
      bumpCalls += 1;
      return { output: typeof input === "number" ? input + 1 : 0 };
    }
    if (handler === "downstream") {
      seen.downstream = input;
      return { output: input };
    }
    return { output: `ran:${handler}` };
  };
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs: substrate.blobs,
    directors: createDefaultDirectorRegistry(),
    authorize,
    invokeStep,
    invokeAction,
    effects: substrate.effects,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(definition),
    loopFns,
  };
  const loopBodies = new Map(
    enumerateInlineLoopBodies(definition).map((b) => [b.ref, b.definition]),
  );
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  return { env, bumpCalls: () => bumpCalls };
}

/**
 * Trim a completed run's log to the instant the loop container's OWN
 * `StepCompleted` landed -- POST-settlement, so the recovered container is
 * `completed` and `nextSchedulable` will never re-offer it -- and rewrite
 * that record's inline output to `persistedOutput`, the log a previous
 * build of the runtime would have written.
 *
 * Trimming anywhere earlier lands in the already-covered mid-iteration
 * window, where the container is still in flight and `runLoop` replays and
 * RECOMPUTES the output. The assertion below therefore guards the cut: the
 * tail must be the container's own completion and the run must have
 * reached its downstream step in the original pass.
 */
function logAsPersistedBy(
  events: readonly WorkflowEvent[],
  persistedOutput: unknown,
): WorkflowEvent[] {
  const trimmed: WorkflowEvent[] = [];
  for (const event of events) {
    trimmed.push(event);
    if (event.kind === "StepCompleted" && event.stepId === "rework") break;
  }
  const tail = trimmed[trimmed.length - 1];
  if (tail?.kind !== "StepCompleted" || tail.stepId !== "rework") {
    throw new Error("run log has no StepCompleted for the loop container");
  }
  if (trimmed.length === events.length) {
    throw new Error("the loop container's completion is the whole log");
  }
  trimmed[trimmed.length - 1] = {
    ...tail,
    output: { ref: `inline:${JSON.stringify(persistedOutput)}` },
  };
  return trimmed;
}

// The record a build of the runtime that predates the `final` key wrote for
// this loop: converged after three iterations, carrying the converging
// iteration's input.
const preFinalShape = { outcome: "converged", iterations: 3, carry: 2 };

describe("loop output shape across a crash boundary", () => {
  test("a resumed run feeds the downstream step the persisted output, never a recompute", async () => {
    const workflow = buildWorkflow("steps.rework.output");
    const substrate = createSubstrate();

    seen.downstream = undefined;
    const original = buildEnv(workflow, createInMemoryRepoStore(), substrate);
    const first = await runtimeRun(workflow, original.env).complete;
    expect(first.terminalStatus).toBe("completed");
    expect(original.bumpCalls()).toBe(3);
    expect(seen.downstream).toEqual({
      ...preFinalShape,
      final: { bump: 3 },
    });

    seen.downstream = undefined;
    const resumed = buildEnv(workflow, createInMemoryRepoStore(), substrate);
    const second = await runtimeRun(workflow, resumed.env, {
      runId: first.runId,
      resumeFromEvents: logAsPersistedBy(first.events, preFinalShape),
    }).complete;

    // Zero body iterations in the resumed process: the settled container was
    // never re-offered, so `runLoop` was never re-entered and the output was
    // never recomputed. This is what distinguishes the post-settlement window
    // from a mid-iteration crash, where the loop re-drives.
    expect(resumed.bumpCalls()).toBe(0);
    // Downstream therefore read the stale persisted value verbatim. A log
    // that predates the `final` key still settles green, because no step in
    // a run authored before the key can read it.
    expect(second.terminalStatus).toBe("completed");
    expect(second.outputs.rework).toEqual(preFinalShape);
    expect(seen.downstream).toEqual(preFinalShape);
  });

  test("a downstream step reading a key the persisted output lacks fails the resumed run", async () => {
    const workflow = buildWorkflow("steps.rework.output.final");
    const substrate = createSubstrate();

    const original = buildEnv(workflow, createInMemoryRepoStore(), substrate);
    const first = await runtimeRun(workflow, original.env).complete;
    expect(first.terminalStatus).toBe("completed");

    const resumed = buildEnv(workflow, createInMemoryRepoStore(), substrate);
    const second = await runtimeRun(workflow, resumed.env, {
      runId: first.runId,
      resumeFromEvents: logAsPersistedBy(first.events, preFinalShape),
    }).complete;

    // No recompute means no repair: the missing key is a hard failure. This
    // is the cost a rename or a removal would impose on every run that
    // crashed after its loop settled, and the reason `final` was added
    // alongside `carry` rather than replacing it.
    expect(resumed.bumpCalls()).toBe(0);
    expect(second.terminalStatus).toBe("failed");
    expect(
      second.events.filter((e) => e.kind === "StepFailed").map((e) => e.stepId),
    ).toContain("downstream");
  });
});
