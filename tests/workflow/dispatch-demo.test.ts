// End-to-end dispatch-demo shape on the extended engine.
//
// Authors the interchange-demo-dispatch orchestration as one workflow
// using the new primitives -- deterministic `action` effects (baseline
// capture, git commit), a `map` fan-out over a level's tasks, and a
// bounded rework `loop` (the amendment loop) whose `while`/`carry` are
// pure registry functions and whose `onExhausted` routes to an
// `escalation`. Runs three scenarios end-to-end without an LLM: the
// amendment loop converging, the loop exhausting to escalation, and a
// mid-loop crash resuming to exactly-once effects.
//
// The demo's real agents (planner/critic/fixer/...) are stubbed behind
// the invokeStep seam here; porting them onto that seam is separate
// follow-up work (they target an older @intx/agent API). This test
// validates that the engine expresses the demo's orchestration shape.

import { describe, test, expect } from "bun:test";

import {
  createDefaultDirectorRegistry,
  defineAgent,
  type AgentDefinition,
  type BaseEnv,
} from "@intx/agent";

import {
  action,
  createEffectContext,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createLoopIteration,
  createNoopDrainController,
  defineWorkflow,
  escalation,
  loop,
  map,
  runtimeRun,
  step,
  type ActionInvoker,
  type EffectLedger,
  type LoopFn,
  type RunResult,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

function agent(id: string): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

function roundOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "round" in value) {
    const round = value.round;
    if (typeof round === "number") return round;
  }
  return 1;
}

function verdictOf(childOutput: unknown): string {
  if (
    typeof childOutput === "object" &&
    childOutput !== null &&
    "critic" in childOutput
  ) {
    const critic = childOutput.critic;
    if (typeof critic === "object" && critic !== null && "verdict" in critic) {
      const verdict = critic.verdict;
      if (typeof verdict === "string") return verdict;
    }
  }
  return "amend";
}

// The amendment loop body: a fixer reworks, then the critic re-judges.
const amendBody = defineWorkflow({
  id: "amend-body",
  trigger: { type: "manual" },
  steps: {
    fix: step({ agent: agent("fixer"), input: { from: "trigger.payload" } }),
    critic: step({
      agent: agent("critic"),
      input: { from: "trigger.payload" },
    }),
  },
});

const dispatch = defineWorkflow({
  id: "dispatch",
  trigger: { type: "manual" },
  steps: {
    captureBaseline: action({
      handler: "captureBaseline",
      effect: { requires: ["shell:run"] },
    }),
    plan: step({ agent: agent("planner"), after: ["captureBaseline"] }),
    runLevel: map({
      over: { from: "steps.plan.output.tasks" },
      step: step({ agent: agent("implementer") }),
      after: ["plan"],
    }),
    commit: action({
      handler: "commit",
      effect: { requires: ["git:commit"] },
      input: { from: "steps.runLevel.output" },
      after: ["runLevel"],
    }),
    critique: step({
      agent: agent("critic"),
      input: { literal: { round: 1 } },
      after: ["commit"],
    }),
    amend: loop({
      body: amendBody,
      while: "shouldAmend",
      carry: "nextRound",
      input: { literal: { round: 1 } },
      maxIterations: 3,
      onExhausted: "escalate",
      after: ["critique"],
    }),
    consolidate: step({ agent: agent("consolidator"), after: ["amend"] }),
    escalate: escalation({ to: "operator", after: ["amend"] }),
  },
});

const loopFns = (ref: string): LoopFn => {
  if (ref === "shouldAmend")
    return (childOutput) => verdictOf(childOutput) === "amend";
  if (ref === "nextRound")
    return (_childOutput, currentInput) => ({
      round: roundOf(currentInput) + 1,
    });
  throw new Error(`unknown loop fn ${ref}`);
};

// `converge` decides the critic's verdict: once the round reaches the
// threshold the critic passes; otherwise it asks for another amendment.
function makeInvokeStep(convergeAtRound: number): StepInvoker {
  return async ({ agent: a, input }) => {
    switch (a.id) {
      case "planner":
        return { output: { tasks: [{ id: "t1" }, { id: "t2" }] } };
      case "implementer":
        return { output: { done: true } };
      case "fixer":
        return { output: { reworked: true } };
      case "consolidator":
        return { output: { consolidated: true } };
      case "critic": {
        const round = roundOf(input);
        const verdict = round >= convergeAtRound ? "pass" : "amend";
        return { output: { verdict, round } };
      }
      default:
        return { output: null };
    }
  };
}

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

function buildEnv(opts: {
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
  blobs: ReturnType<typeof createInMemoryBlobSubstrate>;
  effects: EffectLedger;
  convergeAtRound: number;
  effectRuns: { n: number };
}): WorkflowRuntimeEnv {
  const { repoStore, blobs, effects, convergeAtRound, effectRuns } = opts;
  const clock = () => new Date();
  const authorize: WorkflowAuthorizeFn = async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
  const invokeAction: ActionInvoker = async ({
    handler,
    input,
    requires,
    authzContext,
  }) => {
    const ctx = createEffectContext({
      authorize,
      effects,
      requires,
      authzContext,
      input,
    });
    const output = await ctx.perform({
      effectId: handler,
      capability: requires[0] ?? "shell:run",
      run: async () => {
        effectRuns.n += 1;
        return { handler, done: true };
      },
    });
    return { output };
  };
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs,
    directors: createDefaultDirectorRegistry(),
    authorize,
    invokeStep: makeInvokeStep(convergeAtRound),
    invokeAction,
    effects,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(dispatch),
    loopFns,
  };
  env.runLoopIteration = createLoopIteration(env);
  return env;
}

function loopOutcome(result: RunResult): unknown {
  const amend = result.outputs.amend;
  if (typeof amend === "object" && amend !== null && "outcome" in amend) {
    return amend.outcome;
  }
  throw new Error("amend loop output missing outcome");
}

describe("dispatch demo shape", () => {
  test("the amendment loop converges and the level consolidates", async () => {
    const env = buildEnv({
      repoStore: createInMemoryRepoStore(),
      blobs: createInMemoryBlobSubstrate(),
      effects: inMemoryLedger(),
      convergeAtRound: 2,
      effectRuns: { n: 0 },
    });
    const result = await runtimeRun(dispatch, env).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(loopOutcome(result)).toBe("converged");
    // The baseline and the commit both ran; consolidate ran; the loop
    // converged so escalation was pruned.
    expect(result.outputs.consolidate).toEqual({ consolidated: true });
    expect("escalate" in result.outputs).toBe(false);
  });

  test("the amendment loop exhausts and routes to escalation", async () => {
    const env = buildEnv({
      repoStore: createInMemoryRepoStore(),
      blobs: createInMemoryBlobSubstrate(),
      effects: inMemoryLedger(),
      // The critic never passes, so the loop exhausts at its cap.
      convergeAtRound: 99,
      effectRuns: { n: 0 },
    });
    const result = await runtimeRun(dispatch, env).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(loopOutcome(result)).toBe("exhausted");
    // Escalation ran; consolidate was pruned.
    expect("escalate" in result.outputs).toBe(true);
    expect("consolidate" in result.outputs).toBe(false);
  });

  test("a mid-amendment crash resumes to exactly-once effects", async () => {
    const blobs = createInMemoryBlobSubstrate();
    const effects = inMemoryLedger();
    const effectRuns = { n: 0 };
    const env1 = buildEnv({
      repoStore: createInMemoryRepoStore(),
      blobs,
      effects,
      convergeAtRound: 2,
      effectRuns,
    });
    const result1 = await runtimeRun(dispatch, env1).complete;
    expect(result1.terminalStatus).toBe("completed");
    // captureBaseline + commit = two deterministic effects.
    const effectsAfterRun1 = effectRuns.n;
    expect(effectsAfterRun1).toBe(2);

    // Crash right after the first amendment iteration's ChildSpawned.
    const trimmed: WorkflowEvent[] = [];
    for (const e of result1.events) {
      trimmed.push(e);
      if (e.kind === "ChildSpawned" && e.childRunId === "amend__0") break;
    }

    const env2 = buildEnv({
      repoStore: createInMemoryRepoStore(),
      blobs,
      effects,
      convergeAtRound: 2,
      effectRuns,
    });
    const result2 = await runtimeRun(dispatch, env2, {
      runId: result1.runId,
      resumeFromEvents: trimmed,
    }).complete;

    expect(result2.terminalStatus).toBe("completed");
    expect(loopOutcome(result2)).toBe("converged");
    // captureBaseline and commit both completed before the crash, so on
    // resume their durable StepCompleted events replay from the seed log
    // and their handlers are never re-invoked -- the two effects stay at
    // one execution each. (The in-flight amendment iteration re-drives its
    // body, which holds no effects.) The shared ledger's own dedup path is
    // covered directly in action.test.ts.
    expect(effectRuns.n).toBe(effectsAfterRun1);
  });
});
