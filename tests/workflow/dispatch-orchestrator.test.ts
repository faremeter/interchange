// The interchange-demo dispatch orchestrator on the extended engine, driven by a
// stubbed invoker: the OUTER per-level iteration.
//
// `dispatch-demo.test.ts` authors a single level (captureBaseline -> plan ->
// runLevel map -> commit -> critique -> amend loop -> consolidate/escalate).
// The real demo runs `plan` once and then walks the plan's LEVELS, running that
// per-level pipeline once per level, and short-circuits the whole run when a
// level cannot be made to pass. This test authors that outer iteration as an
// engine loop whose body is the per-level pipeline -- a loop nested inside a
// loop (the body carries the amendment loop). It establishes the nested-loop
// composition, the level-cursor carry, and the failure short-circuit.
//
// Structural fidelity only: the planner/critic/fixer agents are stubbed behind
// the invokeStep seam and the git effects are deterministic, keeping this the
// fast, LLM-free check of the engine's routing and effect behaviour.

import { describe, test, expect } from "bun:test";

import {
  createDefaultDirectorRegistry,
  defineAgent,
  type AgentDefinition,
  type BaseEnv,
} from "@intx/agent";

import {
  action,
  awaitSignal,
  createEffectContext,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  createSpawnLoopIteration,
  defineWorkflow,
  enumerateInlineLoopBodies,
  escalation,
  gate,
  loop,
  loopBodyRunId,
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

function agent(id: string): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

interface PlanTask {
  readonly id: string;
  readonly level: number;
}

function roundOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "round" in value) {
    const round = value.round;
    if (typeof round === "number") return round;
  }
  throw new Error("expected a numeric round");
}

function cursorOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "cursor" in value) {
    const cursor = value.cursor;
    if (typeof cursor === "number") return cursor;
  }
  throw new Error("expected a numeric cursor");
}

function levelCountOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "levelCount" in value) {
    const levelCount = value.levelCount;
    if (typeof levelCount === "number") return levelCount;
  }
  throw new Error("expected a numeric levelCount");
}

function levelOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "level" in value) {
    const level = value.level;
    if (typeof level === "number") return level;
  }
  throw new Error("expected a numeric level");
}

function toPlanTask(value: unknown): PlanTask {
  if (
    typeof value === "object" &&
    value !== null &&
    "id" in value &&
    "level" in value
  ) {
    const id = value.id;
    const level = value.level;
    if (typeof id === "string" && typeof level === "number") {
      return { id, level };
    }
  }
  throw new Error("malformed plan task");
}

function tasksOf(value: unknown): PlanTask[] {
  if (typeof value === "object" && value !== null && "tasks" in value) {
    const { tasks } = value;
    if (Array.isArray(tasks)) return tasks.map((t: unknown) => toPlanTask(t));
  }
  throw new Error("expected a tasks array");
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
  throw new Error("amend body output missing a critic verdict");
}

// The distinct levels of a plan, ascending -- the sequence the outer loop walks.
function distinctLevels(tasks: readonly PlanTask[]): number[] {
  const seen = new Set<number>();
  for (const task of tasks) seen.add(task.level);
  return Array.from(seen).sort((a, b) => a - b);
}

function attemptOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "attempt" in value) {
    const attempt = value.attempt;
    if (typeof attempt === "number") return attempt;
  }
  throw new Error("expected a numeric attempt");
}

function taskCountOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "taskCount" in value) {
    const taskCount = value.taskCount;
    if (typeof taskCount === "number") return taskCount;
  }
  throw new Error("expected a numeric taskCount");
}

function decisionOf(value: unknown): string {
  if (typeof value === "object" && value !== null && "decision" in value) {
    const decision = value.decision;
    if (typeof decision === "string") return decision;
  }
  throw new Error("expected an operator decision");
}

// The Phase-5 loop keys its `while` off the build gate's `clean` field only.
// The build is the body's unconditional first step, so it always exists; the
// fix path (attribution..rebuild) is pruned on a clean build and must never be
// read here.
function buildClean(childOutput: unknown): boolean {
  if (
    typeof childOutput === "object" &&
    childOutput !== null &&
    "build" in childOutput
  ) {
    const build = childOutput.build;
    if (typeof build === "object" && build !== null && "clean" in build) {
      const clean = build.clean;
      if (typeof clean === "boolean") return clean;
    }
  }
  throw new Error("phase5 body output missing build.clean");
}

function verifyFixed(childOutput: unknown): boolean {
  if (
    typeof childOutput === "object" &&
    childOutput !== null &&
    "verify" in childOutput
  ) {
    const verify = childOutput.verify;
    if (typeof verify === "object" && verify !== null && "fixed" in verify) {
      const fixed = verify.fixed;
      if (typeof fixed === "boolean") return fixed;
    }
  }
  throw new Error("fix body output missing verify.fixed");
}

// The amendment loop body: a fixer reworks, then the critic re-judges. Identical
// in shape to the sibling dispatch-demo's amend body.
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

// The per-level pipeline, authored as the OUTER loop's body. `pickLevel` slices
// the plan's tasks down to the cursor's level (a host-JS transform, because a
// selector cannot index an array by a runtime cursor). A level that never
// passes exhausts the amendment loop and routes to `levelFailed`, which throws
// to fail the body run -- the outer loop then short-circuits the whole run.
const perLevelBody = defineWorkflow({
  id: "per-level-body",
  trigger: { type: "manual" },
  steps: {
    pickLevel: action({
      handler: "pickLevel",
      input: { from: "trigger.payload" },
    }),
    implement: map({
      over: { from: "steps.pickLevel.output.tasksAtLevel" },
      step: step({
        agent: agent("implementer"),
        input: { from: "trigger.payload" },
      }),
      after: ["pickLevel"],
    }),
    commit: action({
      handler: "commit",
      effect: { requires: ["git:commit"] },
      input: { from: "steps.pickLevel.output" },
      after: ["implement"],
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
      onExhausted: "levelFailed",
      after: ["critique"],
    }),
    levelFailed: action({ handler: "levelFailed", after: ["amend"] }),
  },
});

// One fix attempt (the innermost, retry level). The fixer reworks and the
// deterministic `verify` reports whether the rework holds.
const fixBody = defineWorkflow({
  id: "fix-body",
  trigger: { type: "manual" },
  steps: {
    fixAttempt: step({
      agent: agent("phase5-fixer"),
      input: { from: "trigger.payload" },
    }),
    verify: action({
      handler: "verify",
      input: { from: "trigger.payload" },
      after: ["fixAttempt"],
    }),
  },
});

// One attributed task's fix (the per-task level). Once the task crosses the
// escalation threshold, `escGate` diverts through `operatorWait` -- an
// `awaitSignal` that parks the task mid-fix and relays up through the task loop
// and the Phase-5 loop to the run's signal channel. `abortCheck` throws on
// "abort" (failing the run) and passes on "continue". `escJoin` is the diamond
// join (an action, so the join is the codebase's tested action-join shape); the
// nested `retryLoop` then retries the fix until `verify` passes, failing the run
// via `retryFailed` if it cannot. Escalation gates whether to proceed to the
// retries, it does not replace them.
const taskBody = defineWorkflow({
  id: "task-body",
  trigger: { type: "manual" },
  steps: {
    escGate: gate({
      when: { from: "trigger.payload.escalate" },
      then: "operatorWait",
      else: "escSkip",
    }),
    operatorWait: awaitSignal({ name: "operator", after: ["escGate"] }),
    abortCheck: action({
      handler: "abortCheck",
      input: { from: "steps.operatorWait.output" },
      after: ["operatorWait"],
    }),
    escSkip: action({ handler: "noop", after: ["escGate"] }),
    escJoin: action({ handler: "noop", after: ["abortCheck", "escSkip"] }),
    retryLoop: loop({
      body: fixBody,
      while: "taskUnfixed",
      carry: "nextAttempt",
      input: { literal: { attempt: 0 } },
      maxIterations: 3,
      onExhausted: "retryFailed",
      after: ["escJoin"],
    }),
    retryFailed: action({ handler: "retryFailed", after: ["retryLoop"] }),
  },
});

// One Phase-5 verification round. `build` is a deterministic effect whose
// `clean` field drives both the in-round `cleanGate` (which prunes the fix path
// on a clean build) and the loop's `buildDirty` while. A dirty build attributes
// the failures, then `taskLoop` walks the attributed tasks -- each task retried
// by its own nested `retryLoop` (so the fix nesting is phase5 -> taskLoop ->
// retryLoop) -- and rebuilds; the NEXT round's build is the re-gate that closes
// Phase 5 out. An exhausted task loop routes to `tasksExhausted`, which throws.
const phase5Body = defineWorkflow({
  id: "phase5-body",
  trigger: { type: "manual" },
  steps: {
    build: action({
      handler: "buildGate",
      effect: { requires: ["shell:run"] },
      input: { from: "trigger.payload" },
    }),
    cleanGate: gate({
      when: { from: "steps.build.output.clean" },
      then: "skipFix",
      else: "attribution",
      after: ["build"],
    }),
    skipFix: action({ handler: "noop", after: ["cleanGate"] }),
    attribution: step({ agent: agent("attributor"), after: ["cleanGate"] }),
    pickFixTasks: action({ handler: "pickFixTasks", after: ["attribution"] }),
    taskLoop: loop({
      body: taskBody,
      while: "moreTasks",
      carry: "nextTask",
      input: {
        merge: [
          { literal: { cursor: 0, escalate: false } },
          { from: "steps.pickFixTasks.output" },
        ],
      },
      maxIterations: 5,
      onExhausted: "tasksExhausted",
      after: ["pickFixTasks"],
    }),
    tasksExhausted: action({
      handler: "tasksExhausted",
      after: ["taskLoop"],
    }),
    rebuild: action({
      handler: "rebuild",
      effect: { requires: ["git:commit"] },
      input: { from: "trigger.payload" },
      after: ["taskLoop"],
    }),
  },
});

const fullDemo = defineWorkflow({
  id: "full-demo",
  trigger: { type: "manual" },
  steps: {
    captureBaseline: action({
      handler: "captureBaseline",
      effect: { requires: ["shell:run"] },
    }),
    plan: step({ agent: agent("planner"), after: ["captureBaseline"] }),
    parsePlan: action({
      handler: "parsePlan",
      input: { from: "steps.plan.output" },
      after: ["plan"],
    }),
    perLevelLoop: loop({
      body: perLevelBody,
      while: "moreLevels",
      carry: "advanceLevel",
      input: {
        merge: [
          { literal: { cursor: 0 } },
          {
            project: { from: "steps.parsePlan.output" },
            fields: ["tasks", "levelCount"],
          },
        ],
      },
      maxIterations: 10,
      onExhausted: "escalate",
      after: ["parsePlan"],
    }),
    phase5: loop({
      body: phase5Body,
      while: "buildDirty",
      carry: "nextVerifyRound",
      input: { literal: { round: 1 } },
      maxIterations: 5,
      onExhausted: "phase5Failed",
      after: ["perLevelLoop"],
    }),
    phase5Failed: action({ handler: "phase5Failed", after: ["phase5"] }),
    consolidate: step({
      agent: agent("consolidator"),
      after: ["phase5"],
    }),
    escalate: escalation({ to: "operator", after: ["perLevelLoop"] }),
  },
});

// The loop-fn registry closes over `escalateAtTask` so the Phase-5 task loop's
// carry can flag the task that must escalate to the operator. The functions stay
// pure -- they read only their inputs and a fixed threshold -- so the engine can
// re-run them deterministically on resume.
function makeLoopFns(escalateAtTask: number): (ref: string) => LoopFn {
  return (ref: string): LoopFn => {
    if (ref === "shouldAmend")
      return (childOutput) => verdictOf(childOutput) === "amend";
    if (ref === "nextRound")
      return (_childOutput, currentInput) => ({
        round: roundOf(currentInput) + 1,
      });
    // The outer per-level loop walks the level cursor: continue while the next
    // cursor is still a valid level, threading the plan tasks forward unchanged.
    if (ref === "moreLevels")
      return (_childOutput, currentInput) =>
        cursorOf(currentInput) + 1 < levelCountOf(currentInput);
    if (ref === "advanceLevel")
      return (_childOutput, currentInput) => ({
        cursor: cursorOf(currentInput) + 1,
        tasks: tasksOf(currentInput),
        levelCount: levelCountOf(currentInput),
      });
    // The Phase-5 loop repeats verification rounds while the build stays dirty.
    if (ref === "buildDirty")
      return (childOutput) => buildClean(childOutput) === false;
    if (ref === "nextVerifyRound")
      return (_childOutput, currentInput) => ({
        round: roundOf(currentInput) + 1,
      });
    // The task loop walks the attributed-task cursor, flagging the next task for
    // operator escalation once it reaches the threshold.
    if (ref === "moreTasks")
      return (_childOutput, currentInput) =>
        cursorOf(currentInput) + 1 < taskCountOf(currentInput);
    if (ref === "nextTask")
      return (_childOutput, currentInput) => {
        const cursor = cursorOf(currentInput) + 1;
        return {
          cursor,
          taskCount: taskCountOf(currentInput),
          escalate: cursor >= escalateAtTask,
        };
      };
    // The retry loop repeats fix attempts while `verify` still reports the task
    // as unfixed.
    if (ref === "taskUnfixed")
      return (childOutput) => verifyFixed(childOutput) === false;
    if (ref === "nextAttempt")
      return (_childOutput, currentInput) => ({
        attempt: attemptOf(currentInput) + 1,
      });
    throw new Error(`unknown loop fn ${ref}`);
  };
}

interface DemoPlan {
  readonly tasks: readonly PlanTask[];
}

// `convergeAtRound` decides the critic verdict: once the amendment round reaches
// the threshold the critic passes; otherwise it asks for another amendment. Each
// invocation bumps the agent's run count so a crash-resume test can see a step
// re-drive.
function makeInvokeStep(
  convergeAtRound: number,
  plan: DemoPlan,
  trace: DemoTrace,
): StepInvoker {
  return async ({ agent: a, input }) => {
    trace.agentRuns.set(a.id, agentRunsOf(trace, a.id) + 1);
    switch (a.id) {
      case "planner":
        return { output: { tasks: plan.tasks } };
      case "implementer":
        return { output: { done: true } };
      case "fixer":
        return { output: { reworked: true } };
      case "phase5-fixer":
        return { output: { reworked: true } };
      case "attributor":
        return { output: { attributed: true } };
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

interface DemoTrace {
  readonly levelsPicked: number[];
  readonly committedLevels: number[];
  readonly operatorDecisions: string[];
  // Per-agent invocation counts, so a crash-resume test can observe an agent
  // step re-driving (running again) on resume rather than replaying complete.
  readonly agentRuns: Map<string, number>;
}

function emptyTrace(): DemoTrace {
  return {
    levelsPicked: [],
    committedLevels: [],
    operatorDecisions: [],
    agentRuns: new Map(),
  };
}

function agentRunsOf(trace: DemoTrace, id: string): number {
  return trace.agentRuns.get(id) ?? 0;
}

interface EffectRuns {
  baseline: number;
  commit: number;
  build: number;
  rebuild: number;
}

function buildEnv(opts: {
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
  blobs: ReturnType<typeof createInMemoryBlobSubstrate>;
  effects: EffectLedger;
  convergeAtRound: number;
  cleanAtRound: number;
  fixAtAttempt: number;
  fixTaskCount: number;
  escalateAtTask: number;
  plan: DemoPlan;
  effectRuns: EffectRuns;
  trace: DemoTrace;
}): WorkflowRuntimeEnv {
  const {
    repoStore,
    blobs,
    effects,
    convergeAtRound,
    cleanAtRound,
    fixAtAttempt,
    fixTaskCount,
    escalateAtTask,
    plan,
    effectRuns,
    trace,
  } = opts;
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
    // Pure host transforms: the parse bridge and the per-level slice. Neither
    // touches an external effect, so neither runs through the EffectContext.
    if (handler === "parsePlan") {
      const tasks = tasksOf(input);
      return {
        output: { tasks, levelCount: distinctLevels(tasks).length },
      };
    }
    if (handler === "pickLevel") {
      const cursor = cursorOf(input);
      const tasks = tasksOf(input);
      const level = distinctLevels(tasks)[cursor];
      if (level === undefined) {
        throw new Error(
          `pickLevel: cursor ${String(cursor)} past the last level`,
        );
      }
      trace.levelsPicked.push(level);
      return {
        output: {
          cursor,
          level,
          tasksAtLevel: tasks.filter((t) => t.level === level),
        },
      };
    }
    // A deterministic stand-in for attribution naming the failing tasks: it
    // yields the task count the Phase-5 task loop walks. The attributor agent
    // does the modelled work; this action owns the loop-driving count so no
    // agent output feeds a loop.
    if (handler === "pickFixTasks") {
      return { output: { taskCount: fixTaskCount } };
    }
    // The clean-branch no-op the Phase-5 `cleanGate` routes to when the build
    // is already clean, and the escalation diamond join.
    if (handler === "noop") {
      return { output: { skipped: true } };
    }
    // A deterministic per-task verifier: the rework holds once the attempt
    // reaches `fixAtAttempt`.
    if (handler === "verify") {
      return { output: { fixed: attemptOf(input) >= fixAtAttempt } };
    }
    // Resolve an operator escalation: record the decision, and throw on "abort"
    // so the run fails (a "continue" falls through to the fix attempt).
    if (handler === "abortCheck") {
      const decision = decisionOf(input);
      trace.operatorDecisions.push(decision);
      if (decision === "abort") {
        throw new Error(
          "operator aborted the run at a verification escalation",
        );
      }
      return { output: { decision } };
    }
    // A level whose amendment loop exhausts routes here; throwing fails the
    // body run so the outer loop short-circuits the whole run.
    if (handler === "levelFailed") {
      throw new Error("level failed: amendment loop exhausted without passing");
    }
    // The Phase-5 exhaustion sinks fail the run: an escalation would merely
    // complete, so a throw is what enforces `failed` (matching the standalone).
    if (handler === "retryFailed") {
      throw new Error("retry loop exhausted without fixing the task");
    }
    if (handler === "tasksExhausted") {
      throw new Error(
        "task loop exhausted without walking every attributed task",
      );
    }
    if (handler === "phase5Failed") {
      throw new Error("phase5 verification cap reached without a clean build");
    }

    // The git-commit-shaped and build-gate effects run through the capability-
    // and ledger-checked EffectContext.
    const capability = requires[0];
    if (capability === undefined) {
      throw new Error(`action ${handler} declared no effect capability`);
    }
    const ctx = createEffectContext({
      authorize,
      effects,
      requires,
      authzContext,
      input,
    });
    const output = await ctx.perform({
      effectId: handler,
      capability,
      run: async () => {
        if (handler === "captureBaseline") {
          effectRuns.baseline += 1;
          return { handler, captured: true };
        }
        if (handler === "buildGate") {
          effectRuns.build += 1;
          return { handler, clean: roundOf(input) >= cleanAtRound };
        }
        if (handler === "rebuild") {
          effectRuns.rebuild += 1;
          return { handler, rebuilt: true };
        }
        effectRuns.commit += 1;
        trace.committedLevels.push(levelOf(input));
        return { handler, committed: true };
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
    invokeStep: makeInvokeStep(convergeAtRound, plan, trace),
    invokeAction,
    effects,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(fullDemo),
    loopFns: makeLoopFns(escalateAtTask),
  };
  const loopBodies = new Map(
    enumerateInlineLoopBodies(fullDemo).map((b) => [b.ref, b.definition]),
  );
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  return env;
}

function outcomeOf(loopOutput: unknown): unknown {
  if (
    typeof loopOutput === "object" &&
    loopOutput !== null &&
    "outcome" in loopOutput
  ) {
    return loopOutput.outcome;
  }
  throw new Error("loop output missing outcome");
}

function iterationsOf(loopOutput: unknown): unknown {
  if (
    typeof loopOutput === "object" &&
    loopOutput !== null &&
    "iterations" in loopOutput
  ) {
    return loopOutput.iterations;
  }
  throw new Error("loop output missing iterations");
}

function noEffects(): EffectRuns {
  return { baseline: 0, commit: 0, build: 0, rebuild: 0 };
}

// A per-task escalation threshold no run below reaches, so the operator
// escalation never fires and no `awaitSignal` parks in the scenarios that do not
// exercise it.
const NEVER_ESCALATE = Number.POSITIVE_INFINITY;

// The number of attributed tasks the Phase-5 task loop walks per dirty round.
const FIX_TASK_COUNT = 2;

const TWO_LEVEL_PLAN: DemoPlan = {
  tasks: [
    { id: "t1", level: 0 },
    { id: "t2", level: 0 },
    { id: "t3", level: 1 },
  ],
};

describe("dispatch orchestrator: outer per-level iteration", () => {
  test("walks every level in order and consolidates", async () => {
    const effectRuns = noEffects();
    const trace = emptyTrace();
    const env = buildEnv({
      repoStore: createInMemoryRepoStore(),
      blobs: createInMemoryBlobSubstrate(),
      effects: inMemoryLedger(),
      convergeAtRound: 2,
      // Phase 5's first build is clean, so verification converges immediately
      // and the run reaches consolidate.
      cleanAtRound: 1,
      fixAtAttempt: 0,
      fixTaskCount: FIX_TASK_COUNT,
      escalateAtTask: NEVER_ESCALATE,
      plan: TWO_LEVEL_PLAN,
      effectRuns,
      trace,
    });
    const result = await runtimeRun(fullDemo, env).complete;

    expect(result.terminalStatus).toBe("completed");
    // The outer loop ran once per level, in ascending order, and converged.
    expect(outcomeOf(result.outputs.perLevelLoop)).toBe("converged");
    expect(iterationsOf(result.outputs.perLevelLoop)).toBe(2);
    expect(trace.levelsPicked).toEqual([0, 1]);
    // The baseline ran once; commit ran once per level, on the level's own
    // tasks, in order.
    expect(effectRuns.baseline).toBe(1);
    expect(effectRuns.commit).toBe(2);
    expect(trace.committedLevels).toEqual([0, 1]);
    // The run converged, so consolidate ran and escalation was pruned.
    expect(result.outputs.consolidate).toEqual({ consolidated: true });
    expect("escalate" in result.outputs).toBe(false);
  });

  test("a failed level short-circuits before any later level runs", async () => {
    const effectRuns = noEffects();
    const trace = emptyTrace();
    const env = buildEnv({
      repoStore: createInMemoryRepoStore(),
      blobs: createInMemoryBlobSubstrate(),
      effects: inMemoryLedger(),
      // The critic never passes, so level 0's amendment loop exhausts and the
      // level fails.
      convergeAtRound: 99,
      cleanAtRound: 1,
      fixAtAttempt: 0,
      fixTaskCount: FIX_TASK_COUNT,
      escalateAtTask: NEVER_ESCALATE,
      plan: TWO_LEVEL_PLAN,
      effectRuns,
      trace,
    });
    const result = await runtimeRun(fullDemo, env).complete;

    // The run failed on level 0, and level 1 never started -- the per-level
    // short-circuit. (Phase 5 is the loop's normal successor; a throwing
    // iteration does not prune it, so a stray verification build may begin
    // before the failed run settles. The terminal status is what matters here.)
    expect(result.terminalStatus).toBe("failed");
    expect(trace.levelsPicked).toEqual([0]);
    expect(trace.committedLevels).toEqual([0]);
  });
});

const ONE_LEVEL_PLAN: DemoPlan = { tasks: [{ id: "t1", level: 0 }] };

// Env for the signal-free Phase-5 scenarios: the task loop never escalates, so
// no `awaitSignal` parks. The operator-escalation scenarios set the threshold.
function phase5Env(opts: {
  cleanAtRound: number;
  fixAtAttempt: number;
  effectRuns: EffectRuns;
  trace: DemoTrace;
}): WorkflowRuntimeEnv {
  return buildEnv({
    repoStore: createInMemoryRepoStore(),
    blobs: createInMemoryBlobSubstrate(),
    effects: inMemoryLedger(),
    // The single level's amendment loop converges at round 2, so the run
    // reaches Phase 5 in every scenario below.
    convergeAtRound: 2,
    cleanAtRound: opts.cleanAtRound,
    fixAtAttempt: opts.fixAtAttempt,
    fixTaskCount: FIX_TASK_COUNT,
    escalateAtTask: NEVER_ESCALATE,
    plan: ONE_LEVEL_PLAN,
    effectRuns: opts.effectRuns,
    trace: opts.trace,
  });
}

describe("dispatch orchestrator: phase-5 verification loop", () => {
  test("converges on a clean first build without fixing or rebuilding", async () => {
    const effectRuns = noEffects();
    const trace = emptyTrace();
    const env = phase5Env({
      cleanAtRound: 1,
      fixAtAttempt: 0,
      effectRuns,
      trace,
    });
    const result = await runtimeRun(fullDemo, env).complete;

    expect(result.terminalStatus).toBe("completed");
    // Phase 5 ran one round; the clean build pruned the fix path, so no rebuild
    // and no fixer.
    expect(outcomeOf(result.outputs.phase5)).toBe("converged");
    expect(iterationsOf(result.outputs.phase5)).toBe(1);
    expect(effectRuns.build).toBe(1);
    expect(effectRuns.rebuild).toBe(0);
    expect(agentRunsOf(trace, "phase5-fixer")).toBe(0);
    expect(result.outputs.consolidate).toEqual({ consolidated: true });
  });

  test("walks the attributed tasks, retrying each, and re-gates next round", async () => {
    const effectRuns = noEffects();
    const trace = emptyTrace();
    // Round 1's build is dirty; each of the two attributed tasks needs a second
    // fix attempt; round 2's build is clean.
    const env = phase5Env({
      cleanAtRound: 2,
      fixAtAttempt: 1,
      effectRuns,
      trace,
    });
    const result = await runtimeRun(fullDemo, env).complete;

    expect(result.terminalStatus).toBe("completed");
    // Two rounds: round 1 builds dirty, walks the task loop, and rebuilds; round
    // 2 builds clean and converges. Build ran per round; rebuild ran once.
    expect(outcomeOf(result.outputs.phase5)).toBe("converged");
    expect(iterationsOf(result.outputs.phase5)).toBe(2);
    expect(effectRuns.build).toBe(2);
    expect(effectRuns.rebuild).toBe(1);
    // The depth-3 nesting genuinely iterated: the task loop walked both tasks and
    // each task's retry loop ran two attempts, so the fixer ran 2 * 2 = 4 times.
    expect(agentRunsOf(trace, "phase5-fixer")).toBe(FIX_TASK_COUNT * 2);
    expect(result.outputs.consolidate).toEqual({ consolidated: true });
  });

  test("fails the run when the build never goes clean (verification cap)", async () => {
    const effectRuns = noEffects();
    const trace = emptyTrace();
    // The build never goes clean, but every round's tasks fix, so Phase 5
    // rebuilds each round until the outer cap fires.
    const env = phase5Env({
      cleanAtRound: 99,
      fixAtAttempt: 0,
      effectRuns,
      trace,
    });
    const result = await runtimeRun(fullDemo, env).complete;

    // The cap (maxIterations: 5) fired without a clean build; phase5Failed threw.
    expect(result.terminalStatus).toBe("failed");
    expect(effectRuns.build).toBe(5);
    expect(effectRuns.rebuild).toBe(5);
  });

  test("fails the run when a task cannot be fixed (retry-loop cap)", async () => {
    const effectRuns = noEffects();
    const trace = emptyTrace();
    // Round 1's build is dirty and the fixer never recovers, so the first task's
    // retry loop exhausts and retryFailed throws, failing the run.
    const env = phase5Env({
      cleanAtRound: 99,
      fixAtAttempt: 99,
      effectRuns,
      trace,
    });
    const result = await runtimeRun(fullDemo, env).complete;

    // The run failed in round 1's task loop, which built exactly once. (The
    // inner retry exhaustion fails the task body, so the task loop THROWS rather
    // than routing; a throwing loop does not prune its `rebuild` successor, so a
    // stray rebuild may begin before the failed run settles. The terminal status
    // and the single build are what matter here.)
    expect(result.terminalStatus).toBe("failed");
    expect(effectRuns.build).toBe(1);
  });
});

// Env for the operator-escalation scenarios: the task loop escalates once its
// cursor reaches `escalateAtTask`, parking that task on the `operator` signal.
function escalationEnv(opts: {
  cleanAtRound: number;
  fixAtAttempt: number;
  escalateAtTask: number;
  effectRuns: EffectRuns;
  trace: DemoTrace;
}): WorkflowRuntimeEnv {
  return buildEnv({
    repoStore: createInMemoryRepoStore(),
    blobs: createInMemoryBlobSubstrate(),
    effects: inMemoryLedger(),
    convergeAtRound: 2,
    cleanAtRound: opts.cleanAtRound,
    fixAtAttempt: opts.fixAtAttempt,
    fixTaskCount: FIX_TASK_COUNT,
    escalateAtTask: opts.escalateAtTask,
    plan: ONE_LEVEL_PLAN,
    effectRuns: opts.effectRuns,
    trace: opts.trace,
  });
}

describe("dispatch orchestrator: phase-5 operator escalation", () => {
  test("resumes the task when the operator answers continue", async () => {
    const effectRuns = noEffects();
    const trace = emptyTrace();
    // Round 1 builds dirty and the task loop walks both tasks; the second task
    // escalates to the operator before its retry loop.
    const env = escalationEnv({
      cleanAtRound: 2,
      fixAtAttempt: 0,
      escalateAtTask: 1,
      effectRuns,
      trace,
    });
    const run = runtimeRun(fullDemo, env);
    // The channel queues the delivery until the relayed park subscribes, so
    // delivering up front is safe.
    await run.signal("operator", { decision: "continue" }, "op-1");
    const result = await run.complete;

    expect(result.terminalStatus).toBe("completed");
    // The escalation fired once and the operator's "continue" let the task's
    // retry loop run; Phase 5 then converged on the next round's clean build.
    expect(trace.operatorDecisions).toEqual(["continue"]);
    expect(outcomeOf(result.outputs.phase5)).toBe("converged");
    expect(iterationsOf(result.outputs.phase5)).toBe(2);
    expect(result.outputs.consolidate).toEqual({ consolidated: true });
  });

  test("fails the run when the operator answers abort", async () => {
    const effectRuns = noEffects();
    const trace = emptyTrace();
    // The second task escalates and the operator aborts.
    const env = escalationEnv({
      cleanAtRound: 99,
      fixAtAttempt: 0,
      escalateAtTask: 1,
      effectRuns,
      trace,
    });
    const run = runtimeRun(fullDemo, env);
    await run.signal("operator", { decision: "abort" }, "op-1");
    const result = await run.complete;

    // The operator abort threw at abortCheck, failing the run mid-task.
    expect(result.terminalStatus).toBe("failed");
    expect(trace.operatorDecisions).toEqual(["abort"]);
  });
});

// A dirty-then-clean Phase-5 env sharing a caller-provided repo store, blob
// substrate, effect ledger, and trace, so a crash-and-resume pair can be driven
// against one durable substrate while observing agent re-drives. Round 1 builds
// dirty, walks the task loop (no escalation), and rebuilds; round 2 builds clean
// and converges.
function verificationEnv(opts: {
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
  blobs: ReturnType<typeof createInMemoryBlobSubstrate>;
  effects: EffectLedger;
  effectRuns: EffectRuns;
  trace: DemoTrace;
}): WorkflowRuntimeEnv {
  return buildEnv({
    repoStore: opts.repoStore,
    blobs: opts.blobs,
    effects: opts.effects,
    convergeAtRound: 2,
    cleanAtRound: 2,
    fixAtAttempt: 0,
    fixTaskCount: FIX_TASK_COUNT,
    escalateAtTask: NEVER_ESCALATE,
    plan: ONE_LEVEL_PLAN,
    effectRuns: opts.effectRuns,
    trace: opts.trace,
  });
}

// Truncate a run's durable log right after the given loop iteration's body was
// spawned, so a resume re-drives that iteration's body from scratch (from an
// empty child log).
function trimAtLoopSpawn(
  events: readonly WorkflowEvent[],
  runId: string,
  loopId: string,
  index: number,
): WorkflowEvent[] {
  const childRunId = loopBodyRunId(runId, loopId, index);
  const trimmed: WorkflowEvent[] = [];
  for (const e of events) {
    trimmed.push(e);
    if (e.kind === "ChildSpawned" && e.childRunId === childRunId)
      return trimmed;
  }
  throw new Error(
    `no ChildSpawned for loop ${loopId} iteration ${String(index)}`,
  );
}

// Truncate a run's durable log right before the given top-level step started, so
// a resume re-drives that step (and everything after it) fresh while every
// earlier step replays complete.
function trimBeforeStep(
  events: readonly WorkflowEvent[],
  stepId: string,
): WorkflowEvent[] {
  const trimmed: WorkflowEvent[] = [];
  for (const e of events) {
    if (e.kind === "StepStarted" && e.stepId === stepId) return trimmed;
    trimmed.push(e);
  }
  throw new Error(`no StepStarted for step ${stepId}`);
}

describe("dispatch orchestrator: crash-safe exactly-once effects", () => {
  test("holds exactly-once Phase-5 effects across a crash inside the loop body", async () => {
    const blobs = createInMemoryBlobSubstrate();
    const effects = inMemoryLedger();
    const effectRuns = noEffects();
    const trace = emptyTrace();
    const result1 = await runtimeRun(
      fullDemo,
      verificationEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        effectRuns,
        trace,
      }),
    ).complete;
    expect(result1.terminalStatus).toBe("completed");
    // Round 1 built dirty and rebuilt; round 2 built clean.
    expect(effectRuns.build).toBe(2);
    expect(effectRuns.rebuild).toBe(1);
    const buildAfterRun1 = effectRuns.build;
    const rebuildAfterRun1 = effectRuns.rebuild;
    const phase5FixerAfterRun1 = agentRunsOf(trace, "phase5-fixer");

    // Crash right after the first Phase-5 iteration's body was spawned, then
    // resume against a fresh repo store but the SAME blobs and effect ledger.
    const trimmed = trimAtLoopSpawn(result1.events, result1.runId, "phase5", 0);
    const result2 = await runtimeRun(
      fullDemo,
      verificationEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        effectRuns,
        trace,
      }),
      { runId: result1.runId, resumeFromEvents: trimmed },
    ).complete;

    expect(result2.terminalStatus).toBe("completed");
    // The resumed run re-drives the Phase-5 iteration bodies -- including the
    // nested task and retry loops -- from an empty child log, so the fixer agent
    // runs again; but the build and rebuild effects dedup against the shared
    // ledger, so neither effect handler runs a second time.
    expect(agentRunsOf(trace, "phase5-fixer")).toBeGreaterThan(
      phase5FixerAfterRun1,
    );
    expect(effectRuns.build).toBe(buildAfterRun1);
    expect(effectRuns.rebuild).toBe(rebuildAfterRun1);
  });

  test("re-runs the Phase-5 effects across the same crash without ledger dedup", async () => {
    const blobs = createInMemoryBlobSubstrate();
    // A ledger that never dedups, proving the exactly-once test above is not
    // vacuous: without the ledger, the re-driven iterations re-execute.
    const defeatedLedger: EffectLedger = {
      async lookup() {
        return undefined;
      },
      async record() {
        // Intentionally durable-nothing.
      },
    };
    const effectRuns = noEffects();
    const trace = emptyTrace();
    const result1 = await runtimeRun(
      fullDemo,
      verificationEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects: defeatedLedger,
        effectRuns,
        trace,
      }),
    ).complete;
    expect(result1.terminalStatus).toBe("completed");
    expect(effectRuns.build).toBe(2);
    expect(effectRuns.rebuild).toBe(1);

    const trimmed = trimAtLoopSpawn(result1.events, result1.runId, "phase5", 0);
    const result2 = await runtimeRun(
      fullDemo,
      verificationEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects: defeatedLedger,
        effectRuns,
        trace,
      }),
      { runId: result1.runId, resumeFromEvents: trimmed },
    ).complete;

    expect(result2.terminalStatus).toBe("completed");
    // Both Phase-5 iterations re-drove: rounds 1 and 2 each re-ran their build
    // effect (+2), and round 1 re-ran its rebuild effect (+1), because nothing
    // dedups them.
    expect(effectRuns.build).toBe(4);
    expect(effectRuns.rebuild).toBe(2);
  });
});

// A cleanly-completing dispatch-orchestrator env sharing a caller-provided durable
// substrate, so a crash-and-resume pair can be driven across it. The single
// level's amendment loop converges at round 1 and Phase 5's first build is
// clean, so the run reaches consolidate without any fixing.
function completingEnv(opts: {
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
  blobs: ReturnType<typeof createInMemoryBlobSubstrate>;
  effects: EffectLedger;
  effectRuns: EffectRuns;
  trace: DemoTrace;
}): WorkflowRuntimeEnv {
  return buildEnv({
    repoStore: opts.repoStore,
    blobs: opts.blobs,
    effects: opts.effects,
    convergeAtRound: 1,
    cleanAtRound: 1,
    fixAtAttempt: 0,
    fixTaskCount: FIX_TASK_COUNT,
    escalateAtTask: NEVER_ESCALATE,
    plan: ONE_LEVEL_PLAN,
    effectRuns: opts.effectRuns,
    trace: opts.trace,
  });
}

// The interchange-demo-dispatch orchestrator carries seven bespoke on-disk
// resume detectors (its resume/case-*.ts) that reconstruct interrupted state
// from git and the filesystem, because the hand-rolled orchestrator keeps no
// durable journal. This engine-authored demo keeps one, so those detectors are
// unnecessary: a crash at each of their scenarios is recovered by generic
// journal resume. The mapping from each host case to the engine mechanism that
// subsumes it, and the test here (or above) that demonstrates that mechanism:
//
//   case-1  mid-task implementer (reset, re-spawn)   -> re-drive an incomplete
//           agent step on resume            [test: re-drives an incomplete ...]
//   case-2  submitOutput / state-write race          -> the atomic journal
//           commit removes the race: a journaled completion is kept, an
//           un-journaled one re-drives    [tests: both below]
//   case-3  fix-agent crashed dirty (reset, re-run)  -> re-drive an incomplete
//           agent step on resume; same mechanism as case-1  [test: re-drives ...]
//   case-4  mid-rebuild, do not redo committed work  -> effect exactly-once
//           across a loop-body re-drive   [test: the crash-safe block above]
//   case-5  commit / boundary-write                  -> the journal IS the
//           boundary record; a re-driven commit effect dedups against the
//           ledger  [test: re-enters plan, asserting commit stays exactly-once]
//   case-6  pre-plan                                 -> re-enter from a pre-work
//           crash                          [test: re-enters plan ...]
//   case-7  mid-Phase-5 fix loop (orphan build log)  -> effect exactly-once
//           across a loop-body re-drive; the orphan-log cleanup is moot
//                                          [test: the crash-safe block above]
//
// Cases 3/4/5 are git-working-tree and commit-reachability reconstructions in
// the host; under a journal they are moot, because the journal -- not the
// working tree -- is the source of truth.
describe("dispatch orchestrator: the seven resume cases via engine resume", () => {
  test("re-enters plan from a pre-work crash while keeping completed effects (cases 2, 5, 6)", async () => {
    const blobs = createInMemoryBlobSubstrate();
    const effects = inMemoryLedger();
    const effectRuns = noEffects();
    const trace = emptyTrace();
    const result1 = await runtimeRun(
      fullDemo,
      completingEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        effectRuns,
        trace,
      }),
    ).complete;
    expect(result1.terminalStatus).toBe("completed");
    const plannerRuns1 = agentRunsOf(trace, "planner");
    expect(plannerRuns1).toBe(1);

    // Crash before `plan` ever started (case-6's "planning, no tasks" state).
    // On resume `plan` re-drives. `captureBaseline` is kept by step-replay (its
    // StepCompleted is journaled in the seed); the downstream `commit`/`build`
    // effects are re-driven but dedup against the shared ledger. Either way no
    // handler runs a second time.
    const trimmed = trimBeforeStep(result1.events, "plan");
    const result2 = await runtimeRun(
      fullDemo,
      completingEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        effectRuns,
        trace,
      }),
      { runId: result1.runId, resumeFromEvents: trimmed },
    ).complete;

    expect(result2.terminalStatus).toBe("completed");
    // `plan` re-drove (the planner ran a second time) -- the engine re-enters
    // the pre-work state without a bespoke detector.
    expect(agentRunsOf(trace, "planner")).toBe(plannerRuns1 + 1);
    // Every effect that had completed before the crash stays exactly-once: the
    // journaled completions are kept, not re-executed (cases 2, 5).
    expect(effectRuns.baseline).toBe(1);
    expect(effectRuns.commit).toBe(1);
    expect(effectRuns.build).toBe(1);
  });

  test("re-drives an incomplete agent step inside a loop body on resume (cases 1, 3)", async () => {
    const blobs = createInMemoryBlobSubstrate();
    const effects = inMemoryLedger();
    const effectRuns = noEffects();
    const trace = emptyTrace();
    const result1 = await runtimeRun(
      fullDemo,
      completingEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        effectRuns,
        trace,
      }),
    ).complete;
    expect(result1.terminalStatus).toBe("completed");
    const plannerRuns1 = agentRunsOf(trace, "planner");
    const implementerRuns1 = agentRunsOf(trace, "implementer");
    expect(implementerRuns1).toBe(1);

    // Crash with the per-level body in flight (case-1/3: an implementer/fixer
    // that never durably completed). The per-level body re-drives from an empty
    // child log, re-running its agent steps -- the engine's equivalent of the
    // host resetting the task to `pending` and re-spawning.
    const trimmed = trimAtLoopSpawn(
      result1.events,
      result1.runId,
      "perLevelLoop",
      0,
    );
    const result2 = await runtimeRun(
      fullDemo,
      completingEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        effectRuns,
        trace,
      }),
      { runId: result1.runId, resumeFromEvents: trimmed },
    ).complete;

    expect(result2.terminalStatus).toBe("completed");
    // `plan` completed before the crash, so it replays and the planner does NOT
    // run again; the in-flight per-level body's implementer DOES re-run.
    expect(agentRunsOf(trace, "planner")).toBe(plannerRuns1);
    expect(agentRunsOf(trace, "implementer")).toBeGreaterThan(implementerRuns1);
    // The level's commit effect dedups against the shared ledger, so the
    // re-driven body does not double-commit.
    expect(effectRuns.commit).toBe(1);
  });
});
