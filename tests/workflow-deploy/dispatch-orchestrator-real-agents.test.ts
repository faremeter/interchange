// The interchange-demo dispatch orchestrator on the extended workflow engine,
// driven by REAL @intx/agent agents through the production step-invoker seam.
//
// `tests/workflow/dispatch-orchestrator.test.ts` proves the full composition -- the
// outer per-level loop, the Phase-5 verification loop, crash-resume exactly-once
// and the seven resume cases -- with a stubbed `invokeStep`. This test proves
// the SAME composition runs end-to-end with real agents: every agent step is a
// genuine `defineAgent` reactor with an arktype-validated terminal tool, built
// via `createAgent` and driven by deterministic mock inference from
// `@intx/inference-testing`, wired through the production
// `createWorkflowStepInvoker` into an in-process `runtimeRun(definition, env)` --
// the same in-process harness the per-level real-agent sibling uses, NOT the
// deploy hub/sidecar/subprocess harness.
//
// The authored workflow mirrors the shape test: captureBaseline -> plan ->
// parsePlan -> perLevelLoop(pickLevel -> implement map -> commit -> critique ->
// amend loop) -> phase5(build -> cleanGate -> attribution -> task loop, each
// task retried by its own retry loop -> rebuild) -> consolidate. The Phase-5 fix
// nesting is depth-3: phase5 -> taskLoop -> retryLoop. As in the shape test,
// every loop convergence decider keys off a DETERMINISTIC host action, never an
// agent: `buildDirty` reads the `build` action's `clean`, `moreTasks` reads a
// carried cursor, the retry loop's `taskUnfixed` reads the `verify` action's
// `fixed`, and only the amendment loop's `critic` verdict -- a real agent output
// lifted through `extractAgentPayload` -- drives routing. Keeping agent output
// out of every loop `while`/`carry` is what keeps the real-agent composition
// deterministic; a real agent's reply cannot destabilize a loop.
//
// Two things the shape test cannot prove are deliberately scoped out here and
// covered elsewhere. The operator escalation (an `awaitSignal` park relayed up
// through the fix and Phase-5 loops) is pure engine mechanics with no inference
// in it, and both its continue and abort branches are driven with a real
// `run.signal(...)` in the shape test; the one agent it gates (the Phase-5
// fixer) is exercised below. The seven resume cases are mapped and tested in the
// shape test. The plan is single-level on purpose: the amendment loop's `round`
// resets to 1 each level, and the gate-critic matcher keys on `round` alone, so
// a multi-level plan would serve a level's round-1 verdict to another level.
//
// Scenarios:
//   1. Clean-first Phase 5: the first build is clean, so the fix path is
//      pruned and Phase 5 converges without running its attributor/fixer.
//   2. Dirty-then-clean Phase 5: round 1 builds dirty, so the real attributor
//      and Phase-5 fixer run, rebuild commits, and round 2 builds clean.
//   3. Crash-resume exactly-once through Phase 5: a crash at the Phase-5 loop's
//      first ChildSpawned on the dirty path re-drives the Phase-5 body -- its
//      attributor and fixer re-run through the real step-invoker -- while the
//      shared ledger holds build/rebuild to one execution. A defeated-ledger
//      probe proves the assertion non-vacuous.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { type } from "arktype";

import {
  createAgent,
  createDefaultDirectorRegistry,
  defineAgent,
  defineTool,
  type AgentDefinition,
  type BaseEnv,
  type ToolBundle,
} from "@intx/agent";
import { noopAuditStore } from "@intx/agent/testing";
import { setupHarness, wire, type Harness } from "@intx/inference-testing";
import { createIsogitStore } from "@intx/storage-isogit/node";
import { createWorkflowStepInvoker } from "@intx/workflow-host";
import type {
  InferenceSource,
  ToolCall,
  ToolResult,
} from "@intx/types/runtime";
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
  type RunResult,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

// The amendment and fix loop caps, and the Phase-5 verification cap. `loop()`
// rejects a non-positive/non-integer bound, so each loop carries a positive
// integer cap; hitting a cap is what routes to a throwing failure sink.
const AMENDMENT_CAP = 3;
const RETRY_CAP = 3;
const TASK_CAP = 5;
const VERIFICATION_CAP = 5;

// The number of attributed tasks the Phase-5 task loop walks per dirty round.
const FIX_TASK_COUNT = 2;

// A single level with two tasks, so the per-level `implement` map fans two
// DISTINCT inner agents -- one per task -- proving per-task payload fidelity.
const TASK_IDS = ["t1", "t2"] as const;
const PLAN_TASKS = TASK_IDS.map((id) => ({ id, level: 0 }));

const SOURCE: InferenceSource = {
  id: "anthropic:full-demo",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-full-demo",
  model: "claude-full-demo",
};

const PLAN_TOOL = "emit_plan";
const IMPLEMENT_TOOL = "emit_implementation";
const VERDICT_TOOL = "emit_verdict";
const FIX_TOOL = "emit_fix";
const ATTRIBUTE_TOOL = "emit_attribution";
const PHASE5_FIX_TOOL = "emit_phase5_fix";
const CONSOLIDATE_TOOL = "emit_consolidation";

const PlanArgs = type({
  tasks: type({ id: "string", level: "number" }).array(),
});
const ImplementArgs = type({ taskId: "string", done: "boolean" });
const VerdictArgs = type({ verdict: "'amend' | 'pass'", round: "number" });
const FixArgs = type({ reworked: "boolean" });
const AttributeArgs = type({ attributed: "boolean" });
const ConsolidateArgs = type({ consolidated: "boolean" });

type ArkSchema = (data: unknown) => unknown;

// A terminal tool whose `run` validates the model's arguments with the role's
// arktype schema and returns them verbatim as the structured tool result.
// Invalid arguments surface loudly rather than passing through silently.
function terminalTool(id: string, name: string, schema: ArkSchema) {
  return defineTool<BaseEnv>({
    id,
    definitions: [{ name }],
    factory: (): ToolBundle => ({
      definitions: [
        {
          name,
          description: `emit the ${name} structured output`,
          inputSchema: { type: "object", properties: {} },
        },
      ],
      async run(call: ToolCall, _signal: AbortSignal): Promise<ToolResult> {
        const validated = schema(call.arguments);
        if (validated instanceof type.errors) {
          return {
            callId: call.id,
            content: `invalid ${name} arguments: ${validated.summary}`,
            isError: true,
          };
        }
        return {
          callId: call.id,
          content: JSON.stringify(call.arguments),
          isError: false,
        };
      },
    }),
  });
}

function roleAgent(
  id: string,
  toolName: string,
  schema: ArkSchema,
): AgentDefinition<BaseEnv> {
  return defineAgent({
    id,
    systemPrompt: `you are the ${id} for the full-demo dispatch pipeline`,
    tools: [terminalTool(`@intx-test/full-demo/${toolName}`, toolName, schema)],
    capabilities: [],
    inference: {
      sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
    },
  });
}

const plannerAgent = roleAgent("planner", PLAN_TOOL, PlanArgs);
const implementerAgent = roleAgent(
  "implementer",
  IMPLEMENT_TOOL,
  ImplementArgs,
);
// The verdict-emitting critic drives the amendment loop; distinct from the
// Phase-5 attributor/fixer below.
const criticAgent = roleAgent("critic", VERDICT_TOOL, VerdictArgs);
const fixerAgent = roleAgent("fixer", FIX_TOOL, FixArgs);
// Phase-5's agents carry DISTINCT role markers so their scripted turns never
// collide with the amendment fixer's (which keys on role + taskId, with no
// round/attempt discriminator).
const attributorAgent = roleAgent("attributor", ATTRIBUTE_TOOL, AttributeArgs);
const phase5FixerAgent = roleAgent("phase5-fixer", PHASE5_FIX_TOOL, FixArgs);
const consolidatorAgent = roleAgent(
  "consolidator",
  CONSOLIDATE_TOOL,
  ConsolidateArgs,
);

// The amendment loop body: a fixer reworks, then the critic re-judges. The
// loop's `shouldAmend` reads the verdict off the body's `critic` child-output
// key, lifted through `extractAgentPayload`.
const amendBody = defineWorkflow({
  id: "amend-body",
  trigger: { type: "manual" },
  steps: {
    fix: step({ agent: fixerAgent, input: { from: "trigger.payload" } }),
    critic: step({ agent: criticAgent, input: { from: "trigger.payload" } }),
  },
});

// One fix attempt (the innermost, retry level): the phase5-fixer reworks the
// task and the deterministic `verify` action reports whether the rework holds.
const fixBody = defineWorkflow({
  id: "fix-body",
  trigger: { type: "manual" },
  steps: {
    fixAttempt: step({
      agent: phase5FixerAgent,
      input: { from: "trigger.payload" },
    }),
    verify: action({
      handler: "verify",
      input: { from: "trigger.payload" },
      after: ["fixAttempt"],
    }),
  },
});

// One attributed task's fix (the per-task level): a nested `retryLoop` retries
// the fix until `verify` passes, failing the run via `retryFailed` if it cannot.
// Signal-free -- the operator escalation is covered by the shape test -- so a
// task is just its retry loop.
const taskBody = defineWorkflow({
  id: "task-body",
  trigger: { type: "manual" },
  steps: {
    retryLoop: loop({
      body: fixBody,
      while: "taskUnfixed",
      carry: "nextAttempt",
      input: { literal: { attempt: 0 } },
      maxIterations: RETRY_CAP,
      onExhausted: "retryFailed",
    }),
    retryFailed: action({ handler: "retryFailed", after: ["retryLoop"] }),
  },
});

// One Phase-5 verification round. `build` is a deterministic effect keyed on the
// round; its `clean` drives both the in-round `cleanGate` and the loop's
// `buildDirty` while. A dirty build attributes the failures, then `taskLoop`
// walks the attributed tasks -- each retried by its own nested `retryLoop`, so
// the fix nesting is phase5 -> taskLoop -> retryLoop -- and rebuilds; the next
// round's build is the re-gate. An exhausted task loop throws via
// `tasksExhausted`.
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
    attribution: step({
      agent: attributorAgent,
      input: { from: "trigger.payload" },
      after: ["cleanGate"],
    }),
    pickFixTasks: action({ handler: "pickFixTasks", after: ["attribution"] }),
    taskLoop: loop({
      body: taskBody,
      while: "moreTasks",
      carry: "nextTask",
      input: {
        merge: [
          { literal: { cursor: 0 } },
          { from: "steps.pickFixTasks.output" },
        ],
      },
      maxIterations: TASK_CAP,
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
      // Key the rebuild on the deterministic round payload, NEVER on the
      // fixer's agent output: the ledger dedups by hash(runId, stepId,
      // effectId, input), and a re-driven agent's reply is not stable across a
      // crash-resume, so keying the effect on it would silently defeat dedup.
      input: { from: "trigger.payload" },
      after: ["taskLoop"],
    }),
  },
});

// The per-level pipeline, authored as the outer loop's body. `pickLevel` slices
// the plan tasks down to the cursor's level (a host action, because a selector
// cannot index by a runtime cursor). A level whose amendment loop exhausts
// throws via `levelFailed`, short-circuiting the whole run.
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
        agent: implementerAgent,
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
      agent: criticAgent,
      input: { literal: { round: 1 } },
      after: ["commit"],
    }),
    amend: loop({
      body: amendBody,
      while: "shouldAmend",
      carry: "nextRound",
      input: { literal: { round: 1 } },
      maxIterations: AMENDMENT_CAP,
      onExhausted: "levelFailed",
      after: ["critique"],
    }),
    levelFailed: action({ handler: "levelFailed", after: ["amend"] }),
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
    plan: step({ agent: plannerAgent, after: ["captureBaseline"] }),
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
    // Per-level exhaustion (more levels than the cap) escalates to the operator,
    // mirroring the shape test and the sibling per-level real-agent pipeline.
    escalate: escalation({ to: "operator", after: ["perLevelLoop"] }),
    phase5: loop({
      body: phase5Body,
      while: "buildDirty",
      carry: "nextVerifyRound",
      input: { literal: { round: 1 } },
      maxIterations: VERIFICATION_CAP,
      onExhausted: "phase5Failed",
      after: ["perLevelLoop"],
    }),
    phase5Failed: action({ handler: "phase5Failed", after: ["phase5"] }),
    consolidate: step({ agent: consolidatorAgent, after: ["phase5"] }),
  },
});

// --- Faithful host-side extraction of a real agent step's structured output ---
// Every real agent step output is the step-invoker's `{ reply, turn }` envelope;
// the terminal-tool call arguments do NOT survive on `turn`, so the reply string
// is the only structured surface. This is the one extraction mechanism; the
// parse-action, the loop's `verdictOf`, and every agent assertion go through it.
// It fails loudly on any malformed shape rather than defaulting.
function extractAgentPayload(stepOutput: unknown): Record<string, unknown> {
  if (
    typeof stepOutput !== "object" ||
    stepOutput === null ||
    !("reply" in stepOutput)
  ) {
    throw new Error(
      `agent step output is not a { reply } envelope: ${JSON.stringify(stepOutput)}`,
    );
  }
  const reply = stepOutput.reply;
  if (typeof reply !== "string") {
    throw new Error(
      `agent step reply is not a string: ${JSON.stringify(stepOutput)}`,
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(reply);
  } catch (cause) {
    throw new Error(`agent step reply is not JSON: ${JSON.stringify(reply)}`, {
      cause,
    });
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`agent step reply is not a JSON object: ${reply}`);
  }
  const record: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(parsed)) {
    record[k] = v;
  }
  return record;
}

interface PlanTask {
  readonly id: string;
  readonly level: number;
}

// Lift the planner's `{ tasks }` (with levels) out of its envelope, validating
// the shape. The `level` field is load-bearing: `pickLevel` slices on it.
function extractPlanTasks(stepOutput: unknown): PlanTask[] {
  const payload = extractAgentPayload(stepOutput);
  const tasks = payload.tasks;
  if (!Array.isArray(tasks) || tasks.length === 0) {
    throw new Error(
      `planner output has no non-empty tasks array: ${JSON.stringify(payload)}`,
    );
  }
  return tasks.map((task: unknown) => {
    if (
      typeof task === "object" &&
      task !== null &&
      "id" in task &&
      "level" in task
    ) {
      const id = task.id;
      const level = task.level;
      if (typeof id === "string" && typeof level === "number") {
        return { id, level };
      }
    }
    throw new Error(
      `planner task is not a { id, level }: ${JSON.stringify(task)}`,
    );
  });
}

function roundOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "round" in value) {
    const round = value.round;
    if (typeof round === "number") return round;
  }
  throw new Error(`expected a numeric round: ${JSON.stringify(value)}`);
}

function cursorOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "cursor" in value) {
    const cursor = value.cursor;
    if (typeof cursor === "number") return cursor;
  }
  throw new Error(`expected a numeric cursor: ${JSON.stringify(value)}`);
}

function levelCountOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "levelCount" in value) {
    const levelCount = value.levelCount;
    if (typeof levelCount === "number") return levelCount;
  }
  throw new Error(`expected a numeric levelCount: ${JSON.stringify(value)}`);
}

function attemptOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "attempt" in value) {
    const attempt = value.attempt;
    if (typeof attempt === "number") return attempt;
  }
  throw new Error(`expected a numeric attempt: ${JSON.stringify(value)}`);
}

function tasksOf(value: unknown): PlanTask[] {
  if (typeof value === "object" && value !== null && "tasks" in value) {
    const tasks = (value as Record<string, unknown>).tasks;
    if (Array.isArray(tasks)) {
      return tasks.map((task: unknown) => {
        if (
          typeof task === "object" &&
          task !== null &&
          "id" in task &&
          "level" in task
        ) {
          const id = task.id;
          const level = task.level;
          if (typeof id === "string" && typeof level === "number") {
            return { id, level };
          }
        }
        throw new Error(
          `carry task is not a { id, level }: ${JSON.stringify(task)}`,
        );
      });
    }
  }
  throw new Error(`carry state has no tasks array: ${JSON.stringify(value)}`);
}

function distinctLevels(tasks: readonly PlanTask[]): number[] {
  const seen = new Set<number>();
  for (const task of tasks) seen.add(task.level);
  return Array.from(seen).sort((a, b) => a - b);
}

// The amendment loop's verdict lives at `childOutput.critic` -- the critic
// step's envelope. Throws on a missing/malformed verdict rather than defaulting.
function verdictOf(childOutput: unknown): string {
  if (
    typeof childOutput !== "object" ||
    childOutput === null ||
    !("critic" in childOutput)
  ) {
    throw new Error(
      `amend body child output has no critic step: ${JSON.stringify(childOutput)}`,
    );
  }
  const payload = extractAgentPayload(childOutput.critic);
  const verdict = payload.verdict;
  if (verdict !== "amend" && verdict !== "pass") {
    throw new Error(
      `critic verdict is not "amend" | "pass": ${JSON.stringify(payload)}`,
    );
  }
  return verdict;
}

// The Phase-5 deciders read DETERMINISTIC action outputs, never an agent's.
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
  throw new Error(
    `phase5 body output missing build.clean: ${JSON.stringify(childOutput)}`,
  );
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
  throw new Error(
    `fix body output missing verify.fixed: ${JSON.stringify(childOutput)}`,
  );
}

const loopFns = (ref: string): LoopFn => {
  if (ref === "shouldAmend")
    return (childOutput) => verdictOf(childOutput) === "amend";
  if (ref === "nextRound")
    return (_childOutput, currentInput) => ({
      round: roundOf(currentInput) + 1,
    });
  if (ref === "moreLevels")
    return (_childOutput, currentInput) =>
      cursorOf(currentInput) + 1 < levelCountOf(currentInput);
  if (ref === "advanceLevel")
    return (_childOutput, currentInput) => ({
      cursor: cursorOf(currentInput) + 1,
      tasks: tasksOf(currentInput),
      levelCount: levelCountOf(currentInput),
    });
  if (ref === "buildDirty")
    return (childOutput) => buildClean(childOutput) === false;
  if (ref === "nextVerifyRound")
    return (_childOutput, currentInput) => ({
      round: roundOf(currentInput) + 1,
    });
  // The task loop walks the attributed-task cursor (signal-free: no escalation).
  if (ref === "moreTasks")
    return (_childOutput, currentInput) =>
      cursorOf(currentInput) + 1 < taskCountOf(currentInput);
  if (ref === "nextTask")
    return (_childOutput, currentInput) => ({
      cursor: cursorOf(currentInput) + 1,
      taskCount: taskCountOf(currentInput),
    });
  // The retry loop repeats fix attempts while `verify` reports the task unfixed.
  if (ref === "taskUnfixed")
    return (childOutput) => verifyFixed(childOutput) === false;
  if (ref === "nextAttempt")
    return (_childOutput, currentInput) => ({
      attempt: attemptOf(currentInput) + 1,
    });
  throw new Error(`unknown loop fn ${ref}`);
};

function taskCountOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "taskCount" in value) {
    const taskCount = value.taskCount;
    if (typeof taskCount === "number") return taskCount;
  }
  throw new Error(`expected a numeric taskCount: ${JSON.stringify(value)}`);
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

function defeatedLedger(): EffectLedger {
  return {
    async lookup() {
      return undefined;
    },
    async record() {
      // Intentionally durable-nothing, so a re-driven effect re-executes.
    },
  };
}

const workflowAuthorize: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

// --- Mock-inference scripting ---------------------------------------------
// Each role is scripted as a two-turn matcher pair: a terminal-tool turn and a
// follow-up text turn, keyed on the role's system-prompt marker. Over-
// provisioning is safe (unused matchers never fire); a shortfall surfaces as an
// UnmatchedFetchError, or -- if a pool is exhausted -- as the bounded-drain
// guard below. Sized for the crash-resume re-drives too.
const POOL = 16;

function enqueueResponse(harness: Harness, chunks: Uint8Array[]) {
  const stream = harness.scenario.createStream();
  stream.enqueueAll(chunks, { startAt: harness.clock.now() + 1 });
  return stream;
}

// Register the two-turn matcher pair for a role with a fixed structured output.
function scriptFixedRole(
  harness: Harness,
  role: string,
  toolName: string,
  args: Record<string, unknown>,
): void {
  const marker = `you are the ${role} for`;
  const argsJSON = JSON.stringify(args);
  for (let i = 0; i < POOL; i += 1) {
    const turn1 = enqueueResponse(
      harness,
      wire.completeResponse("anthropic", {
        toolCalls: [
          { callId: `call-${role}-${String(i)}`, name: toolName, argsJSON },
        ],
      }),
    );
    harness.scenario.whenRequestBodyMatches(
      (body) => body.includes(marker) && !body.includes("tool_result"),
      turn1,
    );
    const turn2 = enqueueResponse(
      harness,
      wire.completeResponse("anthropic", { text: argsJSON }),
    );
    harness.scenario.whenRequestBodyMatches(
      (body) => body.includes(marker) && body.includes("tool_result"),
      turn2,
    );
  }
}

// True when `body` carries the map item `{ id: "<taskId>", ... }`. The runtime
// serializes the item into the inner step's input, which the agent embeds in its
// user message via a second JSON.stringify, so the quotes arrive doubly escaped.
// Keying on the escaped `\"id\":\"<taskId>\"` entry is boundary-safe: `t1`
// cannot substring-match `t10`.
function bodyHasTaskId(body: string, taskId: string): boolean {
  return body.includes(`\\"id\\":\\"${taskId}\\"`);
}

// Register the two-turn matcher pair for the implementer, which runs once PER
// TASK inside the `implement` map. Both turns additionally key on the task id,
// so each task's agent receives and returns ITS OWN task.
function scriptImplementerPerTask(harness: Harness, taskId: string): void {
  const marker = `you are the implementer for`;
  const argsJSON = JSON.stringify({ taskId, done: true });
  for (let i = 0; i < POOL; i += 1) {
    const turn1 = enqueueResponse(
      harness,
      wire.completeResponse("anthropic", {
        toolCalls: [
          {
            callId: `call-impl-${taskId}-${String(i)}`,
            name: IMPLEMENT_TOOL,
            argsJSON,
          },
        ],
      }),
    );
    harness.scenario.whenRequestBodyMatches(
      (body) =>
        body.includes(marker) &&
        bodyHasTaskId(body, taskId) &&
        !body.includes("tool_result"),
      turn1,
    );
    const turn2 = enqueueResponse(
      harness,
      wire.completeResponse("anthropic", { text: argsJSON }),
    );
    harness.scenario.whenRequestBodyMatches(
      (body) =>
        body.includes(marker) &&
        bodyHasTaskId(body, taskId) &&
        body.includes("tool_result"),
      turn2,
    );
  }
}

// True when `body` carries the round's escaped `round\":N` fragment with N
// standing alone (not the leading digits of a longer number).
function bodyHasRound(body: string, round: number): boolean {
  const marker = `round\\":${String(round)}`;
  let at = body.indexOf(marker);
  while (at !== -1) {
    const next = body[at + marker.length];
    if (next === undefined || next < "0" || next > "9") return true;
    at = body.indexOf(marker, at + marker.length);
  }
  return false;
}

// Register the critic's two-turn matcher pair per round: the verdict is "pass"
// once the round reaches `convergeAtRound`, else "amend". Keys on the round
// fragment so each round routes to its own verdict.
function scriptCritic(harness: Harness, convergeAtRound: number): void {
  const marker = `you are the critic for`;
  for (let round = 1; round <= AMENDMENT_CAP + 1; round += 1) {
    const verdict = round >= convergeAtRound ? "pass" : "amend";
    const argsJSON = JSON.stringify({ verdict, round });
    for (let i = 0; i < POOL; i += 1) {
      const turn1 = enqueueResponse(
        harness,
        wire.completeResponse("anthropic", {
          toolCalls: [
            {
              callId: `call-critic-${String(round)}-${String(i)}`,
              name: VERDICT_TOOL,
              argsJSON,
            },
          ],
        }),
      );
      harness.scenario.whenRequestBodyMatches(
        (body) =>
          body.includes(marker) &&
          bodyHasRound(body, round) &&
          !body.includes("tool_result"),
        turn1,
      );
      const turn2 = enqueueResponse(
        harness,
        wire.completeResponse("anthropic", { text: argsJSON }),
      );
      harness.scenario.whenRequestBodyMatches(
        (body) =>
          body.includes(marker) &&
          bodyHasRound(body, round) &&
          body.includes("tool_result"),
        turn2,
      );
    }
  }
}

function scriptFullDemo(harness: Harness, convergeAtRound: number): void {
  scriptFixedRole(harness, "planner", PLAN_TOOL, { tasks: PLAN_TASKS });
  for (const taskId of TASK_IDS) {
    scriptImplementerPerTask(harness, taskId);
  }
  scriptCritic(harness, convergeAtRound);
  scriptFixedRole(harness, "fixer", FIX_TOOL, { reworked: true });
  scriptFixedRole(harness, "attributor", ATTRIBUTE_TOOL, { attributed: true });
  scriptFixedRole(harness, "phase5-fixer", PHASE5_FIX_TOOL, { reworked: true });
  scriptFixedRole(harness, "consolidator", CONSOLIDATE_TOOL, {
    consolidated: true,
  });
}

// --- Test suite ------------------------------------------------------------
describe("dispatch orchestrator with real agents", () => {
  let harness: Harness;
  let baseDir: string;

  beforeEach(() => {
    harness = setupHarness();
    baseDir = mkdtempSync(join(tmpdir(), "dispatch-orchestrator-"));
  });

  afterEach(() => {
    harness.dispose();
    rmSync(baseDir, { recursive: true, force: true });
  });

  // Build the runtime env over caller-supplied substrates: real agents through
  // the production step-invoker (each step gets its own isogit-backed context
  // store and workdir under a fresh per-invocation dir), a git/shell-shaped
  // `invokeAction` over the supplied ledger plus the pure host transforms
  // (parsePlan, pickLevel, verify), and the loop wiring. `cleanAtRound` and
  // `fixAtAttempt` drive the deterministic Phase-5 build/verify results.
  function buildEnv(opts: {
    repoStore: ReturnType<typeof createInMemoryRepoStore>;
    blobs: ReturnType<typeof createInMemoryBlobSubstrate>;
    effects: EffectLedger;
    baseDir: string;
    cleanAtRound: number;
    fixAtAttempt: number;
    fixTaskCount: number;
    effectRuns: { n: number };
  }): WorkflowRuntimeEnv {
    const {
      repoStore,
      blobs,
      effects,
      baseDir: envBaseDir,
      cleanAtRound,
      fixAtAttempt,
      fixTaskCount,
      effectRuns,
    } = opts;
    const clock = () => new Date();
    let stepDir = 0;

    const invokeStep: StepInvoker = createWorkflowStepInvoker({
      workflowAuthorize,
      buildEnv: async (): Promise<Omit<BaseEnv, "authorize">> => {
        const dir = join(envBaseDir, `step-${String(stepDir++)}`);
        const storage = await createIsogitStore(join(dir, "ctx"));
        return {
          sources: [SOURCE],
          defaultSource: SOURCE.id,
          storage,
          workdir: join(dir, "workspace"),
          audit: noopAuditStore(),
          directors: createDefaultDirectorRegistry(),
          deps: harness.deps,
        };
      },
      agentFactory: createAgent,
    });

    const invokeAction: ActionInvoker = async ({
      handler,
      input,
      requires,
      authzContext,
    }) => {
      // Pure host transforms: the parse bridge, the per-level slice, and the
      // deterministic verifier. None touches an external effect.
      if (handler === "parsePlan") {
        const tasks = extractPlanTasks(input);
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
      if (handler === "noop") {
        return { output: { skipped: true } };
      }
      if (handler === "verify") {
        return {
          output: { fixed: attemptOf(input) >= fixAtAttempt },
        };
      }
      // The throwing failure sinks fail the run (an escalation would merely
      // complete).
      if (handler === "levelFailed") {
        throw new Error(
          "level failed: amendment loop exhausted without passing",
        );
      }
      if (handler === "retryFailed") {
        throw new Error("retry loop exhausted without fixing the task");
      }
      if (handler === "tasksExhausted") {
        throw new Error(
          "task loop exhausted without walking every attributed task",
        );
      }
      if (handler === "phase5Failed") {
        throw new Error(
          "phase5 verification cap reached without a clean build",
        );
      }

      // The git/shell-shaped effects run through the capability- and ledger-
      // checked EffectContext.
      const capability = requires[0];
      if (capability === undefined) {
        throw new Error(`action ${handler} declared no effect capability`);
      }
      const ctx = createEffectContext({
        authorize: workflowAuthorize,
        effects,
        requires,
        authzContext,
        input,
      });
      const output = await ctx.perform({
        effectId: handler,
        capability,
        run: async () => {
          effectRuns.n += 1;
          if (handler === "buildGate") {
            return {
              handler,
              clean: roundOf(input) >= cleanAtRound,
            };
          }
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
      authorize: workflowAuthorize,
      invokeStep,
      invokeAction,
      effects,
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(fullDemo),
      loopFns,
    };
    const loopBodies = new Map(
      enumerateInlineLoopBodies(fullDemo).map((b) => [b.ref, b.definition]),
    );
    env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
    return env;
  }

  // Run the workflow while continuously draining the harness so every agent's
  // parked inference fetch is served. The pass cap turns a pool-exhausted or
  // unmatched request -- which would otherwise park a fetch forever and hang --
  // into a loud failure.
  const MAX_DRAIN_PASSES = 2000;
  async function drive(
    env: WorkflowRuntimeEnv,
    resume?: { runId: string; resumeFromEvents: readonly WorkflowEvent[] },
  ): Promise<RunResult> {
    const run =
      resume === undefined
        ? runtimeRun(fullDemo, env)
        : runtimeRun(fullDemo, env, {
            runId: resume.runId,
            resumeFromEvents: resume.resumeFromEvents,
          });
    let settled = false;
    const complete = run.complete.finally(() => {
      settled = true;
    });
    let passes = 0;
    while (!settled) {
      if (passes++ > MAX_DRAIN_PASSES) {
        throw new Error(
          `drive exceeded ${String(MAX_DRAIN_PASSES)} drain passes without ` +
            `settling; likely an unmatched or pool-exhausted inference request`,
        );
      }
      await harness.run();
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    return complete;
  }

  function phase5OutcomeOf(result: RunResult): {
    outcome: unknown;
    iterations: unknown;
  } {
    const phase5 = result.outputs.phase5;
    if (
      typeof phase5 !== "object" ||
      phase5 === null ||
      !("outcome" in phase5) ||
      !("iterations" in phase5)
    ) {
      throw new Error(
        `phase5 output missing outcome/iterations: ${JSON.stringify(phase5)}`,
      );
    }
    return { outcome: phase5.outcome, iterations: phase5.iterations };
  }

  test("converges end-to-end with a clean first build", async () => {
    scriptFullDemo(harness, 1);
    const effectRuns = { n: 0 };
    const result = await drive(
      buildEnv({
        repoStore: createInMemoryRepoStore(),
        blobs: createInMemoryBlobSubstrate(),
        effects: inMemoryLedger(),
        baseDir: join(baseDir, "clean"),
        cleanAtRound: 1,
        fixAtAttempt: 0,
        fixTaskCount: FIX_TASK_COUNT,
        effectRuns,
      }),
    );

    expect(result.terminalStatus).toBe("completed");
    // Phase 5 converged on the first clean build; the fix path was pruned.
    expect(phase5OutcomeOf(result)).toEqual({
      outcome: "converged",
      iterations: 1,
    });
    expect(extractAgentPayload(result.outputs.consolidate)).toEqual({
      consolidated: true,
    });
  });

  test("fixes a dirty build through the real attributor and phase5 fixer", async () => {
    scriptFullDemo(harness, 1);
    const effectRuns = { n: 0 };
    const result = await drive(
      buildEnv({
        repoStore: createInMemoryRepoStore(),
        blobs: createInMemoryBlobSubstrate(),
        effects: inMemoryLedger(),
        baseDir: join(baseDir, "dirty"),
        // Round 1 builds dirty, so the real attributor runs and the task loop
        // walks both attributed tasks; each task's retry loop needs a SECOND
        // fixer attempt before `verify` passes (fixAtAttempt: 1), so the depth-3
        // nesting (phase5 -> taskLoop -> retryLoop) genuinely iterates with the
        // real fixer. Round 2 builds clean and converges.
        cleanAtRound: 2,
        fixAtAttempt: 1,
        fixTaskCount: FIX_TASK_COUNT,
        effectRuns,
      }),
    );

    expect(result.terminalStatus).toBe("completed");
    // Reaching `iterations: 2` proves the whole dirty fix path ran with real
    // agents: round 1's build was dirty, so the real attributor and the task
    // loop's per-task retry loops ran, and the run only converges once every
    // scripted fixer/verify attempt is served -- with fixAtAttempt: 1 each
    // task's retry loop MUST have iterated twice, or `verify` would never pass
    // and the retry loop would exhaust into a failed run. An unscripted agent
    // would hang into the drain guard.
    expect(phase5OutcomeOf(result)).toEqual({
      outcome: "converged",
      iterations: 2,
    });
    // Consolidate ran through the real step-invoker envelope after Phase 5.
    expect(extractAgentPayload(result.outputs.consolidate)).toEqual({
      consolidated: true,
    });
  });

  // Crash right after the first Phase-5 iteration's body was spawned on the
  // dirty path, then resume against a fresh repo store but the SAME blobs and
  // effect ledger. The resumed run re-drives the Phase-5 body -- its attributor
  // and fixer re-run through the real step-invoker -- while the build/rebuild
  // effects dedup against the shared ledger. Returns the effect-run counts
  // before and after the resume so a caller can assert exactly-once or the probe.
  async function runPhase5CrashResume(
    effects: EffectLedger,
  ): Promise<{ afterRun1: number; afterResume: number; result2: RunResult }> {
    scriptFullDemo(harness, 1);
    const blobs = createInMemoryBlobSubstrate();
    const effectRuns = { n: 0 };

    const result1 = await drive(
      buildEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        baseDir: join(baseDir, "run1"),
        cleanAtRound: 2,
        fixAtAttempt: 0,
        fixTaskCount: FIX_TASK_COUNT,
        effectRuns,
      }),
    );
    expect(result1.terminalStatus).toBe("completed");
    const afterRun1 = effectRuns.n;

    const trimmed: WorkflowEvent[] = [];
    for (const e of result1.events) {
      trimmed.push(e);
      if (
        e.kind === "ChildSpawned" &&
        e.childRunId === loopBodyRunId(result1.runId, "phase5", 0)
      )
        break;
    }

    const result2 = await drive(
      buildEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        baseDir: join(baseDir, "resume"),
        cleanAtRound: 2,
        fixAtAttempt: 0,
        fixTaskCount: FIX_TASK_COUNT,
        effectRuns,
      }),
      { runId: result1.runId, resumeFromEvents: trimmed },
    );
    return { afterRun1, afterResume: effectRuns.n, result2 };
  }

  test("a crash inside Phase 5 resumes through real agents to exactly-once effects", async () => {
    const { afterRun1, afterResume, result2 } =
      await runPhase5CrashResume(inMemoryLedger());

    // The resume re-drove the Phase-5 body through the real step-invoker and
    // still converged and consolidated.
    expect(result2.terminalStatus).toBe("completed");
    expect(phase5OutcomeOf(result2)).toEqual({
      outcome: "converged",
      iterations: 2,
    });
    expect(extractAgentPayload(result2.outputs.consolidate)).toEqual({
      consolidated: true,
    });
    // The shared ledger held every effect to one execution across the crash:
    // the re-driven build/rebuild deduped, so resume added no new runs.
    expect(afterResume).toBe(afterRun1);
  });

  test("the Phase-5 crash-resume exactly-once claim is non-vacuous under a defeated ledger", async () => {
    const { afterRun1, afterResume } =
      await runPhase5CrashResume(defeatedLedger());
    expect(afterResume).toBeGreaterThan(afterRun1);
  });
});
