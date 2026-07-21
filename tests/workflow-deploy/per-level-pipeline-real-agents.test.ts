// Per-level dispatch pipeline on the extended workflow engine, driven by
// REAL @intx/agent agents through the production step-invoker seam.
//
// The engine-level sibling `tests/workflow/dispatch-demo.test.ts` proves
// the routing/resume shape with a stubbed `invokeStep`; this test proves
// the SAME pipeline subsumes the interchange-demo per-level orchestration
// while running real agents end-to-end. Every step's agent is a genuine
// `defineAgent` reactor with an arktype-validated terminal tool, built via
// `createAgent` and driven by deterministic mock inference from
// `@intx/inference-testing`. The agents are wired into the runtime through
// the production `createWorkflowStepInvoker` adapter, threaded as
// `invokeStep` into an in-process `runtimeRun(definition, env)` -- the same
// in-process harness `single-step-conversation-durability.test.ts` uses,
// NOT the deploy hub/sidecar/subprocess harness.
//
// The pipeline is one `defineWorkflow`: plan -> parsePlan (action) ->
// implementers (map) -> commit (action) -> critique (map) -> gate (step) ->
// amend (loop) -> consolidate / escalate. The amendment loop's body is its
// own `defineWorkflow` (fix map -> rebuild action -> recritique map ->
// regate step); its `while`/`carry` LoopFns read the body's `regate`
// child-output key, mirroring how the sibling's `verdictOf` reads
// `childOutput.critic`. The loop's own `routeLoopOutcome` handles
// pass-vs-escalate routing, so a plain gate-critic STEP feeds the loop
// rather than a redundant `gate` primitive.
//
// Why the `parsePlan` action exists. The production step-invoker surfaces
// every real agent step's output as a `{ reply, turn }` envelope -- the
// reply string plus the FINAL assistant turn. The workflow's structural
// selectors (`map.over`, `input.from`) do pure path navigation over that
// envelope; they cannot destructure it or parse the agent's structured
// output out of the reply string. So a real agent's structured output must
// be host-parsed at a seam the engine provides -- here a parse-`action`
// (host JS) that lifts the planner's `{ tasks }` out of the envelope into a
// selector-reachable `steps.parsePlan.output.tasks`. Empirically the
// terminal-tool call arguments do NOT survive on `output.turn` (the turn is
// the final text turn, whose content is a single text block), so the reply
// text -- the JSON the planner's follow-up turn surfaces -- is the ONLY
// structured surface the seam exposes; the parse-action reads it. The
// stubbed sibling reads `steps.plan.output.tasks` directly only because its
// stub returns a bare `{ tasks }` with no `{ reply, turn }` wrapper. A bare
// `map.over` selector into a real agent step cannot consume that wrapper.
//
// Four scenarios run the full pipeline with real agents:
//   1. Convergence: the gate-critic returns "amend" until the round hits a
//      threshold, then "pass". The loop converges, consolidate runs, and
//      escalate is pruned.
//   2. Exhaustion: the gate-critic never passes. The loop exhausts at its
//      cap and routes to escalate; consolidate is pruned.
//   3. Crash-resume exactly-once: a mid-loop crash re-drives the final
//      amendment iteration through the real step-invoker on resume, and the
//      shared effect ledger holds every effect to one execution. The stubbed
//      loop-resume tests (packages/workflow runtime) cover this dedup
//      mechanically; proven HERE is the same dedup composed with real agents
//      re-driven through the production seam.
//   4. Defeated-ledger probe: the identical crash under a ledger that never
//      dedups re-executes the re-driven effect, so the effect count rises --
//      proving scenario 3's exactly-once assertion is non-vacuous (the crash
//      truly re-drives the effect rather than replaying it from the log).

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
import { createIsogitStore } from "@intx/storage-isogit";
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

// The amendment loop cap. `loop()` throws on a non-positive/non-integer
// maxIterations, so the loop MUST carry a positive-integer bound. This is
// a deliberate divergence from the real interchange-demo, whose amendment
// loop is unbounded (bounded only by operator escalation): modelling it as
// a native `loop` effect requires this cap, and hitting it is exactly what
// routes to the escalation branch.
const AMENDMENT_CAP = 3;

// The planner emits two tasks, so every `map` fan-out (implementers,
// critique, and the loop body's fix/recritique) runs two DISTINCT inner
// agents -- one per task. This is a real fan-out with per-task fidelity:
// each task's inner agent must receive ITS OWN task item and return ITS
// OWN taskId, which the per-task matchers below enforce. The runtime runs
// the inner steps sequentially (runMap in packages/workflow), so this is
// about per-task payload fidelity, not a concurrency race: without
// per-task matchers, task t2's implementer would be served t1's hardcoded
// output, and the fan-out would prove nothing.
const TASK_IDS = ["t1", "t2"] as const;
const TASKS = TASK_IDS.map((id) => ({ id }));

const SOURCE: InferenceSource = {
  id: "anthropic:per-level-pipeline",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-per-level-pipeline",
  model: "claude-per-level-pipeline",
};

// Each agent role owns a terminal tool: the model calls it with the role's
// structured output, the tool validates the args with arktype and echoes
// them back as the tool result, and the model's follow-up turn surfaces the
// same structure as the step reply. `verdict` is the load-bearing field the
// loop reads off the gate-critic role.
const PLAN_TOOL = "emit_plan";
const IMPLEMENT_TOOL = "emit_implementation";
const CRITIQUE_TOOL = "emit_critique";
const VERDICT_TOOL = "emit_verdict";
const CONSOLIDATE_TOOL = "emit_consolidation";

const PlanArgs = type({ tasks: type({ id: "string" }).array() });
const ImplementArgs = type({ taskId: "string", done: "boolean" });
const CritiqueArgs = type({ taskId: "string", note: "string" });
const VerdictArgs = type({ verdict: "'amend' | 'pass'", round: "number" });
const ConsolidateArgs = type({ consolidated: "boolean" });

type ArkSchema = (data: unknown) => unknown;

// A terminal tool whose `run` validates the model's arguments with the
// role's arktype schema and returns them verbatim as the structured tool
// result. Invalid arguments surface loudly as a tool error rather than a
// silent pass-through -- the schema is genuinely exercised on every call.
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
    systemPrompt: `you are the ${id} for the per-level dispatch pipeline`,
    tools: [
      terminalTool(
        `@intx-test/per-level-pipeline/${toolName}`,
        toolName,
        schema,
      ),
    ],
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
const criticAgent = roleAgent("critic", CRITIQUE_TOOL, CritiqueArgs);
const gateCriticAgent = roleAgent("gate-critic", VERDICT_TOOL, VerdictArgs);
const consolidatorAgent = roleAgent(
  "consolidator",
  CONSOLIDATE_TOOL,
  ConsolidateArgs,
);

// The loop body: fix reworks the blocking tasks, rebuild is a
// deterministic git-commit-shaped action, recritique re-judges, and regate
// re-runs the gate-critic. The body's terminal step is `regate`, so the
// loop's `shouldAmend`/`nextRound` LoopFns read the verdict off the body's
// `regate` child-output key.
const amendBody = defineWorkflow({
  id: "amend-body",
  trigger: { type: "manual" },
  steps: {
    fix: map({
      over: { from: "trigger.payload.tasks" },
      // Thread the per-item task into the inner agent explicitly so each
      // task's fix agent receives ITS OWN task. `runMap` rebinds
      // `trigger.payload` to the current item, so this selector resolves to
      // { id: "t1" } / { id: "t2" }. `fix` is the body's first step (the
      // default-input convention would inject the same `trigger.payload`
      // here), but naming it keeps all four maps' per-task threading uniform
      // and self-evident rather than relying on first-step positioning.
      step: step({
        agent: implementerAgent,
        input: { from: "trigger.payload" },
      }),
    }),
    rebuild: action({
      handler: "rebuild",
      effect: { requires: ["git:commit"] },
      // Key the git effect on the deterministic iteration payload
      // ({ round, tasks }), NOT on the fix map's { reply, turn } agent
      // output. The ledger dedups by hash(runId, stepId, effectId, input),
      // so a re-driven iteration must reconstruct the IDENTICAL input for
      // dedup to fire -- and a real agent's turn (timestamps, ids) is not
      // stable across a re-drive. Deterministic effect input is what makes
      // the crash-resume exactly-once guarantee hold; keying an effect on
      // upstream agent output would silently defeat it.
      input: { from: "trigger.payload" },
      after: ["fix"],
    }),
    recritique: map({
      over: { from: "trigger.payload.tasks" },
      // Thread the per-item task into the inner critic explicitly: this map
      // is not the body's first step, so without a named selector its inner
      // step would receive `null` and every task's recritique agent would
      // see the same empty input. `runMap` rebinds `trigger.payload` to the
      // current task.
      step: step({
        agent: criticAgent,
        input: { from: "trigger.payload" },
      }),
      after: ["rebuild"],
    }),
    regate: step({
      agent: gateCriticAgent,
      input: { from: "trigger.payload" },
      after: ["recritique"],
    }),
  },
});

const pipeline = defineWorkflow({
  id: "per-level-pipeline",
  trigger: { type: "manual" },
  steps: {
    plan: step({ agent: plannerAgent }),
    // Bridge the real planner's `{ reply, turn }` envelope into a
    // selector-reachable `{ tasks }`. The handler lifts the planner's tasks
    // out of the reply (the only structured surface the seam exposes) and
    // fails loudly if they are absent or malformed. Downstream maps and the
    // loop input then fan over `steps.parsePlan.output.tasks` -- the REAL
    // planner output, threaded structurally.
    parsePlan: action({
      handler: "parsePlan",
      input: { from: "steps.plan.output" },
      after: ["plan"],
    }),
    implementers: map({
      over: { from: "steps.parsePlan.output.tasks" },
      // Thread the current fan-out item into the inner agent explicitly.
      // The default-input convention only injects `trigger.payload` for a
      // map whose inner step is the workflow's FIRST record entry; this map
      // is not first (plan/parsePlan precede it), so without an explicit
      // selector the inner step would receive `null` and every task's agent
      // would see the same empty input. `runMap` rebinds `trigger.payload`
      // to the per-item value, so this selector resolves to the task item
      // ({ id: "t1" } / { id: "t2" }) -- the per-task input the per-task
      // matchers key on.
      step: step({
        agent: implementerAgent,
        input: { from: "trigger.payload" },
      }),
      after: ["parsePlan"],
    }),
    commit: action({
      handler: "commit",
      effect: { requires: ["git:commit"] },
      input: { from: "steps.implementers.output" },
      after: ["implementers"],
    }),
    critique: map({
      over: { from: "steps.parsePlan.output.tasks" },
      // Thread the per-item task into the inner critic explicitly, for the
      // same reason as `implementers` above: a non-first map's inner step
      // gets no default `trigger.payload` input, so it must name the
      // per-item selector `runMap` rebinds to the current task.
      step: step({
        agent: criticAgent,
        input: { from: "trigger.payload" },
      }),
      after: ["commit"],
    }),
    gate: step({
      agent: gateCriticAgent,
      // The v0 gate-critic judges round 1 over the real planner tasks: merge
      // the literal round with the tasks projected out of parsePlan.
      input: {
        merge: [
          { literal: { round: 1 } },
          { project: { from: "steps.parsePlan.output" }, fields: ["tasks"] },
        ],
      },
      after: ["critique"],
    }),
    // The gate step above is the v0 gate-critic; the loop's own routing
    // (routeLoopOutcome) is what selects consolidate-vs-escalate off the
    // body's regate verdict. The loop is seeded with a round-1 { round,
    // tasks } object -- the round literal merged with the REAL planner tasks
    // projected out of parsePlan -- which its inner maps fan over and its
    // `nextRound` carry threads forward each round.
    amend: loop({
      body: amendBody,
      while: "shouldAmend",
      carry: "nextRound",
      input: {
        merge: [
          { literal: { round: 1 } },
          { project: { from: "steps.parsePlan.output" }, fields: ["tasks"] },
        ],
      },
      maxIterations: AMENDMENT_CAP,
      onExhausted: "escalate",
      after: ["gate"],
    }),
    consolidate: step({ agent: consolidatorAgent, after: ["amend"] }),
    escalate: escalation({ to: "operator", after: ["amend"] }),
  },
});

// -------------------------------------------------------------------------
// The single, faithful host-side extraction of an agent step's structured
// output.
//
// Every real agent step's output is the step-invoker's `{ reply, turn }`
// envelope. The terminal-tool call arguments do NOT survive on `turn` (the
// final assistant turn is the text turn, whose content is a single text
// block), so the reply string -- the JSON the role's follow-up turn
// surfaces -- is the only structured surface. This helper is that one
// extraction mechanism; the parse-action, the loop's `verdictOf`, and the
// consolidate assertion all go through it, so the whole test speaks one
// faithful language. It FAILS LOUDLY on any malformed shape rather than
// defaulting -- a silent fallback here would mask a broken step output.
// -------------------------------------------------------------------------
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

// Lift the planner's `{ tasks }` out of its `{ reply, turn }` envelope,
// validating the shape. Shared by the `parsePlan` action handler and the
// loop-input tasks projection so both thread the SAME real planner output.
function extractPlanTasks(stepOutput: unknown): { id: string }[] {
  const payload = extractAgentPayload(stepOutput);
  const tasks = payload.tasks;
  if (!Array.isArray(tasks) || tasks.length === 0) {
    throw new Error(
      `planner output has no non-empty tasks array: ${JSON.stringify(payload)}`,
    );
  }
  const out: { id: string }[] = [];
  for (const task of tasks) {
    if (typeof task !== "object" || task === null || !("id" in task)) {
      throw new Error(
        `planner task is not a { id } object: ${JSON.stringify(task)}`,
      );
    }
    const id = task.id;
    if (typeof id !== "string") {
      throw new Error(
        `planner task id is not a string: ${JSON.stringify(task)}`,
      );
    }
    out.push({ id });
  }
  return out;
}

function roundOf(value: unknown): number {
  if (typeof value === "object" && value !== null && "round" in value) {
    const round = value.round;
    if (typeof round === "number") return round;
  }
  throw new Error(`carry state has no numeric round: ${JSON.stringify(value)}`);
}

function tasksOf(value: unknown): { id: string }[] {
  if (typeof value === "object" && value !== null && "tasks" in value) {
    const tasks = value.tasks;
    if (Array.isArray(tasks)) {
      const out: { id: string }[] = [];
      for (const task of tasks) {
        if (typeof task === "object" && task !== null && "id" in task) {
          const id = task.id;
          if (typeof id === "string") {
            out.push({ id });
            continue;
          }
        }
        throw new Error(
          `carry task is not a { id } object: ${JSON.stringify(task)}`,
        );
      }
      return out;
    }
  }
  throw new Error(`carry state has no tasks array: ${JSON.stringify(value)}`);
}

// The loop body's child output is keyed by the body's step ids, so the
// verdict lives at `childOutput.regate` -- the gate-critic step's
// `{ reply, turn }` envelope. This mirrors the sibling test's `verdictOf`
// reading `childOutput.critic.verdict` for a body whose step is `critic`.
// It THROWS on a missing/malformed verdict rather than defaulting, so an
// extraction failure surfaces as a loud loop error instead of silently
// reading as "amend" and masking a real bug.
function verdictOf(childOutput: unknown): string {
  if (
    typeof childOutput !== "object" ||
    childOutput === null ||
    !("regate" in childOutput)
  ) {
    throw new Error(
      `loop body child output has no regate step: ${JSON.stringify(childOutput)}`,
    );
  }
  const payload = extractAgentPayload(childOutput.regate);
  const verdict = payload.verdict;
  if (verdict !== "amend" && verdict !== "pass") {
    throw new Error(
      `gate-critic verdict is not "amend" | "pass": ${JSON.stringify(payload)}`,
    );
  }
  return verdict;
}

// The loop's own output shape is `{ outcome, iterations, carry }`. Project
// the two fields the routing assertions read so a shape drift surfaces as a
// clear failure rather than a silent `undefined` comparison.
function loopOutcome(result: RunResult): {
  outcome: unknown;
  iterations: unknown;
} {
  const amend = result.outputs.amend;
  if (
    typeof amend !== "object" ||
    amend === null ||
    !("outcome" in amend) ||
    !("iterations" in amend)
  ) {
    throw new Error(
      `amend loop output missing outcome/iterations: ${JSON.stringify(amend)}`,
    );
  }
  return { outcome: amend.outcome, iterations: amend.iterations };
}

// Project the `taskId` field out of every entry of a map step's array
// output, each parsed through the faithful `extractAgentPayload`. This is
// the per-task fidelity claim's read side: a map that fanned two DISTINCT
// tasks over two DISTINCT inner agents yields two payloads carrying t1 and
// t2 respectively. It FAILS LOUDLY if the map output is not an array or an
// entry has no string `taskId`, so a fan-out that collapsed both tasks to a
// single hardcoded output (or dropped the per-task keying) surfaces as a
// clear failure rather than a silent pass.
function mapTaskIds(mapOutput: unknown): string[] {
  if (!Array.isArray(mapOutput)) {
    throw new Error(
      `map step output is not an array: ${JSON.stringify(mapOutput)}`,
    );
  }
  return mapOutput.map((entry) => {
    const taskId = extractAgentPayload(entry).taskId;
    if (typeof taskId !== "string") {
      throw new Error(
        `map entry payload has no string taskId: ${JSON.stringify(entry)}`,
      );
    }
    return taskId;
  });
}

const loopFns = (ref: string): LoopFn => {
  if (ref === "shouldAmend")
    return (childOutput) => verdictOf(childOutput) === "amend";
  if (ref === "nextRound")
    // Carry the real planner tasks (threaded into iteration 0 via the loop
    // input's project selector) forward so each round's inner maps keep
    // fanning over the actual planner output, not a constant.
    return (_childOutput, currentInput) => ({
      round: roundOf(currentInput) + 1,
      tasks: tasksOf(currentInput),
    });
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

// A ledger whose `lookup` never hits (always a miss) but whose `record`
// still stores. It defeats the exactly-once dedup: on resume, the
// re-driven iteration's `perform` sees no prior record and re-runs the
// effect. Used only by the defeated-ledger probe, which asserts the
// re-driven effect count goes strictly UP -- proving the primary
// exactly-once assertion is non-vacuous (the crash point truly forces a
// re-run that the real ledger dedups).
function defeatedLedger(): EffectLedger {
  const store = new Map<string, { output: unknown }>();
  return {
    async lookup() {
      return undefined;
    },
    async record(effectKey, output) {
      store.set(effectKey, { output });
    },
  };
}

// Allow the two authz shapes the pipeline exercises: per-tool invocation
// (every agent's terminal tool) and per-effect invocation (the git-commit-
// shaped `commit`/`rebuild` actions). Anything else is denied loudly.
const workflowAuthorize: WorkflowAuthorizeFn = (resource, action_) => {
  if (
    action_ === "invoke" &&
    (resource.startsWith("tool:") || resource.startsWith("effect:"))
  ) {
    return Promise.resolve({
      effect: "allow" as const,
      matchingGrants: [],
      resolvedBy: null,
    });
  }
  throw new Error(
    `per-level-pipeline test authorize: unexpected ${resource}/${action_}`,
  );
};

// -------------------------------------------------------------------------
// Deterministic mock inference.
//
// Each agent invocation is a two-turn dance: the model calls the role's
// terminal tool with structured arguments (turn 1), the tool validates +
// echoes them, and the model surfaces the same JSON as its final text reply
// (turn 2). Body-aware matchers route each fetch by the role's system
// prompt and by whether the request carries a prior tool_result (turn 2) or
// not (turn 1). The gate-critic's verdict is a deterministic function of the
// round encoded in the request body, mirroring the sibling's
// `convergeAtRound` but flowing through the terminal tool rather than a bare
// switch.
//
// The per-level fan-out is two tasks (t1, t2), so the implementer and
// critic roles each run once PER TASK inside their maps. A role-keyed
// matcher alone would serve BOTH tasks the same hardcoded output, which
// would look like it proves parallel fan-out and prove nothing. So the
// implementer and critic matchers ALSO key on the task id as it appears in
// the request body -- and return that task's OWN taskId -- so each task's
// inner agent genuinely receives and returns its own task. This is the
// fidelity the per-task fidelity assertion (see the convergence scenario)
// then claims. The planner, consolidator, and gate-critic stay role-keyed:
// the planner emits both tasks, the consolidator runs once for the level,
// and the gate-critic judges the WHOLE level per round (not per task).
// -------------------------------------------------------------------------

// A generous per-role matcher pool. Each role is invoked a small fixed
// number of times across the pipeline; over-provisioning is safe -- unused
// matchers never fire, and a shortfall surfaces loudly as an
// UnmatchedFetchError from `harness.run()`. Sized for the crash-resume
// drives too: a resumed run re-drives the in-flight iteration's turns,
// drawing extra pulls from the non-round-partitioned fixed-role pool.
const POOL = 16;

function enqueueResponse(harness: Harness, chunks: Uint8Array[]) {
  const stream = harness.scenario.createStream();
  stream.enqueueAll(chunks, { startAt: harness.clock.now() + 1 });
  return stream;
}

// Register the two-turn matcher pair for a role whose structured output is
// fixed across invocations (planner, implementer, critic, consolidator).
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

// True when `body` carries the map item `{ id: "<taskId>" }`. The runtime
// serializes the item with `JSON.stringify` into the inner step's input,
// which the agent then embeds in its user message via a SECOND
// `JSON.stringify`, so the item's quotes arrive doubly escaped: the raw
// bytes read `\"id\":\"t1\"` (each `"` as backslash-quote). Keying on the
// full `\"id\":\"<taskId>\"` object entry -- with the escaped closing quote
// right after the id -- is boundary-safe: `t1` cannot substring-match a
// longer id like `t10`, because the marker requires the closing `\"` to
// follow the id immediately. This mirrors the `bodyHasRound` discipline of
// demanding a delimiter after the value rather than a bare `includes`.
function bodyHasTaskId(body: string, taskId: string): boolean {
  return body.includes(`\\"id\\":\\"${taskId}\\"`);
}

// Register the two-turn matcher pair for a role that runs once PER TASK
// inside a map (implementer, critic). Like `scriptFixedRole`, but BOTH turn
// matchers additionally key on the task id in the request body, and the
// caller passes THAT task's own structured `args` (its own taskId). Two
// tasks served the same hardcoded output would make the fan-out vacuous;
// keying on the task id makes each task's inner agent receive and return
// its own task, which the per-task fidelity assertion then claims.
function scriptFixedRolePerTask(
  harness: Harness,
  role: string,
  toolName: string,
  taskId: string,
  args: Record<string, unknown>,
): void {
  const marker = `you are the ${role} for`;
  const argsJSON = JSON.stringify(args);
  for (let i = 0; i < POOL; i += 1) {
    const turn1 = enqueueResponse(
      harness,
      wire.completeResponse("anthropic", {
        toolCalls: [
          {
            callId: `call-${role}-${taskId}-${String(i)}`,
            name: toolName,
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
// standing alone -- i.e. not the leading digits of a longer number. The
// runtime serializes the step input with `JSON.stringify` and embeds the
// result in the agent's user message, so the quotes arrive escaped (the
// round reads as `round\":N`, not `"round":N`). A plain substring test
// would let round 1 match a `round\":10` body and silently route to the
// wrong verdict; requiring the next character to be a non-digit keeps the
// matcher correct no matter how large `AMENDMENT_CAP` grows.
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

// Register the gate-critic's two-turn matcher pair per round. The verdict
// is "pass" once the round reaches `convergeAtRound`, else "amend"; the
// matcher keys on the round fragment the runtime threads into the
// request's user message so each round routes to its own verdict.
function scriptGateCritic(harness: Harness, convergeAtRound: number): void {
  const marker = `you are the gate-critic for`;
  for (let round = 1; round <= AMENDMENT_CAP + 1; round += 1) {
    const verdict = round >= convergeAtRound ? "pass" : "amend";
    const argsJSON = JSON.stringify({ verdict, round });
    for (let i = 0; i < POOL; i += 1) {
      const turn1 = enqueueResponse(
        harness,
        wire.completeResponse("anthropic", {
          toolCalls: [
            {
              callId: `call-gate-${String(round)}-${String(i)}`,
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

function scriptWorkflow(harness: Harness, convergeAtRound: number): void {
  scriptFixedRole(harness, "planner", PLAN_TOOL, { tasks: TASKS });
  // The implementer and critic run once per task inside their maps; each
  // task gets its own matcher returning its own taskId, so distinct agents
  // run with distinct inputs and produce distinct, correct outputs.
  for (const taskId of TASK_IDS) {
    scriptFixedRolePerTask(harness, "implementer", IMPLEMENT_TOOL, taskId, {
      taskId,
      done: true,
    });
    scriptFixedRolePerTask(harness, "critic", CRITIQUE_TOOL, taskId, {
      taskId,
      note: "looks reasonable",
    });
  }
  scriptFixedRole(harness, "consolidator", CONSOLIDATE_TOOL, {
    consolidated: true,
  });
  scriptGateCritic(harness, convergeAtRound);
}

describe("per-level pipeline with real agents", () => {
  let harness: Harness;
  let baseDir: string;

  beforeEach(() => {
    harness = setupHarness();
    baseDir = mkdtempSync(join(tmpdir(), "per-level-pipeline-"));
  });

  afterEach(() => {
    harness.dispose();
    rmSync(baseDir, { recursive: true, force: true });
  });

  // Build the runtime env over caller-supplied substrates: real agents
  // through the production step-invoker adapter (each step gets its own
  // isogit-backed context store and workdir under a fresh per-invocation dir
  // rooted at `baseDir`), a git-commit-shaped `invokeAction` over the
  // supplied effect ledger, and the loop wiring. Taking the repoStore, blob
  // substrate, and effect ledger as parameters lets a crash-resume test
  // share the ledger and blobs across a simulated crash while giving the
  // resumed run a fresh repoStore (empty child log) and a distinct `baseDir`.
  function buildEnv(opts: {
    repoStore: ReturnType<typeof createInMemoryRepoStore>;
    blobs: ReturnType<typeof createInMemoryBlobSubstrate>;
    effects: EffectLedger;
    baseDir: string;
    effectRuns: { n: number };
  }): WorkflowRuntimeEnv {
    const { repoStore, blobs, effects, baseDir: envBaseDir, effectRuns } = opts;
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
      // The parse-action bridge: a pure host-side transform, no external
      // effect, that lifts the real planner's `{ tasks }` out of its
      // `{ reply, turn }` envelope into a selector-reachable output. Fails
      // loudly (via extractPlanTasks) on a missing/malformed plan.
      if (handler === "parsePlan") {
        return { output: { tasks: extractPlanTasks(input) } };
      }
      // The git-commit-shaped effects (`commit`, `rebuild`): run through the
      // capability- and ledger-checked EffectContext, exactly as the
      // sibling's action handlers do.
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
      drain: createNoopDrainController(pipeline),
      loopFns,
    };
    env.runLoopIteration = createLoopIteration(env);
    return env;
  }

  // Run the pipeline while continuously draining the harness so every
  // agent's parked inference fetch is served. `agent.send` blocks until the
  // reactor replies, and the reactor replies only once `harness.run()` fires
  // the response stream on the virtual clock; the runtime drives many steps
  // sequentially, so the harness must be re-driven as each new fetch arrives.
  async function drivePipeline(
    env: WorkflowRuntimeEnv,
    resume?: { runId: string; resumeFromEvents: readonly WorkflowEvent[] },
  ): Promise<RunResult> {
    const run =
      resume === undefined
        ? runtimeRun(pipeline, env)
        : runtimeRun(pipeline, env, {
            runId: resume.runId,
            resumeFromEvents: resume.resumeFromEvents,
          });
    let settled = false;
    const complete = run.complete.finally(() => {
      settled = true;
    });
    while (!settled) {
      await harness.run();
      // Yield so in-flight runtime work (repoStore commits, agent teardown)
      // can park the next inference fetch before the next drain pass.
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    return complete;
  }

  test("the amendment loop converges through real agents and the level consolidates", async () => {
    scriptWorkflow(harness, 2);
    const effectRuns = { n: 0 };
    const result = await drivePipeline(
      buildEnv({
        repoStore: createInMemoryRepoStore(),
        blobs: createInMemoryBlobSubstrate(),
        effects: inMemoryLedger(),
        baseDir,
        effectRuns,
      }),
    );

    expect(result.terminalStatus).toBe("completed");

    // The real gate-critic drove routing: the loop ran two rounds (round 1
    // amend, round 2 pass) and converged, so consolidate ran on the
    // converged branch and the escalation was pruned.
    expect(loopOutcome(result)).toEqual({
      outcome: "converged",
      iterations: 2,
    });
    expect(extractAgentPayload(result.outputs.consolidate)).toEqual({
      consolidated: true,
    });
    expect("escalate" in result.outputs).toBe(false);

    // Per-task fidelity: the implementers and critique maps each fanned two
    // DISTINCT tasks over two DISTINCT inner agents, and each inner agent
    // received its own task item and returned its own taskId. Proving the
    // outputs carry t1 and t2 respectively is what claims the fan-out --
    // without it, a 2-task map serving both tasks the same hardcoded output
    // would look parallel and prove nothing.
    expect(mapTaskIds(result.outputs.implementers)).toEqual(["t1", "t2"]);
    expect(mapTaskIds(result.outputs.critique)).toEqual(["t1", "t2"]);

    // The deterministic commit and each converged rebuild ran their effects.
    expect(effectRuns.n).toBeGreaterThan(0);
  });

  test("the amendment loop exhausts through real agents and routes to escalation", async () => {
    // The gate-critic never passes, so the loop exhausts at AMENDMENT_CAP.
    scriptWorkflow(harness, AMENDMENT_CAP + 2);
    const effectRuns = { n: 0 };
    const result = await drivePipeline(
      buildEnv({
        repoStore: createInMemoryRepoStore(),
        blobs: createInMemoryBlobSubstrate(),
        effects: inMemoryLedger(),
        baseDir,
        effectRuns,
      }),
    );

    expect(result.terminalStatus).toBe("completed");

    // The gate-critic never passed, so the loop ran the full AMENDMENT_CAP
    // rounds and exhausted; exhaustion routed to escalate and pruned
    // consolidate.
    expect(loopOutcome(result)).toEqual({
      outcome: "exhausted",
      iterations: AMENDMENT_CAP,
    });
    expect("escalate" in result.outputs).toBe(true);
    expect("consolidate" in result.outputs).toBe(false);
  });

  // Drive the pipeline to completion, then simulate a mid-loop crash by
  // truncating the durable parent log right after the final amendment
  // iteration's `ChildSpawned` -- before that child's log lands. Resuming
  // with a FRESH repoStore but the SAME blobs and effect ledger leaves the
  // iteration's child log empty, so the runtime re-drives that iteration
  // (its fix/critic agents re-run through the real step-invoker and its
  // `rebuild` action runs again). The re-driven effect's effectKey looks up
  // the record the completed run already wrote; a real ledger dedups it, a
  // defeated one does not. Returns the effect-run counts before and after
  // the resume so a caller can assert either exactly-once or the probe.
  async function runCrashResume(
    effects: EffectLedger,
  ): Promise<{ afterRun1: number; afterResume: number; result2: RunResult }> {
    scriptWorkflow(harness, 2);
    const blobs = createInMemoryBlobSubstrate();
    const effectRuns = { n: 0 };

    const result1 = await drivePipeline(
      buildEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        baseDir: join(baseDir, "run1"),
        effectRuns,
      }),
    );
    expect(result1.terminalStatus).toBe("completed");
    const afterRun1 = effectRuns.n;

    // Crash right after the final amendment iteration's child is spawned,
    // before its child log is durable, so the resume must re-drive it.
    const trimmed: WorkflowEvent[] = [];
    for (const e of result1.events) {
      trimmed.push(e);
      if (e.kind === "ChildSpawned" && e.childRunId === "amend__1") break;
    }

    const result2 = await drivePipeline(
      buildEnv({
        repoStore: createInMemoryRepoStore(),
        blobs,
        effects,
        baseDir: join(baseDir, "resume"),
        effectRuns,
      }),
      { runId: result1.runId, resumeFromEvents: trimmed },
    );
    return { afterRun1, afterResume: effectRuns.n, result2 };
  }

  test("a mid-loop crash resumes through real agents to exactly-once effects", async () => {
    const { afterRun1, afterResume, result2 } =
      await runCrashResume(inMemoryLedger());

    // The resume re-drove the final amendment iteration through the real
    // step-invoker and still converged: consolidate ran, escalate pruned.
    expect(result2.terminalStatus).toBe("completed");
    expect(loopOutcome(result2)).toEqual({
      outcome: "converged",
      iterations: 2,
    });
    expect(extractAgentPayload(result2.outputs.consolidate)).toEqual({
      consolidated: true,
    });
    expect("escalate" in result2.outputs).toBe(false);

    // The shared ledger held every effect to one execution across the crash:
    // the re-driven iteration's rebuild deduped, so resume added no new runs.
    expect(afterResume).toBe(afterRun1);
  });

  test("the crash-resume exactly-once claim is non-vacuous under a defeated ledger", async () => {
    // The identical crash, but the ledger never dedups. The re-driven
    // iteration's rebuild re-executes, so the effect count rises strictly
    // above the completed-run total. If it did not, the crash point would be
    // replaying an already-complete effect from the durable log rather than
    // re-driving it, and the exactly-once assertion above would prove nothing.
    const { afterRun1, afterResume } = await runCrashResume(defeatedLedger());
    expect(afterResume).toBeGreaterThan(afterRun1);
  });
});
