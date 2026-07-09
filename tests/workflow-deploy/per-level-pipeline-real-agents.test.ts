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
// Two scenarios run the full pipeline with real agents:
//   1. Convergence: the gate-critic returns "amend" until the round hits a
//      threshold, then "pass". The loop converges, consolidate runs, and
//      escalate is pruned.
//   2. Exhaustion: the gate-critic never passes. The loop exhausts at its
//      cap and routes to escalate; consolidate is pruned.

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

// The planner emits a single task so each `map` fan-out runs one inner
// agent -- a real map without the parallel-ordering surface a multi-item
// fan-out would add to the deterministic mock-inference script.
const TASK_ID = "t1";
const TASKS = [{ id: TASK_ID }];

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
      step: step({ agent: implementerAgent }),
    }),
    rebuild: action({
      handler: "rebuild",
      effect: { requires: ["git:commit"] },
      input: { from: "steps.fix.output" },
      after: ["fix"],
    }),
    recritique: map({
      over: { from: "trigger.payload.tasks" },
      step: step({ agent: criticAgent }),
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
      step: step({ agent: implementerAgent }),
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
      step: step({ agent: criticAgent }),
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
// switch. A single task per level means at most one fetch per role is in
// flight at a time, so role-keyed body matchers never race.
// -------------------------------------------------------------------------

// A generous per-role matcher pool. Each role is invoked a small fixed
// number of times across the pipeline; over-provisioning is safe -- unused
// matchers never fire, and a shortfall surfaces loudly as an
// UnmatchedFetchError from `harness.run()`.
const POOL = 8;

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
  scriptFixedRole(harness, "implementer", IMPLEMENT_TOOL, {
    taskId: TASK_ID,
    done: true,
  });
  scriptFixedRole(harness, "critic", CRITIQUE_TOOL, {
    taskId: TASK_ID,
    note: "looks reasonable",
  });
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
});
