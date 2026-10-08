// onFailure routing for a `step`. A step whose retries are exhausted and that
// carries `onFailure` routes its permanent failure to the named handler
// instead of failing the run: it settles `routed`, its normal after-dependents
// are pruned via the skip-sentinel mechanism a gate/loop uses, and the handler
// branch is selected. On success the mirror happens -- the handler branch is
// pruned. A diamond-join reachable from the selected side stays live.
// "Ran" vs "pruned" is whether `invokeStep` was called for the step's agent:
// a pruned step is completed with a skip-sentinel without ever invoking.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  sleep,
  step,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

// A sibling that settles only after several microtasks, waking the drive
// loop's Promise.race in the middle of a concurrent prune.
async function settleLate(): Promise<{ output: null }> {
  for (let i = 0; i < 8; i += 1) await Promise.resolve();
  return { output: null };
}

function agent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
}

function buildEnv(
  def: WorkflowDefinition,
  opts: { invokeStep?: StepInvoker; drain?: WorkflowRuntimeEnv["drain"] } = {},
): {
  env: WorkflowRuntimeEnv;
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
} {
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: opts.invokeStep ?? (async () => ({ output: null })),
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: opts.drain ?? createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
  };
  return { env, repoStore };
}

// An already-aborted drain reporting "cancel": the window after drain fires
// but before the supervisor commits CancelRequested, while the run stays
// `running`. The routing prune must complete here rather than bail.
function abortedDrain(): WorkflowRuntimeEnv["drain"] {
  const controller = new AbortController();
  controller.abort();
  return { signal: controller.signal, behaviorFor: () => "cancel" };
}

// A pruned step's skip sentinel is committed straight to the log -- it never
// rides the drive loop's in-process stepOutputs -- so it is read back from
// the log, not the live RunResult.outputs.
async function skipSentinelOf(
  repoStore: ReturnType<typeof createInMemoryRepoStore>,
  env: WorkflowRuntimeEnv,
  stepId: string,
): Promise<unknown> {
  const log = await repoStore.read("r");
  const completed = log.find(
    (e): e is Extract<WorkflowEvent, { kind: "StepCompleted" }> =>
      e.kind === "StepCompleted" && e.stepId === stepId,
  );
  if (!completed) throw new Error(`no StepCompleted for ${stepId}`);
  return env.blobs.resolveRef(completed.output.ref);
}

describe("onFailure step routing", () => {
  test("a failing step routes to its handler and the run completes", async () => {
    const def = defineWorkflow({
      id: "of-route",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const invoked: string[] = [];
    const { env, repoStore } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        if (req.agent.id === "unit") throw new Error("unit boom");
        return { output: null };
      },
    });
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    const res = await run.complete;

    expect(res.terminalStatus).toBe("completed");
    // The handler ran; the normal successor was pruned (never invoked).
    expect(invoked).toContain("rescue");
    expect(invoked).not.toContain("normal");
    expect(await skipSentinelOf(repoStore, env, "normal")).toEqual({
      skipped: true,
      onFailureStepId: "unit",
      settled: "routed",
    });
    const log = await repoStore.read("r");
    expect(
      log.some(
        (e) =>
          e.kind === "StepFailed" &&
          e.stepId === "unit" &&
          e.routedTo === "rescue",
      ),
    ).toBe(true);
    expect(log.some((e) => e.kind === "RunFailed")).toBe(false);
  });

  test("the routed unit's own output is the failure sentinel", async () => {
    // The unit's own output is the failure sentinel, so an input selector on
    // steps.unit.output resolves it.
    const def = defineWorkflow({
      id: "of-sentinel",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({
          agent: agent("rescue"),
          after: ["unit"],
          input: { from: "steps.unit.output" },
        }),
      },
    });
    let handlerInput: unknown;
    const { env } = buildEnv(def, {
      invokeStep: async (req) => {
        if (req.agent.id === "unit") throw new Error("unit boom");
        handlerInput = req.input;
        return { output: null };
      },
    });
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    const res = await run.complete;

    expect(res.terminalStatus).toBe("completed");
    expect(handlerInput).toEqual({
      failed: true,
      stepId: "unit",
      error: { message: "unit boom" },
    });
  });

  test("a succeeding step prunes its handler branch", async () => {
    const def = defineWorkflow({
      id: "of-success",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const invoked: string[] = [];
    const { env, repoStore } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    const res = await run.complete;

    expect(res.terminalStatus).toBe("completed");
    // The unit and its normal successor ran; the handler was pruned.
    expect(invoked).toContain("unit");
    expect(invoked).toContain("normal");
    expect(invoked).not.toContain("rescue");
    expect(await skipSentinelOf(repoStore, env, "rescue")).toEqual({
      skipped: true,
      onFailureStepId: "unit",
      settled: "completed",
    });
  });

  test("a diamond join reachable from the handler stays live on failure", async () => {
    const def = defineWorkflow({
      id: "of-diamond",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
        join: step({ agent: agent("join"), after: ["rescue", "normal"] }),
      },
    });
    const invoked: string[] = [];
    const { env } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        if (req.agent.id === "unit") throw new Error("unit boom");
        return { output: null };
      },
    });
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    const res = await run.complete;

    expect(res.terminalStatus).toBe("completed");
    // rescue ran, normal pruned, and the join still ran: reachable from the
    // selected handler branch, so collectBranchClosure spares it.
    expect(invoked).toContain("rescue");
    expect(invoked).toContain("join");
    expect(invoked).not.toContain("normal");
  });

  test("a routed failure under drain still prunes the normal branch", async () => {
    // A drain fires the unit's abort while the run is running; the routing
    // prune must complete anyway, or the normal branch runs.
    const def = defineWorkflow({
      id: "of-drain-fail",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const invoked: string[] = [];
    const { env } = buildEnv(def, {
      drain: abortedDrain(),
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        if (req.agent.id === "unit") throw new Error("unit boom");
        return { output: null };
      },
    });
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;
    void res;
    expect(invoked).not.toContain("normal");
  });

  test("a success under drain still prunes the handler branch", async () => {
    const def = defineWorkflow({
      id: "of-drain-succ",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const invoked: string[] = [];
    const { env } = buildEnv(def, {
      drain: abortedDrain(),
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;
    void res;
    expect(invoked).not.toContain("rescue");
  });

  test("a routed failure prunes a depth-2 branch with a sleep under a mid-prune sibling", async () => {
    // A deep pruned branch ends in a resumable sleep, with a sibling settling
    // mid-prune: leaf-first ordering must keep the sleep from being scheduled
    // (it would arm a long timer and hang).
    const def = defineWorkflow({
      id: "of-deep-fail",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
        n2: step({ agent: agent("n2"), after: ["normal"] }),
        nap: sleep({ duration: 9_999_999, after: ["n2"] }),
        sib: step({ agent: agent("sib") }),
      },
    });
    const invoked: string[] = [];
    const { env } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        if (req.agent.id === "unit") throw new Error("unit boom");
        if (req.agent.id === "sib") return settleLate();
        return { output: null };
      },
    });
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toContain("rescue");
    expect(invoked).not.toContain("normal");
    expect(invoked).not.toContain("n2");
  });

  test("a success prunes a depth-2 handler branch with a sleep under a mid-prune sibling", async () => {
    const def = defineWorkflow({
      id: "of-deep-succ",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        r2: step({ agent: agent("r2"), after: ["rescue"] }),
        nap: sleep({ duration: 9_999_999, after: ["r2"] }),
        sib: step({ agent: agent("sib") }),
      },
    });
    const invoked: string[] = [];
    const { env } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        if (req.agent.id === "sib") return settleLate();
        return { output: null };
      },
    });
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toContain("unit");
    expect(invoked).not.toContain("rescue");
    expect(invoked).not.toContain("r2");
  });
});
