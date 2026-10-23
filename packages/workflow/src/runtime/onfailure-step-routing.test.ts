// onFailure routing for a `step`. A step whose retries are exhausted and that
// carries `onFailure` routes its permanent failure to the named handler
// instead of failing the run: it settles `routed`, its normal after-dependents
// are pruned via the skip-sentinel mechanism a gate/loop uses, and the handler
// branch is selected. On success the mirror happens -- the handler branch is
// pruned. A diamond-join reachable from the selected side stays live.
//
// "Ran" vs "pruned" is distinguished by whether `invokeStep` was called for a
// step's agent: a real step invokes the agent, a pruned step is completed with
// a skip-sentinel without ever invoking.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  awaitSignal,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  gate,
  runtimeRun,
  sleep,
  step,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { bufferedEvents, flushChain, whenRunNextBuffers } from "./commit-chain";
import { createTerminalChildHandle } from "./suspendable-child-handle";

import { holdFirstUntilSecond } from "./skip-race.test-helpers";

// A sibling whose invocation settles only after several microtasks, waking the
// drive loop's Promise.race in the middle of a concurrent prune.
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
    spawnChild: async () => createTerminalChildHandle("completed"),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: opts.drain ?? createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
  };
  return { env, repoStore };
}

// An already-aborted drain reporting "cancel": the window after drain fires
// but before the supervisor commits CancelRequested, so the run stays
// `running`. The routing prune must complete in this window rather than bail.
function abortedDrain(): WorkflowRuntimeEnv["drain"] {
  const controller = new AbortController();
  controller.abort();
  return {
    signal: controller.signal,
    behaviorFor: () => "cancel",
    hold: null,
  };
}

// A pruned step reaches a terminal StepCompleted carrying a skip sentinel, but
// that sentinel is committed straight to the log -- it never rides the drive
// loop's in-process stepOutputs, so it is read back from the log, not the live
// RunResult.outputs.
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
    // The handler reads steps.unit.output on the live path; the unit's own
    // output is the failure sentinel, so an input selector resolves it.
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

  test("a step that fails before it starts skips its handler branch", async () => {
    // A throwing input selector fails the unit before its StepStarted, a
    // failure that does not route. The handler reads the trigger, so it would
    // be invoked if the scheduler offered it.
    const def = defineWorkflow({
      id: "of-prestart",
      trigger: { type: "manual" },
      steps: {
        unit: step({
          agent: agent("unit"),
          onFailure: "rescue",
          input: { from: "trigger.payload.missing" },
        }),
        rescue: step({
          agent: agent("rescue"),
          after: ["unit"],
          input: { from: "trigger.payload" },
        }),
      },
    });
    const invoked: string[] = [];
    const { env, repoStore } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).toEqual([]);
    expect(await skipSentinelOf(repoStore, env, "rescue")).toEqual({
      skipped: true,
      onFailureStepId: "unit",
      settled: "failed",
    });
  });

  test("a resume does not route a step that failed before it started", async () => {
    const def = defineWorkflow({
      id: "of-prestart-resume",
      trigger: { type: "manual" },
      steps: {
        unit: step({
          agent: agent("unit"),
          onFailure: "rescue",
          input: { from: "trigger.payload.missing" },
        }),
        rescue: step({
          agent: agent("rescue"),
          after: ["unit"],
          input: { from: "trigger.payload" },
        }),
      },
    });
    // A flush queued the moment the unit's StepStarted is buffered, then a
    // resume from what that flush made durable, is a host lost right after
    // a sibling's durable commit lands there.
    const live = buildEnv(def);
    const inner = live.repoStore;
    let durable: readonly WorkflowEvent[] | undefined;
    live.env.repoStore = {
      read: (id) => inner.read(id),
      append: (id, event) => inner.append(id, event),
      appendBatch: async (id, events) => {
        await inner.appendBatch(id, events);
        const startsUnit = events.some(
          (e) => e.kind === "StepStarted" && e.stepId === "unit",
        );
        if (durable === undefined && startsUnit) {
          durable = [...(await inner.read(id))];
        }
      },
      subscribe: (id, opts) => inner.subscribe(id, opts),
    };
    const flushOnStart = (): void => {
      void whenRunNextBuffers("r").then(() => {
        const started = bufferedEvents("r").some(
          (e) => e.kind === "StepStarted" && e.stepId === "unit",
        );
        if (started) void flushChain(live.env, "r");
        else flushOnStart();
      });
    };
    flushOnStart();
    await runtimeRun(def, live.env, { runId: "r", triggerPayload: null })
      .complete;
    if (durable === undefined) throw new Error("the unit never started");

    const invoked: string[] = [];
    const resume = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const res = await runtimeRun(def, resume.env, {
      runId: "r",
      triggerPayload: null,
      resumeFromEvents: durable,
    }).complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).toEqual([]);
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
    // rescue ran, normal pruned, and the join still ran (reachable from the
    // selected handler branch, so collectBranchClosure spares it).
    expect(invoked).toContain("rescue");
    expect(invoked).toContain("join");
    expect(invoked).not.toContain("normal");
  });

  test("two succeeding units skip a join below both handlers once", async () => {
    const def = defineWorkflow({
      id: "of-shared-join",
      trigger: { type: "manual" },
      steps: {
        u1: step({ agent: agent("u1"), onFailure: "h1" }),
        u2: step({ agent: agent("u2"), onFailure: "h2" }),
        h1: step({ agent: agent("h1"), after: ["u1"] }),
        h2: step({ agent: agent("h2"), after: ["u2"] }),
        join: step({ agent: agent("join"), after: ["h1", "h2"] }),
      },
    });
    const invoked: string[] = [];
    const { env, repoStore } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    env.blobs = holdFirstUntilSecond(env.blobs, "join.input");
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("completed");
    expect([...invoked].sort()).toEqual(["u1", "u2"]);
    const joinStarts = (await repoStore.read("r")).filter(
      (e) => e.kind === "StepStarted" && e.stepId === "join",
    );
    expect(joinStarts).toHaveLength(1);
  });

  test("a routed failure under drain still prunes the normal branch", async () => {
    // A drain fires the unit's abort while the run is still running. The
    // routing prune must complete anyway, or the normal branch runs.
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
    // The onFailure route pruning a deep branch that ends in a resumable
    // sleep, with a sibling settling mid-prune: leaf-first ordering must keep
    // the sleep from being scheduled (it would arm a long timer and hang).
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

// A gate may take the handler as its branch, so the handler runs without its
// unit, and the handler may also wait on a step its unit does not lead to. A
// failure that does not route still never starts the handler.
describe("onFailure handler shapes", () => {
  const gated = defineWorkflow({
    id: "of-gated",
    trigger: { type: "manual" },
    steps: {
      pick: gate({
        when: { from: "trigger.payload.flagged" },
        then: "rescue",
        else: "unit",
      }),
      unit: step({
        agent: agent("unit"),
        after: ["pick"],
        onFailure: "rescue",
        input: { from: "trigger.payload.task" },
      }),
      // A gate's pick skips the unit, whose output a selector cannot read.
      rescue: step({
        agent: agent("rescue"),
        after: ["unit"],
        input: { from: "trigger.payload" },
      }),
    },
  });

  const joined = defineWorkflow({
    id: "of-joined",
    trigger: { type: "manual" },
    steps: {
      lookup: step({ agent: agent("lookup") }),
      unit: step({
        agent: agent("unit"),
        onFailure: "rescue",
        input: { from: "trigger.payload.task" },
      }),
      rescue: step({
        agent: agent("rescue"),
        after: ["unit", "lookup"],
        input: { from: "steps.lookup.output" },
      }),
    },
  });

  async function runShape(
    def: WorkflowDefinition,
    triggerPayload: unknown,
    unitFails: boolean,
  ) {
    const invoked: string[] = [];
    const inputs = new Map<string, unknown>();
    const { env, repoStore } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        inputs.set(req.agent.id, req.input);
        if (req.agent.id === "unit" && unitFails) {
          throw new Error("unit boom");
        }
        return { output: `${req.agent.id}-out` };
      },
    });
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload })
      .complete;
    return { res, invoked, inputs, env, repoStore };
  }

  test("a gate that picks the handler runs it without its unit", async () => {
    const { res, invoked } = await runShape(
      gated,
      { flagged: true, task: "t" },
      false,
    );

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toEqual(["rescue"]);
  });

  test("a gate that picks the unit routes the unit's failure to the handler", async () => {
    const { res, invoked } = await runShape(
      gated,
      { flagged: false, task: "t" },
      true,
    );

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toEqual(["unit", "rescue"]);
  });

  test("a gate that picks a succeeding unit prunes the handler", async () => {
    const { res, invoked, env, repoStore } = await runShape(
      gated,
      { flagged: false, task: "t" },
      false,
    );

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toEqual(["unit"]);
    expect(await skipSentinelOf(repoStore, env, "rescue")).toEqual({
      skipped: true,
      onFailureStepId: "unit",
      settled: "completed",
    });
  });

  test("a gate that picks a unit failing before it starts never starts the handler", async () => {
    const { res, invoked, env, repoStore } = await runShape(
      gated,
      { flagged: false },
      false,
    );

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).toEqual([]);
    expect(await skipSentinelOf(repoStore, env, "rescue")).toEqual({
      skipped: true,
      onFailureStepId: "unit",
      settled: "failed",
    });
  });

  test("a handler that also waits on a unit-independent step reads it after the route", async () => {
    const { res, invoked, inputs } = await runShape(
      joined,
      { task: "t" },
      true,
    );

    expect(res.terminalStatus).toBe("completed");
    expect(invoked.sort()).toEqual(["lookup", "rescue", "unit"]);
    expect(inputs.get("rescue")).toBe("lookup-out");
  });

  test("a succeeding unit prunes a handler that also waits on another step", async () => {
    const { res, invoked, env, repoStore } = await runShape(
      joined,
      { task: "t" },
      false,
    );

    expect(res.terminalStatus).toBe("completed");
    expect(invoked.sort()).toEqual(["lookup", "unit"]);
    expect(await skipSentinelOf(repoStore, env, "rescue")).toEqual({
      skipped: true,
      onFailureStepId: "unit",
      settled: "completed",
    });
  });

  test("a unit failing before it starts skips a handler that also waits on another step", async () => {
    const { res, invoked, env, repoStore } = await runShape(joined, {}, false);

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).not.toContain("rescue");
    expect(await skipSentinelOf(repoStore, env, "rescue")).toEqual({
      skipped: true,
      onFailureStepId: "unit",
      settled: "failed",
    });
  });

  // The gate's skip of the unit makes the unit read as completed, and a
  // resume must not take that for a success that prunes the handler.
  test("a resume after the gate picks the handler still runs it", async () => {
    const triggerPayload = { flagged: true, task: "t" };
    const live = buildEnv(gated);
    const liveRes = await runtimeRun(gated, live.env, {
      runId: "r",
      triggerPayload,
    }).complete;
    expect(liveRes.terminalStatus).toBe("completed");
    const emitted = await live.repoStore.read("r");
    const handlerStart = emitted.findIndex(
      (e) => e.kind === "StepStarted" && e.stepId === "rescue",
    );
    expect(handlerStart).toBeGreaterThan(-1);

    const invoked: string[] = [];
    const resume = buildEnv(gated, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const res = await runtimeRun(gated, resume.env, {
      runId: "r",
      triggerPayload,
      resumeFromEvents: emitted.slice(0, handlerStart),
    }).complete;

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toEqual(["rescue"]);
  });
});

// A branch that does not hold a unit can still skip the unit's handler, here
// through `w0` in the gate's untaken branch. Whichever lands first, the skip
// or the unit's failure, the failure must not go unhandled while the run
// completes.
describe("onFailure handler a branch skips", () => {
  test.each(["skip", "cancel"])(
    "a %s during route preparation cannot leave routed skips behind",
    async (outcome) => {
      const preparing = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      const def = defineWorkflow({
        id: `of-prepare-${outcome}`,
        trigger: { type: "manual" },
        steps: {
          unit: step({ agent: agent("unit"), onFailure: "rescue" }),
          hold: step({
            agent: agent("hold"),
            input: { from: "trigger.payload" },
          }),
          pick: gate({
            after: ["hold"],
            when: { from: "trigger.payload" },
            then: "keep",
            else: "w0",
          }),
          keep: step({ agent: agent("keep"), after: ["pick"] }),
          w0: step({ agent: agent("w0"), after: ["pick"] }),
          rescue: step({ agent: agent("rescue"), after: ["unit", "w0"] }),
          normal: step({
            agent: agent("normal"),
            after: ["unit"],
            input: { from: "trigger.payload" },
          }),
        },
      });
      const { env } = buildEnv(def);
      const inner = env.blobs;
      env.blobs = {
        ...inner,
        async recordOutput(stepId, attempt, value) {
          if (
            stepId === "normal.input" &&
            typeof value === "object" &&
            value !== null &&
            "skipped" in value
          ) {
            preparing.resolve(undefined);
            await release.promise;
          }
          return inner.recordOutput(stepId, attempt, value);
        },
      };
      const invoked: string[] = [];
      env.invokeStep = async (request) => {
        invoked.push(request.agent.id);
        if (request.agent.id === "unit") throw new Error("unit failed");
        if (request.agent.id === "hold") await preparing.promise;
        if (request.agent.id === "keep") {
          if (outcome === "cancel")
            await run.cancel("self", "cancel prepared route");
          release.resolve(undefined);
        }
        return { output: null };
      };
      const run = runtimeRun(def, env, {
        runId: `of-prepare-${outcome}`,
        triggerPayload: true,
      });
      const result = await run.complete;
      expect(result.terminalStatus).toBe(
        outcome === "cancel" ? "cancelled" : "failed",
      );
      expect(invoked).not.toContain("rescue");
      expect(invoked.includes("normal")).toBe(outcome === "skip");
      const failure = result.events.find(
        (event) => event.kind === "StepFailed" && event.stepId === "unit",
      );
      if (failure?.kind === "StepFailed")
        expect(failure.routedTo).toBeUndefined();
    },
  );

  // `hold` waits on the gate, so the unit fails after the gate's skip.
  const skipFirst = defineWorkflow({
    id: "of-skip-first",
    trigger: { type: "manual" },
    steps: {
      pick: gate({
        when: { from: "trigger.payload.flagged" },
        then: "keep",
        else: "w0",
      }),
      keep: step({ agent: agent("keep"), after: ["pick"] }),
      w0: step({ agent: agent("w0"), after: ["pick"] }),
      hold: step({ agent: agent("hold"), after: ["pick"] }),
      unit: step({
        agent: agent("unit"),
        after: ["hold"],
        onFailure: "rescue",
      }),
      rescue: step({
        agent: agent("rescue"),
        after: ["unit", "w0"],
        input: { from: "steps.unit.output" },
      }),
      normal: step({
        agent: agent("normal"),
        after: ["unit"],
        input: { from: "trigger.payload" },
      }),
    },
  });

  const routeFirst = defineWorkflow({
    id: "of-route-first",
    trigger: { type: "manual" },
    steps: {
      pick: gate({
        when: { from: "trigger.payload.flagged" },
        then: "keep",
        else: "w0",
      }),
      keep: step({ agent: agent("keep"), after: ["pick"] }),
      w0: step({ agent: agent("w0"), after: ["pick"] }),
      unit: step({ agent: agent("unit"), onFailure: "rescue" }),
      rescue: step({
        agent: agent("rescue"),
        after: ["unit", "w0"],
        input: { from: "steps.unit.output" },
      }),
    },
  });

  const triggerPayload = { flagged: true };

  function failureOf(
    log: readonly WorkflowEvent[],
    stepId: string,
  ): Extract<WorkflowEvent, { kind: "StepFailed" }> {
    const failed = log.find(
      (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
        e.kind === "StepFailed" && e.stepId === stepId,
    );
    if (!failed) throw new Error(`no StepFailed for ${stepId}`);
    return failed;
  }

  test("a unit that fails after a branch skipped its handler fails the run", async () => {
    const invoked: string[] = [];
    const { env, repoStore } = buildEnv(skipFirst, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        if (req.agent.id === "unit") throw new Error("unit boom");
        return { output: null };
      },
    });
    const res = await runtimeRun(skipFirst, env, { runId: "r", triggerPayload })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).not.toContain("rescue");
    expect(invoked).toContain("normal");
    expect(
      failureOf(await repoStore.read("r"), "unit").routedTo,
    ).toBeUndefined();
  });

  test("a unit that crashed after a branch skipped its handler fails the run on resume", async () => {
    const live = buildEnv(skipFirst);
    await runtimeRun(skipFirst, live.env, { runId: "r", triggerPayload })
      .complete;
    const emitted = await live.repoStore.read("r");
    const unitStart = emitted.findIndex(
      (e) => e.kind === "StepStarted" && e.stepId === "unit",
    );
    expect(unitStart).toBeGreaterThan(-1);
    const window = emitted.slice(0, unitStart + 1);
    expect(
      window.some((e) => e.kind === "StepCompleted" && e.stepId === "rescue"),
    ).toBe(true);

    const invoked: string[] = [];
    const resume = buildEnv(skipFirst, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const res = await runtimeRun(skipFirst, resume.env, {
      runId: "r",
      triggerPayload,
      resumeFromEvents: window,
    }).complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).not.toContain("rescue");
    expect(invoked).toContain("normal");
    expect(failureOf(await resume.repoStore.read("r"), "unit").routedTo).toBe(
      undefined,
    );
  });

  test("a branch that skips a handler after its unit routed there fails the handler and the run", async () => {
    // The unit has already routed when the run resumes, and the gate has not
    // run yet, so the gate's skip reaches the handler second.
    const live = buildEnv(routeFirst);
    await runtimeRun(routeFirst, live.env, { runId: "r", triggerPayload })
      .complete;
    const [runStarted] = await live.repoStore.read("r");
    if (runStarted?.kind !== "RunStarted") throw new Error("no RunStarted");
    const at = runStarted.at;
    const window: WorkflowEvent[] = [
      runStarted,
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "unit",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "StepFailed",
        seq: 3,
        at,
        stepId: "unit",
        attempt: 1,
        error: { message: "unit boom" },
        retriesExhausted: true,
        routedTo: "rescue",
      },
    ];

    const invoked: string[] = [];
    const resume = buildEnv(routeFirst, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const res = await runtimeRun(routeFirst, resume.env, {
      runId: "r",
      triggerPayload,
      resumeFromEvents: window,
    }).complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).not.toContain("rescue");
    expect(
      failureOf(await resume.repoStore.read("r"), "rescue").error.message,
    ).toMatch(/unit routed its failure to rescue, but a branch skipped rescue/);
  });

  // Two units may share a handler that waits on both. A success prunes it, so
  // when one unit has routed there, the other's success would leave that
  // failure unhandled.
  test("a shared handler fails when one unit routed to it and the other succeeds", async () => {
    const shared = defineWorkflow({
      id: "of-shared-handler",
      trigger: { type: "manual" },
      steps: {
        first: step({ agent: agent("first"), onFailure: "rescue" }),
        second: step({ agent: agent("second"), onFailure: "rescue" }),
        rescue: step({
          agent: agent("rescue"),
          after: ["first", "second"],
          input: { from: "steps.first.output" },
        }),
      },
    });
    const live = buildEnv(shared);
    await runtimeRun(shared, live.env, { runId: "r", triggerPayload: null })
      .complete;
    const [runStarted] = await live.repoStore.read("r");
    if (runStarted?.kind !== "RunStarted") throw new Error("no RunStarted");
    const at = runStarted.at;
    // `first` has routed and `second` has not run, so its success comes second.
    const window: WorkflowEvent[] = [
      runStarted,
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "first",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "StepFailed",
        seq: 3,
        at,
        stepId: "first",
        attempt: 1,
        error: { message: "first boom" },
        retriesExhausted: true,
        routedTo: "rescue",
      },
    ];

    const invoked: string[] = [];
    const resume = buildEnv(shared, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const res = await runtimeRun(shared, resume.env, {
      runId: "r",
      triggerPayload: null,
      resumeFromEvents: window,
    }).complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).toEqual(["second"]);
    expect(
      failureOf(await resume.repoStore.read("r"), "rescue").error.message,
    ).toMatch(/first routed its failure to rescue/);
  });

  test("an awaitSignal timeout that skips a handler after its unit routed there fails the handler and the run", async () => {
    const timedOut = defineWorkflow({
      id: "of-await-timeout",
      trigger: { type: "manual" },
      steps: {
        wait: awaitSignal({ name: "go", timeout: 20, onTimeout: "late" }),
        late: step({ agent: agent("late"), after: ["wait"] }),
        normal: step({ agent: agent("normal"), after: ["wait"] }),
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({
          agent: agent("rescue"),
          after: ["unit", "normal"],
          input: { from: "steps.unit.output" },
        }),
      },
    });
    const live = buildEnv(timedOut);
    await runtimeRun(timedOut, live.env, { runId: "r", triggerPayload: null })
      .complete;
    const [runStarted] = await live.repoStore.read("r");
    if (runStarted?.kind !== "RunStarted") throw new Error("no RunStarted");
    const at = runStarted.at;
    // The unit has routed and the gate has not started, so its timeout skip
    // reaches the handler second.
    const window: WorkflowEvent[] = [
      runStarted,
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "unit",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "StepFailed",
        seq: 3,
        at,
        stepId: "unit",
        attempt: 1,
        error: { message: "unit boom" },
        retriesExhausted: true,
        routedTo: "rescue",
      },
    ];

    const invoked: string[] = [];
    const resume = buildEnv(timedOut, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const res = await runtimeRun(timedOut, resume.env, {
      runId: "r",
      triggerPayload: null,
      resumeFromEvents: window,
    }).complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).not.toContain("rescue");
    expect(
      failureOf(await resume.repoStore.read("r"), "rescue").error.message,
    ).toMatch(/unit routed its failure to rescue, but a branch skipped rescue/);
  });

  test("a branch that reaches a handler already running lets it finish", async () => {
    // `unit` routes to `rescue`, which runs once g1's taken side `s` is done
    // and g1 skipped `d`. g2 then skips `y`, whose closure reaches `d` and the
    // running `rescue`: the handler must finish and keep its output.
    const def = defineWorkflow({
      id: "of-live-handler",
      trigger: { type: "manual" },
      steps: {
        g1: gate({ when: { from: "trigger.payload" }, then: "s", else: "d" }),
        s: step({ agent: agent("s"), after: ["g1"] }),
        slow: step({ agent: agent("slow") }),
        g2: gate({
          after: ["slow"],
          when: { from: "trigger.payload" },
          then: "k",
          else: "y",
        }),
        k: step({ agent: agent("k"), after: ["g2"] }),
        y: step({ agent: agent("y"), after: ["g2"] }),
        d: step({ agent: agent("d"), after: ["g1", "y"] }),
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({
          agent: agent("rescue"),
          after: ["unit", "d", "s"],
          input: { from: "trigger.payload" },
        }),
      },
    });
    const runId = "r";
    const g2Done = (events: readonly WorkflowEvent[]) =>
      events.some((e) => e.kind === "StepCompleted" && e.stepId === "g2");
    const rescueStarted = Promise.withResolvers<undefined>();
    const invoked: string[] = [];
    const { env, repoStore } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        if (req.agent.id === "unit") throw new Error("unit boom");
        if (req.agent.id === "slow") await rescueStarted.promise;
        if (req.agent.id === "rescue") {
          rescueStarted.resolve(undefined);
          while (
            !g2Done(bufferedEvents(runId)) &&
            !g2Done(await repoStore.read(runId))
          ) {
            await whenRunNextBuffers(runId);
          }
        }
        return { output: `${req.agent.id}-out` };
      },
    });
    const res = await runtimeRun(def, env, { runId, triggerPayload: true })
      .complete;

    expect(invoked).toContain("rescue");
    expect(res.terminalStatus).toBe("completed");
    expect(res.outputs["rescue"]).toBe("rescue-out");
  });
});
