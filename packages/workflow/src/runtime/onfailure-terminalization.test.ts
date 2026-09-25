// A unit carrying onFailure whose WORK succeeds but whose terminalization --
// pruning the handler branch, or committing StepCompleted -- fails (a transient
// durable-store blip) must NOT be inverted into a routed failure or retried.
// It lands a bare failure so the run fails loudly via the verdict. Before the
// fix, the terminalization throw fell into the same catch as an invocation
// failure and routed the succeeded unit to its onFailure handler.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  action,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  step,
  StepOutcomeCommitError,
  type Primitive,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

function agent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
}

// A deployed `{ ref }` childWorkflow unit resolved by the env's spawnChild.
function cwUnit(): Primitive {
  return {
    kind: "childWorkflow",
    id: "",
    definition: { ref: "child-ref" },
    drainBehavior: "cancel",
    onFailure: "rescue",
  };
}

// Wrap the in-memory blobs so recordOutput throws ONCE when its key matches --
// a deterministic transient store blip in the success-terminalization window.
// recordOutput is the single choke point: the unit's own StepCompleted records
// under its bare stepId, and each skip StepStarted records under `<id>.input`.
function faultingBlobs(
  inner: WorkflowRuntimeEnv["blobs"],
  faultOnKey: string,
): WorkflowRuntimeEnv["blobs"] {
  let armed = true;
  return {
    ...inner,
    async recordOutput(stepId, attempt, value) {
      if (armed && stepId === faultOnKey) {
        armed = false;
        throw new Error("store blip");
      }
      return inner.recordOutput(stepId, attempt, value);
    },
  };
}

function buildEnv(
  def: WorkflowDefinition,
  opts: {
    invokeAction?: WorkflowRuntimeEnv["invokeAction"];
    blobs?: WorkflowRuntimeEnv["blobs"];
  } = {},
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
    blobs: opts.blobs ?? createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: async () => ({ output: null }),
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
    ...(opts.invokeAction !== undefined
      ? { invokeAction: opts.invokeAction }
      : {}),
  };
  return { env, repoStore };
}

function unitRoutedTo(log: readonly WorkflowEvent[]): unknown {
  const failed = log.find(
    (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
      e.kind === "StepFailed" && e.stepId === "unit",
  );
  return failed?.routedTo;
}

// The handler reads the trigger rather than the unit's output, so it is
// invoked whenever the scheduler offers it, not only when the unit completed.
function rescueStep(): Primitive {
  return step({
    agent: agent("rescue"),
    after: ["unit"],
    input: { from: "trigger.payload" },
  });
}

function recordInvocations(env: WorkflowRuntimeEnv): (string | undefined)[] {
  const invoked: (string | undefined)[] = [];
  env.invokeStep = async (req) => {
    invoked.push(req.authzContext.stepId);
    return { output: null };
  };
  return invoked;
}

function stepDef(id: string): WorkflowDefinition {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: {
      unit: step({ agent: agent("unit"), onFailure: "rescue" }),
      rescue: rescueStep(),
      normal: step({ agent: agent("normal"), after: ["unit"] }),
    },
  });
}

describe("onFailure success-terminalization failures do not route", () => {
  test("a step whose StepCompleted commit fails lands a bare failure, not routed", async () => {
    const def = stepDef("term-step-complete");
    // Fault on the unit's own output key -- the StepCompleted after a
    // successful invocation.
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
    expect(invoked).not.toContain("rescue");
  });

  test("a step whose success prune fails lands a bare failure, not routed", async () => {
    const def = stepDef("term-step-prune");
    // Fault inside the success prune: the handler-branch skip StepStarted
    // records under `rescue.input`.
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "rescue.input"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
    expect(invoked).not.toContain("rescue");
  });

  test("an action whose StepCompleted commit fails lands a bare failure, not routed", async () => {
    const def = defineWorkflow({
      id: "term-action",
      trigger: { type: "manual" },
      steps: {
        unit: action({ handler: "do", onFailure: "rescue" }),
        rescue: rescueStep(),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const { env, repoStore } = buildEnv(def, {
      invokeAction: async () => ({ output: null }),
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
    expect(invoked).not.toContain("rescue");
  });

  test("a childWorkflow whose StepCompleted commit fails lands a bare failure, not routed", async () => {
    const def = defineWorkflow({
      id: "term-child",
      trigger: { type: "manual" },
      steps: {
        unit: cwUnit(),
        rescue: rescueStep(),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    // The child completes; the unit's StepCompleted (key "unit") then faults.
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
    expect(invoked).not.toContain("rescue");
  });

  test("a multi-attempt step whose terminalization fails is not re-invoked", async () => {
    // The sentinel branch sits above the retry branch: a step that already did
    // its work must not be re-invoked when its terminal fails to land.
    const def = defineWorkflow({
      id: "term-noreinvoke",
      trigger: { type: "manual" },
      steps: {
        unit: step({
          agent: agent("unit"),
          onFailure: "rescue",
          retry: { maxAttempts: 2, initialBackoffMs: 1 },
        }),
        rescue: rescueStep(),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const { env, repoStore } = buildEnv(def, {
      blobs: faultingBlobs(createInMemoryBlobSubstrate(), "unit"),
    });
    const invoked = recordInvocations(env);
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked.filter((id) => id === "unit")).toHaveLength(1);
    expect(invoked).not.toContain("rescue");
    expect(unitRoutedTo(await repoStore.read("r"))).toBeUndefined();
  });

  test("a multi-attempt step whose invoker could not commit its outcome is neither retried nor routed", async () => {
    // The host commits a cold step's turn inside the invocation, after the
    // agent ran; losing that commit must not re-run the agent's work.
    const def = defineWorkflow({
      id: "term-uncommitted-outcome",
      trigger: { type: "manual" },
      steps: {
        unit: step({
          agent: agent("unit"),
          onFailure: "rescue",
          retry: { maxAttempts: 2, initialBackoffMs: 1 },
        }),
        rescue: rescueStep(),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const invoked: (string | undefined)[] = [];
    const { env, repoStore } = buildEnv(def);
    env.invokeStep = async (req) => {
      invoked.push(req.authzContext.stepId);
      if (req.authzContext.stepId === "unit") {
        throw new StepOutcomeCommitError(new Error("push blip"));
      }
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked.filter((id) => id === "unit")).toHaveLength(1);
    expect(invoked).not.toContain("rescue");
    const log = await repoStore.read("r");
    expect(unitRoutedTo(log)).toBeUndefined();
    const failed = log.find(
      (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
        e.kind === "StepFailed" && e.stepId === "unit",
    );
    expect(failed?.retriesExhausted).toBe(true);
  });

  test("a multi-attempt step whose invocation fails once still retries and completes", async () => {
    // Boundary pin: an ordinary invocation failure on attempt 1 still retries
    // -- the sentinel only diverts a post-success terminalization throw.
    const def = defineWorkflow({
      id: "term-boundary",
      trigger: { type: "manual" },
      steps: {
        unit: step({
          agent: agent("unit"),
          onFailure: "rescue",
          retry: { maxAttempts: 2, initialBackoffMs: 1 },
        }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    let unitInvocations = 0;
    const { env } = buildEnv(def);
    env.invokeStep = async (req) => {
      if (req.authzContext.stepId === "unit") {
        unitInvocations += 1;
        if (unitInvocations === 1) throw new Error("attempt 1 boom");
      }
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("completed");
    expect(unitInvocations).toBe(2);
  });

  test("an unrouted failure whose handler branch cannot be skipped stops the run until resume skips it", async () => {
    // Neither the unit's output nor the skip of its handler branch can be
    // recorded. The handler reads the trigger, so it would be invoked if the
    // scheduler offered it.
    const def = defineWorkflow({
      id: "term-unskippable-handler",
      trigger: { type: "manual" },
      steps: {
        unit: step({ agent: agent("unit"), onFailure: "rescue" }),
        rescue: step({
          agent: agent("rescue"),
          after: ["unit"],
          input: { from: "trigger.payload" },
        }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const inner = createInMemoryBlobSubstrate();
    let storeDown = true;
    const { env, repoStore } = buildEnv(def, {
      blobs: {
        ...inner,
        async recordOutput(stepId, attempt, value) {
          if (stepId === "unit" || (storeDown && stepId === "rescue.input")) {
            throw new Error("store down");
          }
          return inner.recordOutput(stepId, attempt, value);
        },
      },
    });
    const invoked: string[] = [];
    env.invokeStep = async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    };

    await expect(
      runtimeRun(def, env, { runId: "r", triggerPayload: null }).complete,
    ).rejects.toThrow("store down");
    expect(invoked).toEqual(["unit"]);
    // The failure is durable, so resume does not read the unit as a crash
    // mid-invocation, which would route it.
    const failed = (await repoStore.read("r")).find(
      (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
        e.kind === "StepFailed" && e.stepId === "unit",
    );
    expect(failed?.retriesExhausted).toBe(true);
    expect(failed?.routedTo).toBeUndefined();

    storeDown = false;
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("failed");
    expect(invoked).not.toContain("rescue");
    const skip = res.events.find(
      (e): e is Extract<WorkflowEvent, { kind: "StepCompleted" }> =>
        e.kind === "StepCompleted" && e.stepId === "rescue",
    );
    if (skip === undefined) throw new Error("rescue was not skipped");
    expect(await env.blobs.resolveRef(skip.output.ref)).toEqual({
      skipped: true,
      onFailureStepId: "unit",
      settled: "failed",
    });
  });
});
