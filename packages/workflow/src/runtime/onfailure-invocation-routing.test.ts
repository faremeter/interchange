// onFailure routing for the other two invocation units, `action` and
// `childWorkflow`. Both route their permanent failure in runPrimitiveSafe's
// stillRunning arm (that is where their terminal StepFailed is committed,
// unlike `step`). The childWorkflow carve-out: only a `failed` child routes; a
// `cancelled` child keeps its bare-failure disposition so an operator's stop
// is not mistaken for a failure the fallback handler should service.

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

// A childWorkflow reaches the runtime as a deployed `{ ref }` (an authored
// inline child is a deploy-step bug the runtime rejects), resolved by the env's
// spawnChild. Hand-build the deployed shape so the test drives the real
// spawn path.
function cwUnit(): Primitive {
  return {
    kind: "childWorkflow",
    id: "",
    definition: { ref: "child-ref" },
    drainBehavior: "cancel",
    onFailure: "rescue",
  };
}

function buildEnv(
  def: WorkflowDefinition,
  opts: {
    invokeAction?: WorkflowRuntimeEnv["invokeAction"];
    spawnChild?: WorkflowRuntimeEnv["spawnChild"];
    drain?: WorkflowRuntimeEnv["drain"];
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
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: async () => ({ output: null }),
    spawnChild:
      opts.spawnChild ?? (async () => ({ terminalStatus: "completed" })),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: opts.drain ?? createNoopDrainController(def),
    ...(opts.invokeAction !== undefined
      ? { invokeAction: opts.invokeAction }
      : {}),
  };
  return { env, repoStore };
}

function routedTo(log: readonly WorkflowEvent[], stepId: string): unknown {
  const failed = log.find(
    (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
      e.kind === "StepFailed" && e.stepId === stepId,
  );
  if (!failed) throw new Error(`no StepFailed for ${stepId}`);
  return failed.routedTo;
}

function abortedDrain(): WorkflowRuntimeEnv["drain"] {
  const controller = new AbortController();
  controller.abort();
  return { signal: controller.signal, behaviorFor: () => "cancel" };
}

describe("onFailure action / childWorkflow routing", () => {
  test("a failing action routes to its handler and the run completes", async () => {
    const invoked: string[] = [];
    const def = defineWorkflow({
      id: "act-route",
      trigger: { type: "manual" },
      steps: {
        unit: action({ handler: "do", onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const { env, repoStore } = buildEnv(def, {
      invokeAction: async () => {
        throw new Error("action boom");
      },
    });
    // Track only agent steps; the action has no agent.
    env.invokeStep = async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toContain("rescue");
    expect(invoked).not.toContain("normal");
    expect(routedTo(await repoStore.read("r"), "unit")).toBe("rescue");
  });

  test("a failed child routes to its handler and the run completes", async () => {
    const invoked: string[] = [];
    const def = defineWorkflow({
      id: "cw-route",
      trigger: { type: "manual" },
      steps: {
        unit: cwUnit(),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const { env, repoStore } = buildEnv(def, {
      spawnChild: async () => ({ terminalStatus: "failed" }),
    });
    env.invokeStep = async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toContain("rescue");
    expect(invoked).not.toContain("normal");
    expect(routedTo(await repoStore.read("r"), "unit")).toBe("rescue");
  });

  test("a cancelled child does NOT route -- it keeps its bare failure", async () => {
    const def = defineWorkflow({
      id: "cw-cancel",
      trigger: { type: "manual" },
      steps: {
        unit: cwUnit(),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
      },
    });
    const { env, repoStore } = buildEnv(def, {
      spawnChild: async () => ({ terminalStatus: "cancelled" }),
    });
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    // The carve-out: a cancelled child is a bare failure, so the run fails and
    // the unit's StepFailed carries no routedTo (it was not routed).
    expect(res.terminalStatus).toBe("failed");
    expect(routedTo(await repoStore.read("r"), "unit")).toBeUndefined();
  });

  test("a succeeding action prunes its handler branch", async () => {
    const invoked: string[] = [];
    const def = defineWorkflow({
      id: "act-success",
      trigger: { type: "manual" },
      steps: {
        unit: action({ handler: "do", onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const { env } = buildEnv(def, {
      invokeAction: async () => ({ output: null }),
    });
    env.invokeStep = async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toContain("normal");
    expect(invoked).not.toContain("rescue");
  });

  test("a succeeding childWorkflow prunes its handler branch", async () => {
    const invoked: string[] = [];
    const def = defineWorkflow({
      id: "cw-success",
      trigger: { type: "manual" },
      steps: {
        unit: cwUnit(),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const { env } = buildEnv(def, {
      spawnChild: async () => ({ terminalStatus: "completed" }),
    });
    env.invokeStep = async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toContain("normal");
    expect(invoked).not.toContain("rescue");
  });

  test("a failing action under drain still prunes the normal branch", async () => {
    const invoked: string[] = [];
    const def = defineWorkflow({
      id: "act-drain",
      trigger: { type: "manual" },
      steps: {
        unit: action({ handler: "do", onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const { env } = buildEnv(def, {
      drain: abortedDrain(),
      invokeAction: async () => {
        throw new Error("action boom");
      },
    });
    env.invokeStep = async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    };
    const res = await runtimeRun(def, env, { runId: "r", triggerPayload: null })
      .complete;
    void res;
    expect(invoked).not.toContain("normal");
  });

  test("a cancel landing before an action fails settles it cancelled, not routed", async () => {
    // Cancellation wins over routing: a cancel committed before the action
    // throws makes the safe runner's catch reload a cancelling phase, so the
    // unit settles cancelled rather than routed.
    const def = defineWorkflow({
      id: "act-cancel",
      trigger: { type: "manual" },
      steps: {
        unit: action({ handler: "do", onFailure: "rescue" }),
        rescue: step({ agent: agent("rescue"), after: ["unit"] }),
        normal: step({ agent: agent("normal"), after: ["unit"] }),
      },
    });
    const handleRef: { current?: ReturnType<typeof runtimeRun> } = {};
    const { env, repoStore } = buildEnv(def, {
      invokeAction: async () => {
        await handleRef.current?.cancel("supervisor-operator", "test");
        throw new Error("action boom");
      },
    });
    handleRef.current = runtimeRun(def, env, {
      runId: "r",
      triggerPayload: null,
    });
    const res = await handleRef.current.complete;

    expect(res.terminalStatus).toBe("cancelled");
    // The unit is CancelPropagated, not StepFailed -- so it was never routed.
    const unitFailed = (await repoStore.read("r")).find(
      (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
        e.kind === "StepFailed" && e.stepId === "unit",
    );
    expect(unitFailed?.routedTo).toBeUndefined();
  });
});
