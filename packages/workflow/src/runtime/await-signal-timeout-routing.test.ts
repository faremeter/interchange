// awaitSignal onTimeout routing. A timed gate whose timer fires routes to its
// onTimeout target (a successor) instead of failing, pruning the normal
// successor branch via the same skip-sentinel mechanism a gate/loop uses; a
// delivered signal takes the normal branch and prunes the onTimeout branch. A
// timed gate with NO onTimeout preserves the prior fail-on-timeout behavior.
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
  runtimeRun,
  step,
  type SignalChannel,
  type StepInvoker,
  type WorkflowDefinition,
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

function buildEnv(
  def: WorkflowDefinition,
  opts: { invokeStep?: StepInvoker; signalChannel?: SignalChannel } = {},
): {
  env: WorkflowRuntimeEnv;
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
} {
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: opts.signalChannel ?? createInMemorySignalChannel(),
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
    drain: createNoopDrainController(def),
  };
  return { env, repoStore };
}

describe("awaitSignal onTimeout routing", () => {
  test("a fired timer routes to onTimeout and prunes the normal successor", async () => {
    const def = defineWorkflow({
      id: "to-route",
      trigger: { type: "manual" },
      steps: {
        gate: awaitSignal({ name: "go", timeout: 20, onTimeout: "recover" }),
        recover: step({ agent: agent("recover"), after: ["gate"] }),
        normal: step({ agent: agent("normal"), after: ["gate"] }),
      },
    });
    const invoked: string[] = [];
    const { env, repoStore } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    // No signal delivered: the 20ms timer fires and routes to recover.
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    const res = await run.complete;

    expect(res.terminalStatus).toBe("completed");
    // The onTimeout target ran; the normal successor was pruned (never invoked).
    expect(invoked).toEqual(["recover"]);
    const log = await repoStore.read("r");
    expect(
      log.some((e) => e.kind === "StepCompleted" && e.stepId === "gate"),
    ).toBe(true);
    // normal still reaches a terminal StepCompleted -- but as a skip-sentinel.
    expect(
      log.some((e) => e.kind === "StepCompleted" && e.stepId === "normal"),
    ).toBe(true);
    expect(log.some((e) => e.kind === "RunFailed")).toBe(false);
  });

  test("a delivered signal takes the normal branch and prunes the onTimeout branch", async () => {
    const def = defineWorkflow({
      id: "sig-route",
      trigger: { type: "manual" },
      steps: {
        gate: awaitSignal({ name: "go", timeout: 5_000, onTimeout: "recover" }),
        recover: step({ agent: agent("recover"), after: ["gate"] }),
        normal: step({ agent: agent("normal"), after: ["gate"] }),
      },
    });
    const invoked: string[] = [];
    const channel = createInMemorySignalChannel();
    const { env, repoStore } = buildEnv(def, {
      signalChannel: channel,
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    // Deliver the signal well before the 5s timer; the signal wins.
    await channel.deliver("go", { ok: true }, "sig-1");
    const res = await run.complete;

    expect(res.terminalStatus).toBe("completed");
    // The normal successor ran; the onTimeout branch was pruned.
    expect(invoked).toEqual(["normal"]);
    const log = await repoStore.read("r");
    expect(
      log.some((e) => e.kind === "SignalReceived" && e.signalName === "go"),
    ).toBe(true);
  });

  test("a diamond join downstream of both branches stays live after a timeout", async () => {
    const def = defineWorkflow({
      id: "to-diamond",
      trigger: { type: "manual" },
      steps: {
        gate: awaitSignal({ name: "go", timeout: 20, onTimeout: "recover" }),
        recover: step({ agent: agent("recover"), after: ["gate"] }),
        normal: step({ agent: agent("normal"), after: ["gate"] }),
        join: step({ agent: agent("join"), after: ["recover", "normal"] }),
      },
    });
    const invoked: string[] = [];
    const { env } = buildEnv(def, {
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
    });
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    const res = await run.complete;

    expect(res.terminalStatus).toBe("completed");
    // recover ran, normal pruned, and the merge step still ran (it is reachable
    // from the taken branch, so collectBranchClosure does not prune it).
    expect(invoked).toContain("recover");
    expect(invoked).toContain("join");
    expect(invoked).not.toContain("normal");
  });

  test("a timed gate with no onTimeout still fails on timeout", async () => {
    const def = defineWorkflow({
      id: "to-fail",
      trigger: { type: "manual" },
      steps: {
        gate: awaitSignal({ name: "go", timeout: 20 }),
      },
    });
    const { env, repoStore } = buildEnv(def);
    // No signal, no onTimeout: the fired timer fails the step (prior behavior).
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    const res = await run.complete;

    expect(res.terminalStatus).toBe("failed");
    const log = await repoStore.read("r");
    expect(
      log.some(
        (e) =>
          e.kind === "StepFailed" &&
          e.stepId === "gate" &&
          /timed out/.test(e.error.message),
      ),
    ).toBe(true);
  });
});
