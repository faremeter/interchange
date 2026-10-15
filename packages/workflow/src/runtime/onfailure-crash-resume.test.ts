// Crash-resume of an onFailure route. A routed unit emits no StepCompleted, so
// its live failure sentinel lives only in the in-process stepOutputs map. On
// resume the pre-loop reconciliation must reconstruct it from the routed
// StepFailed, or the handler -- which reads `steps.<unit>.output.error.message`
// -- fails its input selector against an absent output.
//
// The crash is modeled by slicing the runtime's OWN durably-emitted log at the
// after-routed-StepFailed / before-handler window and re-driving from that
// prefix, so the emitter and the resume path are proven against each other.
//
// A unit whose work finished before the host died must not route at all: its
// StepCompleted has to be durable before the next save point.

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
  type ActionInvoker,
  type RepoStore,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { createTerminalChildHandle } from "./suspendable-child-handle";

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
  invokeStep: StepInvoker,
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
    invokeStep,
    spawnChild: async () => createTerminalChildHandle("completed"),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
  };
  return { env, repoStore };
}

const def = defineWorkflow({
  id: "onfailure-resume",
  trigger: { type: "manual" },
  steps: {
    unit: step({ agent: agent("unit"), onFailure: "rescue" }),
    rescue: step({
      agent: agent("rescue"),
      after: ["unit"],
      input: { from: "steps.unit.output" },
    }),
    normal: step({ agent: agent("normal"), after: ["unit"] }),
  },
});

const actionDef = defineWorkflow({
  id: "onfailure-resume-action",
  trigger: { type: "manual" },
  steps: {
    unit: action({ handler: "unit", onFailure: "rescue" }),
    rescue: action({ handler: "rescue", after: ["unit"] }),
    normal: action({ handler: "normal", after: ["unit"] }),
  },
});

class HostDied extends Error {
  constructor() {
    super("host died before the next save point");
    this.name = "HostDied";
  }
}

// The host dies at the first write that would land anything after the unit's
// StepCompleted: the save point a buffered completion waits for.
function diesAfterUnitCompletes(inner: RepoStore): RepoStore {
  let completed = false;
  function admit(events: readonly WorkflowEvent[]): void {
    if (completed) throw new HostDied();
    const at = events.findIndex(
      (e) => e.kind === "StepCompleted" && e.stepId === "unit",
    );
    if (at === -1) return;
    if (at !== events.length - 1) throw new HostDied();
    completed = true;
  }
  return {
    ...inner,
    async append(runId, event) {
      admit([event]);
      await inner.append(runId, event);
    },
    async appendBatch(runId, events) {
      admit(events);
      await inner.appendBatch(runId, events);
    },
  };
}

describe("onFailure route crash-resume", () => {
  test("a routed unit's sentinel is reconstructed so the handler resolves on resume", async () => {
    // Live run: the unit fails, routes to rescue, and the run completes.
    const live = buildEnv(def, async (req) => {
      if (req.agent.id === "unit") throw new Error("unit boom");
      return { output: null };
    });
    const liveRes = await runtimeRun(def, live.env, {
      runId: "run-live",
      triggerPayload: null,
    }).complete;
    expect(liveRes.terminalStatus).toBe("completed");

    const emitted = await live.repoStore.read("run-live");
    // Crash window: after the unit's routed StepFailed is durable, before the
    // handler runs. The prune skips for `normal` precede the StepFailed (one
    // atomic batch), so they are in the window too.
    const routedIdx = emitted.findIndex(
      (e) =>
        e.kind === "StepFailed" &&
        e.stepId === "unit" &&
        e.routedTo === "rescue",
    );
    expect(routedIdx).toBeGreaterThan(-1);
    const handlerStartIdx = emitted.findIndex(
      (e) => e.kind === "StepStarted" && e.stepId === "rescue",
    );
    expect(handlerStartIdx).toBeGreaterThan(routedIdx);
    const window = emitted.slice(0, handlerStartIdx);

    // Resume from the window. The unit is already routed, so it must NOT be
    // re-invoked; the handler runs and reads the reconstructed sentinel.
    const invoked: string[] = [];
    let handlerInput: unknown;
    const resume = buildEnv(def, async (req) => {
      invoked.push(req.agent.id);
      if (req.agent.id === "rescue") handlerInput = req.input;
      return { output: null };
    });
    const resumedRes = await runtimeRun(def, resume.env, {
      runId: "run-live",
      resumeFromEvents: window,
    }).complete;

    expect(resumedRes.terminalStatus).toBe("completed");
    expect(invoked).not.toContain("unit"); // the routed unit is not re-driven
    expect(invoked).not.toContain("normal"); // its normal dependent stays pruned
    expect(invoked).toContain("rescue");
    // The handler resolved `steps.unit.output` -- the reconstructed sentinel.
    expect(handlerInput).toEqual({
      failed: true,
      stepId: "unit",
      error: { message: "unit boom" },
    });
  });

  test("a unit that crashes mid-invocation routes to its handler on resume", async () => {
    // Live run so the unit gets a durable StepStarted; its work is irrelevant.
    const live = buildEnv(def, async () => ({ output: null }));
    const liveRes = await runtimeRun(def, live.env, {
      runId: "run-crash",
      triggerPayload: null,
    }).complete;
    expect(liveRes.terminalStatus).toBe("completed");

    const emitted = await live.repoStore.read("run-crash");
    // Slice right after the unit's StepStarted: the unit is in-flight with no
    // terminal and no prune -- the crash-mid-invocation window.
    const startedIdx = emitted.findIndex(
      (e) => e.kind === "StepStarted" && e.stepId === "unit",
    );
    expect(startedIdx).toBeGreaterThan(-1);
    const window = emitted.slice(0, startedIdx + 1);
    expect(
      window.some(
        (e) =>
          (e.kind === "StepFailed" || e.kind === "StepCompleted") &&
          e.stepId === "unit",
      ),
    ).toBe(false);

    const invoked: string[] = [];
    let handlerInput: unknown;
    const resume = buildEnv(def, async (req) => {
      invoked.push(req.agent.id);
      if (req.agent.id === "rescue") handlerInput = req.input;
      return { output: null };
    });
    const resumedRes = await runtimeRun(def, resume.env, {
      runId: "run-crash",
      resumeFromEvents: window,
    }).complete;

    expect(resumedRes.terminalStatus).toBe("completed");
    // The crashed unit is not re-invoked (at-most-once); it routes instead of
    // going fatal, its normal dependent stays pruned, and the handler runs.
    expect(invoked).not.toContain("unit");
    expect(invoked).not.toContain("normal");
    expect(invoked).toContain("rescue");
    // The reconstructed sentinel the handler reads is message-only: the crash
    // `code` must not leak into steps.<unit>.output (toEqual is exact, so an
    // extra `code` key would fail), though the durable event keeps it below.
    expect(handlerInput).toEqual({
      failed: true,
      stepId: "unit",
      error: {
        message:
          "step unit crashed mid-invocation; the invoked primitive is non-deterministic and unrecorded, so it is not re-invoked (at-most-once)",
      },
    });

    const routed = resumedRes.events.find(
      (e): e is Extract<WorkflowEvent, { kind: "StepFailed" }> =>
        e.kind === "StepFailed" && e.stepId === "unit",
    );
    expect(routed?.routedTo).toBe("rescue");
    expect(routed?.error.code).toBe("crash-mid-invocation");
  });

  test("a unit that finished before the host died does not route on resume", async () => {
    const inner = createInMemoryRepoStore();
    const live = buildEnv(def, async () => ({ output: { done: true } }));
    await expect(
      runtimeRun(
        def,
        { ...live.env, repoStore: diesAfterUnitCompletes(inner) },
        { runId: "run-finished", triggerPayload: null },
      ).complete,
    ).rejects.toBeInstanceOf(HostDied);

    const durable = await inner.read("run-finished");
    expect(
      durable.some((e) => e.kind === "StepCompleted" && e.stepId === "unit"),
    ).toBe(true);

    const invoked: string[] = [];
    const resume = buildEnv(def, async (req) => {
      invoked.push(req.agent.id);
      return { output: null };
    });
    const resumedRes = await runtimeRun(def, resume.env, {
      runId: "run-finished",
      resumeFromEvents: durable,
    }).complete;

    expect(resumedRes.terminalStatus).toBe("completed");
    expect(invoked).toEqual(["normal"]);
    expect(
      resumedRes.events.some(
        (e) => e.kind === "StepFailed" && e.stepId === "unit",
      ),
    ).toBe(false);
  });

  test("an action that finished before the host died does not route on resume", async () => {
    const inner = createInMemoryRepoStore();
    const live = buildEnv(actionDef, async () => ({ output: null }));
    await expect(
      runtimeRun(
        actionDef,
        {
          ...live.env,
          repoStore: diesAfterUnitCompletes(inner),
          invokeAction: async () => ({ output: { done: true } }),
        },
        { runId: "run-action-finished", triggerPayload: null },
      ).complete,
    ).rejects.toBeInstanceOf(HostDied);

    const durable = await inner.read("run-action-finished");
    expect(
      durable.some((e) => e.kind === "StepCompleted" && e.stepId === "unit"),
    ).toBe(true);

    const invoked: string[] = [];
    const invokeAction: ActionInvoker = async ({ handler }) => {
      invoked.push(handler);
      return { output: null };
    };
    const resume = buildEnv(actionDef, async () => ({ output: null }));
    const resumedRes = await runtimeRun(
      actionDef,
      { ...resume.env, invokeAction },
      { runId: "run-action-finished", resumeFromEvents: durable },
    ).complete;

    expect(resumedRes.terminalStatus).toBe("completed");
    expect(invoked).toEqual(["normal"]);
    expect(
      resumedRes.events.some(
        (e) => e.kind === "StepFailed" && e.stepId === "unit",
      ),
    ).toBe(false);
  });
});
