// The resume classifier recovers a step that crashed across the park boundary.
//
// When an agent step's `StepStarted` is durable but its `SignalAwaited` never
// flushed (the crash-mid-park window), the step reduces to `in-flight` and is
// otherwise settled as a terminal crash-mid-invocation failure. When the host
// wires `readParkedApprovalOps` and the reactor left a durable pending approval
// operation, the classifier instead reconstructs the missing `SignalAwaited`,
// re-parks the step on the original correlation channel WITHOUT re-invoking the
// agent, and the run resumes on an approver decision. Absent the binding, or
// with no pending op, the step still settles terminal -- the pre-recovery
// behavior.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { correlationIdFromSignalName, signalName } from "@intx/types";
import type { ConversationTurn } from "@intx/types/runtime";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  resumeFromLog,
  runtimeRun,
  step,
  type ParkedApprovalOp,
  type SignalChannel,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "m" }] },
});

const replyTurn: ConversationTurn = {
  role: "assistant",
  content: [{ type: "text", text: "done" }],
  timestamp: 0,
};

const at = new Date().toISOString();

function buildEnv(
  def: WorkflowDefinition,
  opts: {
    repoStore: ReturnType<typeof createInMemoryRepoStore>;
    invokeStep: StepInvoker;
    signalChannel: SignalChannel;
    readParkedApprovalOps?: WorkflowRuntimeEnv["readParkedApprovalOps"];
  },
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  return {
    repoStore: opts.repoStore,
    scheduler: createInMemoryScheduler({ repoStore: opts.repoStore, clock }),
    signalChannel: opts.signalChannel,
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: opts.invokeStep,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
    ...(opts.readParkedApprovalOps !== undefined
      ? { readParkedApprovalOps: opts.readParkedApprovalOps }
      : {}),
  };
}

// Seed the crash-mid-park window: RunStarted + StepStarted are durable, the
// park's SignalAwaited never flushed, so the step reduces to in-flight.
async function seedCrashedPark(
  repoStore: ReturnType<typeof createInMemoryRepoStore>,
  runId: string,
): Promise<void> {
  await repoStore.append(runId, {
    kind: "RunStarted",
    seq: 1,
    at,
    runId,
    definitionHash: "x",
    trigger: { type: "manual", payload: undefined },
  });
  await repoStore.append(runId, {
    kind: "StepStarted",
    seq: 2,
    at,
    stepId: "s",
    attempt: 1,
    input: { ref: "inline:null" },
  });
}

const oneStep = defineWorkflow({
  id: "resume-crashed-park",
  trigger: { type: "manual" },
  steps: { s: step({ agent }) },
});

describe("resume classifier recovers a crash-mid-park approval step", () => {
  test("a durable pending approval op re-parks the step without re-invoking the agent", async () => {
    const runId = "run-crashed-park";
    const corr = "corr-crashed-park";
    const channel = createInMemorySignalChannel();
    const repoStore = createInMemoryRepoStore();
    const readCalls: { runId: string; stepId: string; attempt: number }[] = [];
    let invoked = 0;
    const invokeStep: StepInvoker = async (req) => {
      invoked += 1;
      // The agent runs only on the RESUME invoke carrying the decision, never
      // as a fresh re-invocation of the crashed park.
      if (req.resume === undefined) {
        throw new Error("crashed park must not re-invoke the agent fresh");
      }
      return { output: { reply: "done", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, {
      repoStore,
      invokeStep,
      signalChannel: channel,
      readParkedApprovalOps: async (args) => {
        readCalls.push(args);
        return [{ correlationId: corr }];
      },
    });

    await seedCrashedPark(repoStore, runId);

    const handle = runtimeRun(oneStep, env, { runId });
    await new Promise((r) => setTimeout(r, 50));

    // The classifier consulted the binding for exactly the crashed step-attempt.
    expect(readCalls).toEqual([{ runId, stepId: "s", attempt: 1 }]);
    // The agent was not re-invoked as a fresh park.
    expect(invoked).toBe(0);

    // A reconstructed SignalAwaited on the original correlation channel moved
    // the step to durable awaiting-signal.
    const events = await repoStore.read(runId);
    const controlPlaneAwaited = events.filter(
      (e) =>
        e.kind === "SignalAwaited" &&
        correlationIdFromSignalName(e.signalName) === corr,
    );
    expect(controlPlaneAwaited).toHaveLength(1);
    expect(resumeFromLog(runId, events).steps.get("s")?.phase).toBe(
      "awaiting-signal",
    );

    // An approver decision resumes the run to completion, invoking the agent
    // exactly once (the resume invoke).
    await channel.deliver(signalName(corr), { outcome: "approved" }, "sig-1");
    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(invoked).toBe(1);
  });

  test("no durable pending approval op settles the crashed step terminal", async () => {
    const runId = "run-crashed-park-empty";
    const channel = createInMemorySignalChannel();
    const repoStore = createInMemoryRepoStore();
    let invoked = 0;
    const invokeStep: StepInvoker = async () => {
      invoked += 1;
      return { output: { reply: "done", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, {
      repoStore,
      invokeStep,
      signalChannel: channel,
      // Binding wired but the store holds no pending approval op: the classifier
      // takes the terminal-failure fallback, unchanged from the no-binding path.
      readParkedApprovalOps: async () => [],
    });

    await seedCrashedPark(repoStore, runId);

    const result = await runtimeRun(oneStep, env, { runId }).complete;

    expect(invoked).toBe(0);
    expect(result.terminalStatus).toBe("failed");
    const failures = result.events.filter((e) => e.kind === "StepFailed");
    expect(failures.length).toBeGreaterThan(0);
    for (const f of failures) {
      if (f.kind !== "StepFailed") throw new Error("unreachable");
      expect(f.error.code).toBe("crash-mid-invocation");
    }
  });

  test("more than one durable pending approval op for a step-attempt fails loud", async () => {
    const runId = "run-crashed-park-dup";
    const channel = createInMemorySignalChannel();
    const repoStore = createInMemoryRepoStore();
    const invokeStep: StepInvoker = async () => ({
      output: { reply: "done", turn: replyTurn },
    });
    const twoOps: ParkedApprovalOp[] = [
      { correlationId: "corr-a" },
      { correlationId: "corr-b" },
    ];
    const env = buildEnv(oneStep, {
      repoStore,
      invokeStep,
      signalChannel: channel,
      readParkedApprovalOps: async () => twoOps,
    });

    await seedCrashedPark(repoStore, runId);

    await expect(runtimeRun(oneStep, env, { runId }).complete).rejects.toThrow(
      /parks on at most one control-plane suspension/,
    );
  });
});
