// `env.onPark` firing at a control-plane suspension.
//
// When a workflow agent step parks on a reserved
// `signalName(correlationId)` channel (the suspend/resume bridge), the
// runtime body notifies the host via `env.onPark` so the host can register
// the correlation out-of-band. A plain `awaitSignal` gate parked on an
// author-chosen name is NOT a control-plane suspension, so it fires no
// notify. The emit is gated on the fresh `SignalAwaited` commit, so a single
// park fires exactly once.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName } from "@intx/types";
import type { ConversationTurn } from "@intx/types/runtime";

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
  type WorkflowPark,
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

function buildEnv(
  def: WorkflowDefinition,
  opts: {
    invokeStep: StepInvoker;
    signalChannel: SignalChannel;
    onPark: (park: WorkflowPark) => void;
  },
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  return {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
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
    onPark: opts.onPark,
  };
}

describe("env.onPark at a control-plane suspension", () => {
  test("an agent-step suspend fires onPark once with the correlation and approval kind", async () => {
    const oneStep = defineWorkflow({
      id: "park-suspend",
      trigger: { type: "manual" },
      steps: { s: step({ agent }) },
    });
    const channel = createInMemorySignalChannel();
    const parks: WorkflowPark[] = [];
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume === undefined) {
        return { suspend: { correlationId: "corr-1" } };
      }
      return { output: { reply: "done", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, {
      invokeStep,
      signalChannel: channel,
      onPark: (park) => parks.push(park),
    });

    const handle = runtimeRun(oneStep, env, { runId: "run-1" });

    // Let the first invocation land and the step park before delivering.
    await new Promise((r) => setTimeout(r, 50));

    // The park fired exactly once, carrying the correlation the suspend
    // returned and the approval kind (the only control-plane signal kind).
    expect(parks).toEqual([
      { runId: "run-1", correlationId: "corr-1", kind: "approval" },
    ]);

    await channel.deliver(signalName("corr-1"), { outcome: "approved" }, "s-1");

    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");

    // The completed run did not re-fire onPark for the same park.
    expect(parks).toHaveLength(1);
  });

  test("a plain awaitSignal gate on an author-chosen name fires no onPark", async () => {
    const gateWorkflow = defineWorkflow({
      id: "await-gate",
      trigger: { type: "manual" },
      steps: { g: awaitSignal({ name: "human-approval" }) },
    });
    const channel = createInMemorySignalChannel();
    const parks: WorkflowPark[] = [];
    const invokeStep: StepInvoker = async () => {
      throw new Error("awaitSignal must not invoke a step");
    };
    const env = buildEnv(gateWorkflow, {
      invokeStep,
      signalChannel: channel,
      onPark: (park) => parks.push(park),
    });

    const handle = runtimeRun(gateWorkflow, env, { runId: "run-2" });

    // Let the gate park on its author-chosen signal channel.
    await new Promise((r) => setTimeout(r, 50));

    // The gate parked (SignalAwaited committed) but no control-plane notify
    // fired -- "human-approval" is not a reserved `signalName(...)` channel.
    const parked = await env.repoStore.read("run-2");
    expect(parked.some((e) => e.kind === "SignalAwaited")).toBe(true);
    expect(parks).toEqual([]);

    await channel.deliver("human-approval", { ok: true }, "s-2");

    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(parks).toEqual([]);
  });
});
