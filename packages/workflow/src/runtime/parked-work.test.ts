// What a host may stop while a step is parked: a parked step waits idle, but
// a map around it or a timeout on it lives only in the process, so the
// process stays counted as working.

import { afterEach, expect, test } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName } from "@intx/types";
import type { ApprovalSnapshot, ConversationTurn } from "@intx/types/runtime";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  map,
  runtimeRun,
  step,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { waitForEvent } from "@intx/workflow/testing";
import { isQuiet, isWorking, whenQuiet } from "./activity";
import { createTerminalChildHandle } from "./suspendable-child-handle";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "m" }] },
});

const snapshot: ApprovalSnapshot = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

const reply: ConversationTurn = {
  role: "assistant",
  content: [{ type: "text", text: "done" }],
  timestamp: 0,
};

// The first call parks for approval; the call after the decision replies.
const parkOnce: StepInvoker = async (req) =>
  req.resume === undefined
    ? {
        suspend: {
          correlationId: "corr-park",
          kind: "approval",
          approvalSnapshot: snapshot,
        },
      }
    : { output: { reply: "done", turn: reply } };

const finishing: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const finish of finishing.splice(0)) await finish();
});

// Runs `definition` until its step parks.
async function park(
  definition: WorkflowDefinition,
  parkedStepId: string,
): Promise<void> {
  if (!isQuiet())
    throw new Error("an earlier test left work or a delivery running");
  const repoStore = createInMemoryRepoStore();
  const channel = createInMemorySignalChannel();
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock: () => new Date() }),
    signalChannel: channel,
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: parkOnce,
    spawnChild: async () => createTerminalChildHandle("completed"),
    clock: () => new Date(),
    newId: (prefix) => `${prefix}-id`,
    drain: createNoopDrainController(definition),
    hasUpstreamSignalResolver: true,
  };
  const run = runtimeRun(definition, env, { runId: "run-parked" });
  finishing.push(async () => {
    await channel.deliver(
      signalName("corr-park"),
      { outcome: "approved" },
      "sig-park",
    );
    await run.complete;
  });
  await waitForEvent(
    repoStore,
    "run-parked",
    (event) => event.kind === "SignalAwaited" && event.stepId === parkedStepId,
  );
}

// Whether the process still counts as working once it settled into the park.
// Everything in memory after the park is microtasks, so once they drain the
// step waits on its signal, and only a count held across that wait is left.
async function workingWhileParked(
  definition: WorkflowDefinition,
  parkedStepId: string,
): Promise<boolean> {
  await park(definition, parkedStepId);
  await new Promise((resolve) => setImmediate(resolve));
  return isWorking();
}

test("a parked step waits idle", async () => {
  const definition = defineWorkflow({
    id: "parked-step",
    trigger: { type: "manual" },
    steps: { s: step({ agent }) },
  });
  await park(definition, "s");
  await expect(whenQuiet()).resolves.toBeUndefined();
});

test("a map whose item is parked counts as working, since resume cannot continue it", async () => {
  const definition = defineWorkflow({
    id: "parked-map",
    trigger: { type: "manual" },
    steps: {
      items: map({ over: { literal: ["a"] }, step: step({ agent }) }),
    },
  });
  expect(await workingWhileParked(definition, "items[0]")).toBe(true);
});

test("a parked step with a timeout counts as working, since its deadline lives in the process", async () => {
  const definition = defineWorkflow({
    id: "parked-timed-step",
    trigger: { type: "manual" },
    steps: { s: step({ agent, timeout: 300_000 }) },
  });
  expect(await workingWhileParked(definition, "s")).toBe(true);
});
