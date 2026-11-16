// A step that fails while it waits on an approval retires that approval. The
// run can go on without the step, to a retry, to the step's handler, or on its
// other branches, and the failure names only the step: without the retire
// nothing in the log says which approval is no longer wanted, and whoever
// offers it for a decision keeps offering it.

import { describe, test, expect } from "bun:test";

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
  runtimeRun,
  step,
  type RepoStore,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { waitForEvent } from "@intx/workflow/testing";
import { createTerminalChildHandle } from "./suspendable-child-handle";

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

const snapshot: ApprovalSnapshot = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

// Every ask parks on an approval named by `correlationOf`, and nothing ever
// decides one, so a timed step leaves its approval by its deadline alone.
function buildEnv(
  def: WorkflowDefinition,
  repoStore: RepoStore,
  correlationOf: (ask: { stepId?: string; attempt?: number }) => string,
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const invokeStep: StepInvoker = async (req) => {
    if (req.resume !== undefined) {
      return { output: { reply: "done", turn: replyTurn } };
    }
    return {
      suspend: {
        correlationId: correlationOf(req.authzContext),
        kind: "approval",
        approvalSnapshot: snapshot,
      },
    };
  };
  return {
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
}

const retiresOf = (log: readonly WorkflowEvent[]): string[] =>
  log.flatMap((event) =>
    event.kind === "SignalAwaitAbandoned" ? [event.signalName] : [],
  );

describe("a step that fails while it waits on an approval", () => {
  test("retires the approval before the failure that leads to its retry", async () => {
    const runId = "retry-retires-approval";
    const def = defineWorkflow({
      id: "timed-approval-retry",
      trigger: { type: "manual" },
      steps: {
        ask: step({
          agent,
          timeout: 20,
          retry: { maxAttempts: 2, initialBackoffMs: 1 },
        }),
      },
    });
    const repoStore = createInMemoryRepoStore();

    const result = await runtimeRun(
      def,
      buildEnv(def, repoStore, (ask) => `corr-${String(ask.attempt)}`),
      { runId },
    ).complete;

    // The second attempt times out as well and the run fails, so the log is
    // the same however slow the machine is.
    expect(result.terminalStatus).toBe("failed");
    const log = await repoStore.read(runId);
    const failedFirst = log.findIndex(
      (event) => event.kind === "StepFailed" && event.attempt === 1,
    );
    expect(log[failedFirst - 1]).toMatchObject({
      kind: "SignalAwaitAbandoned",
      stepId: "ask",
      signalName: signalName("corr-1"),
    });
    expect(
      log.findIndex(
        (event) =>
          event.kind === "SignalAwaited" &&
          event.signalName === signalName("corr-2"),
      ),
    ).toBeGreaterThan(failedFirst);
    expect(retiresOf(log)).toEqual([
      signalName("corr-1"),
      signalName("corr-2"),
    ]);
  });

  test("retires the approval in the durable log when its failure is final and another step keeps the run going", async () => {
    const runId = "final-failure-retires-approval";
    const def = defineWorkflow({
      id: "timed-approval-final",
      trigger: { type: "manual" },
      steps: {
        ask: step({ agent, timeout: 20 }),
        other: step({ agent }),
      },
    });
    const repoStore = createInMemoryRepoStore();
    const run = runtimeRun(
      def,
      buildEnv(def, repoStore, (ask) => `corr-${String(ask.stepId)}`),
      { runId },
    );
    const ended = run.complete.then((done) => {
      throw new Error(`the run ended ${done.terminalStatus}`);
    });
    void ended.catch(() => undefined);

    // `other` has no deadline and nothing decides its approval, so the run
    // writes nothing after `ask` fails: the retire has to be durable on its
    // own for anyone to read it.
    await Promise.race([
      waitForEvent(
        repoStore,
        runId,
        (event) => event.kind === "StepFailed" && event.stepId === "ask",
      ),
      ended,
    ]);
    const log = await repoStore.read(runId);
    const failed = log.findIndex(
      (event) => event.kind === "StepFailed" && event.stepId === "ask",
    );
    expect(log[failed - 1]).toMatchObject({
      kind: "SignalAwaitAbandoned",
      stepId: "ask",
      signalName: signalName("corr-ask"),
    });
    expect(log[failed]).not.toHaveProperty("routedTo");
    expect(retiresOf(log)).toEqual([signalName("corr-ask")]);
    expect(
      log.some(
        (event) =>
          event.kind === "SignalAwaited" &&
          event.signalName === signalName("corr-other"),
      ),
    ).toBe(true);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("retires the approval before the failure that routes to its handler", async () => {
    const runId = "handler-retires-approval";
    const def = defineWorkflow({
      id: "timed-approval-handler",
      trigger: { type: "manual" },
      steps: {
        timed: step({ agent, timeout: 20, onFailure: "handler" }),
        handler: step({ agent, after: ["timed"] }),
      },
    });
    const repoStore = createInMemoryRepoStore();
    const run = runtimeRun(
      def,
      buildEnv(def, repoStore, (ask) => `corr-${String(ask.stepId)}`),
      { runId },
    );
    // A run that ends before the handler asks says why.
    const ended = run.complete.then((done) => {
      throw new Error(`the run ended ${done.terminalStatus}`);
    });
    void ended.catch(() => undefined);

    // The handler has no deadline, so it holds its approval until the test
    // is done.
    await Promise.race([
      waitForEvent(
        repoStore,
        runId,
        (event) =>
          event.kind === "SignalAwaited" &&
          event.signalName === signalName("corr-handler"),
      ),
      ended,
    ]);
    const log = await repoStore.read(runId);
    const retired = log.findIndex(
      (event) =>
        event.kind === "SignalAwaitAbandoned" &&
        event.stepId === "timed" &&
        event.signalName === signalName("corr-timed"),
    );
    const failed = log.findIndex(
      (event) => event.kind === "StepFailed" && event.stepId === "timed",
    );
    expect(retired).not.toBe(-1);
    expect(retired).toBeLessThan(failed);
    expect(log[failed]).toMatchObject({ routedTo: "handler" });
    expect(retiresOf(log)).toEqual([signalName("corr-timed")]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });
});
