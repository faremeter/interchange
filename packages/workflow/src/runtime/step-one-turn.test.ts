// An agent step runs one turn per invocation; the workflow decides when it
// runs again. A run log that an earlier version wrote with a step parked on an
// input channel (a step that serviced more than one trigger) cannot continue,
// so its resume fails and names the redeploy. A deployed run does not get
// that far, because the projector refuses the definition; this check
// protects runs in memory. A step with a timeout inside an onTrigger body
// runs its deadline only during its own turn: a turn longer than the timeout
// fails, and the section can stay idle between events for longer than it.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import type { ApprovalSnapshot, ConversationTurn } from "@intx/types/runtime";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  createSpawnLoopIteration,
  defineWorkflow,
  runtimeRun,
  sectionBodyRunId,
  step,
  type BlobSubstrate,
  type Primitive,
  type RepoStore,
  type SignalChannel,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { waitForEvent, waitForNthEvent } from "@intx/workflow/testing";

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

const approvalSnapshot: ApprovalSnapshot = {
  name: "gate",
  description: "gate",
  inputSchema: { type: "object" },
  arguments: {},
};

function buildEnv(args: {
  def: WorkflowDefinition;
  repoStore: RepoStore;
  blobs: BlobSubstrate;
  signalChannel: SignalChannel;
  invokeStep: StepInvoker;
  bodies?: ReadonlyMap<string, WorkflowDefinition>;
}): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const env: WorkflowRuntimeEnv = {
    repoStore: args.repoStore,
    scheduler: createInMemoryScheduler({ repoStore: args.repoStore, clock }),
    signalChannel: args.signalChannel,
    blobs: args.blobs,
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: args.invokeStep,
    spawnChild: () => Promise.reject(new Error("replaced below")),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(args.def),
    hasUpstreamSignalResolver: true,
  };
  const bodies = args.bodies ?? new Map<string, WorkflowDefinition>();
  env.spawnSuspendableChild = createSpawnLoopIteration(env, bodies);
  env.spawnChild = createSpawnLoopIteration(env, bodies);
  return env;
}

describe("a step parked on an input channel by an earlier version", () => {
  test("fails its resume and names the redeploy", async () => {
    const def = defineWorkflow({
      id: "step-input-park",
      trigger: { type: "manual" },
      steps: { s: step({ agent }) },
    });
    const runId = "run-step-input-park";
    const blobs = createInMemoryBlobSubstrate();

    // Write a log with the step parked: run the step until it parks on an
    // approval, then record that park as the input park an earlier version
    // wrote between two triggers.
    const repoA = createInMemoryRepoStore();
    void runtimeRun(
      def,
      buildEnv({
        def,
        repoStore: repoA,
        blobs,
        signalChannel: createInMemorySignalChannel(),
        invokeStep: async () => ({
          suspend: {
            correlationId: "corr-1",
            kind: "approval",
            approvalSnapshot,
          },
        }),
      }),
      { runId },
    );
    await waitForEvent(
      repoA,
      runId,
      (e) => e.kind === "SignalAwaited" && e.stepId === "s",
    );
    const seed = (await repoA.read(runId)).map((e) =>
      e.kind === "SignalAwaited" ? { ...e, parkKind: "input" as const } : e,
    );

    let invoked = 0;
    const result = await runtimeRun(
      def,
      buildEnv({
        def,
        repoStore: createInMemoryRepoStore(),
        blobs,
        signalChannel: createInMemorySignalChannel(),
        invokeStep: async () => {
          invoked += 1;
          return { output: { reply: "done", turn: replyTurn } };
        },
      }),
      { runId, resumeFromEvents: seed },
    ).complete;

    expect(result.terminalStatus).toBe("failed");
    expect(invoked).toBe(0);
    const failed = result.events.find(
      (e) => e.kind === "StepFailed" && e.stepId === "s",
    );
    if (failed?.kind !== "StepFailed") throw new Error("no StepFailed for s");
    expect(failed.error.message).toMatch(
      /parked on an input channel.*redeploy the deployment with the step inside onTrigger/,
    );
  });
});

describe("a step with a timeout in an onTrigger body", () => {
  const timeoutMs = 50;

  // An onTrigger section on mail whose body is one agent step with the
  // timeout. Each event starts a new body run.
  function startTimedSection(invokeStep: StepInvoker) {
    const body = defineWorkflow({
      id: "timed-body",
      trigger: { type: "manual" },
      steps: { reply: step({ agent, timeout: timeoutMs }) },
    });
    const section: Primitive = {
      kind: "onTrigger",
      id: "",
      on: { type: "mail", to: "run_timed@t.example" },
      body: { ref: "body-ref" },
      drainBehavior: "wait",
    };
    const def = defineWorkflow({ id: "timed-section", steps: { section } });
    const runId = "run-timed-section";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: channel,
        invokeStep,
        bodies: new Map([["body-ref", body]]),
      }),
      { runId, triggerPayload: { text: "event-0" } },
    );
    return { runId, repoStore, channel, run };
  }

  // The control for the idle case below: it shows that the timeout does
  // apply to the body step, so the idle case passes because the idle time is
  // outside the turn and not because the timeout is not armed.
  test("fails a body turn that runs longer than the timeout", async () => {
    let abortedAfterMs: number | undefined;
    const { runId, repoStore, run } = startTimedSection(
      ({ signal }) =>
        new Promise((resolve, reject) => {
          const startedAt = Date.now();
          const slowTurn = setTimeout(() => {
            resolve({ output: { reply: "done", turn: replyTurn } });
          }, timeoutMs * 4);
          signal.addEventListener("abort", () => {
            clearTimeout(slowTurn);
            abortedAfterMs = Date.now() - startedAt;
            reject(new Error("turn aborted"));
          });
        }),
    );

    const failed = await waitForEvent(
      repoStore,
      sectionBodyRunId(runId, "section", 0),
      (e) =>
        (e.kind === "StepFailed" || e.kind === "StepCompleted") &&
        e.stepId === "reply",
    );
    expect(failed.kind).toBe("StepFailed");
    if (abortedAfterMs === undefined)
      throw new Error("the turn was not aborted");
    expect(abortedAfterMs).toBeGreaterThanOrEqual(timeoutMs - 5);
    expect(abortedAfterMs).toBeLessThan(timeoutMs * 4);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("does not fail while the section is idle between events", async () => {
    const { runId, repoStore, channel, run } = startTimedSection(async () => ({
      output: { reply: "done", turn: replyTurn },
    }));

    const inputPark = (count: number) =>
      waitForNthEvent(
        repoStore,
        runId,
        (e) => e.kind === "SignalAwaited" && e.parkKind === "input",
        count,
      );
    const first = await inputPark(1);
    if (first.kind !== "SignalAwaited") throw new Error("no input park");
    // Stay idle for several times the step timeout.
    await new Promise((resolve) => setTimeout(resolve, timeoutMs * 4));
    await channel.deliver(first.signalName, { text: "event-1" }, "ev-1");
    await inputPark(2);

    for (const eventIndex of [0, 1]) {
      const bodyLog = await repoStore.read(
        sectionBodyRunId(runId, "section", eventIndex),
      );
      expect(bodyLog.some((e) => e.kind === "StepFailed")).toBe(false);
      expect(bodyLog.some((e) => e.kind === "RunCompleted")).toBe(true);
    }
    const log = await repoStore.read(runId);
    expect(log.some((e) => e.kind === "StepFailed")).toBe(false);
    expect(log.some((e) => e.kind === "RunFailed")).toBe(false);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });
});
