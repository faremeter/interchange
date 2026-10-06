import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

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
  type SignalChannel,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { waitForNthEvent } from "@intx/workflow/testing";

const agent = defineAgent({
  id: "chat",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "m" }] },
});

function env(
  def: WorkflowDefinition,
  repoStore: RepoStore,
  signalChannel: SignalChannel,
  invokeStep: StepInvoker,
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  return {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel,
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (p) => `${p}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
    hasUpstreamSignalResolver: true,
  };
}

async function inputPark(
  repoStore: RepoStore,
  runId: string,
  n: number,
): Promise<string> {
  const e = await waitForNthEvent(
    repoStore,
    runId,
    (ev) => ev.kind === "SignalAwaited" && ev.parkKind === "input",
    n,
  );
  if (e.kind !== "SignalAwaited") throw new Error(e.kind);
  return e.signalName;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// A fast invoker that honors abort (as the real adapter does).
const fastInvoker: StepInvoker = async (req) => {
  if (req.signal.aborted) throw new Error("aborted");
  return { output: { ok: true } };
};

const hangUntilAborted = (req: Parameters<StepInvoker>[0]) =>
  new Promise<never>((_, reject) => {
    req.signal.addEventListener("abort", () =>
      reject(new Error("step aborted")),
    );
  });

// Serves the first trigger promptly, then hangs on every later turn.
const hangAfterFirstTurn: StepInvoker = (req) =>
  req.resume === undefined ? fastInvoker(req) : hangUntilAborted(req);

describe("step timeout vs idle time between triggers", () => {
  test("idle time between triggers does not count against a per-turn timeout", async () => {
    const runId = "run-idle";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const def = defineWorkflow({
      id: "idle",
      trigger: { type: "mail", to: "run@t.example" },
      steps: { s: step({ agent, triggers: 3, timeout: 200 }) },
    });
    const run = runtimeRun(def, env(def, repoStore, channel, fastInvoker), {
      runId,
      triggerPayload: { text: "1" },
    });
    const ch1 = await inputPark(repoStore, runId, 1);
    await sleep(400); // idle > timeout, no turn in progress
    const idleLog = await repoStore.read(runId);
    expect(
      idleLog.some((e) => e.kind === "StepFailed" || e.kind === "RunFailed"),
    ).toBe(false);
    await channel.deliver(ch1, { text: "2" }, "sig-2");
    const ch2 = await inputPark(repoStore, runId, 2);
    await channel.deliver(ch2, { text: "3" }, "sig-3");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
  }, 5000);

  test("(control) a single turn that hangs past the timeout is aborted", async () => {
    const runId = "run-hang";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const def = defineWorkflow({
      id: "hang",
      trigger: { type: "mail", to: "run@t.example" },
      steps: { s: step({ agent, timeout: 200 }) },
    });
    const run = runtimeRun(
      def,
      env(def, repoStore, channel, hangUntilAborted),
      {
        runId,
        triggerPayload: { text: "1" },
      },
    );
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
  }, 5000);

  test("a later turn that hangs past the timeout is aborted by the re-armed timer", async () => {
    const runId = "run-hang-turn-2";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const def = defineWorkflow({
      id: "hang-turn-2",
      trigger: { type: "mail", to: "run@t.example" },
      steps: { s: step({ agent, triggers: 2, timeout: 200 }) },
    });
    const run = runtimeRun(
      def,
      env(def, repoStore, channel, hangAfterFirstTurn),
      { runId, triggerPayload: { text: "1" } },
    );
    const ch1 = await inputPark(repoStore, runId, 1);
    await sleep(400); // idle > timeout; must not fail
    await channel.deliver(ch1, { text: "2" }, "sig-2");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
  }, 5000);

  test("an in-turn approval park counts against the timeout", async () => {
    const runId = "run-approval";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const def = defineWorkflow({
      id: "approval",
      trigger: { type: "mail", to: "run@t.example" },
      steps: { s: step({ agent, timeout: 200 }) },
    });
    const suspending: StepInvoker = async () => ({
      suspend: {
        correlationId: "corr-1",
        kind: "approval",
        approvalSnapshot: {
          name: "gate",
          description: "gate",
          inputSchema: { type: "object" },
          arguments: {},
        },
      },
    });
    const run = runtimeRun(def, env(def, repoStore, channel, suspending), {
      runId,
      triggerPayload: { text: "1" },
    });
    // No decision is ever delivered; the park outlasts the timeout.
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
  }, 5000);

  test("crash-resume into an idle input park gets a fresh, unstarted timer", async () => {
    const runId = "run-crash-idle";
    const def = defineWorkflow({
      id: "crash-idle",
      trigger: { type: "mail", to: "run@t.example" },
      steps: { s: step({ agent, triggers: 2, timeout: 200 }) },
    });

    // Drive a real run up to the point it has parked on the input channel
    // after servicing its first trigger, so the durable log carries the
    // runtime's OWN emitted SignalAwaited{parkKind: "input"} rather than a
    // hand-authored seed.
    const liveRepoStore = createInMemoryRepoStore();
    const liveChannel = createInMemorySignalChannel();
    const live = runtimeRun(
      def,
      env(def, liveRepoStore, liveChannel, fastInvoker),
      { runId, triggerPayload: { text: "1" } },
    );
    const liveCh = await inputPark(liveRepoStore, runId, 1);
    await sleep(400); // idle > timeout, simulating a crash well past the deadline

    // Slice the durable log at the crash window: everything committed up to
    // (and including) the input park, no StepCompleted for the step.
    const emitted = await liveRepoStore.read(runId);
    const parkIdx = emitted.findIndex(
      (e) => e.kind === "SignalAwaited" && e.parkKind === "input",
    );
    expect(parkIdx).toBeGreaterThan(-1);
    const crashWindow = emitted.slice(0, parkIdx + 1);
    expect(crashWindow.some((e) => e.kind === "StepCompleted")).toBe(false);

    // Let the live run finish so it does not outlive the test.
    await liveChannel.deliver(liveCh, { text: "2" }, "live-sig-2");
    expect((await live.complete).terminalStatus).toBe("completed");

    // Resume with a brand-new env/repoStore -- a fresh process re-driving the
    // durable log, not the live run continuing.
    const resumeRepoStore = createInMemoryRepoStore();
    const resumeChannel = createInMemorySignalChannel();
    const resumed = runtimeRun(
      def,
      env(def, resumeRepoStore, resumeChannel, fastInvoker),
      { runId, resumeFromEvents: crashWindow },
    );
    // The recovered park re-established, and stays idle past `timeout`
    // (again) without failing: the resumed step's re-park into "input" must
    // stay untimed, not merely get a delayed one-time grace period.
    const ch2 = await inputPark(resumeRepoStore, runId, 1);
    await sleep(400); // idle > timeout, no turn in progress on the resumed run
    const midLog = await resumeRepoStore.read(runId);
    expect(
      midLog.some((e) => e.kind === "StepFailed" || e.kind === "RunFailed"),
    ).toBe(false);
    await resumeChannel.deliver(ch2, { text: "2" }, "sig-2");
    const result = await resumed.complete;
    expect(result.terminalStatus).toBe("completed");
  }, 5000);

  test("crash-resume into an idle input park still times out a hanging next turn", async () => {
    const runId = "run-crash-hang";
    const def = defineWorkflow({
      id: "crash-hang",
      trigger: { type: "mail", to: "run@t.example" },
      steps: { s: step({ agent, triggers: 2, timeout: 200 }) },
    });
    const liveRepoStore = createInMemoryRepoStore();
    const liveChannel = createInMemorySignalChannel();
    const live = runtimeRun(
      def,
      env(def, liveRepoStore, liveChannel, fastInvoker),
      { runId, triggerPayload: { text: "1" } },
    );
    const liveCh = await inputPark(liveRepoStore, runId, 1);
    const emitted = await liveRepoStore.read(runId);
    const parkIdx = emitted.findIndex(
      (e) => e.kind === "SignalAwaited" && e.parkKind === "input",
    );
    const crashWindow = emitted.slice(0, parkIdx + 1);
    await liveChannel.deliver(liveCh, { text: "2" }, "live-sig-2");
    expect((await live.complete).terminalStatus).toBe("completed");

    const resumeRepoStore = createInMemoryRepoStore();
    const resumeChannel = createInMemorySignalChannel();
    const resumed = runtimeRun(
      def,
      env(def, resumeRepoStore, resumeChannel, hangAfterFirstTurn),
      { runId, resumeFromEvents: crashWindow },
    );
    const ch2 = await inputPark(resumeRepoStore, runId, 1);
    await resumeChannel.deliver(ch2, { text: "2" }, "sig-2");
    const result = await resumed.complete;
    expect(result.terminalStatus).toBe("failed");
  }, 5000);
});
