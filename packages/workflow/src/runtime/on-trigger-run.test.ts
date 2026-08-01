// runOnTrigger: a long-lived onTrigger section services each occurrence of
// its trigger as an EVENT -- spawning the body as a child run resolved by the
// deployed bodyRef, awaiting the body's terminal, then re-arming on a
// snapshot-less input park to await the next occurrence. The container never
// self-completes; it settles only when a body run ends non-completed
// (terminal-is-final) or the run is cancelled.
//
// This exercises the runtime driver in isolation with a test-wired spawnChild
// that runs each body to a terminal status -- no deploy orchestrator or
// suspension machinery. The DEPLOYED section form (a `{ ref }` body) is built
// directly, since the deploy step is a separate layer.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  type Primitive,
  type RepoStore,
  type SignalChannel,
  type SpawnChildWorkflow,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const noopInvokeStep: StepInvoker = () => {
  throw new Error("onTrigger test: invokeStep must not be called");
};

function buildEnv(args: {
  def: WorkflowDefinition;
  repoStore: RepoStore;
  signalChannel: SignalChannel;
  spawnChild: SpawnChildWorkflow;
}): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  return {
    repoStore: args.repoStore,
    scheduler: createInMemoryScheduler({ repoStore: args.repoStore, clock }),
    signalChannel: args.signalChannel,
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: noopInvokeStep,
    spawnChild: args.spawnChild,
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(args.def),
  };
}

// The DEPLOYED form of a section carries a body ref (deploy materializes the
// inline body to a workflow asset); build it directly since runOnTrigger runs
// the body by ref.
function sectionWorkflow(): WorkflowDefinition {
  const section: Primitive = {
    kind: "onTrigger",
    id: "",
    on: { type: "mail", to: "ins_sec@t.example" },
    body: { ref: "body-ref" },
    drainBehavior: "wait",
  };
  return defineWorkflow({ id: "on-trigger-run", steps: { section } });
}

// The input channel the section mints per re-arm is opaque; the delivering
// owner discovers the current one from the reduced state. Here we read it from
// the durable log: the Nth input `SignalAwaited`'s signal name.
async function waitForInputPark(
  repoStore: RepoStore,
  runId: string,
  count: number,
): Promise<string> {
  for (let i = 0; i < 200; i += 1) {
    const events = await repoStore.read(runId);
    const inputAwaits = events.filter(
      (e) => e.kind === "SignalAwaited" && e.parkKind === "input",
    );
    const latest = inputAwaits[inputAwaits.length - 1];
    if (inputAwaits.length >= count && latest?.kind === "SignalAwaited") {
      return latest.signalName;
    }
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`timed out waiting for input park #${String(count)}`);
}

describe("runOnTrigger", () => {
  test("services each event as a body child run, re-arming on an input park", async () => {
    const runId = "sec-run";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const spawnRefs: string[] = [];
    const spawnInputs: unknown[] = [];
    const spawnChild: SpawnChildWorkflow = async ({ definitionRef, input }) => {
      spawnRefs.push(definitionRef);
      spawnInputs.push(input);
      return { terminalStatus: "completed" };
    };
    const def = sectionWorkflow();
    const run = runtimeRun(
      def,
      buildEnv({ def, repoStore, signalChannel: channel, spawnChild }),
      { runId, triggerPayload: { text: "event-0" } },
    );

    // Event 0 (the firing trigger) spawns body 0; the section re-arms on an
    // input park. Deliver event 1 on that channel; body 1 spawns and it
    // re-arms again.
    const ch1 = await waitForInputPark(repoStore, runId, 1);
    await channel.deliver(ch1, { text: "event-1" }, "sig-1");
    await waitForInputPark(repoStore, runId, 2);

    const log = await repoStore.read(runId);
    // One run, one container step bracket, two body children, no terminal:
    // the section stays alive between events.
    expect(log.filter((e) => e.kind === "RunStarted").length).toBe(1);
    expect(log.filter((e) => e.kind === "StepStarted").length).toBe(1);
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(2);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(2);
    expect(
      log.some((e) => e.kind === "RunCompleted" || e.kind === "RunFailed"),
    ).toBe(false);
    // Each event's payload threads to its body child as input; the body is
    // resolved by ref; children are scoped per event.
    expect(spawnRefs).toEqual(["body-ref", "body-ref"]);
    expect(spawnInputs).toEqual([{ text: "event-0" }, { text: "event-1" }]);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual(["section__0", "section__1"]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("a failed body run ends the section (terminal-is-final)", async () => {
    const runId = "sec-fail";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const spawnChild: SpawnChildWorkflow = async () => ({
      terminalStatus: "failed",
    });
    const def = sectionWorkflow();
    const run = runtimeRun(
      def,
      buildEnv({ def, repoStore, signalChannel: channel, spawnChild }),
      { runId, triggerPayload: { text: "event-0" } },
    );

    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
    const log = await repoStore.read(runId);
    // The first body failed, so the section never re-armed for a second event.
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(1);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "input"),
    ).toBe(false);
  });
});
