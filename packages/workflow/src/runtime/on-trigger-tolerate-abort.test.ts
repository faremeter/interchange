// A `tolerate` onTrigger section ends on a parent-abort, it does not re-arm.
//
// `onBodyFailure: "tolerate"` keeps the section listening for the next event
// when a body FAILS ON ITS OWN. It must NOT keep listening when the container
// itself is being torn down (a drain or operator cancel): the parent is
// aborting, so there is nothing to re-arm for. An in-process suspendable body's
// parent-abort teardown surfaces as a `failed` terminal (it runs under the
// workflow-process principal and cannot durably self-cancel to `cancelled`), so
// `runOnTrigger` keys the "always end" decision on the container abort, not on
// the `cancelled` terminal alone. Without that, a torn-down `tolerate` section
// re-arms onto an input park whose signal is already aborted and never resolves,
// wedging the run.
//
// This drives the REAL `runOnTrigger` with a body that settles `failed` once the
// section abort fires -- the terminal a `createSuspendableChildHandle` local
// teardown produces -- while the container is awaiting the body terminal (not
// proxy-parked), the reachable window for the wedge.

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
  type SpawnSuspendableChild,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const noopInvokeStep: StepInvoker = () => {
  throw new Error("test: invokeStep must not be called");
};

function buildEnv(args: {
  def: WorkflowDefinition;
  repoStore: RepoStore;
  signalChannel: SignalChannel;
  spawnSuspendableChild: SpawnSuspendableChild;
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
    spawnChild: async () => ({ terminalStatus: "completed" }),
    spawnSuspendableChild: args.spawnSuspendableChild,
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(args.def),
  };
}

function toleratingSection(): WorkflowDefinition {
  const section: Primitive = {
    kind: "onTrigger",
    id: "",
    on: { type: "mail", to: "run_sec@t.example" },
    body: { ref: "body-ref" },
    drainBehavior: "wait",
    onBodyFailure: "tolerate",
  };
  return defineWorkflow({ id: "on-trigger-tolerate", steps: { section } });
}

async function waitForEvent(
  repoStore: RepoStore,
  runId: string,
  pred: (e: { kind: string }) => boolean,
): Promise<void> {
  for (let i = 0; i < 200; i += 1) {
    const events = await repoStore.read(runId);
    if (events.some(pred)) return;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error("timed out waiting for event");
}

describe("tolerate section on a parent-abort local teardown", () => {
  test("a body that settles failed on abort ends the section, it does not re-arm", async () => {
    const runId = "sec-tol-abort";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();

    // The body's next() blocks until the section's abort fires, then settles
    // `failed` -- exactly what `createSuspendableChildHandle` now does on a
    // parent-abort local teardown.
    const spawnSuspendableChild: SpawnSuspendableChild = async ({ signal }) => {
      let done = false;
      return {
        next: async () => {
          if (done) throw new Error("next after terminal");
          done = true;
          if (!signal.aborted) {
            await new Promise<void>((r) =>
              signal.addEventListener("abort", () => r(), { once: true }),
            );
          }
          return { kind: "terminal", terminalStatus: "failed" };
        },
        resume: async () => undefined,
        deliverSignal: async () => undefined,
      };
    };

    const def = toleratingSection();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild,
      }),
      { runId, triggerPayload: { text: "event-0" } },
    );

    // The body is spawned and its next() is pending (container awaiting the
    // terminal, NOT proxy-parked). Cancel: the section step-local abort fires,
    // the body settles failed, and runOnTrigger reaches the tolerate check.
    await waitForEvent(repoStore, runId, (e) => e.kind === "ChildSpawned");
    await run.cancel("supervisor-operator", "operator teardown");

    const outcome = await Promise.race([
      run.complete
        .then((r) => `settled:${r.terminalStatus}`)
        .catch((e) => `err:${String(e)}`),
      new Promise<string>((r) => setTimeout(() => r("HANG"), 2000)),
    ]);

    // The operator teardown terminates the run. A HANG -- a re-arm waiting for a
    // next event that never comes -- is the regression this guards.
    expect(outcome).not.toBe("HANG");
  });

  test("crash-resume of an abort-teardown tolerate section stays ended, does not resurrect", async () => {
    const runId = "sec-tol-abort-resume";
    // The crash window: the abort-teardown body settled `failed` with
    // `abortedTeardown` durably recorded, but the container had not yet
    // committed its own StepFailed. Absent the durable teardown cause, the
    // resume classifier would see `failed` + `tolerate` and re-arm the section
    // for the next event -- resurrecting a section the drain/cancel tore down.
    const at = new Date(0).toISOString();
    const seed: WorkflowEvent[] = [
      {
        kind: "RunStarted",
        seq: 1,
        at,
        runId,
        definitionHash: "x",
        trigger: { type: "manual", payload: { text: "event-0" } },
      },
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "section",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "ChildSpawned",
        seq: 3,
        at,
        stepId: "section",
        childRunId: "section__0",
        childDefinitionRef: "body-ref",
      },
      {
        kind: "ChildCompleted",
        seq: 4,
        at,
        childRunId: "section__0",
        terminalStatus: "failed",
        abortedTeardown: true,
      },
    ];
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    let spawns = 0;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      spawns += 1;
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => undefined,
        deliverSignal: async () => undefined,
      };
    };
    const def = toleratingSection();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: createInMemorySignalChannel(),
        spawnSuspendableChild,
      }),
      { runId, resumeFromEvents: seed },
    );

    const result = await run.complete;
    // The abort-teardown section ends (RunFailed); it does NOT resurrect to
    // await the next event under `tolerate`.
    expect(result.terminalStatus).toBe("failed");
    const log = await repoStore.read(runId);
    expect(spawns).toBe(0);
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(1);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "input"),
    ).toBe(false);
  });
});
