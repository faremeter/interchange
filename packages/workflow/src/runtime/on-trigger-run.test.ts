// runOnTrigger: a long-lived onTrigger section services each occurrence of
// its trigger as an EVENT -- spawning the body via the suspendable-child
// seam, awaiting the body's terminal (proxying any body approval park up on
// the SAME correlation via this run's own park machinery), then re-arming on
// a snapshot-less input park to await the next occurrence. The container never
// self-completes; it settles only when a body run ends non-completed
// (terminal-is-final) or the run is cancelled.
//
// This exercises the runtime driver in isolation with a test-wired
// spawnSuspendableChild -- no sidecar. The DEPLOYED section form (a `{ ref }`
// body) is built directly, since the deploy step is a separate layer.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";
import { signalName } from "@intx/types";
import type { ApprovalSnapshot } from "@intx/types/runtime";

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

const at = new Date().toISOString();

// The durable parent log a section leaves when a body child is parked
// mid-approval: the container `StepStarted`, the `ChildSpawned` for event 0's
// body, and the approval `SignalAwaited` the container proxied up on the body's
// correlation. With `grantDelivered` it also carries the `SignalReceived` that
// landed before the crash relayed it -- the narrow window where the container
// reduces to `in-flight` with its `awaitingSignal` stripped.
function midApprovalSeed(
  runId: string,
  corr: string,
  opts: { grantDelivered: boolean },
): WorkflowEvent[] {
  const events: WorkflowEvent[] = [
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
      kind: "SignalAwaited",
      seq: 4,
      at,
      stepId: "section",
      signalName: signalName(corr),
      parkKind: "approval",
    },
  ];
  if (opts.grantDelivered) {
    events.push({
      kind: "SignalReceived",
      seq: 5,
      at,
      signalName: signalName(corr),
      signalId: "grant-1",
      payload: { approved: true },
    });
  }
  return events;
}

const noopInvokeStep: StepInvoker = () => {
  throw new Error("onTrigger test: invokeStep must not be called");
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

async function waitForPark(
  repoStore: RepoStore,
  runId: string,
  parkKind: "input" | "approval" | "signal-relay",
  count: number,
): Promise<string> {
  for (let i = 0; i < 200; i += 1) {
    const events = await repoStore.read(runId);
    const awaits = events.filter(
      (e) => e.kind === "SignalAwaited" && e.parkKind === parkKind,
    );
    const latest = awaits[awaits.length - 1];
    if (awaits.length >= count && latest?.kind === "SignalAwaited") {
      return latest.signalName;
    }
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error(`timed out waiting for ${parkKind} park #${String(count)}`);
}

describe("runOnTrigger", () => {
  test("services each event as a body child run, re-arming on an input park", async () => {
    const runId = "sec-run";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const spawnRefs: string[] = [];
    const spawnInputs: unknown[] = [];
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      definitionRef,
      input,
    }) => {
      spawnRefs.push(definitionRef);
      spawnInputs.push(input);
      let done = false;
      return {
        next: async () => {
          if (done) throw new Error("next after terminal");
          done = true;
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async () => {
          throw new Error("resume on a non-suspending body");
        },
        deliverSignal: async () => undefined,
      };
    };
    const def = sectionWorkflow();
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

    const ch1 = await waitForPark(repoStore, runId, "input", 1);
    await channel.deliver(ch1, { text: "event-1" }, "sig-1");
    await waitForPark(repoStore, runId, "input", 2);

    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "RunStarted").length).toBe(1);
    expect(log.filter((e) => e.kind === "StepStarted").length).toBe(1);
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(2);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(2);
    expect(
      log.some((e) => e.kind === "RunCompleted" || e.kind === "RunFailed"),
    ).toBe(false);
    expect(spawnRefs).toEqual(["body-ref", "body-ref"]);
    expect(spawnInputs).toEqual([{ text: "event-0" }, { text: "event-1" }]);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual(["section__0", "section__1"]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("proxies a body approval park up on the same correlation and resumes the child", async () => {
    const runId = "sec-approve";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const snapshot: ApprovalSnapshot = {
      name: "charge_card",
      description: "Charge the customer",
      inputSchema: {},
      arguments: { amount: 100 },
    };
    let resumedCorr: string | undefined;
    let resumedDecision: unknown;
    const bodyCorr = "body-corr-1";
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      let stage = 0;
      return {
        next: async () => {
          stage += 1;
          if (stage === 1) {
            return {
              kind: "park",
              park: { correlationId: bodyCorr, approvalSnapshot: snapshot },
            };
          }
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async (correlationId, decision) => {
          resumedCorr = correlationId;
          resumedDecision = decision;
        },
        deliverSignal: async () => undefined,
      };
    };
    const def = sectionWorkflow();
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

    // The body parked on approval -> the section proxied it up as an approval
    // park on the SAME correlation, carrying the body's snapshot. Grant it.
    const approvalChannel = await waitForPark(repoStore, runId, "approval", 1);
    expect(approvalChannel).toBe(signalName(bodyCorr));
    await channel.deliver(approvalChannel, { approved: true }, "grant-1");

    // The section relayed the grant into the child; the body completed and the
    // section re-armed for the next event on an input park.
    await waitForPark(repoStore, runId, "input", 1);
    expect(resumedCorr).toBe(bodyCorr);
    expect(resumedDecision).toEqual({ approved: true });

    const log = await repoStore.read(runId);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "approval"),
    ).toBe(true);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("a failed body run ends the section (terminal-is-final)", async () => {
    const runId = "sec-fail";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const spawnSuspendableChild: SpawnSuspendableChild = async () => ({
      next: async () => ({ kind: "terminal", terminalStatus: "failed" }),
      resume: async () => undefined,
      deliverSignal: async () => undefined,
    });
    const def = sectionWorkflow();
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

    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(1);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "input"),
    ).toBe(false);
  });

  test("resumes a body parked mid-approval and relays the grant after restart", async () => {
    const runId = "sec-resume";
    const corr = "body-corr-1";
    const seed = midApprovalSeed(runId, corr, { grantDelivered: false });

    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    let resumedCorr: string | undefined;
    let resumedDecision: unknown;
    let spawnResume: readonly WorkflowEvent[] | undefined;
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      resumeFromEvents,
    }) => {
      spawnResume = resumeFromEvents;
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async (correlationId, decision) => {
          resumedCorr = correlationId;
          resumedDecision = decision;
        },
        deliverSignal: async () => undefined,
      };
    };

    const def = sectionWorkflow();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild,
      }),
      { runId, resumeFromEvents: seed },
    );

    // The container was re-offered on restart: it re-spawned the body from its
    // log and re-parked on the same correlation. Delivering the grant relays it
    // into the body, which completes, and the section re-arms for the next
    // event. (The in-memory channel queues a pre-await delivery, so this need
    // not race the re-park.)
    await channel.deliver(signalName(corr), { approved: true }, "grant-1");
    await waitForPark(repoStore, runId, "input", 1);

    expect(spawnResume).toEqual([]);
    expect(resumedCorr).toBe(corr);
    expect(resumedDecision).toEqual({ approved: true });
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("relays an approval grant that landed before the crash relayed it", async () => {
    const runId = "sec-resume-grant";
    const corr = "body-corr-2";
    // The grant's SignalReceived is durable but was never relayed into the body:
    // the container reduces to in-flight with awaitingSignal stripped, so the
    // correlation and decision live only in the log.
    const seed = midApprovalSeed(runId, corr, { grantDelivered: true });

    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    let resumedCorr: string | undefined;
    let resumedDecision: unknown;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => ({
      next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
      resume: async (correlationId, decision) => {
        resumedCorr = correlationId;
        resumedDecision = decision;
      },
      deliverSignal: async () => undefined,
    });

    const def = sectionWorkflow();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild,
      }),
      { runId, resumeFromEvents: seed },
    );

    // No fresh delivery: the driver recovers the decision from the durable
    // SignalReceived and relays it directly into the re-spawned body, which
    // completes and re-arms.
    await waitForPark(repoStore, runId, "input", 1);

    expect(resumedCorr).toBe(corr);
    expect(resumedDecision).toEqual({ approved: true });
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  // The fail-loud multi-section-same-name guard in `driveContainerSignalRelay`
  // (two sections concurrently proxy-parked on the same author name is refused,
  // as the container-scoped binding would otherwise diverge from the reducer's
  // Map-order consume) is not unit-covered here: it needs two concurrent
  // long-lived sections whose bodies park on the same name at the same time,
  // whose deterministic construction is fragile (long-lived sections + the
  // run-failure cascade) and whose topology is deferred to correlated
  // author-signals. It stands as defense-in-depth; the single-section path this
  // pass ships is always one active awaiter and stays faithful.
  test("proxies a body author-signal await up as a signal-relay and relays the signal", async () => {
    const runId = "sec-signal";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    let delivered:
      | { name: string; payload: unknown; signalId: string }
      | undefined;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      let stage = 0;
      let release: (() => void) | null = null;
      return {
        next: async () => {
          stage += 1;
          if (stage === 1) return { kind: "signal-park", name: "go" };
          if (stage === 2) {
            // The body is awaiting "go"; block until the section relays it,
            // modelling a real gate that only advances once the signal lands.
            await new Promise<void>((r) => {
              release = r;
            });
            return { kind: "terminal", terminalStatus: "completed" };
          }
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async () => undefined,
        deliverSignal: async (name, payload, signalId) => {
          delivered = { name, payload, signalId };
          if (release !== null) {
            const r = release;
            release = null;
            r();
          }
        },
      };
    };
    const def = sectionWorkflow();
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

    // The section proxied the body's "go" gate up as a signal-relay await on the
    // SAME name. Deliver it and the section relays it (with the original
    // signalId) into the body, which completes and re-arms.
    await waitForPark(repoStore, runId, "signal-relay", 1);
    await channel.deliver("go", { ok: true }, "sig-1");
    await waitForPark(repoStore, runId, "input", 1);

    expect(delivered).toEqual({
      name: "go",
      payload: { ok: true },
      signalId: "sig-1",
    });
    const log = await repoStore.read(runId);
    expect(
      log.some(
        (e) =>
          e.kind === "SignalAwaited" &&
          e.parkKind === "signal-relay" &&
          e.signalName === "go",
      ),
    ).toBe(true);
    expect(
      log.some(
        (e) =>
          e.kind === "SignalReceived" &&
          e.signalName === "go" &&
          e.signalId === "sig-1",
      ),
    ).toBe(true);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("retires a body signal-relay await the body abandons after a timeout", async () => {
    const runId = "sec-signal-abandon";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    let deliveredCount = 0;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      let stage = 0;
      return {
        next: async () => {
          stage += 1;
          if (stage === 1) return { kind: "signal-park", name: "go" };
          // The body's gate timed out; it moved on WITHOUT the signal, so the
          // body's next event arrives before any "go" is delivered.
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async () => undefined,
        deliverSignal: async () => {
          deliveredCount += 1;
        },
      };
    };
    const def = sectionWorkflow();
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

    // The body advanced past its gate before any signal landed; the section
    // retired the stale relay await and drove the body to completion + re-arm.
    await waitForPark(repoStore, runId, "input", 1);

    const log = await repoStore.read(runId);
    expect(
      log.some(
        (e) => e.kind === "SignalAwaited" && e.parkKind === "signal-relay",
      ),
    ).toBe(true);
    expect(
      log.some(
        (e) => e.kind === "SignalAwaitAbandoned" && e.signalName === "go",
      ),
    ).toBe(true);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);
    expect(deliveredCount).toBe(0);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });
});
