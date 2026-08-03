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
  awaitSignal,
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

// The durable parent log a section leaves when a body child is parked
// mid-signal-relay: the container `StepStarted`, the `ChildSpawned` for event
// 0's body, and the `SignalAwaited` the container proxied up on the body's
// AUTHOR name (parkKind `"signal-relay"`). With `signalDelivered` it also
// carries the `SignalReceived` that landed before the crash relayed it -- the
// window where the container reduces to `in-flight` with the payload only in
// the log (recovered via the FIFO pairing).
function midSignalRelaySeed(
  runId: string,
  name: string,
  opts: { signalDelivered: boolean },
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
      signalName: name,
      parkKind: "signal-relay",
    },
  ];
  if (opts.signalDelivered) {
    events.push({
      kind: "SignalReceived",
      seq: 5,
      at,
      signalName: name,
      signalId: "relay-1",
      payload: { text: "delivered" },
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

  test("advances to the next event with a trigger delivered before the crash spawned its body", async () => {
    const runId = "sec-resume-input";
    const inputChannel = signalName("input-corr-1");
    // Event 0's body completed and the section re-armed an input park for event
    // 1; that park's SignalReceived is durable but ChildSpawned section__1 was
    // never written. The container reduces to in-flight with awaitingSignal
    // stripped, so the delivered trigger lives only in the log -- the input
    // sibling of the delivered-grant window. Resume must advance to event 1 with
    // that payload, not re-park and drop it.
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
        terminalStatus: "completed",
      },
      {
        kind: "SignalAwaited",
        seq: 5,
        at,
        stepId: "section",
        signalName: inputChannel,
        parkKind: "input",
      },
      {
        kind: "SignalReceived",
        seq: 6,
        at,
        signalName: inputChannel,
        signalId: "trigger-1",
        payload: { text: "event-1" },
      },
    ];
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    const spawnInputs: unknown[] = [];
    const spawnSuspendableChild: SpawnSuspendableChild = async ({ input }) => {
      spawnInputs.push(input);
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => {
          throw new Error("no resume expected");
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

    // Advances to event 1 on the durable trigger, spawns section__1 with the
    // delivered payload, which completes and re-arms a fresh input park (#2).
    await waitForPark(repoStore, runId, "input", 2);
    expect(spawnInputs).toEqual([{ text: "event-1" }]);
    const log = await repoStore.read(runId);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual(["section__0", "section__1"]);
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(2);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("does not advance on an already-consumed input older than the last spawn (no double-spawn)", async () => {
    const runId = "sec-resume-stale-input";
    const inputChannel1 = signalName("input-corr-a");
    // Event 1 was fully serviced: its input await (seq 5) delivered (seq 6) and
    // spawned section__1 (seq 7), which completed (seq 8). The crash landed
    // before event 2 re-armed. The last input await (seq 5) is OLDER than the
    // highest ChildSpawned (seq 7), so it belongs to an ALREADY-spawned event;
    // advancing on it would double-spawn. The seq discriminator refuses, so the
    // resume re-parks fresh and services event 2 only on a NEW delivery.
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
        terminalStatus: "completed",
      },
      {
        kind: "SignalAwaited",
        seq: 5,
        at,
        stepId: "section",
        signalName: inputChannel1,
        parkKind: "input",
      },
      {
        kind: "SignalReceived",
        seq: 6,
        at,
        signalName: inputChannel1,
        signalId: "trigger-1",
        payload: { text: "event-1" },
      },
      {
        kind: "ChildSpawned",
        seq: 7,
        at,
        stepId: "section",
        childRunId: "section__1",
        childDefinitionRef: "body-ref",
      },
      {
        kind: "ChildCompleted",
        seq: 8,
        at,
        childRunId: "section__1",
        terminalStatus: "completed",
      },
    ];
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    const spawnInputs: unknown[] = [];
    const spawnSuspendableChild: SpawnSuspendableChild = async ({ input }) => {
      spawnInputs.push(input);
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => {
          throw new Error("no resume expected");
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

    // Re-parks fresh for event 2 (input await #2) rather than advancing on the
    // stale event-1 input. No section__2 is spawned until a NEW delivery.
    const ch2 = await waitForPark(repoStore, runId, "input", 2);
    const mid = await repoStore.read(runId);
    expect(
      mid.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual(["section__0", "section__1"]);
    expect(spawnInputs).toEqual([]);

    // A genuinely new event-2 delivery on the fresh channel spawns section__2.
    await channel.deliver(ch2, { text: "event-2" }, "trigger-2");
    await waitForPark(repoStore, runId, "input", 3);
    const log = await repoStore.read(runId);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual(["section__0", "section__1", "section__2"]);
    expect(spawnInputs).toEqual([{ text: "event-2" }]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  // The widened same-name guard in `driveContainerSignalRelay` (ANY other step
  // awaiting the same author name is refused, not just a signal-relay sibling)
  // is unit-covered below for a plain author `awaitSignal` gate -- the case a
  // signal-relay-only guard would have admitted and mis-bound. The
  // two-concurrent-sections topology stays deferred (its deterministic
  // construction is fragile and correlated author-signals are not yet wired);
  // the guard stands as defense-in-depth there.
  test("refuses a plain author awaitSignal sibling awaiting the same name", async () => {
    const runId = "sec-guard-plain-gate";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const section: Primitive = {
      kind: "onTrigger",
      id: "",
      on: { type: "mail", to: "ins_sec@t.example" },
      body: { ref: "body-ref" },
      drainBehavior: "wait",
    };
    // A plain author `awaitSignal` gate sharing the body's author name. It
    // reduces with NO parkKind (controlParkKindOf -> "approval"), so a
    // signal-relay-only guard would have MISSED it and let the container-scoped
    // FIFO mis-bind a delivery the reducer routed to this gate.
    const def = defineWorkflow({
      id: "on-trigger-guard",
      steps: { section, gate: awaitSignal({ name: "go" }) },
    });
    let signalParked = false;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      let stage = 0;
      return {
        next: async () => {
          stage += 1;
          if (stage === 1) {
            // Wait until the plain `gate` sibling is parked on "go" so the guard
            // deterministically observes the collision, then signal-park.
            for (let i = 0; i < 300; i += 1) {
              const events = await repoStore.read(runId);
              if (
                events.some(
                  (e) =>
                    e.kind === "SignalAwaited" &&
                    e.stepId === "gate" &&
                    e.signalName === "go",
                )
              ) {
                break;
              }
              await new Promise((r) => setTimeout(r, 10));
            }
            signalParked = true;
            return { kind: "signal-park", name: "go" };
          }
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async () => undefined,
        deliverSignal: async () => undefined,
      };
    };
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

    // The body signal-parks on "go" -> the guard runs. The section fails on the
    // guard, but its StepFailed stays BUFFERED while the plain gate keeps the run
    // parked (a failed sibling does not auto-cancel a live awaiter), so cancel to
    // flush the durable log and unwind.
    for (let i = 0; i < 500 && !signalParked; i += 1) {
      await new Promise((r) => setTimeout(r, 10));
    }
    expect(signalParked).toBe(true);
    await run.cancel("supervisor-operator", "test done");
    const result = await run.complete.catch(() => undefined);
    const events = result?.events ?? (await repoStore.read(runId));

    // The widened guard refused loudly: the section failed with the same-name
    // message BEFORE the container emitted its own signal-relay await (a
    // signal-relay-only guard would have admitted the plain gate and mis-bound).
    const sectionFailed = events.find(
      (e) => e.kind === "StepFailed" && e.stepId === "section",
    );
    expect(sectionFailed?.kind).toBe("StepFailed");
    if (sectionFailed?.kind === "StepFailed") {
      expect(sectionFailed.error.message).toContain(
        "already awaiting the same signal name go",
      );
    }
    expect(
      events.some(
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "section" &&
          e.parkKind === "signal-relay",
      ),
    ).toBe(false);
  });

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

  test("resumes a body parked mid-signal-relay and re-drives the await after restart", async () => {
    const runId = "sec-relay-resume";
    const name = "go";
    const seed = midSignalRelaySeed(runId, name, { signalDelivered: false });

    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    let spawnResume: readonly WorkflowEvent[] | undefined;
    let delivered:
      | { name: string; payload: unknown; signalId: string }
      | undefined;
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      resumeFromEvents,
    }) => {
      spawnResume = resumeFromEvents;
      let release: (() => void) | null = null;
      return {
        next: async () => {
          // The re-spawned body re-parked on "go" silently; block until the
          // recovered section re-drives the await and relays.
          await new Promise<void>((r) => {
            release = r;
          });
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async () => undefined,
        deliverSignal: async (n, payload, signalId) => {
          delivered = { name: n, payload, signalId };
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
      { runId, resumeFromEvents: seed },
    );

    // The container re-adopted its durable signal-relay await and re-drove the
    // race from the recovered seq (no re-emit). Delivering the signal relays it
    // into the re-spawned body, which completes and re-arms for the next event.
    await channel.deliver(name, { text: "late" }, "sig-2");
    await waitForPark(repoStore, runId, "input", 1);

    // The body child has no separate durable log in this in-memory runtime
    // test, so it re-spawns from an empty log (as the mid-approval resume does).
    expect(spawnResume).toEqual([]);
    expect(delivered).toEqual({
      name,
      payload: { text: "late" },
      signalId: "sig-2",
    });
    const log = await repoStore.read(runId);
    // No SECOND signal-relay SignalAwaited was emitted -- the durable one was
    // re-adopted.
    expect(
      log.filter(
        (e) => e.kind === "SignalAwaited" && e.parkKind === "signal-relay",
      ).length,
    ).toBe(1);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("relays a signal that landed before the crash relayed it (relay-signal-grant)", async () => {
    const runId = "sec-relay-grant";
    const name = "go";
    // The signal's SignalReceived is durable but was never relayed: the
    // container reduced to in-flight, so the payload lives only in the log.
    const seed = midSignalRelaySeed(runId, name, { signalDelivered: true });

    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    let delivered:
      | { name: string; payload: unknown; signalId: string }
      | undefined;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      // Model the body's signal channel: `deliverSignal` may land before the
      // body's `next()` awaits (the relay recovery delivers before the loop
      // pulls next()), so `next()` completes if the signal already arrived.
      let release: (() => void) | null = null;
      let received = false;
      return {
        next: async () => {
          if (!received) {
            await new Promise<void>((r) => {
              release = r;
            });
          }
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async () => undefined,
        deliverSignal: async (n, payload, signalId) => {
          delivered = { name: n, payload, signalId };
          received = true;
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
      { runId, resumeFromEvents: seed },
    );

    // No fresh delivery: the driver recovers the signal from the durable
    // SignalReceived (FIFO-bound to the await) and relays it -- with its
    // original id -- into the re-spawned body, which completes and re-arms.
    await waitForPark(repoStore, runId, "input", 1);

    expect(delivered).toEqual({
      name,
      payload: { text: "delivered" },
      signalId: "relay-1",
    });
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });
});
