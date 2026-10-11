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
  action,
  awaitSignal,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  loopBodyRunId,
  runtimeRun,
  RuntimeResumeUnsupportedError,
  sectionBodyRunId,
  type Primitive,
  type RepoStore,
  type SignalChannel,
  type SpawnSuspendableChild,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { waitForEvent, waitForNthEvent } from "@intx/workflow/testing";
import { createTerminalChildHandle } from "./suspendable-child-handle";

const at = new Date().toISOString();

/** Body run id `runOnTrigger` mints for the section step id `"section"`. */
function sectionEventRunId(runId: string, eventIndex: number): string {
  return sectionBodyRunId(runId, "section", eventIndex);
}

// The durable parent log a section leaves when a body child is parked
// mid-approval: the container `StepStarted`, the `ChildSpawned` for event 0's
// body, and the approval `SignalAwaited` the container proxied up on the body's
// correlation. With `grantDelivered` it also carries the `SignalReceived` that
// landed before the crash relayed it -- the narrow window where the container
// reduces to `in-flight` with its `awaitingSignal` stripped. `childRunId`
// defaults to the minted id; a legacy resume passes `section__<index>`.
function midApprovalSeed(
  runId: string,
  corr: string,
  opts: { grantDelivered: boolean },
  childRunId: string = sectionEventRunId(runId, 0),
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
      childRunId,
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
// the log. A park with `correlationId` is addressed by that id; a park
// without one stays name-FIFO.
function midSignalRelaySeed(
  runId: string,
  name: string,
  opts: { signalDelivered: boolean; correlationId?: string },
  childRunId: string = sectionEventRunId(runId, 0),
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
      childRunId,
      childDefinitionRef: "body-ref",
    },
    {
      kind: "SignalAwaited",
      seq: 4,
      at,
      stepId: "section",
      signalName: name,
      parkKind: "signal-relay",
      ...(opts.correlationId !== undefined
        ? { correlationId: opts.correlationId }
        : {}),
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
      ...(opts.correlationId !== undefined
        ? { correlationId: opts.correlationId }
        : {}),
    });
  }
  return events;
}

function relayCorrelationId(events: readonly WorkflowEvent[]): string {
  const awaited = events.find(
    (e) => e.kind === "SignalAwaited" && e.parkKind === "signal-relay",
  );
  if (
    awaited?.kind !== "SignalAwaited" ||
    awaited.correlationId === undefined
  ) {
    throw new Error("expected a correlated signal-relay park");
  }
  return awaited.correlationId;
}

function correlationForStep(
  events: readonly WorkflowEvent[],
  stepId: string,
): string {
  const awaited = events.find(
    (e) =>
      e.kind === "SignalAwaited" &&
      e.stepId === stepId &&
      e.parkKind === "signal-relay",
  );
  if (
    awaited?.kind !== "SignalAwaited" ||
    awaited.correlationId === undefined
  ) {
    throw new Error(`${stepId} relay park has no correlationId`);
  }
  return awaited.correlationId;
}

function startGateBesideSection(runId: string): {
  repoStore: RepoStore;
  channel: SignalChannel;
  delivered: { name: string; payload: unknown; signalId: string }[];
  run: ReturnType<typeof runtimeRun>;
} {
  const repoStore = createInMemoryRepoStore();
  const channel = createInMemorySignalChannel();
  const delivered: { name: string; payload: unknown; signalId: string }[] = [];
  const section: Primitive = {
    kind: "onTrigger",
    id: "",
    on: { type: "mail", to: "run_sec@t.example" },
    body: { ref: "body-ref" },
    drainBehavior: "wait",
  };
  const def = defineWorkflow({
    id: "on-trigger-guard",
    steps: {
      section,
      gate: awaitSignal({ name: "go" }),
      // The gate's receive and completion stay buffered until the next
      // segment flush. This step is that flush: it cannot park until
      // the gate has completed, and its SignalAwaited lands only with
      // the buffer.
      next: awaitSignal({ name: "next", after: ["gate"] }),
    },
  });
  const spawnSuspendableChild: SpawnSuspendableChild = async () => {
    let stage = 0;
    let release: (() => void) | null = null;
    return {
      next: async () => {
        stage += 1;
        if (stage === 1) {
          await waitForEvent(
            repoStore,
            runId,
            (e) =>
              e.kind === "SignalAwaited" &&
              e.stepId === "gate" &&
              e.signalName === "go",
          );
          return { kind: "signal-park", name: "go" };
        }
        await new Promise<void>((resolve) => {
          release = resolve;
        });
        return { kind: "terminal", terminalStatus: "completed" };
      },
      resume: async () => undefined,
      deliverSignal: async (name, payload, signalId) => {
        delivered.push({ name, payload, signalId });
        if (release !== null) {
          const pending = release;
          release = null;
          pending();
        }
      },
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
  return { repoStore, channel, delivered, run };
}

// The durable parent log a section leaves when a body child is parked
// mid-`sleep`: the container `StepStarted` and the `ChildSpawned` for event 0's
// body, and NO container `SignalAwaited`. A bare sleep body parks entirely
// inside the child (its own `awaiting-timer` step) and surfaces no container
// park, so the container reduces to `in-flight` -- the shape that distinguishes
// a mid-sleep body from the approval/signal-relay parks above.
function midSleepSeed(
  runId: string,
  childRunId: string = sectionEventRunId(runId, 0),
): WorkflowEvent[] {
  return [
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
      childRunId,
      childDefinitionRef: "body-ref",
    },
  ];
}

// A body child's OWN durable log, reduced to a single non-terminal step in the
// named phase. `planOnTriggerResume` inspects this to decide whether an
// in-flight body is re-adoptable (a bare sleep parked in `awaiting-timer`) or
// must stay refused (an un-relayed author `awaitSignal`, still `awaiting-signal`).
function bodyChildLog(
  childRunId: string,
  tail:
    | { phase: "awaiting-timer" }
    | { phase: "awaiting-signal"; signalName: string }
    | { phase: "in-flight" }
    | { phase: "in-flight-fired" },
): WorkflowEvent[] {
  const events: WorkflowEvent[] = [
    {
      kind: "RunStarted",
      seq: 1,
      at,
      runId: childRunId,
      definitionHash: "x",
      trigger: { type: "manual", payload: undefined },
    },
    {
      kind: "StepStarted",
      seq: 2,
      at,
      stepId: "body-step",
      attempt: 1,
      input: { ref: "inline:null" },
    },
  ];
  if (tail.phase === "awaiting-timer") {
    events.push({
      kind: "TimerSet",
      seq: 3,
      at,
      timerId: "body-timer-1",
      fireAt: new Date(Date.now() + 60_000).toISOString(),
      stepId: "body-step",
    });
  } else if (tail.phase === "awaiting-signal") {
    events.push({
      kind: "SignalAwaited",
      seq: 3,
      at,
      stepId: "body-step",
      signalName: tail.signalName,
    });
  } else if (tail.phase === "in-flight-fired") {
    // The body's sleep timer already fired: TimerSet moves the step to
    // awaiting-timer, TimerFired moves it back to in-flight and clears the
    // pending timer -- the post-fire window commit 1's short-circuit settles.
    events.push(
      {
        kind: "TimerSet",
        seq: 3,
        at,
        timerId: "body-timer-1",
        fireAt: at,
        stepId: "body-step",
      },
      { kind: "TimerFired", seq: 4, at, timerId: "body-timer-1" },
    );
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
    spawnChild: async () => createTerminalChildHandle("completed"),
    spawnSuspendableChild: args.spawnSuspendableChild,
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(args.def),
    hasUpstreamSignalResolver: true,
  };
}

// The DEPLOYED form of a section carries a body ref (deploy materializes the
// inline body to a workflow asset); build it directly since runOnTrigger runs
// the body by ref.
function sectionWorkflow(
  opts: { onBodyFailure?: "end" | "tolerate" } = {},
): WorkflowDefinition {
  const section: Primitive = {
    kind: "onTrigger",
    id: "",
    on: { type: "mail", to: "run_sec@t.example" },
    body: { ref: "body-ref" },
    drainBehavior: "wait",
    ...(opts.onBodyFailure !== undefined
      ? { onBodyFailure: opts.onBodyFailure }
      : {}),
  };
  return defineWorkflow({ id: "on-trigger-run", steps: { section } });
}

async function waitForPark(
  repoStore: RepoStore,
  runId: string,
  parkKind: "input" | "approval" | "signal-relay",
  count: number,
): Promise<string> {
  // Strict equality on an optional field. The reducer reads an absent
  // parkKind as "approval"; this does not, matching the poll it replaced, and
  // no caller relies on the reducer's reading.
  const event = await waitForNthEvent(
    repoStore,
    runId,
    (e) => e.kind === "SignalAwaited" && e.parkKind === parkKind,
    count,
  );
  if (event.kind !== "SignalAwaited") {
    throw new Error(`expected SignalAwaited, got ${event.kind}`);
  }
  return event.signalName;
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
    ).toEqual([sectionEventRunId(runId, 0), sectionEventRunId(runId, 1)]);

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
    const bodyRunId = sectionEventRunId(runId, 0);
    const seed = midApprovalSeed(runId, corr, { grantDelivered: false });
    const bodyLog = bodyChildLog(bodyRunId, {
      phase: "awaiting-signal",
      signalName: signalName(corr),
    });

    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    await repoStore.appendBatch(bodyRunId, bodyLog);
    const channel = createInMemorySignalChannel();
    let resumedCorr: string | undefined;
    let resumedDecision: unknown;
    let spawnedId: string | undefined;
    let spawnResume: readonly WorkflowEvent[] | undefined;
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      childRunId,
      resumeFromEvents,
    }) => {
      spawnedId = childRunId;
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

    expect(spawnedId).toBe(bodyRunId);
    expect(spawnResume).toEqual(bodyLog);
    expect(resumedCorr).toBe(corr);
    expect(resumedDecision).toEqual({ approved: true });
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("resumes a body parked mid-sleep after restart", async () => {
    const runId = "sec-sleep-resume";
    const seed = midSleepSeed(runId);
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    // The body child's own log leaves its sleep step parked in `awaiting-timer`.
    const bodyRunId = sectionEventRunId(runId, 0);
    await repoStore.appendBatch(
      bodyRunId,
      bodyChildLog(bodyRunId, { phase: "awaiting-timer" }),
    );
    const channel = createInMemorySignalChannel();
    let spawned = false;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      spawned = true;
      // This mock stands in for a re-adopted bare sleep: it needs no
      // container-side drive and completes on its own. The body's actual
      // timer re-adoption is exercised end-to-end by the deploy-level test;
      // here we verify the container-level plan -> spawn -> re-arm.
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => undefined,
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

    // The recovered container re-adopted the mid-sleep body and awaited its
    // terminal; on completion it re-arms on an input park for the next event.
    await waitForPark(repoStore, runId, "input", 1);
    expect(spawned).toBe(true);
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);
    expect(log.some((e) => e.kind === "RunFailed")).toBe(false);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("resumes a body whose sleep timer already fired before the container re-drove", async () => {
    // The race-robust window: on restart the host scheduler re-arms and fires
    // the body's timer before the container re-reads the child state, so the
    // body child is in-flight (post-fire), not awaiting-timer. The container
    // re-adopts it just the same (no awaiting-signal park), and the body's own
    // in-flight short-circuit settles it.
    const runId = "sec-sleep-fired-resume";
    const seed = midSleepSeed(runId);
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const bodyRunId = sectionEventRunId(runId, 0);
    await repoStore.appendBatch(
      bodyRunId,
      bodyChildLog(bodyRunId, { phase: "in-flight-fired" }),
    );
    const channel = createInMemorySignalChannel();
    let spawned = false;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      spawned = true;
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => undefined,
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

    await waitForPark(repoStore, runId, "input", 1);
    expect(spawned).toBe(true);
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);
    expect(log.some((e) => e.kind === "RunFailed")).toBe(false);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("refuses a resume when the body is parked on an un-relayed author signal", async () => {
    const runId = "sec-authorsig-refuse";
    const seed = midSleepSeed(runId);
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    // The body child is parked on an author `awaitSignal` gate whose container
    // relay await never flushed. Re-adopting it would re-park the body on a
    // signal the container never relays -- a hang -- so resume must fail loud.
    const bodyRunId = sectionEventRunId(runId, 0);
    await repoStore.appendBatch(
      bodyRunId,
      bodyChildLog(bodyRunId, {
        phase: "awaiting-signal",
        signalName: "author-go",
      }),
    );
    const channel = createInMemorySignalChannel();
    let spawned = false;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      spawned = true;
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => undefined,
        deliverSignal: async () => undefined,
      };
    };

    const def = sectionWorkflow();
    const result = await runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild,
      }),
      { runId, resumeFromEvents: seed },
    ).complete;

    // Fail loud, not hang: the planner refuses before the body is re-spawned,
    // and the section never re-arms.
    expect(result.terminalStatus).toBe("failed");
    expect(spawned).toBe(false);
    const log = await repoStore.read(runId);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "input"),
    ).toBe(false);
    const failure = log.find((e) => e.kind === "StepFailed");
    if (failure?.kind !== "StepFailed")
      throw new Error("expected a StepFailed");
    expect(failure.error.message).toContain("un-relayed signal");
  });

  test("refuses a resume when the body is parked on an un-relayed control-plane approval", async () => {
    const runId = "sec-approval-refuse";
    const seed = midSleepSeed(runId);
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    // The body's agent step suspended for approval on a reserved control-plane
    // channel, and the container's proxy park never flushed. Like the author
    // case, re-adopting it re-parks the body silently on a decision the container
    // will never relay -- a hang -- so resume must fail loud, not re-adopt.
    const bodyRunId = sectionEventRunId(runId, 0);
    await repoStore.appendBatch(
      bodyRunId,
      bodyChildLog(bodyRunId, {
        phase: "awaiting-signal",
        signalName: signalName("body-corr"),
      }),
    );
    const channel = createInMemorySignalChannel();
    let spawned = false;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      spawned = true;
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => undefined,
        deliverSignal: async () => undefined,
      };
    };

    const def = sectionWorkflow();
    const result = await runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild,
      }),
      { runId, resumeFromEvents: seed },
    ).complete;

    expect(result.terminalStatus).toBe("failed");
    expect(spawned).toBe(false);
    const log = await repoStore.read(runId);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "input"),
    ).toBe(false);
    const failure = log.find((e) => e.kind === "StepFailed");
    if (failure?.kind !== "StepFailed")
      throw new Error("expected a StepFailed");
    expect(failure.error.message).toContain("un-relayed signal");
  });

  test("a re-adopted in-flight body whose resume is refused fails loud even under tolerate", async () => {
    const runId = "sec-tolerate-refuse";
    const seed = midSleepSeed(runId);
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    // An in-flight body with no author gate is re-adopted, but its own resume
    // classifier refuses (a mid-flight childWorkflow raises
    // RuntimeResumeUnsupportedError, which REJECTS the body run). The seam
    // re-throws that rejection, so it is loud -- `tolerate` re-arms only on a
    // RETURNED failed terminal, never on a rejection.
    const bodyRunId = sectionEventRunId(runId, 0);
    await repoStore.appendBatch(
      bodyRunId,
      bodyChildLog(bodyRunId, { phase: "in-flight" }),
    );
    const channel = createInMemorySignalChannel();
    const spawnSuspendableChild: SpawnSuspendableChild = async () => ({
      next: async () => {
        throw new RuntimeResumeUnsupportedError(
          "body-step",
          "in-flight",
          "body resume refused (simulated mid-flight childWorkflow)",
        );
      },
      resume: async () => undefined,
      deliverSignal: async () => undefined,
    });

    const def = sectionWorkflow({ onBodyFailure: "tolerate" });
    const result = await runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild,
      }),
      { runId, resumeFromEvents: seed },
    ).complete;

    // The rejection fails the section; tolerate does NOT swallow it and the
    // section does NOT re-arm for the next event.
    expect(result.terminalStatus).toBe("failed");
    const log = await repoStore.read(runId);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "input"),
    ).toBe(false);
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
    const event0 = sectionEventRunId(runId, 0);
    const event1 = sectionEventRunId(runId, 1);
    const inputChannel = signalName("input-corr-1");
    // Event 0's body completed and the section re-armed an input park for event
    // 1; that park's SignalReceived is durable but the next body's ChildSpawned
    // was never written. The container reduces to in-flight with awaitingSignal
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
        childRunId: event0,
        childDefinitionRef: "body-ref",
      },
      {
        kind: "ChildCompleted",
        seq: 4,
        at,
        childRunId: event0,
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

    // Advances to event 1 on the durable trigger, spawns that event's body with
    // the delivered payload, which completes and re-arms a fresh input park (#2).
    await waitForPark(repoStore, runId, "input", 2);
    expect(spawnInputs).toEqual([{ text: "event-1" }]);
    const log = await repoStore.read(runId);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual([event0, event1]);
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(2);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("does not advance on an already-consumed input older than the last spawn (no double-spawn)", async () => {
    const runId = "sec-resume-stale-input";
    const event0 = sectionEventRunId(runId, 0);
    const event1 = sectionEventRunId(runId, 1);
    const event2 = sectionEventRunId(runId, 2);
    const inputChannel1 = signalName("input-corr-a");
    // Event 1 was fully serviced: its input await (seq 5) delivered (seq 6) and
    // spawned event 1's body (seq 7), which completed (seq 8). The crash landed
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
        childRunId: event0,
        childDefinitionRef: "body-ref",
      },
      {
        kind: "ChildCompleted",
        seq: 4,
        at,
        childRunId: event0,
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
        childRunId: event1,
        childDefinitionRef: "body-ref",
      },
      {
        kind: "ChildCompleted",
        seq: 8,
        at,
        childRunId: event1,
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
    // stale event-1 input. Event 2's body is not spawned until a NEW delivery.
    const ch2 = await waitForPark(repoStore, runId, "input", 2);
    const mid = await repoStore.read(runId);
    expect(
      mid.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual([event0, event1]);
    expect(spawnInputs).toEqual([]);

    // A genuinely new event-2 delivery on the fresh channel spawns event 2.
    await channel.deliver(ch2, { text: "event-2" }, "trigger-2");
    await waitForPark(repoStore, runId, "input", 3);
    const log = await repoStore.read(runId);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual([event0, event1, event2]);
    expect(spawnInputs).toEqual([{ text: "event-2" }]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  // A plain author gate beside an onTrigger section shares the author name.
  // The section relay carries a correlationId, so it is not on the name
  // queue and the name-mode guard does not treat the gate as a conflict.
  test("parks a correlated section relay beside a plain author gate on the same name", async () => {
    const runId = "sec-guard-plain-gate";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const section: Primitive = {
      kind: "onTrigger",
      id: "",
      on: { type: "mail", to: "run_sec@t.example" },
      body: { ref: "body-ref" },
      drainBehavior: "wait",
    };
    // A plain author gate shares the section's author name and stays
    // name-addressed. The section relay carries a correlationId, so the
    // name-mode guard does not treat the gate as a conflict.
    const def = defineWorkflow({
      id: "on-trigger-guard",
      steps: { section, gate: awaitSignal({ name: "go" }) },
    });
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      let stage = 0;
      return {
        next: async () => {
          stage += 1;
          if (stage === 1) {
            // Wait until the plain `gate` sibling is parked on "go", then
            // signal-park. The gate flushes its SignalAwaited durably before
            // it waits on the channel, so that event is the park.
            await waitForEvent(
              repoStore,
              runId,
              (e) =>
                e.kind === "SignalAwaited" &&
                e.stepId === "gate" &&
                e.signalName === "go",
            );
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

    // The section emits its own correlated relay. The plain gate stays
    // name-addressed. Cancel to unwind the gate, which is still parked.
    const relay = await waitForEvent(
      repoStore,
      runId,
      (e) =>
        e.kind === "SignalAwaited" &&
        e.stepId === "section" &&
        e.parkKind === "signal-relay" &&
        e.signalName === "go",
    );
    if (relay.kind !== "SignalAwaited") {
      throw new Error(`expected SignalAwaited, got ${relay.kind}`);
    }
    expect(relay.correlationId).toEqual(expect.any(String));
    const events = await repoStore.read(runId);
    const gate = events.find(
      (e) => e.kind === "SignalAwaited" && e.stepId === "gate",
    );
    expect(gate?.kind).toBe("SignalAwaited");
    if (gate?.kind === "SignalAwaited") {
      expect(gate.signalName).toBe("go");
      expect(gate.correlationId).toBeUndefined();
    }
    expect(events.some((e) => e.kind === "StepFailed")).toBe(false);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("a name delivery completes the gate and the correlation completes the section", async () => {
    const runId = "sec-gate-then-section";
    const { repoStore, channel, delivered, run } =
      startGateBesideSection(runId);
    try {
      const relay = await waitForEvent(
        repoStore,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "section" &&
          e.parkKind === "signal-relay" &&
          e.signalName === "go",
      );
      if (relay.kind !== "SignalAwaited" || relay.correlationId === undefined) {
        throw new Error("expected a correlated section relay");
      }
      const correlationId = relay.correlationId;

      await channel.deliver("go", { ok: false }, "sig-name-only");
      // `next` parks only after the gate completes, and that park flushes
      // the gate's buffered SignalReceived and StepCompleted.
      await waitForEvent(
        repoStore,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "next" &&
          e.signalName === "next",
      );
      expect(delivered).toEqual([]);

      await channel.deliver("go", { ok: true }, "sig-1", correlationId);
      await waitForEvent(
        repoStore,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "section" &&
          e.parkKind === "input",
      );
      expect(delivered).toEqual([
        { name: "go", payload: { ok: true }, signalId: "sig-1" },
      ]);
      const log = await repoStore.read(runId);
      expect(
        log.some((e) => e.kind === "StepCompleted" && e.stepId === "gate"),
      ).toBe(true);
      const nameReceived = log.find(
        (e) => e.kind === "SignalReceived" && e.signalId === "sig-name-only",
      );
      expect(nameReceived?.kind).toBe("SignalReceived");
      if (nameReceived?.kind === "SignalReceived") {
        expect(nameReceived.correlationId).toBeUndefined();
      }
      expect(
        log.some(
          (e) =>
            e.kind === "SignalReceived" &&
            e.signalId === "sig-1" &&
            e.correlationId === correlationId,
        ),
      ).toBe(true);
    } finally {
      await run.cancel("supervisor-operator", "test done");
      await run.complete.catch(() => undefined);
    }
  });

  test("a correlated delivery completes the section and leaves the gate parked", async () => {
    const runId = "sec-section-then-gate";
    const { repoStore, channel, delivered, run } =
      startGateBesideSection(runId);
    try {
      await waitForEvent(
        repoStore,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "section" &&
          e.parkKind === "signal-relay",
      );
      const correlationId = correlationForStep(
        await repoStore.read(runId),
        "section",
      );
      await channel.deliver("go", { ok: true }, "sig-1", correlationId);
      await waitForEvent(
        repoStore,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "section" &&
          e.parkKind === "input",
      );
      expect(delivered).toEqual([
        { name: "go", payload: { ok: true }, signalId: "sig-1" },
      ]);
      const mid = await repoStore.read(runId);
      expect(
        mid.some((e) => e.kind === "StepCompleted" && e.stepId === "gate"),
      ).toBe(false);
      expect(
        mid.some((e) => e.kind === "SignalAwaited" && e.stepId === "next"),
      ).toBe(false);

      await channel.deliver("go", { ok: false }, "sig-name-only");
      await waitForEvent(
        repoStore,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "next" &&
          e.signalName === "next",
      );
      const log = await repoStore.read(runId);
      expect(
        log.some((e) => e.kind === "StepCompleted" && e.stepId === "gate"),
      ).toBe(true);
      const nameReceived = log.find(
        (e) => e.kind === "SignalReceived" && e.signalId === "sig-name-only",
      );
      expect(nameReceived?.kind).toBe("SignalReceived");
      if (nameReceived?.kind === "SignalReceived") {
        expect(nameReceived.correlationId).toBeUndefined();
      }
    } finally {
      await run.cancel("supervisor-operator", "test done");
      await run.complete.catch(() => undefined);
    }
  });

  test("two sections awaiting the same name do not share a delivery", async () => {
    const runId = "sec-two-sections";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const delivered: { stepId: string; signalId: string }[] = [];
    const releases = new Map<string, () => void>();
    const section = (ref: string, to: string): Primitive => ({
      kind: "onTrigger",
      id: "",
      on: { type: "mail", to },
      body: { ref },
      drainBehavior: "wait",
    });
    const def = defineWorkflow({
      id: "two-sections",
      steps: {
        left: section("left-ref", "run_left@t.example"),
        right: section("right-ref", "run_right@t.example"),
      },
    });
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      parentStepId,
    }) => {
      let stage = 0;
      return {
        next: async () => {
          stage += 1;
          if (stage === 1) return { kind: "signal-park", name: "go" };
          await new Promise<void>((resolve) => {
            releases.set(parentStepId, resolve);
          });
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async () => undefined,
        deliverSignal: async (_name, _payload, signalId) => {
          delivered.push({ stepId: parentStepId, signalId });
          const release = releases.get(parentStepId);
          if (release !== undefined) {
            releases.delete(parentStepId);
            release();
          }
        },
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
    try {
      await waitForNthEvent(
        repoStore,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.parkKind === "signal-relay" &&
          e.signalName === "go",
        2,
      );
      const parked = await repoStore.read(runId);
      const leftId = correlationForStep(parked, "left");
      const rightId = correlationForStep(parked, "right");
      expect(leftId).not.toBe(rightId);

      await channel.deliver("go", { ok: false }, "sig-name-only");
      await channel.deliver("go", { ok: true }, "sig-left", leftId);
      await waitForEvent(
        repoStore,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "left" &&
          e.parkKind === "input",
      );
      expect(delivered).toEqual([{ stepId: "left", signalId: "sig-left" }]);
      const mid = await repoStore.read(runId);
      expect(
        mid.some(
          (e) => e.kind === "SignalReceived" && e.signalId === "sig-name-only",
        ),
      ).toBe(false);
      expect(
        mid.some(
          (e) => e.kind === "SignalReceived" && e.correlationId === rightId,
        ),
      ).toBe(false);

      await channel.deliver("go", { ok: true }, "sig-right", rightId);
      await waitForEvent(
        repoStore,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "right" &&
          e.parkKind === "input",
      );
      expect(delivered).toEqual([
        { stepId: "left", signalId: "sig-left" },
        { stepId: "right", signalId: "sig-right" },
      ]);
    } finally {
      await run.cancel("supervisor-operator", "test done");
      await run.complete.catch(() => undefined);
    }
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

    // The section proxied the body's "go" gate up as a correlated signal-relay
    // await on the SAME name. A name-only delivery does not complete it. The
    // matching correlation does, and the section relays that signal (with the
    // original signalId) into the body, which completes and re-arms.
    await waitForPark(repoStore, runId, "signal-relay", 1);
    const correlationId = relayCorrelationId(await repoStore.read(runId));
    await channel.deliver("go", { ok: false }, "sig-name-only");
    await channel.deliver("go", { ok: true }, "sig-1", correlationId);
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
          e.signalId === "sig-1" &&
          e.correlationId === correlationId,
      ),
    ).toBe(true);
    expect(
      log.some(
        (e) => e.kind === "SignalReceived" && e.signalId === "sig-name-only",
      ),
    ).toBe(false);
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
    const correlationId = relayCorrelationId(log);
    expect(
      log.some(
        (e) =>
          e.kind === "SignalAwaitAbandoned" &&
          e.signalName === "go" &&
          e.correlationId === correlationId,
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

    // This seed does not write a body log, so the body re-spawns from an empty log.
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

  test("reuses the logged correlation when a signal-relay park is still open", async () => {
    const runId = "sec-relay-resume-corr";
    const name = "go";
    const correlationId = "corr-open";
    const seed = midSignalRelaySeed(runId, name, {
      signalDelivered: false,
      correlationId,
    });

    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    let delivered:
      | { name: string; payload: unknown; signalId: string }
      | undefined;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      let release: (() => void) | null = null;
      return {
        next: async () => {
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

    // The open park's id is reused. A name-only delivery does not complete
    // it, and resume does not mint a second relay await.
    await channel.deliver(name, { text: "name-only" }, "sig-name");
    await channel.deliver(name, { text: "late" }, "sig-2", correlationId);
    await waitForPark(repoStore, runId, "input", 1);

    expect(delivered).toEqual({
      name,
      payload: { text: "late" },
      signalId: "sig-2",
    });
    const log = await repoStore.read(runId);
    expect(
      log.filter(
        (e) => e.kind === "SignalAwaited" && e.parkKind === "signal-relay",
      ),
    ).toEqual([
      expect.objectContaining({
        signalName: name,
        correlationId,
      }),
    ]);
    expect(
      log.some(
        (e) =>
          e.kind === "SignalReceived" &&
          e.signalId === "sig-2" &&
          e.correlationId === correlationId,
      ),
    ).toBe(true);
    expect(
      log.some((e) => e.kind === "SignalReceived" && e.signalId === "sig-name"),
    ).toBe(false);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("an abandoned correlation does not satisfy the next park on the same name", async () => {
    const runId = "sec-relay-turn";
    const name = "go";
    const abandonedId = "corr-a";
    const openId = "corr-b";
    const event0 = sectionEventRunId(runId, 0);
    const event1 = sectionEventRunId(runId, 1);
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
        childRunId: event0,
        childDefinitionRef: "body-ref",
      },
      {
        kind: "SignalAwaited",
        seq: 4,
        at,
        stepId: "section",
        signalName: name,
        parkKind: "signal-relay",
        correlationId: abandonedId,
      },
      {
        kind: "SignalAwaitAbandoned",
        seq: 5,
        at,
        stepId: "section",
        signalName: name,
        correlationId: abandonedId,
      },
      {
        kind: "SignalReceived",
        seq: 6,
        at,
        signalName: name,
        signalId: "sig-abandoned",
        payload: { text: "late-for-turn-0" },
        correlationId: abandonedId,
      },
      {
        kind: "ChildCompleted",
        seq: 7,
        at,
        childRunId: event0,
        terminalStatus: "completed",
      },
      {
        kind: "ChildSpawned",
        seq: 8,
        at,
        stepId: "section",
        childRunId: event1,
        childDefinitionRef: "body-ref",
      },
      {
        kind: "SignalAwaited",
        seq: 9,
        at,
        stepId: "section",
        signalName: name,
        parkKind: "signal-relay",
        correlationId: openId,
      },
    ];

    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    let delivered:
      | { name: string; payload: unknown; signalId: string }
      | undefined;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      let release: (() => void) | null = null;
      return {
        next: async () => {
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

    await channel.deliver(name, { text: "turn-0" }, "sig-turn-0", abandonedId);
    await channel.deliver(name, { text: "turn-1" }, "sig-turn-1", openId);
    await waitForPark(repoStore, runId, "input", 1);

    expect(delivered).toEqual({
      name,
      payload: { text: "turn-1" },
      signalId: "sig-turn-1",
    });
    const log = await repoStore.read(runId);
    expect(
      log.filter(
        (e) => e.kind === "SignalAwaited" && e.parkKind === "signal-relay",
      ),
    ).toEqual([
      expect.objectContaining({ correlationId: abandonedId }),
      expect.objectContaining({ correlationId: openId }),
    ]);
    expect(
      log.filter(
        (e) => e.kind === "SignalReceived" && e.correlationId === abandonedId,
      ),
    ).toHaveLength(1);
    expect(
      log.some(
        (e) =>
          e.kind === "SignalReceived" &&
          e.signalId === "sig-turn-1" &&
          e.correlationId === openId,
      ),
    ).toBe(true);

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

  test("two parents on one store run distinct event-0 bodies after the first log is terminal", async () => {
    const parentA = "run_parent_a";
    const parentB = "run_parent_b";
    const repoStore = createInMemoryRepoStore();
    let handlerRuns = 0;
    const spawned: string[] = [];
    const bodyDef = defineWorkflow({
      id: "section-body",
      steps: { work: action({ handler: "work" }) },
    });
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      childRunId,
      input,
    }) => {
      spawned.push(childRunId);
      const child = runtimeRun(
        bodyDef,
        {
          ...buildEnv({
            def: bodyDef,
            repoStore,
            signalChannel: createInMemorySignalChannel(),
            spawnSuspendableChild: async () => {
              throw new Error("section body does not spawn");
            },
          }),
          invokeAction: async () => {
            handlerRuns += 1;
            return { output: { ok: true } };
          },
        },
        { runId: childRunId, triggerPayload: input },
      );
      return {
        next: async () => {
          const result = await child.complete;
          return { kind: "terminal", terminalStatus: result.terminalStatus };
        },
        resume: async () => undefined,
        deliverSignal: async () => undefined,
      };
    };
    const def = sectionWorkflow();
    const runA = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: createInMemorySignalChannel(),
        spawnSuspendableChild,
      }),
      { runId: parentA, triggerPayload: { text: "a" } },
    );
    await waitForPark(repoStore, parentA, "input", 1);
    const firstBody = await repoStore.read(sectionEventRunId(parentA, 0));
    expect(firstBody.some((e) => e.kind === "RunCompleted")).toBe(true);

    const runB = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: createInMemorySignalChannel(),
        spawnSuspendableChild,
      }),
      { runId: parentB, triggerPayload: { text: "b" } },
    );
    await waitForPark(repoStore, parentB, "input", 1);

    expect(spawned).toEqual([
      sectionEventRunId(parentA, 0),
      sectionEventRunId(parentB, 0),
    ]);
    expect(handlerRuns).toBe(2);

    await runA.cancel("supervisor-operator", "test done");
    await runB.cancel("supervisor-operator", "test done");
    await runA.complete.catch(() => undefined);
    await runB.complete.catch(() => undefined);
  });

  test("resumes an in-flight body recorded as section__0 on that id", async () => {
    const runId = "sec-legacy-inflight";
    const legacyId = "section__0";
    const seed = midSleepSeed(runId, legacyId);
    const bodyLog = bodyChildLog(legacyId, { phase: "awaiting-timer" });
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    await repoStore.appendBatch(legacyId, bodyLog);
    let spawnedId: string | undefined;
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      childRunId,
    }) => {
      spawnedId = childRunId;
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => undefined,
        deliverSignal: async () => undefined,
      };
    };
    const def = sectionWorkflow();
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

    await waitForPark(repoStore, runId, "input", 1);
    expect(spawnedId).toBe(legacyId);
    const log = await repoStore.read(runId);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual([legacyId]);
    expect(await repoStore.read(legacyId)).toEqual(bodyLog);
    expect(await repoStore.read(sectionEventRunId(runId, 0))).toEqual([]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("mints a prefixed id for the event after a completed legacy body", async () => {
    const runId = "sec-legacy-next";
    const legacyId = "section__0";
    const nextId = sectionEventRunId(runId, 1);
    const inputChannel = signalName("legacy-next");
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
        childRunId: legacyId,
        childDefinitionRef: "body-ref",
      },
      {
        kind: "ChildCompleted",
        seq: 4,
        at,
        childRunId: legacyId,
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
    ];
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    const spawned: string[] = [];
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      childRunId,
      input,
    }) => {
      spawned.push(childRunId);
      expect(input).toEqual({ text: "event-1" });
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => undefined,
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

    await channel.deliver(inputChannel, { text: "event-1" }, "sig-1");
    await waitForPark(repoStore, runId, "input", 2);
    expect(spawned).toEqual([nextId]);
    const log = await repoStore.read(runId);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual([legacyId, nextId]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("a completed new-style event 0 on an input park next mints event 1", async () => {
    const runId = "sec-new-reawait";
    const event0 = sectionEventRunId(runId, 0);
    const event1 = sectionEventRunId(runId, 1);
    const inputChannel = signalName("new-reawait");
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
        childRunId: event0,
        childDefinitionRef: "body-ref",
      },
      {
        kind: "ChildCompleted",
        seq: 4,
        at,
        childRunId: event0,
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
    ];
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    const spawned: { childRunId: string; input: unknown }[] = [];
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      childRunId,
      input,
    }) => {
      spawned.push({ childRunId, input });
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => undefined,
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

    await channel.deliver(inputChannel, { text: "event-1" }, "sig-1");
    await waitForPark(repoStore, runId, "input", 2);
    expect(spawned).toEqual([
      { childRunId: event1, input: { text: "event-1" } },
    ]);
    const log = await repoStore.read(runId);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual([event0, event1]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("a sibling loop child does not change the section event a resume plans", async () => {
    const runId = "sec-loop-sibling";
    const bodyRunId = sectionEventRunId(runId, 0);
    const loopChildId = loopBodyRunId(runId, "rework", 5);
    const seed: WorkflowEvent[] = [
      ...midSleepSeed(runId),
      {
        kind: "ChildSpawned",
        seq: 4,
        at,
        stepId: "rework",
        childRunId: loopChildId,
        childDefinitionRef: "loop-body",
      },
    ];
    const bodyLog = bodyChildLog(bodyRunId, { phase: "awaiting-timer" });
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    await repoStore.appendBatch(bodyRunId, bodyLog);
    let spawnedId: string | undefined;
    const spawnSuspendableChild: SpawnSuspendableChild = async ({
      childRunId,
    }) => {
      spawnedId = childRunId;
      return {
        next: async () => ({ kind: "terminal", terminalStatus: "completed" }),
        resume: async () => undefined,
        deliverSignal: async () => undefined,
      };
    };
    const def = sectionWorkflow();
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

    await waitForPark(repoStore, runId, "input", 1);
    expect(spawnedId).toBe(bodyRunId);
    const log = await repoStore.read(runId);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toEqual([bodyRunId, loopChildId]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });
});

describe("runOnTrigger onBodyFailure: tolerate", () => {
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

  test("a failed body re-arms the section instead of ending it", async () => {
    const runId = "sec-tolerate";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    let spawn = 0;
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      const terminalStatus = spawn++ === 0 ? "failed" : "completed";
      let done = false;
      return {
        next: async () => {
          if (done) throw new Error("next after terminal");
          done = true;
          return { kind: "terminal", terminalStatus };
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

    // The first body failed, but under `tolerate` the section re-arms on an
    // input park for the next trigger rather than ending.
    const ch1 = await waitForPark(repoStore, runId, "input", 1);
    await channel.deliver(ch1, { text: "event-1" }, "sig-1");
    await waitForPark(repoStore, runId, "input", 2);

    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(2);
    // The failed occurrence still commits ChildCompleted{failed} for
    // observability -- tolerate re-arms, it does not swallow the failure.
    expect(
      log.some(
        (e) => e.kind === "ChildCompleted" && e.terminalStatus === "failed",
      ),
    ).toBe(true);
    // The section itself did not end.
    expect(
      log.some((e) => e.kind === "RunCompleted" || e.kind === "RunFailed"),
    ).toBe(false);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("a cancelled body still ends the section even under tolerate", async () => {
    const runId = "sec-tolerate-cancel";
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const spawnSuspendableChild: SpawnSuspendableChild = async () => ({
      next: async () => ({ kind: "terminal", terminalStatus: "cancelled" }),
      resume: async () => undefined,
      deliverSignal: async () => undefined,
    });
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

    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
    const log = await repoStore.read(runId);
    // Cancellation is never tolerated: no re-arm.
    expect(log.filter((e) => e.kind === "ChildSpawned").length).toBe(1);
    expect(
      log.some((e) => e.kind === "SignalAwaited" && e.parkKind === "input"),
    ).toBe(false);
  });

  test("resume: a failed body under tolerate re-parks rather than ending", async () => {
    const runId = "sec-tolerate-resume";
    const event0 = sectionEventRunId(runId, 0);
    const event1 = sectionEventRunId(runId, 1);
    const ch = signalName("corr-rearm");
    // A section that failed event 0 and re-armed on an input park before the
    // crash: byte-identical durable shape to a completed occurrence's re-arm.
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
        childRunId: event0,
        childDefinitionRef: "body-ref",
      },
      {
        kind: "ChildCompleted",
        seq: 4,
        at,
        childRunId: event0,
        terminalStatus: "failed",
      },
      {
        kind: "SignalAwaited",
        seq: 5,
        at,
        stepId: "section",
        signalName: ch,
        parkKind: "input",
      },
    ];
    const repoStore = createInMemoryRepoStore();
    const channel = createInMemorySignalChannel();
    const spawnSuspendableChild: SpawnSuspendableChild = async () => {
      let done = false;
      return {
        next: async () => {
          if (done) throw new Error("next after terminal");
          done = true;
          return { kind: "terminal", terminalStatus: "completed" };
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
      { runId, resumeFromEvents: seed },
    );

    // On resume the section re-adopts the SAME durable input park (it does not
    // end); delivering the next trigger there advances to the next occurrence.
    await channel.deliver(ch, { text: "event-1" }, "sig-resume");
    await waitForPark(repoStore, runId, "input", 2);

    const log = await repoStore.read(runId);
    expect(log.some((e) => e.kind === "RunFailed")).toBe(false);
    expect(
      log.flatMap((e) => (e.kind === "ChildSpawned" ? [e.childRunId] : [])),
    ).toContain(event1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });
});
