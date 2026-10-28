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

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
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
  createSpawnLoopIteration,
  defineWorkflow,
  enumerateInlineLoopBodies,
  loop,
  loopBodyRunId,
  runtimeRun,
  RuntimeResumeUnsupportedError,
  sectionBodyRunId,
  step,
  type LoadParkedApproval,
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
// in-flight body re-adopts with nothing to relay (a bare sleep parked in
// `awaiting-timer`) or holds a park to relay (an author `awaitSignal`, still
// `awaiting-signal`).
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

// A body re-spawned from its log and parked on `names`. A resume or a delivered
// signal releases a park, and the body completes once none is left: the
// contract of a real handle, reduced to what the section's side needs. A body
// whose log is gone `startsOver`: it parks afresh, which no re-park waiter
// sees. Records the spawn and what reached the body.
function parkedBody(
  names: readonly string[],
  opts: { startsOver?: boolean } = {},
): {
  spawn: SpawnSuspendableChild;
  childRunId?: string;
  resumeFromEvents?: readonly WorkflowEvent[] | undefined;
  resumed?: { correlationId: string; decision: unknown };
  delivered?: { name: string; payload: unknown; signalId: string };
} {
  const open = new Set(names);
  const waiters = new Set<() => void>();
  const settled = () =>
    open.size === 0
      ? ({ kind: "terminal", terminalStatus: "completed" } as const)
      : ({ kind: "released" } as const);
  const until = (done: () => boolean, abort?: AbortSignal) =>
    new Promise<void>((resolve, reject) => {
      if (done()) return resolve();
      const wake = (): void => {
        if (!done()) return;
        waiters.delete(wake);
        resolve();
      };
      waiters.add(wake);
      abort?.addEventListener("abort", () => {
        waiters.delete(wake);
        reject(new Error("aborted"));
      });
    });
  const release = (name: string): void => {
    open.delete(name);
    for (const wake of [...waiters]) wake();
  };
  const body: ReturnType<typeof parkedBody> = {
    spawn: async ({ childRunId, resumeFromEvents }) => {
      body.childRunId = childRunId;
      body.resumeFromEvents = resumeFromEvents;
      return {
        next: async () => {
          await until(() => open.size === 0);
          return { kind: "terminal", terminalStatus: "completed" };
        },
        resume: async (correlationId, decision) => {
          body.resumed = { correlationId, decision };
          release(signalName(correlationId));
        },
        deliverSignal: async (name, payload, signalId) => {
          body.delivered = { name, payload, signalId };
          release(name);
        },
        readParkRelease: async (name) =>
          open.has(name) ? { kind: "open" } : settled(),
        waitForParkRelease: async (name, abort) => {
          await until(() => !open.has(name), abort);
          return settled();
        },
        whenSignalAwaiter: (name) =>
          opts.startsOver === true
            ? new Promise<void>(() => undefined)
            : until(() => open.has(name)),
      };
    },
  };
  return body;
}

// A body that runs until the section is torn down, recording the input each
// spawn of it was given. With `endFirst`, the first spawn ends at once.
function recordingBody(
  inputs: unknown[],
  opts: { endFirst?: boolean } = {},
): SpawnSuspendableChild {
  return async ({ input, signal }) => {
    inputs.push(input);
    const ends = opts.endFirst === true && inputs.length === 1;
    return {
      next: () =>
        ends
          ? Promise.resolve({ kind: "terminal", terminalStatus: "completed" })
          : new Promise((resolve) => {
              signal.addEventListener("abort", () => {
                resolve({ kind: "terminal", terminalStatus: "cancelled" });
              });
            }),
      resume: async () => {
        throw new Error("resume on a body that does not park");
      },
      deliverSignal: async () => undefined,
    };
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
    const body = parkedBody([signalName(corr)]);
    const spawnSuspendableChild = body.spawn;

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

    expect(body.childRunId).toBe(bodyRunId);
    expect(body.resumeFromEvents).toEqual(bodyLog);
    expect(body.resumed).toEqual({
      correlationId: corr,
      decision: { approved: true },
    });
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

  test("relays an author signal the section never relayed after a restart", async () => {
    const runId = "sec-authorsig-fresh";
    const seed = midSleepSeed(runId);
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    // The body is parked on an author `awaitSignal` gate whose section relay
    // await never flushed, so the resumed section relays it fresh.
    const bodyRunId = sectionEventRunId(runId, 0);
    await repoStore.appendBatch(
      bodyRunId,
      bodyChildLog(bodyRunId, {
        phase: "awaiting-signal",
        signalName: "author-go",
      }),
    );
    const channel = createInMemorySignalChannel();
    const body = parkedBody(["author-go"]);

    const def = sectionWorkflow();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: channel,
        spawnSuspendableChild: body.spawn,
      }),
      { runId, resumeFromEvents: seed },
    );

    const relay = await waitForEvent(
      repoStore,
      runId,
      (e) =>
        e.kind === "SignalAwaited" &&
        e.parkKind === "signal-relay" &&
        e.signalName === "author-go",
    );
    if (relay.kind !== "SignalAwaited") throw new Error("expected the relay");
    await channel.deliver(
      "author-go",
      { ok: true },
      "sig-go",
      relay.correlationId,
    );
    await waitForPark(repoStore, runId, "input", 1);

    expect(body.delivered).toEqual({
      name: "author-go",
      payload: { ok: true },
      signalId: "sig-go",
    });

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("fails a resume whose un-relayed body approval has no stored snapshot", async () => {
    const runId = "sec-approval-refuse";
    const seed = midSleepSeed(runId);
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    // The body's agent step suspended for approval on a reserved control-plane
    // channel, and the container's proxy park never flushed. Relaying it fresh
    // needs the snapshot the step stored, and this env reads none, so resume
    // fails loud before it re-spawns the body.
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
    expect(failure.error.message).toContain(
      "no step state holds its approval snapshot",
    );
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
    // The seed writes no body log, so the body starts over.
    const body = parkedBody([signalName(corr)], { startsOver: true });
    const spawnSuspendableChild = body.spawn;

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

    expect(body.resumed).toEqual({
      correlationId: corr,
      decision: { approved: true },
    });
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test.each([
    { event: 0, payload: { text: "event-0" } },
    { event: 1, payload: { text: "event-1" } },
  ])(
    "a body of event $event whose log is gone starts over with that event's input",
    async ({ event, payload }) => {
      const runId = `sec-lost-body-${String(event)}`;
      const def = sectionWorkflow();
      const repoStore1 = createInMemoryRepoStore();
      const channel1 = createInMemorySignalChannel();
      const run1 = runtimeRun(
        def,
        buildEnv({
          def,
          repoStore: repoStore1,
          signalChannel: channel1,
          spawnSuspendableChild: recordingBody([], { endFirst: event === 1 }),
        }),
        { runId, triggerPayload: { text: "event-0" } },
      );
      if (event === 1) {
        const input = await waitForPark(repoStore1, runId, "input", 1);
        await channel1.deliver(input, payload, "sig-1");
      }
      await waitForNthEvent(
        repoStore1,
        runId,
        (e) => e.kind === "ChildSpawned",
        event + 1,
      );
      // A copy: the store hands out its live log, which the cancel below
      // extends.
      const parentLog = [...(await repoStore1.read(runId))];
      await run1.cancel("supervisor-operator", "crash");
      await run1.complete.catch(() => undefined);

      // Only the parent log survives the restart.
      const repoStore2 = createInMemoryRepoStore();
      await repoStore2.appendBatch(runId, parentLog);
      const inputs: unknown[] = [];
      const spawned = recordingBody(inputs);
      const respawned = Promise.withResolvers<undefined>();
      const run2 = runtimeRun(
        def,
        buildEnv({
          def,
          repoStore: repoStore2,
          signalChannel: createInMemorySignalChannel(),
          spawnSuspendableChild: async (args) => {
            const handle = await spawned(args);
            respawned.resolve(undefined);
            return handle;
          },
        }),
        { runId, resumeFromEvents: parentLog },
      );
      await respawned.promise;
      expect(inputs).toEqual([payload]);

      await run2.cancel("supervisor-operator", "test done");
      await run2.complete.catch(() => undefined);
    },
  );

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
    // The re-spawned body re-parked on "go" silently.
    const body = parkedBody([name]);
    const spawnSuspendableChild = body.spawn;

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
    expect(body.resumeFromEvents).toEqual([]);
    expect(body.delivered).toEqual({
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

  test("a resumed relay whose body left the park is abandoned on its correlation", async () => {
    const runId = "sec-relay-resume-closed";
    const name = "go";
    const correlationId = "corr-closed";
    const seed = midSignalRelaySeed(runId, name, {
      signalDelivered: false,
      correlationId,
    });
    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    // The re-spawned body is no longer parked on "go".
    const body = parkedBody([]);

    const def = sectionWorkflow();
    const run = runtimeRun(
      def,
      buildEnv({
        def,
        repoStore,
        signalChannel: createInMemorySignalChannel(),
        spawnSuspendableChild: body.spawn,
      }),
      { runId, resumeFromEvents: seed },
    );
    await waitForPark(repoStore, runId, "input", 1);

    const abandoned = (await repoStore.read(runId)).filter(
      (e) => e.kind === "SignalAwaitAbandoned",
    );
    expect(abandoned).toEqual([
      expect.objectContaining({ signalName: name, correlationId }),
    ]);

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
    const body = parkedBody([name]);
    const spawnSuspendableChild = body.spawn;

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

    expect(body.delivered).toEqual({
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
    const inputChannel = signalName("input-corr-1");
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
        kind: "SignalAwaited",
        seq: 8,
        at,
        stepId: "section",
        signalName: inputChannel,
        parkKind: "input",
      },
      {
        kind: "SignalReceived",
        seq: 9,
        at,
        signalName: inputChannel,
        signalId: "trigger-1",
        payload: { text: "event-1" },
      },
      {
        kind: "ChildSpawned",
        seq: 10,
        at,
        stepId: "section",
        childRunId: event1,
        childDefinitionRef: "body-ref",
      },
      {
        kind: "SignalAwaited",
        seq: 11,
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
    const body = parkedBody([name]);
    const spawnSuspendableChild = body.spawn;

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
    // The seed holds event 1's input park; the second is the re-arm.
    await waitForPark(repoStore, runId, "input", 2);

    expect(body.delivered).toEqual({
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

  test("relays a signal that landed before the crash relayed it", async () => {
    const runId = "sec-relay-grant";
    const name = "go";
    // The signal's SignalReceived is durable but was never relayed: the
    // container reduced to in-flight, so the payload lives only in the log.
    const seed = midSignalRelaySeed(runId, name, { signalDelivered: true });

    const repoStore = createInMemoryRepoStore();
    await repoStore.appendBatch(runId, seed);
    const channel = createInMemorySignalChannel();
    // The seed writes no body log, so the body starts over.
    const body = parkedBody([name], { startsOver: true });
    const spawnSuspendableChild = body.spawn;

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

    expect(body.delivered).toEqual({
      name,
      payload: { text: "delivered" },
      signalId: "relay-1",
    });
    const log = await repoStore.read(runId);
    expect(log.filter((e) => e.kind === "ChildCompleted").length).toBe(1);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("relays a signal that landed before the crash into a body whose park is a loop's relay", async () => {
    const runId = "sec-nested-relay";
    const awaitBody = defineWorkflow({
      id: "sec-await-body",
      trigger: { type: "manual" },
      steps: { hold: awaitSignal({ name: "go" }) },
    });
    const loopBody = defineWorkflow({
      id: "sec-loop-body",
      trigger: { type: "manual" },
      steps: {
        inner: loop({
          body: awaitBody,
          while: "cont",
          carry: "same",
          input: { literal: 0 },
          maxIterations: 1,
          onExhausted: "escalate",
        }),
        escalate: action({ handler: "escalate", after: ["inner"] }),
      },
    });
    const def = sectionWorkflow();
    // The in-process loop spawner stands in for the host's section-body
    // spawner: both drive the body through `createSuspendableChildHandle`.
    const nestedEnv = (repoStore: RepoStore): WorkflowRuntimeEnv => {
      const env = buildEnv({
        def,
        repoStore,
        signalChannel: createInMemorySignalChannel(),
        spawnSuspendableChild: async () => {
          throw new Error("replaced below");
        },
      });
      env.invokeAction = async () => ({ output: "escalated" });
      env.loopFns = (ref) =>
        ref === "cont" ? () => false : (_o, currentInput) => currentInput;
      env.spawnLoopIteration = createSpawnLoopIteration(
        env,
        new Map(
          enumerateInlineLoopBodies(loopBody).map((b) => [b.ref, b.definition]),
        ),
      );
      env.spawnSuspendableChild = createSpawnLoopIteration(
        env,
        new Map([["body-ref", loopBody]]),
      );
      return env;
    };

    const repoStore1 = createInMemoryRepoStore();
    void runtimeRun(def, nestedEnv(repoStore1), {
      runId,
      triggerPayload: { text: "event-0" },
    }).complete;
    const relayName = await waitForPark(repoStore1, runId, "signal-relay", 1);
    const bodyRunId = sectionEventRunId(runId, 0);
    const innerBodyRunId = loopBodyRunId(bodyRunId, "inner", 0);
    const parkedLog = await repoStore1.read(runId);
    const bodyLog = await repoStore1.read(bodyRunId);
    const innerBodyLog = await repoStore1.read(innerBodyRunId);

    // The section took the signal, and the crash landed before it reached the
    // body, whose own park is its loop's relay.
    const delivered: WorkflowEvent = {
      kind: "SignalReceived",
      seq: (parkedLog[parkedLog.length - 1]?.seq ?? 0) + 1,
      at,
      signalName: relayName,
      signalId: "sig-pre",
      payload: { done: true },
      correlationId: relayCorrelationId(parkedLog),
    };
    const repoStore2 = createInMemoryRepoStore();
    await repoStore2.appendBatch(bodyRunId, bodyLog);
    await repoStore2.appendBatch(innerBodyRunId, innerBodyLog);
    const run = runtimeRun(def, nestedEnv(repoStore2), {
      runId,
      resumeFromEvents: [...parkedLog, delivered],
    });

    await waitForPark(repoStore2, runId, "input", 1);
    const innerReceived = (await repoStore2.read(innerBodyRunId)).filter(
      (e) => e.kind === "SignalReceived",
    );
    expect(innerReceived).toEqual([
      expect.objectContaining({ signalName: "go", signalId: "sig-pre" }),
    ]);

    await run.cancel("supervisor-operator", "test done");
    await run.complete.catch(() => undefined);
  });

  test("a resumed section relays a later event's park fresh, not the signal an earlier event took", async () => {
    const runId = "sec-since-spawn";
    const body = defineWorkflow({
      id: "since-spawn-body",
      trigger: { type: "manual" },
      steps: {
        gate: awaitSignal({ name: "author-go" }),
        record: action({
          handler: "record",
          after: ["gate"],
          input: { from: "steps.gate.output" },
        }),
      },
    });
    const def = sectionWorkflow();
    const sectionEnv = (
      repoStore: RepoStore,
      signalChannel: SignalChannel,
      recorded: { runId: string | undefined; input: unknown }[],
    ): WorkflowRuntimeEnv => {
      const env = buildEnv({
        def,
        repoStore,
        signalChannel,
        spawnSuspendableChild: async () => {
          throw new Error("replaced below");
        },
      });
      env.invokeAction = async (req) => {
        recorded.push({ runId: req.authzContext.runId, input: req.input });
        return { output: null };
      };
      env.spawnSuspendableChild = createSpawnLoopIteration(
        env,
        new Map([["body-ref", body]]),
      );
      return env;
    };
    const isRelay = (e: WorkflowEvent): boolean =>
      e.kind === "SignalAwaited" &&
      e.parkKind === "signal-relay" &&
      e.signalName === "author-go";
    const isInput = (e: WorkflowEvent): boolean =>
      e.kind === "SignalAwaited" && e.parkKind === "input";
    const body1 = sectionEventRunId(runId, 1);

    const repoStore1 = createInMemoryRepoStore();
    const channel1 = createInMemorySignalChannel();
    const run1 = runtimeRun(def, sectionEnv(repoStore1, channel1, []), {
      runId,
      triggerPayload: { text: "event-0" },
    });
    const relay0 = await waitForNthEvent(repoStore1, runId, isRelay, 1);
    if (relay0.kind !== "SignalAwaited") throw new Error("expected a relay");
    await channel1.deliver(
      "author-go",
      { n: 1 },
      "sig-1",
      relay0.correlationId,
    );
    const input1 = await waitForNthEvent(repoStore1, runId, isInput, 1);
    if (input1.kind !== "SignalAwaited") throw new Error("expected an input");
    await channel1.deliver(input1.signalName, { text: "event-1" }, "ev-1");
    await waitForEvent(
      repoStore1,
      body1,
      (e) => e.kind === "SignalAwaited" && e.signalName === "author-go",
    );
    const relay1 = await waitForNthEvent(repoStore1, runId, isRelay, 2);
    // Event 1's body parked on the name event 0's body took a signal on, and
    // the crash landed before the section's relay for event 1 flushed.
    const parentLog = (await repoStore1.read(runId)).filter(
      (e) => e.seq < relay1.seq,
    );
    const bodyLog = await repoStore1.read(body1);

    const recorded2: { runId: string | undefined; input: unknown }[] = [];
    const repoStore2 = createInMemoryRepoStore();
    await repoStore2.appendBatch(runId, parentLog);
    await repoStore2.appendBatch(body1, bodyLog);
    const channel2 = createInMemorySignalChannel();
    const run2 = runtimeRun(def, sectionEnv(repoStore2, channel2, recorded2), {
      runId,
      resumeFromEvents: parentLog,
    });
    // A resume that hands event 1's body event 0's signal re-arms on input
    // without relaying again.
    const outcome = await Promise.race([
      waitForNthEvent(repoStore2, runId, isRelay, 2),
      waitForNthEvent(repoStore2, runId, isInput, 2),
    ]);
    if (isRelay(outcome) && outcome.kind === "SignalAwaited") {
      await channel2.deliver(
        "author-go",
        { n: 2 },
        "sig-2",
        outcome.correlationId,
      );
      await waitForNthEvent(repoStore2, runId, isInput, 2);
    }

    expect(recorded2).toEqual([{ runId: body1, input: { n: 2 } }]);

    await run2.cancel("supervisor-operator", "test done");
    await run2.complete.catch(() => undefined);
    await run1.cancel("supervisor-operator", "test done");
    await run1.complete.catch(() => undefined);
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

describe("runOnTrigger queued body parks", () => {
  const agent = defineAgent({
    id: "gate-agent",
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
  // A body with two parallel approval steps: the section relays one, and the
  // other waits for it in memory.
  const body = defineWorkflow({
    id: "two-approval-section-body",
    trigger: { type: "manual" },
    steps: { a: step({ agent }), b: step({ agent }) },
  });

  function realBodyEnv(args: {
    def: WorkflowDefinition;
    repoStore: RepoStore;
    signalChannel: SignalChannel;
    invokeStep: StepInvoker;
    loadParkedApproval?: LoadParkedApproval;
  }): WorkflowRuntimeEnv {
    const env: WorkflowRuntimeEnv = {
      ...buildEnv({
        def: args.def,
        repoStore: args.repoStore,
        signalChannel: args.signalChannel,
        spawnSuspendableChild: async () => {
          throw new Error("replaced below");
        },
      }),
      invokeStep: args.invokeStep,
      ...(args.loadParkedApproval !== undefined
        ? { loadParkedApproval: args.loadParkedApproval }
        : {}),
    };
    env.spawnSuspendableChild = createSpawnLoopIteration(
      env,
      new Map([["body-ref", body]]),
    );
    return env;
  }

  test("relays an approval the section had not reached after a restart", async () => {
    const runId = "sec-two-approvals";
    const stored = new Map<string, ApprovalSnapshot>();
    const key = (args: {
      runId?: string;
      stepId?: string;
      attempt?: number;
      correlationId: string;
    }): string =>
      `${String(args.runId)}/${String(args.stepId)}/${String(args.attempt)}/${args.correlationId}`;
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume !== undefined) {
        return {
          output: {
            reply: "done",
            turn: {
              role: "assistant",
              content: [{ type: "text", text: "done" }],
              timestamp: 0,
            },
          },
        };
      }
      const correlationId = `corr-${String(req.authzContext.stepId)}`;
      const approvalSnapshot: ApprovalSnapshot = {
        name: "gate",
        description: "gate",
        inputSchema: { type: "object" },
        arguments: { step: req.authzContext.stepId },
      };
      stored.set(key({ ...req.authzContext, correlationId }), approvalSnapshot);
      return {
        suspend: { correlationId, kind: "approval", approvalSnapshot },
      };
    };
    const def = sectionWorkflow();

    const repoStore1 = createInMemoryRepoStore();
    const env1 = realBodyEnv({
      def,
      repoStore: repoStore1,
      signalChannel: createInMemorySignalChannel(),
      invokeStep,
    });
    void runtimeRun(def, env1, {
      runId,
      triggerPayload: { text: "event-0" },
    }).complete;
    const bodyRunId = sectionEventRunId(runId, 0);
    const shown = await waitForPark(repoStore1, runId, "approval", 1);
    for (const stepId of ["a", "b"]) {
      await waitForEvent(
        repoStore1,
        bodyRunId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.signalName === signalName(`corr-${stepId}`),
      );
    }
    const parentLog = await repoStore1.read(runId);
    const childLog = await repoStore1.read(bodyRunId);
    const queued =
      shown === signalName("corr-a")
        ? signalName("corr-b")
        : signalName("corr-a");

    const repoStore2 = createInMemoryRepoStore();
    await repoStore2.appendBatch(bodyRunId, childLog);
    const channel2 = createInMemorySignalChannel();
    const run2 = runtimeRun(
      def,
      realBodyEnv({
        def,
        repoStore: repoStore2,
        signalChannel: channel2,
        invokeStep,
        loadParkedApproval: async (args) => stored.get(key(args)),
      }),
      { runId, resumeFromEvents: parentLog },
    );
    await channel2.deliver(shown, { outcome: "approved" }, "g-shown");
    // A run that ends instead of relaying says why.
    const ended = run2.complete.then((result) => {
      const failed = result.events.find((e) => e.kind === "StepFailed");
      throw new Error(
        `the run ended ${result.terminalStatus}: ${failed?.kind === "StepFailed" ? failed.error.message : "no step failed"}`,
      );
    });
    void ended.catch(() => undefined);
    await Promise.race([
      waitForEvent(
        repoStore2,
        runId,
        (e) =>
          e.kind === "SignalAwaited" &&
          e.stepId === "section" &&
          e.signalName === queued,
      ),
      ended,
    ]);
    await channel2.deliver(queued, { outcome: "approved" }, "g-queued");
    // The body completed, so the section re-arms for the next event.
    await Promise.race([waitForPark(repoStore2, runId, "input", 1), ended]);

    await run2.cancel("supervisor-operator", "test done");
    await run2.complete.catch(() => undefined);
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
