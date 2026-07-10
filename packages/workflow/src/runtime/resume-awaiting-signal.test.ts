// Resume contract for an `awaitSignal` gate. Two shapes resume:
//
//   1. The tail is `SignalAwaited` (no signal yet): the gate is
//      `awaiting-signal`. A run re-driving the durable log re-offers the
//      gate (`isResumableAwaitingSignalStep`); runAwaitSignal skips the
//      already-emitted StepStarted/SignalAwaited and RE-PARKS on the
//      signal channel, so a signal delivered later (the operator signals
//      AFTER the restart) resolves it.
//   2. The tail already carries a `SignalReceived` for the gate (no
//      timeout): the reduction moved the gate `in-flight` -- the
//      crash-after-signal-before-`StepCompleted` window
//      (`isResumableReceivedAwaitSignalStep`). runAwaitSignal recovers the
//      payload from the logged event and completes the step without a live
//      deliver.
//
// A timeout-bearing `awaitSignal` left `in-flight` is REFUSED: its reduced
// state is indistinguishable from a fired timeout, so completing it with a
// signal payload would be wrong.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  awaitSignal,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  RuntimeResumeUnsupportedError,
  step,
  type SignalChannel,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const gateOnly = defineWorkflow({
  id: "wait-resume",
  trigger: { type: "manual" },
  steps: { w: awaitSignal({ name: "go" }) },
});

const gateOnlyTimeout = defineWorkflow({
  id: "wait-resume-timeout",
  trigger: { type: "manual" },
  steps: { w: awaitSignal({ name: "go", timeout: 60_000 }) },
});

const twoGatesSameName = defineWorkflow({
  id: "wait-resume-two-gates",
  trigger: { type: "manual" },
  steps: {
    gateA: awaitSignal({ name: "go" }),
    gateB: awaitSignal({ name: "go" }),
  },
});

const gateThenStep = defineWorkflow({
  id: "wait-resume-gate-then-step",
  trigger: { type: "manual" },
  steps: {
    gate: awaitSignal({ name: "go" }),
    after: step({
      agent: defineAgent({
        id: "a",
        systemPrompt: "s",
        tools: [],
        capabilities: [],
        inference: { sources: [{ provider: "anthropic", model: "m" }] },
      }),
      after: ["gate"],
    }),
  },
});

function buildEnv(
  def: WorkflowDefinition,
  opts: { invokeStep?: StepInvoker; signalChannel?: SignalChannel } = {},
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  return {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: opts.signalChannel ?? createInMemorySignalChannel(),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: opts.invokeStep ?? (async () => ({ output: null })),
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
  };
}

const at = new Date().toISOString();

function runStartedSeed(runId: string): WorkflowEvent {
  return {
    kind: "RunStarted",
    seq: 1,
    at,
    runId,
    definitionHash: "x",
    trigger: { type: "manual", payload: undefined },
  };
}

describe("resume awaiting signal", () => {
  test("resumes a seed whose tail is SignalAwaited (no signal yet): re-parks and a live deliver drives it to completion", async () => {
    const runId = "run-await";
    const channel = createInMemorySignalChannel();
    const env = buildEnv(gateOnly, { signalChannel: channel });
    const seed: WorkflowEvent[] = [
      runStartedSeed(runId),
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "w",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      { kind: "SignalAwaited", seq: 3, at, stepId: "w", signalName: "go" },
    ];

    const handle = runtimeRun(gateOnly, env, {
      runId,
      resumeFromEvents: seed,
    });
    // The resumed run re-parks on the signal channel; deliver the awaited
    // signal so the re-armed awaiter resolves and the step completes.
    await new Promise((r) => setTimeout(r, 50));
    await channel.deliver("go", { resumed: true }, "sig-live");

    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");

    const types = result.events.map((e) => e.kind);
    // StepStarted/SignalAwaited are the durable seeds, not re-emitted.
    expect(types.filter((t) => t === "StepStarted").length).toBe(1);
    expect(types.filter((t) => t === "SignalAwaited").length).toBe(1);
    expect(types.filter((t) => t === "SignalReceived").length).toBe(1);
    expect(types.filter((t) => t === "StepCompleted").length).toBe(1);
    expect(types).toContain("RunCompleted");
  });

  test("resumes a seed that already carries the SignalReceived (gate in-flight), completing it without a live deliver", async () => {
    const runId = "run-received";
    const env = buildEnv(gateOnly);
    // The delivery landed durably before the crash: SignalReceived after
    // SignalAwaited moves the gate to `in-flight`. No StepCompleted{w}
    // yet -- the crash-after-signal-before-StepCompleted window.
    const seed: WorkflowEvent[] = [
      runStartedSeed(runId),
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "w",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      { kind: "SignalAwaited", seq: 3, at, stepId: "w", signalName: "go" },
      {
        kind: "SignalReceived",
        seq: 4,
        at,
        signalName: "go",
        signalId: "sig-predelivered",
        payload: { resumed: true },
      },
    ];

    const result = await runtimeRun(gateOnly, env, {
      runId,
      resumeFromEvents: seed,
    }).complete;
    expect(result.terminalStatus).toBe("completed");

    const types = result.events.map((e) => e.kind);
    // The signal was already received; no second SignalReceived is minted.
    expect(types.filter((t) => t === "SignalReceived").length).toBe(1);
    expect(types.filter((t) => t === "StepCompleted").length).toBe(1);
    expect(types).toContain("RunCompleted");

    const completed = result.events.find((e) => e.kind === "StepCompleted");
    if (completed?.kind !== "StepCompleted") throw new Error("unreachable");
    const output = await env.blobs.resolveRef(completed.output.ref);
    expect(output).toEqual({ resumed: true });
  });

  test("a fresh re-drive of a durable log with the gate in-flight-received recovers it and runs its dependents once", async () => {
    // This is the host's re-trigger recovery: a fresh run (NO
    // resumeFromEvents) is driven against a runId whose durable log the
    // crashed process left with the gate `in-flight` (SignalReceived,
    // StepCompleted{gate} not yet). The fresh run's RunStarted is
    // phase-rejected, it adopts the durable log, and
    // `isResumableReceivedAwaitSignalStep` re-offers the gate so
    // runAwaitSignal completes it -- WITHOUT this the run would stall
    // ("no schedulable primitives") because the dependent step is blocked
    // on the non-terminal gate. The dependent agent runs exactly once.
    const runId = "run-redrive";
    let invocations = 0;
    const env = buildEnv(gateThenStep, {
      invokeStep: async () => {
        invocations += 1;
        return { output: { ran: true } };
      },
    });
    const seed: WorkflowEvent[] = [
      runStartedSeed(runId),
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "gate",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      { kind: "SignalAwaited", seq: 3, at, stepId: "gate", signalName: "go" },
      {
        kind: "SignalReceived",
        seq: 4,
        at,
        signalName: "go",
        signalId: "sig-Y",
        payload: { go: true },
      },
    ];
    for (const e of seed) await env.repoStore.append(runId, e);

    // Fresh re-drive: same runId, no resumeFromEvents.
    const result = await runtimeRun(gateThenStep, env, { runId }).complete;
    expect(result.terminalStatus).toBe("completed");
    expect(invocations).toBe(1);
    const events = await env.repoStore.read(runId);
    expect(
      events.some((e) => e.kind === "StepCompleted" && e.stepId === "gate"),
    ).toBe(true);
    expect(
      events.some((e) => e.kind === "StepCompleted" && e.stepId === "after"),
    ).toBe(true);
  });

  test("two concurrent same-name gates both left in-flight-received is refused (ambiguous: payload cannot be bound to a gate)", async () => {
    const runId = "run-ambiguous";
    const env = buildEnv(twoGatesSameName);
    // Both dependency-free gates parked on "go", then two deliveries landed
    // durably before the crash: SignalReceived{sig-1} consumes gateA (first
    // awaiter), SignalReceived{sig-2} then consumes gateB. Both gates are
    // now `in-flight` with no StepCompleted -- the same crash window as the
    // single-gate case, but findConsumedSignal (match by name only) cannot
    // tell which delivery each gate consumed, so completing either would
    // risk binding it to the other gate's payload.
    const seed: WorkflowEvent[] = [
      runStartedSeed(runId),
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "gateA",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      { kind: "SignalAwaited", seq: 3, at, stepId: "gateA", signalName: "go" },
      {
        kind: "StepStarted",
        seq: 4,
        at,
        stepId: "gateB",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      { kind: "SignalAwaited", seq: 5, at, stepId: "gateB", signalName: "go" },
      {
        kind: "SignalReceived",
        seq: 6,
        at,
        signalName: "go",
        signalId: "sig-1",
        payload: { which: "A" },
      },
      {
        kind: "SignalReceived",
        seq: 7,
        at,
        signalName: "go",
        signalId: "sig-2",
        payload: { which: "B" },
      },
    ];

    const result = await runtimeRun(twoGatesSameName, env, {
      runId,
      resumeFromEvents: seed,
    }).complete;

    // Fail-loud: the ambiguous topology is refused rather than silently
    // completing a gate with the wrong payload. The short-circuit guard
    // throws RuntimeResumeUnsupportedError; the body lands it as StepFailed
    // and the run ends `failed`. Neither gate ever reaches StepCompleted, so
    // no wrong payload is bound.
    expect(result.terminalStatus).toBe("failed");
    const types = result.events.map((e) => e.kind);
    expect(types.filter((t) => t === "StepCompleted").length).toBe(0);
    const failures = result.events.filter((e) => e.kind === "StepFailed");
    expect(failures.length).toBeGreaterThan(0);
    for (const f of failures) {
      if (f.kind !== "StepFailed") throw new Error("unreachable");
      expect(f.error.message).toContain(
        "more than one concurrent awaitSignal gate for go consumed a signal",
      );
    }
  });

  test("a timeout-bearing awaitSignal left in-flight stays rejected (indistinguishable from a fired timeout)", async () => {
    const runId = "run-timeout";
    const env = buildEnv(gateOnlyTimeout);
    // A timeout awaitSignal whose timer fired reduces to `in-flight` with
    // no SignalReceived. The reduced state cannot tell that apart from a
    // signal-consumed in-flight, so the runtime declines rather than risk
    // completing a timed-out run with a signal payload.
    const seed: WorkflowEvent[] = [
      runStartedSeed(runId),
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "w",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "SignalAwaited",
        seq: 3,
        at,
        stepId: "w",
        signalName: "go",
        timeoutAt: new Date(Date.now() + 60_000).toISOString(),
      },
      {
        kind: "TimerSet",
        seq: 4,
        at,
        timerId: "timer-1",
        fireAt: new Date(Date.now() + 60_000).toISOString(),
        stepId: "w",
      },
      { kind: "TimerFired", seq: 5, at, timerId: "timer-1" },
    ];

    await expect(
      runtimeRun(gateOnlyTimeout, env, {
        runId,
        resumeFromEvents: seed,
      }).complete,
    ).rejects.toBeInstanceOf(RuntimeResumeUnsupportedError);
  });
});
