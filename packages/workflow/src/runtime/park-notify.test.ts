// `env.onPark` firing at a control-plane suspension.
//
// When a workflow agent step parks on a reserved
// `signalName(correlationId)` channel (the suspend/resume bridge), the
// runtime body notifies the host via `env.onPark` so the host can register
// the correlation out-of-band. A plain `awaitSignal` gate parked on an
// author-chosen name is NOT a control-plane suspension, so it fires no
// notify. The emit is gated on the fresh `SignalAwaited` commit, so a single
// park fires exactly once.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { correlationIdFromSignalName, signalName } from "@intx/types";
import type { ApprovalSnapshot, ConversationTurn } from "@intx/types/runtime";

import {
  awaitSignal,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  resumeFromLog,
  runtimeRun,
  step,
  type SignalChannel,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowPark,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

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

const at = new Date().toISOString();

function buildEnv(
  def: WorkflowDefinition,
  opts: {
    invokeStep: StepInvoker;
    signalChannel: SignalChannel;
    onPark: (park: WorkflowPark) => void;
  },
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  return {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: opts.signalChannel,
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize: async () => ({
      effect: "allow",
      matchingGrants: [],
      resolvedBy: null,
    }),
    invokeStep: opts.invokeStep,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(def),
    onPark: opts.onPark,
  };
}

describe("env.onPark at a control-plane suspension", () => {
  test("a snapshot-less correlated suspend fails the run", async () => {
    const oneStep = defineWorkflow({
      id: "park-suspend",
      trigger: { type: "manual" },
      steps: { s: step({ agent }) },
    });
    const parks: WorkflowPark[] = [];
    // A correlated suspend that carries no approval snapshot -- the shape a
    // director `caps.suspend` produces. The park boundary owns the "an
    // approval park carries a snapshot" invariant, so the runtime fails the
    // run here rather than emit a snapshot-less approval park that would crash
    // the sidecar->hub co-write downstream.
    const invokeStep: StepInvoker = async () => ({
      suspend: { correlationId: "corr-1" },
    });
    const env = buildEnv(oneStep, {
      invokeStep,
      signalChannel: createInMemorySignalChannel(),
      onPark: (park) => parks.push(park),
    });

    const result = await runtimeRun(oneStep, env, { runId: "run-1" }).complete;

    // The throw fires before onPark, so no park escaped.
    expect(parks).toEqual([]);
    expect(result.terminalStatus).toBe("failed");
    const failures = result.events.filter((e) => e.kind === "StepFailed");
    expect(failures.length).toBeGreaterThan(0);
    for (const f of failures) {
      if (f.kind !== "StepFailed") throw new Error("unreachable");
      expect(f.error.message).toContain("carries no approval snapshot");
    }
  });

  test("a snapshot-less correlated suspend commits a durable control-plane SignalAwaited but reduces to failed", async () => {
    // Pins the invariant re-registration enumeration rests on: `parkOnSignal`
    // commits `SignalAwaited` to the durable log BEFORE it checks for the
    // approval snapshot, so a snapshot-less correlated suspend leaves a
    // control-plane `SignalAwaited` in the log -- yet the run fails and the
    // step reduces to `failed`, not `awaiting-signal`. Enumeration keys on the
    // reduced phase, so such a park never surfaces; a consumer that scanned
    // raw `SignalAwaited` events instead would wrongly surface it.
    const oneStep = defineWorkflow({
      id: "park-suspend-durable",
      trigger: { type: "manual" },
      steps: { s: step({ agent }) },
    });
    const invokeStep: StepInvoker = async () => ({
      suspend: { correlationId: "corr-durable" },
    });
    const env = buildEnv(oneStep, {
      invokeStep,
      signalChannel: createInMemorySignalChannel(),
      onPark: () => undefined,
    });
    const runId = "run-durable-fail";

    const result = await runtimeRun(oneStep, env, { runId }).complete;
    expect(result.terminalStatus).toBe("failed");

    const events = await env.repoStore.read(runId);
    const controlPlaneAwaited = events.filter(
      (e) =>
        e.kind === "SignalAwaited" &&
        correlationIdFromSignalName(e.signalName) !== undefined,
    );
    expect(controlPlaneAwaited.length).toBeGreaterThan(0);

    const state = resumeFromLog(runId, events);
    expect(state.steps.get("s")?.phase).toBe("failed");
  });

  test("a plain awaitSignal gate on an author-chosen name fires no onPark", async () => {
    const gateWorkflow = defineWorkflow({
      id: "await-gate",
      trigger: { type: "manual" },
      steps: { g: awaitSignal({ name: "human-approval" }) },
    });
    const channel = createInMemorySignalChannel();
    const parks: WorkflowPark[] = [];
    const invokeStep: StepInvoker = async () => {
      throw new Error("awaitSignal must not invoke a step");
    };
    const env = buildEnv(gateWorkflow, {
      invokeStep,
      signalChannel: channel,
      onPark: (park) => parks.push(park),
    });

    const handle = runtimeRun(gateWorkflow, env, { runId: "run-2" });

    // Let the gate park on its author-chosen signal channel.
    await new Promise((r) => setTimeout(r, 50));

    // The gate parked (SignalAwaited committed) but no control-plane notify
    // fired -- "human-approval" is not a reserved `signalName(...)` channel.
    const parked = await env.repoStore.read("run-2");
    expect(parked.some((e) => e.kind === "SignalAwaited")).toBe(true);
    expect(parks).toEqual([]);

    await channel.deliver("human-approval", { ok: true }, "s-2");

    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(parks).toEqual([]);
  });

  test("a fresh agent-step suspend forwards the approval snapshot onto the park", async () => {
    const oneStep = defineWorkflow({
      id: "park-snapshot",
      trigger: { type: "manual" },
      steps: { s: step({ agent }) },
    });
    const channel = createInMemorySignalChannel();
    const parks: WorkflowPark[] = [];
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume === undefined) {
        return {
          suspend: { correlationId: "corr-snap", approvalSnapshot: snapshot },
        };
      }
      return { output: { reply: "done", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, {
      invokeStep,
      signalChannel: channel,
      onPark: (park) => parks.push(park),
    });

    const handle = runtimeRun(oneStep, env, { runId: "run-snap" });
    await new Promise((r) => setTimeout(r, 50));

    // The live park carries the snapshot the suspend returned, alongside the
    // correlation and approval kind.
    expect(parks).toEqual([
      {
        runId: "run-snap",
        correlationId: "corr-snap",
        kind: "approval",
        approvalSnapshot: snapshot,
      },
    ]);

    await channel.deliver(
      signalName("corr-snap"),
      { outcome: "approved" },
      "sig-1",
    );
    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(parks).toHaveLength(1);
  });

  test("a fresh control-plane park notifies the host only after the suspension is durable", async () => {
    // The durable-before-transmit invariant: parkOnSignal flushes the
    // SignalAwaited to durable storage BEFORE calling env.onPark, so the
    // correlationId reaches the host (and the hub) only once the workflow log
    // can reconstruct it on resume. `commit` buffers and `flush` persists, so
    // a read of the durable log inside the onPark callback observes the
    // control-plane SignalAwaited only when the notify fired after the flush.
    // Were the notify to fire before the flush, this read would find the
    // marker still buffered and absent from the durable log.
    const oneStep = defineWorkflow({
      id: "park-durable-before-notify",
      trigger: { type: "manual" },
      steps: { s: step({ agent }) },
    });
    const runId = "run-durable-notify";
    const channel = createInMemorySignalChannel();
    const parks: WorkflowPark[] = [];
    let durableAtNotify: Promise<boolean> | undefined;
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume === undefined) {
        return {
          suspend: {
            correlationId: "corr-durable-notify",
            approvalSnapshot: snapshot,
          },
        };
      }
      return { output: { reply: "done", turn: replyTurn } };
    };
    // The onPark callback needs the env's repoStore, but the callback is
    // constructed before buildEnv returns, so read the store through a holder
    // assigned immediately after. onPark only fires during the run, well after
    // the assignment; a fire before then is a bug, so fail loud rather than
    // paper over it.
    const holder: { env?: WorkflowRuntimeEnv } = {};
    const env = buildEnv(oneStep, {
      invokeStep,
      signalChannel: channel,
      onPark: (park) => {
        parks.push(park);
        if (holder.env === undefined) {
          throw new Error("onPark fired before env was assigned");
        }
        durableAtNotify = holder.env.repoStore
          .read(runId)
          .then((events) =>
            events.some(
              (e) =>
                e.kind === "SignalAwaited" &&
                correlationIdFromSignalName(e.signalName) !== undefined,
            ),
          );
      },
    });
    holder.env = env;

    const handle = runtimeRun(oneStep, env, { runId });
    await new Promise((r) => setTimeout(r, 50));

    // The host was notified exactly once, and the control-plane SignalAwaited
    // was already durable at that instant.
    expect(parks).toHaveLength(1);
    expect(durableAtNotify).toBeDefined();
    expect(await durableAtNotify).toBe(true);

    await channel.deliver(
      signalName("corr-durable-notify"),
      { outcome: "approved" },
      "sig-durable",
    );
    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(parks).toHaveLength(1);
  });

  test("resuming a durable park re-parks without re-firing onPark", async () => {
    const oneStep = defineWorkflow({
      id: "park-resume",
      trigger: { type: "manual" },
      steps: { s: step({ agent }) },
    });
    const runId = "run-resume-park";
    const channel = createInMemorySignalChannel();
    const parks: WorkflowPark[] = [];
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume === undefined) {
        throw new Error(
          "a durable awaiting-signal park must resume, not re-park",
        );
      }
      return { output: { reply: "done", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, {
      invokeStep,
      signalChannel: channel,
      onPark: (park) => parks.push(park),
    });

    // The post-flush crash window: StepStarted + a durable SignalAwaited on the
    // reserved correlation channel. The step reduces to awaiting-signal, so the
    // resume re-parks through the fresh-emit skip branch and fires no onPark.
    const corr = "corr-resume";
    const seed: WorkflowEvent[] = [
      {
        kind: "RunStarted",
        seq: 1,
        at,
        runId,
        definitionHash: "x",
        trigger: { type: "manual", payload: undefined },
      },
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "s",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "SignalAwaited",
        seq: 3,
        at,
        stepId: "s",
        signalName: signalName(corr),
      },
    ];

    const handle = runtimeRun(oneStep, env, { runId, resumeFromEvents: seed });
    await new Promise((r) => setTimeout(r, 50));
    expect(parks).toEqual([]);

    await channel.deliver(signalName(corr), { outcome: "approved" }, "sig-2");
    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(parks).toEqual([]);
  });

  test("a before-flush crash settles crash-mid-invocation without re-firing onPark", async () => {
    const oneStep = defineWorkflow({
      id: "park-crash",
      trigger: { type: "manual" },
      steps: { s: step({ agent }) },
    });
    const runId = "run-crash";
    const parks: WorkflowPark[] = [];
    let invoked = 0;
    const invokeStep: StepInvoker = async () => {
      invoked += 1;
      return { output: { reply: "done", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, {
      invokeStep,
      signalChannel: createInMemorySignalChannel(),
      onPark: (park) => parks.push(park),
    });

    // The pre-flush crash window: only StepStarted survived (the park's
    // SignalAwaited never flushed). The step reduces to in-flight, so the
    // re-fire settles it as a terminal crash-mid-invocation failure -- it is
    // not re-invoked, does not re-park, and fires no onPark.
    await env.repoStore.append(runId, {
      kind: "RunStarted",
      seq: 1,
      at,
      runId,
      definitionHash: "x",
      trigger: { type: "manual", payload: undefined },
    });
    await env.repoStore.append(runId, {
      kind: "StepStarted",
      seq: 2,
      at,
      stepId: "s",
      attempt: 1,
      input: { ref: "inline:null" },
    });

    const result = await runtimeRun(oneStep, env, { runId }).complete;

    expect(parks).toEqual([]);
    expect(invoked).toBe(0);
    expect(result.terminalStatus).toBe("failed");
    const failures = result.events.filter((e) => e.kind === "StepFailed");
    expect(failures.length).toBeGreaterThan(0);
    for (const f of failures) {
      if (f.kind !== "StepFailed") throw new Error("unreachable");
      expect(f.error.code).toBe("crash-mid-invocation");
    }
  });
});
