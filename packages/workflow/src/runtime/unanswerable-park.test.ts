// A park in a run tree with nothing upstream to answer it must fail at the
// park, not wait.
//
// An untimed park needs a signal from outside the run; a terminal child has
// no such outside, so it would wait forever and no operator would learn an
// approval was wanted. A timed gate is a different case: its own timer
// resolves it in process, so it needs nothing from upstream.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName } from "@intx/types";
import type { ApprovalSnapshot } from "@intx/types/runtime";

import {
  awaitSignal,
  createInMemoryBlobSubstrate,
  loop,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runLocal,
  runtimeRun,
  step,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const snapshot: ApprovalSnapshot = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

const untimedGate = defineWorkflow({
  id: "untimed-gate",
  trigger: { type: "manual" },
  steps: { g: awaitSignal({ name: "approve" }) },
});

const timedGate = defineWorkflow({
  id: "timed-gate",
  trigger: { type: "manual" },
  steps: { g: awaitSignal({ name: "approve", timeout: 50 }) },
});

const approvalStep = defineWorkflow({
  id: "approval-step",
  trigger: { type: "manual" },
  steps: { s: step({ agent }) },
});

/** A step invoker that parks once on an approval, as a tool `ask` does. */
function suspendingInvoker(correlationId: string): StepInvoker {
  let suspended = false;
  return async () => {
    if (suspended) return { output: null };
    suspended = true;
    return {
      suspend: {
        correlationId,
        kind: "approval" as const,
        approvalSnapshot: snapshot,
      },
    };
  };
}

function buildEnv(
  def: WorkflowDefinition,
  opts: {
    hasUpstreamSignalResolver: boolean;
    invokeStep?: StepInvoker;
  },
): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  return {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
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
    hasUpstreamSignalResolver: opts.hasUpstreamSignalResolver,
  };
}

describe("a run tree with no upstream resolver", () => {
  test("fails an author gate at the park rather than waiting forever", async () => {
    const env = buildEnv(untimedGate, { hasUpstreamSignalResolver: false });
    const run = runtimeRun(untimedGate, env, { runId: "run-no-resolver" });

    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");

    const events = await env.repoStore.read("run-no-resolver");
    const failed = events.find((e) => e.kind === "StepFailed");
    const message = failed?.kind === "StepFailed" ? failed.error.message : "";
    // Names the gate, so an author can find it, and points at the boundary
    // rather than at the gate being wrong.
    expect(message).toContain("approve");
    expect(message).toContain("childWorkflow");
    // Must not send the author to a loop or onTrigger body: at depth, that is
    // already where they are.
    expect(message).not.toContain("hold the gate in a loop");
  });

  test("fails an approval park at the park, so no approval is left pending", async () => {
    const correlationId = "corr-no-resolver";
    const env = buildEnv(approvalStep, {
      hasUpstreamSignalResolver: false,
      invokeStep: suspendingInvoker(correlationId),
    });
    const run = runtimeRun(approvalStep, env, { runId: "run-approval-park" });

    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");

    // The refusal precedes the durable suspension, so nothing downstream holds
    // a correlation that will never resolve.
    const events = await env.repoStore.read("run-approval-park");
    expect(events.map((e) => e.kind)).not.toContain("SignalAwaited");
    const failed = events.find((e) => e.kind === "StepFailed");
    const message = failed?.kind === "StepFailed" ? failed.error.message : "";
    expect(message).toContain(signalName(correlationId));
  });

  test("still resolves a timed gate, which needs nothing from upstream", async () => {
    const env = buildEnv(timedGate, { hasUpstreamSignalResolver: false });
    const run = runtimeRun(timedGate, env, { runId: "run-timed-gate" });

    const result = await run.complete;
    // No `onTimeout` route, so the gate fails on expiry -- on its own timer,
    // having genuinely parked, not refused at entry.
    expect(result.terminalStatus).toBe("failed");
    const kinds = (await env.repoStore.read("run-timed-gate")).map(
      (e) => e.kind,
    );
    expect(kinds).toContain("SignalAwaited");
    expect(kinds).toContain("TimerFired");
  });
});

describe("a run tree that can be answered", () => {
  test("parks an untimed gate as before", async () => {
    const env = buildEnv(untimedGate, { hasUpstreamSignalResolver: true });
    const run = runtimeRun(untimedGate, env, { runId: "run-with-resolver" });

    // Nothing delivers, so the run stays parked: the guard must not fire here.
    const settled = await Promise.race([
      run.complete.then(() => "settled" as const),
      new Promise<"pending">((resolve) =>
        setTimeout(() => {
          resolve("pending");
        }, 300),
      ),
    ]);
    expect(settled).toBe("pending");

    const kinds = (await env.repoStore.read("run-with-resolver")).map(
      (e) => e.kind,
    );
    expect(kinds).toContain("SignalAwaited");

    await run.cancel("supervisor-operator", "test teardown");
  });
});

// A gate inside a loop body runs through the suspendable seam, so the refusal
// has to reach it; otherwise the body relays the name onto a container that
// waits on a signal nothing can send.
const loopFns = (ref: string) =>
  ref === "keepGoing"
    ? () => false
    : (_output: unknown, carried: unknown) =>
        (typeof carried === "number" ? carried : 0) + 1;

function loopHolding(gate: ReturnType<typeof awaitSignal>): WorkflowDefinition {
  const body = defineWorkflow({
    id: "loop-body-with-gate",
    trigger: { type: "manual" },
    steps: { hold: gate },
  });
  return defineWorkflow({
    id: "loop-holding-a-gate",
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body,
        while: "keepGoing",
        carry: "nextCount",
        input: { literal: 0 },
        maxIterations: 3,
        onExhausted: "escalate",
      }),
      escalate: step({ agent, after: ["rework"] }),
    },
  });
}

describe("a gate nested in a loop body", () => {
  // runLocal wires the loop iteration executor; a hand-built env without one
  // fails for want of loop support and would pass these for the wrong reason.
  test("is refused when the run tree has no upstream resolver", async () => {
    const def = loopHolding(awaitSignal({ name: "approve" }));
    const run = runLocal(def, {
      runId: "run-loop-gate-no-resolver",
      hasUpstreamSignalResolver: false,
      loopFns,
      authorize: allowAll,
    });

    const settled = await Promise.race([
      run.complete.then(() => "settled" as const),
      new Promise<"pending">((resolve) =>
        setTimeout(() => {
          resolve("pending");
        }, 1500),
      ),
    ]);
    expect(settled).toBe("settled");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");

    // The body's own message names the gate, but it lands in the iteration's
    // log, which a runLocal handle does not expose. The container records the
    // distinguishing half: the loop failed on its first iteration, and the
    // body never got far enough to relay the gate's name up. A parked body
    // would commit `SignalAwaited(approve, "signal-relay")` first, so its
    // absence is the refusal.
    const failed = result.events.find((e) => e.kind === "StepFailed");
    expect(failed?.kind === "StepFailed" ? failed.stepId : "").toBe("rework");
    const message = failed?.kind === "StepFailed" ? failed.error.message : "";
    expect(message).toContain("rework");
    expect(message).toContain("iteration 0");
    expect(result.events.map((e) => e.kind)).not.toContain("SignalAwaited");
  });

  test("still parks when the container can relay a decision down", async () => {
    const def = loopHolding(awaitSignal({ name: "approve" }));
    const run = runLocal(def, {
      runId: "run-loop-gate-with-resolver",
      hasUpstreamSignalResolver: true,
      loopFns,
      authorize: allowAll,
    });

    // Nothing delivers, so it parks: the guard must not reach a body whose
    // container really can relay.
    const settled = await Promise.race([
      run.complete.then(() => "settled" as const),
      new Promise<"pending">((resolve) =>
        setTimeout(() => {
          resolve("pending");
        }, 500),
      ),
    ]);
    expect(settled).toBe("pending");

    await run.cancel("supervisor-operator", "test teardown");

    // The paired control: an answerable container really does relay the body's
    // gate name up, so the same log that carries no `SignalAwaited` when
    // refused carries one here.
    const relayed = (await run.complete).events.find(
      (e) => e.kind === "SignalAwaited",
    );
    if (relayed?.kind !== "SignalAwaited") {
      throw new Error("expected the container to relay the body's gate");
    }
    expect(relayed.stepId).toBe("rework");
    expect(relayed.signalName).toBe("approve");
    expect(relayed.parkKind).toBe("signal-relay");
  });
});
