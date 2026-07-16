// Suspend/resume bridge for an agent `step`.
//
// A step whose `invokeStep` returns `{ suspend: { correlationId } }`
// becomes a durable `awaiting-signal` step parked under
// `signalName(correlationId)`. When the decision is delivered on that
// channel, the step is re-invoked with `resume` carrying the delivered
// payload as `decision`, and the step completes with the RE-INVOCATION's
// reply -- not the raw signal payload.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName } from "@intx/types";
import type { ConversationTurn } from "@intx/types/runtime";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  step,
  type SignalChannel,
  type StepInvokeRequest,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "m" }] },
});

const oneStep = defineWorkflow({
  id: "step-suspend",
  trigger: { type: "manual" },
  steps: { s: step({ agent }) },
});

const replyTurn: ConversationTurn = {
  role: "assistant",
  content: [{ type: "text", text: "done" }],
  timestamp: 0,
};

function buildEnv(
  def: WorkflowDefinition,
  opts: { invokeStep: StepInvoker; signalChannel: SignalChannel },
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
  };
}

describe("step suspend/resume bridge", () => {
  test("a suspended step parks awaiting-signal, resumes on the correlated signal, and completes with the re-invocation reply", async () => {
    const channel = createInMemorySignalChannel();
    const invocations: StepInvokeRequest[] = [];
    const invokeStep: StepInvoker = async (req) => {
      invocations.push(req);
      if (req.resume === undefined) {
        return { suspend: { correlationId: "corr-1" } };
      }
      return { output: { reply: "done", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, { invokeStep, signalChannel: channel });

    const handle = runtimeRun(oneStep, env, { runId: "run-1" });

    // Let the first invocation land and the step park on the signal
    // channel before delivering the decision.
    await new Promise((r) => setTimeout(r, 50));

    // (i) The step parked awaiting-signal, and (ii) SignalAwaited was
    // committed under signalName("corr-1").
    const parked = await env.repoStore.read("run-1");
    const awaited = parked.find((e) => e.kind === "SignalAwaited");
    if (awaited?.kind !== "SignalAwaited") {
      throw new Error("expected a SignalAwaited event while parked");
    }
    expect(awaited.stepId).toBe("s");
    expect(awaited.signalName).toBe(signalName("corr-1"));

    // (iii) Deliver the decision under that name; the step re-invokes.
    await channel.deliver(
      signalName("corr-1"),
      { outcome: "approved" },
      "sig-1",
    );

    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");

    // Two invocations: the initial send and the resume re-invocation.
    expect(invocations).toHaveLength(2);
    expect(invocations[0]?.resume).toBeUndefined();
    // (iii) The re-invocation carries the delivered payload as decision.
    expect(invocations[1]?.resume).toEqual({
      correlationId: "corr-1",
      decision: { outcome: "approved" },
    });

    // (iv) The step completes with the REPLY output, not the raw payload.
    const completed = result.events.find(
      (e) => e.kind === "StepCompleted" && e.stepId === "s",
    );
    if (completed?.kind !== "StepCompleted") {
      throw new Error("expected a StepCompleted for step s");
    }
    const output = await env.blobs.resolveRef(completed.output.ref);
    expect(output).toEqual({ reply: "done", turn: replyTurn });
    expect(result.outputs["s"]).toEqual({ reply: "done", turn: replyTurn });

    // Exactly one StepStarted: the agent step's own entry, not re-emitted
    // across the suspend.
    const starts = result.events.filter(
      (e) => e.kind === "StepStarted" && e.stepId === "s",
    );
    expect(starts).toHaveLength(1);
  });

  test("a resume that suspends again re-parks under the new correlation and completes on the second signal", async () => {
    const channel = createInMemorySignalChannel();
    const decisions: unknown[] = [];
    const invokeStep: StepInvoker = async (req) => {
      if (req.resume === undefined) {
        return { suspend: { correlationId: "corr-A" } };
      }
      decisions.push(req.resume.decision);
      if (req.resume.correlationId === "corr-A") {
        // The agent parked a second time on a different correlation.
        return { suspend: { correlationId: "corr-B" } };
      }
      return { output: { reply: "finished", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, { invokeStep, signalChannel: channel });

    const handle = runtimeRun(oneStep, env, { runId: "run-2" });

    await new Promise((r) => setTimeout(r, 50));
    await channel.deliver(signalName("corr-A"), { step: "A" }, "sig-A");

    // Let the re-park land before delivering the second signal.
    await new Promise((r) => setTimeout(r, 50));
    const parked = await env.repoStore.read("run-2");
    const awaitedNames = parked
      .filter((e) => e.kind === "SignalAwaited")
      .map((e) => (e.kind === "SignalAwaited" ? e.signalName : ""));
    expect(awaitedNames).toContain(signalName("corr-A"));
    expect(awaitedNames).toContain(signalName("corr-B"));

    await channel.deliver(signalName("corr-B"), { step: "B" }, "sig-B");

    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(decisions).toEqual([{ step: "A" }, { step: "B" }]);
    expect(result.outputs["s"]).toEqual({ reply: "finished", turn: replyTurn });
  });
});
