// Crash-resume durability for a suspended agent `step`.
//
// The in-process suspend/resume bridge (`step-suspend-resume.test.ts`)
// parks a step on `signalName(correlationId)` and resumes it in the SAME
// process. This test covers the crash case: the child process DIES while
// the step is parked, and a fresh run re-drives the durable log. The
// durable tail is `StepStarted{s}` + `SignalAwaited{s, __signal__:corr-1}`
// with no `StepCompleted`, so the step reduces to `awaiting-signal`.
//
// A `kind:"step"` step in `awaiting-signal` is a resumable residual
// (`isResumableAwaitingSignalStep`), so the re-driven run does NOT throw
// `RuntimeResumeUnsupportedError`. `runStep` detects the re-entry, recovers
// the reserved channel from the durable `SignalAwaited` (the runtime-minted
// name is not in the definition), RE-PARKS on it WITHOUT re-invoking the
// agent against the original input, and -- once the operator delivers the
// signal after the restart -- re-invokes the agent with the decision and
// completes with the reply.

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
  type WorkflowEvent,
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
  id: "step-suspend-crash",
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

const at = new Date().toISOString();

describe("step suspend crash-resume", () => {
  test("a step left awaiting-signal by a crash re-parks on the reserved channel and completes on a signal delivered after restart", async () => {
    const runId = "run-crash";
    const channel = createInMemorySignalChannel();
    const invocations: StepInvokeRequest[] = [];
    // The pre-crash process already sent the original input and parked; on
    // resume the ONLY invocation must be the resume re-invocation. A send
    // with `resume === undefined` here would prove the agent was restarted
    // from scratch -- the durability bug this test guards against.
    const invokeStep: StepInvoker = async (req) => {
      invocations.push(req);
      if (req.resume === undefined) {
        return { suspend: { correlationId: "corr-1" } };
      }
      return { output: { reply: "done", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, { invokeStep, signalChannel: channel });

    // Durable tail of a process that crashed while the step was parked:
    // StepStarted{s} + SignalAwaited{s, __signal__:corr-1}, no StepCompleted.
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
        signalName: signalName("corr-1"),
      },
    ];

    const handle = runtimeRun(oneStep, env, { runId, resumeFromEvents: seed });

    // (i) The re-driven run re-parks rather than throwing
    // RuntimeResumeUnsupportedError. Let the re-park land, then confirm the
    // step is back awaiting-signal on the same channel with no new
    // SignalAwaited minted (the durable one is re-adopted).
    await new Promise((r) => setTimeout(r, 50));
    const parked = await env.repoStore.read(runId);
    expect(parked.filter((e) => e.kind === "SignalAwaited").length).toBe(1);
    expect(parked.some((e) => e.kind === "StepCompleted")).toBe(false);
    // No invocation happened while merely re-parking: the agent is NOT
    // re-sent the original input on resume.
    expect(invocations).toHaveLength(0);

    // (ii) The operator signals AFTER the restart, on the reserved channel.
    await channel.deliver(
      signalName("corr-1"),
      { outcome: "approved" },
      "sig-1",
    );

    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");

    // Exactly one invocation: the resume re-invocation carrying the
    // delivered decision. No original-input send.
    expect(invocations).toHaveLength(1);
    expect(invocations[0]?.resume).toEqual({
      correlationId: "corr-1",
      decision: { outcome: "approved" },
    });

    // The step completes with the RE-INVOCATION reply, not the raw payload.
    const completed = result.events.find(
      (e) => e.kind === "StepCompleted" && e.stepId === "s",
    );
    if (completed?.kind !== "StepCompleted") {
      throw new Error("expected a StepCompleted for step s");
    }
    const output = await env.blobs.resolveRef(completed.output.ref);
    expect(output).toEqual({ reply: "done", turn: replyTurn });
    expect(result.outputs["s"]).toEqual({ reply: "done", turn: replyTurn });

    // The durable StepStarted is not re-emitted across the crash boundary.
    const starts = result.events.filter(
      (e) => e.kind === "StepStarted" && e.stepId === "s",
    );
    expect(starts).toHaveLength(1);
  });
});
