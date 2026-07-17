// Crash-resume durability for an agent `step` that RETRIED before it
// suspended.
//
// `step-suspend-crash-resume.test.ts` covers the pure-suspend case: a step
// that parked on its FIRST attempt and crash-resumes. There the step's
// `currentAttempt` is 1, so the runtime's crash-resume re-entry happens to
// re-invoke on attempt 1 whether or not it recovers the attempt -- attempt 1
// is right by coincidence.
//
// This test covers the case the coincidence hides: a step that FAILED
// attempt 1 (StepFailed + AttemptScheduled{nextAttempt:2} + TimerFired) and
// then, on attempt 2, suspended on a signal. Its durable tail reduces to
// `awaiting-signal` with `currentAttempt === 2`. The cold-path ContextStore
// is rooted at `.../steps/<stepId>/attempt-<N>`, so the pending-op the
// suspend committed lives under `attempt-2`. The crash-resume re-invoke MUST
// carry attempt 2 so the reopened store is the same `attempt-2` store the
// reactor rehydrates its gate from; re-invoking on the hardcoded attempt 1
// would reopen `attempt-1`, rehydrate no gate, and the delivered decision
// would correlate against nothing -- a silent forever-hang.
//
// The runtime-test level does not exercise the sidecar's per-attempt
// `stepStorageRoot`; the coverage this test owns is that the resume
// re-invocation carries the recovered `authzContext.attempt` (2, not 1),
// which is the input `stepStorageRoot`/`buildEnv` key the store on. The
// sidecar-side keying assertion (a cold-path resume that opens a store with
// no matching pending-op throws) is grounded on that same recovered attempt.

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

// maxAttempts >= 2 so the step is retry-eligible; the seed drives it through
// a first-attempt failure into a second-attempt suspend.
const oneStep = defineWorkflow({
  id: "step-suspend-retry-crash",
  trigger: { type: "manual" },
  steps: {
    s: step({ agent, retry: { maxAttempts: 3, initialBackoffMs: 200 } }),
  },
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

describe("step suspend retry crash-resume", () => {
  test("a step that retried before suspending crash-resumes on the recovered attempt and completes on a signal delivered after restart", async () => {
    const runId = "run-retry-crash";
    const channel = createInMemorySignalChannel();
    const invocations: StepInvokeRequest[] = [];
    // Pre-crash, the process ran attempt 1 (failed), retried to attempt 2,
    // sent the input, and parked. On resume the ONLY invocation must be the
    // resume re-invocation. `resume === undefined` here would prove the agent
    // was restarted from scratch.
    const invokeStep: StepInvoker = async (req) => {
      invocations.push(req);
      if (req.resume === undefined) {
        return { suspend: { correlationId: "corr-1" } };
      }
      return { output: { reply: "done", turn: replyTurn } };
    };
    const env = buildEnv(oneStep, { invokeStep, signalChannel: channel });

    // Durable tail of a process that FAILED attempt 1, retried to attempt 2,
    // and crashed while parked on the signal:
    //   StepStarted{s, attempt:1}
    //   StepFailed{s, attempt:1, retriesExhausted:false}
    //   TimerSet{t1, stepId:s}          -- retry backoff timer
    //   AttemptScheduled{s, nextAttempt:2, t1}  -- step -> awaiting-timer, attempt 2
    //   TimerFired{t1}                  -- step -> in-flight (attempt 2)
    //   SignalAwaited{s, __signal__:corr-1}  -- step -> awaiting-signal, attempt 2
    // No StepCompleted, so the step reduces to awaiting-signal with
    // currentAttempt === 2.
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
        kind: "StepFailed",
        seq: 3,
        at,
        stepId: "s",
        attempt: 1,
        error: { message: "attempt 1 failed" },
        retriesExhausted: false,
      },
      {
        kind: "TimerSet",
        seq: 4,
        at,
        timerId: "t1",
        fireAt: at,
        stepId: "s",
      },
      {
        kind: "AttemptScheduled",
        seq: 5,
        at,
        stepId: "s",
        nextAttempt: 2,
        timerId: "t1",
        fireAt: at,
      },
      {
        kind: "TimerFired",
        seq: 6,
        at,
        timerId: "t1",
      },
      {
        kind: "SignalAwaited",
        seq: 7,
        at,
        stepId: "s",
        signalName: signalName("corr-1"),
      },
    ];

    const handle = runtimeRun(oneStep, env, { runId, resumeFromEvents: seed });

    // (i) The re-driven run re-parks rather than throwing
    // RuntimeResumeUnsupportedError, and does NOT re-invoke the agent merely
    // to re-park. Only one SignalAwaited (the durable one re-adopted); no
    // StepCompleted; no invocation yet.
    await new Promise((r) => setTimeout(r, 50));
    const parked = await env.repoStore.read(runId);
    expect(parked.filter((e) => e.kind === "SignalAwaited").length).toBe(1);
    expect(parked.some((e) => e.kind === "StepCompleted")).toBe(false);
    expect(invocations).toHaveLength(0);

    // (ii) The operator signals AFTER the restart, on the reserved channel.
    await channel.deliver(
      signalName("corr-1"),
      { outcome: "approved" },
      "sig-1",
    );

    const result = await handle.complete;
    expect(result.terminalStatus).toBe("completed");

    // Exactly one invocation: the resume re-invocation carrying the delivered
    // decision. This is where the bug bites: without recovering the attempt,
    // the resume re-invokes with `authzContext.attempt === 1` and buildEnv
    // reopens the wrong `attempt-1` store. Assert the recovered attempt is 2.
    expect(invocations).toHaveLength(1);
    const resumeInvocation = invocations[0];
    if (resumeInvocation === undefined) {
      throw new Error("expected a resume invocation");
    }
    expect(resumeInvocation.authzContext.attempt).toBe(2);
    expect(resumeInvocation.resume).toEqual({
      correlationId: "corr-1",
      decision: { outcome: "approved" },
    });

    // The step completes with the resume re-invocation reply.
    const completed = result.events.find(
      (e) => e.kind === "StepCompleted" && e.stepId === "s",
    );
    if (completed?.kind !== "StepCompleted") {
      throw new Error("expected a StepCompleted for step s");
    }
    expect(completed.attempt).toBe(2);
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
