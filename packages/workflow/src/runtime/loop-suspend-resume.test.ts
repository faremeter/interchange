// Crash-resume durability for a suspended loop iteration body.
//
// A loop iteration runs through the suspendable-child seam, so a body parked on
// an author `awaitSignal` (or an agent `step` on an approval gate) proxies its
// park up onto the loop CONTAINER step as `awaiting-signal`. When the process
// dies while parked, a fresh run re-drives the durable log: `runLoop` re-derives
// its cursor, `planLoopResume` classifies the parked/mid-relay iteration, and
// the drive re-links it -- re-establishing the container's signal-relay race,
// relaying a signal delivered but not relayed before the crash, or re-adopting
// an approval park -- WITHOUT re-running the body's already-completed pre-park
// steps.
//
// Each test runs a body to a real park (a consistent store keeps both the parent
// and child logs), captures that durable state, then resumes against a fresh
// store seeded with it -- the faithful crash model.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { signalName } from "@intx/types";
import type { ConversationTurn } from "@intx/types/runtime";

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
  runtimeRun,
  step,
  type ActionInvoker,
  type BlobSubstrate,
  type LoopFn,
  type RepoStore,
  type SignalChannel,
  type StepInvoker,
  type WorkflowAuthorizeFn,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

import { resolveDrainBehavior, type DrainController } from "./drain";

const loopFns = (ref: string): LoopFn => {
  // Converge after the first iteration; `carry` is unused but must resolve.
  if (ref === "cont") return () => false;
  if (ref === "next")
    return (_o, currentInput) =>
      typeof currentInput === "number" ? currentInput + 1 : 0;
  throw new Error(`unknown loop fn ${ref}`);
};

const noopInvokeStep: StepInvoker = () => {
  throw new Error("loop suspend-resume test: invokeStep must not be called");
};

function buildEnv(args: {
  parentDef: WorkflowDefinition;
  repoStore: RepoStore;
  blobs: BlobSubstrate;
  signalChannel: SignalChannel;
  invokeAction: ActionInvoker;
  invokeStep?: StepInvoker;
  drain?: DrainController;
}): WorkflowRuntimeEnv {
  const clock = (): Date => new Date();
  const authorize: WorkflowAuthorizeFn = async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
  const env: WorkflowRuntimeEnv = {
    repoStore: args.repoStore,
    scheduler: createInMemoryScheduler({ repoStore: args.repoStore, clock }),
    signalChannel: args.signalChannel,
    blobs: args.blobs,
    directors: createDefaultDirectorRegistry(),
    authorize,
    invokeStep: args.invokeStep ?? noopInvokeStep,
    invokeAction: args.invokeAction,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: args.drain ?? createNoopDrainController(args.parentDef),
    loopFns,
  };
  const loopBodies = new Map(
    enumerateInlineLoopBodies(args.parentDef).map((b) => [b.ref, b.definition]),
  );
  env.spawnLoopIteration = createSpawnLoopIteration(env, loopBodies);
  return env;
}

// Poll the durable log until the container step has parked `count` times on the
// given kind, mirroring on-trigger-run.test.ts's waitForPark.
async function waitForContainerPark(
  repoStore: RepoStore,
  runId: string,
  parkKind: "approval" | "signal-relay",
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

// Copy a captured child log verbatim into the resume store so the re-spawned
// body re-adopts its parked gate (the drive reads runs/<childRunId> for the
// child's resumeFromEvents), the consistent-store crash model.
async function seedChildLog(
  repoStore: RepoStore,
  childRunId: string,
  events: readonly WorkflowEvent[],
): Promise<void> {
  for (const event of events) {
    await repoStore.append(childRunId, event);
  }
}

const awaitBody = defineWorkflow({
  id: "await-body",
  trigger: { type: "manual" },
  steps: {
    // A pre-park step whose completion must NOT be re-run on resume.
    pre: action({ handler: "pre", input: { from: "trigger.payload" } }),
    hold: awaitSignal({ name: "go", after: ["pre"] }),
  },
});

const awaitParent = defineWorkflow({
  id: "await-loop-parent",
  trigger: { type: "manual" },
  steps: {
    rework: loop({
      body: awaitBody,
      while: "cont",
      carry: "next",
      input: { literal: 0 },
      maxIterations: 3,
      onExhausted: "escalate",
    }),
    escalate: action({ handler: "escalate", after: ["rework"] }),
  },
});

function awaitInvokeAction(preRuns: { n: number }): ActionInvoker {
  return async ({ handler }) => {
    if (handler === "pre") {
      preRuns.n += 1;
      return { output: { ran: true } };
    }
    if (handler === "escalate") return { output: "escalated" };
    throw new Error(`unknown handler ${handler}`);
  };
}

describe("loop iteration suspend crash-resume", () => {
  test("re-establishes an awaitSignal park and resumes on a signal delivered after restart", async () => {
    const runId = "await-run";
    const blobs = createInMemoryBlobSubstrate();

    // Run 1: drive to the body's awaitSignal park, then capture the durable
    // parent + child logs at that point (the process "crashes" parked).
    const repoStore1 = createInMemoryRepoStore();
    const preRuns1 = { n: 0 };
    const env1 = buildEnv({
      parentDef: awaitParent,
      repoStore: repoStore1,
      blobs,
      signalChannel: createInMemorySignalChannel(),
      invokeAction: awaitInvokeAction(preRuns1),
    });
    void runtimeRun(awaitParent, env1, { runId }).complete;
    await waitForContainerPark(repoStore1, runId, "signal-relay", 1);
    const parentLog = await repoStore1.read(runId);
    const childLog = await repoStore1.read("rework__0");
    // The pre-park action ran once, and the body is parked (not complete).
    expect(preRuns1.n).toBe(1);
    expect(childLog.some((e) => e.kind === "ChildCompleted")).toBe(false);

    // Run 2: resume against a fresh store seeded with the captured logs and a
    // fresh signal channel; deliver the awaited signal after the restart.
    const repoStore2 = createInMemoryRepoStore();
    await seedChildLog(repoStore2, "rework__0", childLog);
    const preRuns2 = { n: 0 };
    const env2 = buildEnv({
      parentDef: awaitParent,
      repoStore: repoStore2,
      blobs,
      signalChannel: createInMemorySignalChannel(),
      invokeAction: awaitInvokeAction(preRuns2),
    });
    const run2 = runtimeRun(awaitParent, env2, {
      runId,
      resumeFromEvents: parentLog,
    });
    // The in-memory channel queues the delivery until the re-established relay
    // subscribes, so ordering against the resume drive does not matter.
    await run2.signal("go", { done: true }, "sig-1");
    const result = await run2.complete;

    expect(result.terminalStatus).toBe("completed");
    // The resumed body re-adopted its parked gate: the pre-park action did NOT
    // re-run on resume (0 executions in run 2, not a shared-ledger dedup).
    expect(preRuns2.n).toBe(0);
    // Converged after one iteration: onExhausted (escalate) is pruned and the
    // loop reports its outcome.
    expect("escalate" in result.outputs).toBe(false);
    expect(result.outputs.rework).toMatchObject({
      outcome: "converged",
      iterations: 1,
    });
  });

  test("relays a signal delivered to the container but not relayed before the crash", async () => {
    const runId = "relay-run";
    const blobs = createInMemoryBlobSubstrate();

    const repoStore1 = createInMemoryRepoStore();
    const preRuns1 = { n: 0 };
    const env1 = buildEnv({
      parentDef: awaitParent,
      repoStore: repoStore1,
      blobs,
      signalChannel: createInMemorySignalChannel(),
      invokeAction: awaitInvokeAction(preRuns1),
    });
    void runtimeRun(awaitParent, env1, { runId }).complete;
    const relayName = await waitForContainerPark(
      repoStore1,
      runId,
      "signal-relay",
      1,
    );
    const parkedLog = await repoStore1.read(runId);
    const childLog = await repoStore1.read("rework__0");

    // Force the delivered-but-unrelayed window: append the container's
    // SignalReceived (the delivery that consumed the relay await, moving the
    // container to in-flight) but NO relay into the body, exactly where a crash
    // after delivery and before relay leaves the log.
    const lastSeq = parkedLog[parkedLog.length - 1]?.seq ?? 0;
    const delivered: WorkflowEvent = {
      kind: "SignalReceived",
      seq: lastSeq + 1,
      at: new Date().toISOString(),
      signalName: relayName,
      signalId: "sig-pre",
      payload: { done: true },
    };
    const parentLog = [...parkedLog, delivered];

    const repoStore2 = createInMemoryRepoStore();
    await seedChildLog(repoStore2, "rework__0", childLog);
    const preRuns2 = { n: 0 };
    const env2 = buildEnv({
      parentDef: awaitParent,
      repoStore: repoStore2,
      blobs,
      signalChannel: createInMemorySignalChannel(),
      invokeAction: awaitInvokeAction(preRuns2),
    });
    // No post-restart delivery: the resume relays the pre-crash signal into the
    // re-adopted body from the log.
    const result = await runtimeRun(awaitParent, env2, {
      runId,
      resumeFromEvents: parentLog,
    }).complete;

    expect(result.terminalStatus).toBe("completed");
    expect(preRuns2.n).toBe(0);
    // Converged after one iteration: onExhausted (escalate) is pruned.
    expect("escalate" in result.outputs).toBe(false);
    expect(result.outputs.rework).toMatchObject({
      outcome: "converged",
      iterations: 1,
    });
  });
});

const replyTurn: ConversationTurn = {
  role: "assistant",
  content: [{ type: "text", text: "done" }],
  timestamp: 0,
};

const approvalAgent = defineAgent({
  id: "gate-agent",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "m" }] },
});

const approvalBody = defineWorkflow({
  id: "approval-body",
  trigger: { type: "manual" },
  steps: { s: step({ agent: approvalAgent }) },
});

const approvalParent = defineWorkflow({
  id: "approval-loop-parent",
  trigger: { type: "manual" },
  steps: {
    rework: loop({
      body: approvalBody,
      while: "cont",
      carry: "next",
      input: { literal: 0 },
      maxIterations: 3,
      onExhausted: "escalate",
    }),
    escalate: action({ handler: "escalate", after: ["rework"] }),
  },
});

describe("loop iteration approval crash-resume", () => {
  test("re-adopts an approval park and re-invokes the step with a grant delivered after restart", async () => {
    const runId = "approval-run";
    const blobs = createInMemoryBlobSubstrate();
    const escalateAction: ActionInvoker = async ({ handler }) => {
      if (handler === "escalate") return { output: "escalated" };
      throw new Error(`unknown handler ${handler}`);
    };
    // Suspends the agent step on first invocation (no resume) and completes it
    // when re-invoked with the delivered decision.
    const makeInvokeStep = (
      invocations: { resume: unknown }[],
    ): StepInvoker => {
      return async (req) => {
        invocations.push({ resume: req.resume });
        if (req.resume === undefined) {
          return {
            suspend: {
              correlationId: "corr-1",
              kind: "approval",
              approvalSnapshot: {
                name: "gate",
                description: "gate",
                inputSchema: { type: "object" },
                arguments: {},
              },
            },
          };
        }
        return { output: { reply: "done", turn: replyTurn } };
      };
    };

    const repoStore1 = createInMemoryRepoStore();
    const invocations1: { resume: unknown }[] = [];
    const env1 = buildEnv({
      parentDef: approvalParent,
      repoStore: repoStore1,
      blobs,
      signalChannel: createInMemorySignalChannel(),
      invokeAction: escalateAction,
      invokeStep: makeInvokeStep(invocations1),
    });
    void runtimeRun(approvalParent, env1, { runId }).complete;
    await waitForContainerPark(repoStore1, runId, "approval", 1);
    const parentLog = await repoStore1.read(runId);
    const childLog = await repoStore1.read("rework__0");
    // The step was invoked once (the suspending original send) and is parked.
    expect(invocations1).toHaveLength(1);

    const repoStore2 = createInMemoryRepoStore();
    await seedChildLog(repoStore2, "rework__0", childLog);
    const invocations2: { resume: unknown }[] = [];
    const channel2 = createInMemorySignalChannel();
    const env2 = buildEnv({
      parentDef: approvalParent,
      repoStore: repoStore2,
      blobs,
      signalChannel: channel2,
      invokeAction: escalateAction,
      invokeStep: makeInvokeStep(invocations2),
    });
    const run2 = runtimeRun(approvalParent, env2, {
      runId,
      resumeFromEvents: parentLog,
    });
    await channel2.deliver(
      signalName("corr-1"),
      { outcome: "approved" },
      "g-1",
    );
    const result = await run2.complete;

    expect(result.terminalStatus).toBe("completed");
    // Exactly one invocation on resume: the re-invocation carrying the grant.
    // The step re-parked without re-sending the original input.
    expect(invocations2).toHaveLength(1);
    expect(invocations2[0]?.resume).toEqual({
      correlationId: "corr-1",
      decision: { outcome: "approved" },
      kind: "approval",
    });
    // Converged after one iteration: onExhausted (escalate) is pruned.
    expect("escalate" in result.outputs).toBe(false);
    expect(result.outputs.rework).toMatchObject({
      outcome: "converged",
      iterations: 1,
    });
  });
});

function createControllableDrain(
  definition: WorkflowDefinition,
): DrainController & { trigger: () => void } {
  const controller = new AbortController();
  return {
    signal: controller.signal,
    behaviorFor: (stepId) => resolveDrainBehavior(definition, stepId),
    trigger: () => controller.abort(),
  };
}

// A loop whose drainBehavior is the explicit `wait` an author sets for a
// human-in-the-loop rework, distinct from the loop's `cancel` default.
const awaitParentWait = defineWorkflow({
  id: "await-loop-parent-wait",
  trigger: { type: "manual" },
  steps: {
    rework: loop({
      body: awaitBody,
      while: "cont",
      carry: "next",
      input: { literal: 0 },
      maxIterations: 3,
      onExhausted: "escalate",
      drainBehavior: "wait",
    }),
    escalate: action({ handler: "escalate", after: ["rework"] }),
  },
});

describe("loop iteration drain", () => {
  test("a parked iteration sheds on drain under the loop's default cancel behavior", async () => {
    const runId = "drain-cancel-run";
    const repoStore = createInMemoryRepoStore();
    const drain = createControllableDrain(awaitParent);
    const run = runtimeRun(
      awaitParent,
      buildEnv({
        parentDef: awaitParent,
        repoStore,
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: createInMemorySignalChannel(),
        invokeAction: awaitInvokeAction({ n: 0 }),
        drain,
      }),
      { runId },
    );
    await waitForContainerPark(repoStore, runId, "signal-relay", 1);

    // A loop container defaults to `drainBehavior: "cancel"`, so the main loop's
    // drain observation aborts the container's step-local controller and the
    // parked iteration sheds. As with any drained cancel-mode step, the runtime
    // body commits StepFailed and the run terminates as failed -- no
    // CancelRequested is issued here (the supervisor's drainTimeout escalation
    // lives outside this layer).
    drain.trigger();
    const result = await run.complete;
    expect(result.terminalStatus).toBe("failed");
  });

  test("a parked iteration sits through drain when the loop declares wait", async () => {
    const runId = "drain-wait-run";
    const repoStore = createInMemoryRepoStore();
    const drain = createControllableDrain(awaitParentWait);
    const run = runtimeRun(
      awaitParentWait,
      buildEnv({
        parentDef: awaitParentWait,
        repoStore,
        blobs: createInMemoryBlobSubstrate(),
        signalChannel: createInMemorySignalChannel(),
        invokeAction: awaitInvokeAction({ n: 0 }),
        drain,
      }),
      { runId },
    );
    await waitForContainerPark(repoStore, runId, "signal-relay", 1);

    // Under an explicit `wait`, the drain observation leaves the container
    // running, so the parked iteration keeps waiting and the run does not
    // settle while drain is pending.
    drain.trigger();
    const early = await Promise.race([
      run.complete.then(() => "settled"),
      new Promise<string>((r) => setTimeout(() => r("pending"), 75)),
    ]);
    expect(early).toBe("pending");

    // A signal delivered after the drain still resumes the iteration.
    await run.signal("go", { done: true }, "sig-1");
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(result.outputs.rework).toMatchObject({
      outcome: "converged",
      iterations: 1,
    });
  });
});
