// Property: the crashed-in-flight settling runs against the ADOPTED
// canonical durable log, not only against a `resumeFromEvents` seed.
//
// The supervisor re-drives a crashed run as a FRESH run: it re-fires the
// parked inbound message with `runId = messageId` and NO
// `resumeFromEvents`. `executeRunBody` then finds the durable log already
// carrying the crashed run's tail (a `RunStarted` + an agent step's
// `StepStarted` with no `StepCompleted`). Keying the settling pass on the
// canonical `state.phase === "running"` -- rather than on whether this
// process received a seed -- settles the residual in-flight step as a
// terminal `StepFailed` (the agent is NOT re-invoked) and lets the run
// settle `RunFailed`, instead of stalling with no schedulable primitive.
//
// A fresh re-fire against a durable log that is ALREADY terminal returns
// the existing terminal result without re-driving (no `RunStarted`, no
// `terminal-phase` throw, no agent invocation). The short-circuit
// reconstructs the terminal `RunResult` from the log for every terminal
// phase -- `failed`, `completed`, and `cancelled` -- and the reconstructed
// result matches the original live-path result byte-for-byte (runId,
// terminalStatus, hydrated outputs, and the full event log).
//
// A second property covers the seeded-resume side of the reload-at-entry
// restructuring: a genuine `resumeFromEvents` seed truncated at a
// completed step's `StepCompleted` adopts that step by skip (it is NOT
// re-invoked) and hydrates its recorded output from the canonical log so
// a downstream step's selector can read it.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  step,
  type RepoStore,
  type StepInvoker,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: id,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

// Copy every event of `source` into `target` verbatim, at its original
// seq -- the shape the supervisor's re-fire recovery observes: a durable
// log written by a prior (crashed) process, not a seed handed to this
// one. `append` preserves the historical seqs, matching how the runtime
// re-fires against an already-populated store.
async function seedStore(
  target: RepoStore,
  runId: string,
  source: readonly WorkflowEvent[],
): Promise<void> {
  for (const event of source) {
    await target.append(runId, event);
  }
}

describe("re-fire adopts a crashed step from the durable log", () => {
  test("a non-terminal surviving log is adopted and its crashed step is settled without re-invoking the agent", async () => {
    const def = defineWorkflow({
      id: "refire-adopt-crash",
      trigger: { type: "manual" },
      steps: {
        s: step({ agent: makeAgent("solo") }),
      },
    });

    // Key the counter off the agent identity: `StepInvokeRequest` carries
    // no top-level `stepId`, so `agent.id` is what distinguishes an
    // invocation, and this driver must not invoke the crashed agent at all.
    const invocationsByAgent = new Map<string, number>();
    const invokeStep: StepInvoker = async ({ agent, input }) => {
      invocationsByAgent.set(
        agent.id,
        (invocationsByAgent.get(agent.id) ?? 0) + 1,
      );
      return { output: { processed: input } };
    };
    const clock = () => new Date();
    const blobs = createInMemoryBlobSubstrate();
    const repoStore1 = createInMemoryRepoStore();
    const env1: WorkflowRuntimeEnv = {
      repoStore: repoStore1,
      scheduler: createInMemoryScheduler({ repoStore: repoStore1, clock }),
      signalChannel: createInMemorySignalChannel(),
      blobs,
      directors: createDefaultDirectorRegistry(),
      authorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      invokeStep,
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
    };

    const result1 = await runtimeRun(def, env1).complete;
    expect(invocationsByAgent.get("solo")).toBe(1);

    // Truncate the first run's log at the agent step's own StepStarted,
    // inclusive: the durable marker is present but StepCompleted never
    // landed (a crash mid-invocation).
    const trimmed: WorkflowEvent[] = [];
    for (const e of result1.events) {
      trimmed.push(e);
      if (e.kind === "StepStarted" && e.stepId === "s") {
        break;
      }
    }

    // Pre-seed a fresh store with the surviving crashed log at its
    // original seqs, then re-fire the SAME runId with NO resumeFromEvents
    // -- the supervisor's fresh re-fire shape. Share the blob substrate so
    // the surviving StepStarted input ref still resolves.
    const repoStore2 = createInMemoryRepoStore();
    await seedStore(repoStore2, result1.runId, trimmed);

    const refireInvocations = new Map<string, number>();
    const refireInvokeStep: StepInvoker = async ({ agent, input }) => {
      refireInvocations.set(
        agent.id,
        (refireInvocations.get(agent.id) ?? 0) + 1,
      );
      return { output: { processed: input } };
    };
    const env2: WorkflowRuntimeEnv = {
      ...env1,
      repoStore: repoStore2,
      blobs,
      scheduler: createInMemoryScheduler({ repoStore: repoStore2, clock }),
      signalChannel: createInMemorySignalChannel(),
      invokeStep: refireInvokeStep,
    };

    const result2 = await runtimeRun(def, env2, {
      runId: result1.runId,
    }).complete;

    // The crashed agent is NOT re-invoked on this driver.
    expect(refireInvocations.get("solo")).toBeUndefined();
    expect(result2.terminalStatus).toBe("failed");
    expect(result2.events.some((e) => e.kind === "RunFailed")).toBe(true);

    const failures = result2.events.filter((e) => e.kind === "StepFailed");
    expect(failures.length).toBeGreaterThan(0);
    for (const f of failures) {
      if (f.kind !== "StepFailed") throw new Error("unreachable");
      expect(f.error.code).toBe("crash-mid-invocation");
    }
  });

  test("a terminal surviving log is returned as-is with no re-drive", async () => {
    // Force the first run to fail so the surviving log is terminal
    // (RunFailed). The re-fire must return that terminal result cleanly:
    // no fresh RunStarted (which would throw terminal-phase), no agent
    // re-invocation.
    const def = defineWorkflow({
      id: "refire-terminal-noop",
      trigger: { type: "manual" },
      steps: {
        s: step({ agent: makeAgent("solo") }),
      },
    });

    const invocationsByAgent = new Map<string, number>();
    const failingInvokeStep: StepInvoker = async ({ agent }) => {
      invocationsByAgent.set(
        agent.id,
        (invocationsByAgent.get(agent.id) ?? 0) + 1,
      );
      throw new Error("agent boom");
    };
    const clock = () => new Date();
    const blobs = createInMemoryBlobSubstrate();
    const repoStore1 = createInMemoryRepoStore();
    const env1: WorkflowRuntimeEnv = {
      repoStore: repoStore1,
      scheduler: createInMemoryScheduler({ repoStore: repoStore1, clock }),
      signalChannel: createInMemorySignalChannel(),
      blobs,
      directors: createDefaultDirectorRegistry(),
      authorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      invokeStep: failingInvokeStep,
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
    };

    const result1 = await runtimeRun(def, env1).complete;
    expect(result1.terminalStatus).toBe("failed");
    expect(result1.events.some((e) => e.kind === "RunFailed")).toBe(true);
    expect(invocationsByAgent.get("solo")).toBe(1);

    // Pre-seed a fresh store with the FULL terminal log, then re-fire the
    // same runId with no resumeFromEvents.
    const repoStore2 = createInMemoryRepoStore();
    await seedStore(repoStore2, result1.runId, result1.events);

    const refireInvocations = new Map<string, number>();
    const refireInvokeStep: StepInvoker = async ({ agent }) => {
      refireInvocations.set(
        agent.id,
        (refireInvocations.get(agent.id) ?? 0) + 1,
      );
      throw new Error("agent boom");
    };
    const env2: WorkflowRuntimeEnv = {
      ...env1,
      repoStore: repoStore2,
      blobs,
      scheduler: createInMemoryScheduler({ repoStore: repoStore2, clock }),
      signalChannel: createInMemorySignalChannel(),
      invokeStep: refireInvokeStep,
    };

    const result2 = await runtimeRun(def, env2, {
      runId: result1.runId,
    }).complete;

    // No re-drive: the agent is not invoked, and the terminal result
    // matches the surviving log.
    expect(refireInvocations.get("solo")).toBeUndefined();
    expect(result2.terminalStatus).toBe("failed");
    expect(result2.events.some((e) => e.kind === "RunFailed")).toBe(true);

    // The re-fire did not append a second terminal event.
    const terminalCount = result2.events.filter(
      (e) =>
        e.kind === "RunFailed" ||
        e.kind === "RunCompleted" ||
        e.kind === "RunCancelled",
    ).length;
    expect(terminalCount).toBe(1);
  });

  test("a completed multi-step terminal log is returned byte-for-byte identical with no re-drive", async () => {
    // Exercises the `completed` branch of the terminal short-circuit and
    // pins reconstruction fidelity: the reload-and-return result must
    // equal the original live-path RunResult field for field (runId,
    // terminalStatus, hydrated outputs, full event log), not merely carry
    // the same terminal status.
    const def = defineWorkflow({
      id: "refire-completed-fidelity",
      trigger: { type: "manual" },
      steps: {
        a: step({ agent: makeAgent("agent-a") }),
        b: step({ agent: makeAgent("agent-b"), after: ["a"] }),
      },
    });

    const invokeStep: StepInvoker = async ({ agent, input }) => ({
      output: { by: agent.id, saw: input },
    });
    const clock = () => new Date();
    const blobs = createInMemoryBlobSubstrate();
    const repoStore1 = createInMemoryRepoStore();
    const env1: WorkflowRuntimeEnv = {
      repoStore: repoStore1,
      scheduler: createInMemoryScheduler({ repoStore: repoStore1, clock }),
      signalChannel: createInMemorySignalChannel(),
      blobs,
      directors: createDefaultDirectorRegistry(),
      authorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      invokeStep,
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
    };

    const result1 = await runtimeRun(def, env1).complete;
    expect(result1.terminalStatus).toBe("completed");

    // Re-fire against the full completed log in a fresh store, sharing the
    // blob substrate so the recorded output refs still resolve.
    const repoStore2 = createInMemoryRepoStore();
    await seedStore(repoStore2, result1.runId, result1.events);

    const refireInvocations = new Map<string, number>();
    const refireInvokeStep: StepInvoker = async ({ agent }) => {
      refireInvocations.set(
        agent.id,
        (refireInvocations.get(agent.id) ?? 0) + 1,
      );
      return { output: { by: agent.id } };
    };
    const env2: WorkflowRuntimeEnv = {
      ...env1,
      repoStore: repoStore2,
      blobs,
      scheduler: createInMemoryScheduler({ repoStore: repoStore2, clock }),
      signalChannel: createInMemorySignalChannel(),
      invokeStep: refireInvokeStep,
    };

    const result2 = await runtimeRun(def, env2, {
      runId: result1.runId,
    }).complete;

    expect(refireInvocations.size).toBe(0);
    expect(result2.runId).toBe(result1.runId);
    expect(result2.terminalStatus).toBe("completed");
    expect(result2.outputs).toEqual(result1.outputs);
    expect(result2.events).toEqual(result1.events);
  });

  test("a cancelled terminal log is returned as-is with no re-drive", async () => {
    // Exercises the `cancelled` branch of the terminal short-circuit. The
    // first run's step blocks until aborted so a mid-flight cancel settles
    // it `cancelled`; the re-fire against that log must return `cancelled`
    // without re-invoking the agent or appending a second terminal event.
    const def = defineWorkflow({
      id: "refire-cancelled-noop",
      trigger: { type: "manual" },
      steps: {
        s: step({ agent: makeAgent("solo") }),
      },
    });

    const blockingInvokeStep: StepInvoker = async ({ signal }) =>
      new Promise((_resolve, reject) => {
        if (signal.aborted) {
          reject(new Error("aborted"));
          return;
        }
        signal.addEventListener("abort", () => reject(new Error("aborted")), {
          once: true,
        });
      });
    const clock = () => new Date();
    const blobs = createInMemoryBlobSubstrate();
    const repoStore1 = createInMemoryRepoStore();
    const env1: WorkflowRuntimeEnv = {
      repoStore: repoStore1,
      scheduler: createInMemoryScheduler({ repoStore: repoStore1, clock }),
      signalChannel: createInMemorySignalChannel(),
      blobs,
      directors: createDefaultDirectorRegistry(),
      authorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      invokeStep: blockingInvokeStep,
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
    };

    const handle = runtimeRun(def, env1);
    // Let the step reach in-flight, then cancel so it settles cancelled.
    await new Promise((resolve) => setTimeout(resolve, 20));
    await handle.cancel("self", "test cancel");
    const result1 = await handle.complete;
    expect(result1.terminalStatus).toBe("cancelled");
    expect(result1.events.some((e) => e.kind === "RunCancelled")).toBe(true);

    const repoStore2 = createInMemoryRepoStore();
    await seedStore(repoStore2, result1.runId, result1.events);

    const refireInvocations = new Map<string, number>();
    const refireInvokeStep: StepInvoker = async ({ agent }) => {
      refireInvocations.set(
        agent.id,
        (refireInvocations.get(agent.id) ?? 0) + 1,
      );
      return { output: null };
    };
    const env2: WorkflowRuntimeEnv = {
      ...env1,
      repoStore: repoStore2,
      blobs,
      scheduler: createInMemoryScheduler({ repoStore: repoStore2, clock }),
      signalChannel: createInMemorySignalChannel(),
      invokeStep: refireInvokeStep,
    };

    const result2 = await runtimeRun(def, env2, {
      runId: result1.runId,
    }).complete;

    expect(refireInvocations.size).toBe(0);
    expect(result2.terminalStatus).toBe("cancelled");
    expect(result2.events).toEqual(result1.events);

    const terminalCount = result2.events.filter(
      (e) =>
        e.kind === "RunFailed" ||
        e.kind === "RunCompleted" ||
        e.kind === "RunCancelled",
    ).length;
    expect(terminalCount).toBe(1);
  });
});

describe("seeded resume adopts a completed step by skip", () => {
  test("a resumeFromEvents seed truncated at a completed step hydrates its output and runs the dependent once", async () => {
    // The seeded-resume side of the reload-at-entry restructuring: state
    // is established from the durable log after the seed is written, so a
    // seed truncated at `a`'s StepCompleted leaves `a` completed (adopted
    // by skip, NOT re-invoked) and `b` schedulable. `a`'s output is
    // hydrated from the canonical log so `b`'s default-input selector
    // (`steps.a.output`) resolves.
    const def = defineWorkflow({
      id: "seeded-resume-adopt-completed",
      trigger: { type: "manual" },
      steps: {
        a: step({ agent: makeAgent("agent-a") }),
        b: step({ agent: makeAgent("agent-b"), after: ["a"] }),
      },
    });

    const invokeStep: StepInvoker = async ({ agent, input }) => ({
      output: { by: agent.id, saw: input },
    });
    const clock = () => new Date();
    const blobs = createInMemoryBlobSubstrate();
    const repoStore1 = createInMemoryRepoStore();
    const env1: WorkflowRuntimeEnv = {
      repoStore: repoStore1,
      scheduler: createInMemoryScheduler({ repoStore: repoStore1, clock }),
      signalChannel: createInMemorySignalChannel(),
      blobs,
      directors: createDefaultDirectorRegistry(),
      authorize: async () => ({
        effect: "allow",
        matchingGrants: [],
        resolvedBy: null,
      }),
      invokeStep,
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
    };

    const result1 = await runtimeRun(def, env1).complete;
    expect(result1.terminalStatus).toBe("completed");

    // Truncate the seed at `a`'s StepCompleted, inclusive: `a` is done,
    // `b` has no StepStarted yet.
    const seed: WorkflowEvent[] = [];
    for (const e of result1.events) {
      seed.push(e);
      if (e.kind === "StepCompleted" && e.stepId === "a") {
        break;
      }
    }
    expect(seed.some((e) => e.kind === "StepStarted" && e.stepId === "b")).toBe(
      false,
    );

    // Resume into a FRESH empty store via resumeFromEvents (not a bare
    // re-fire): state must be established from the seed written into the
    // store. Share the blob substrate so `a`'s recorded output resolves.
    const repoStore2 = createInMemoryRepoStore();
    const resumeInvocations = new Map<string, number>();
    const resumeInvokeStep: StepInvoker = async ({ agent, input }) => {
      resumeInvocations.set(
        agent.id,
        (resumeInvocations.get(agent.id) ?? 0) + 1,
      );
      return { output: { by: agent.id, saw: input } };
    };
    const env2: WorkflowRuntimeEnv = {
      ...env1,
      repoStore: repoStore2,
      blobs,
      scheduler: createInMemoryScheduler({ repoStore: repoStore2, clock }),
      signalChannel: createInMemorySignalChannel(),
      invokeStep: resumeInvokeStep,
    };

    const result2 = await runtimeRun(def, env2, {
      runId: result1.runId,
      resumeFromEvents: seed,
    }).complete;

    // `a` adopted by skip (not re-invoked); `b` runs exactly once.
    expect(resumeInvocations.get("agent-a")).toBeUndefined();
    expect(resumeInvocations.get("agent-b")).toBe(1);
    expect(result2.terminalStatus).toBe("completed");

    // `a`'s output was hydrated from the canonical log, so it is present
    // and `b` saw it through its default-input selector.
    expect(result2.outputs.a).toEqual({ by: "agent-a", saw: null });
    expect(result2.outputs.b).toEqual({
      by: "agent-b",
      saw: { by: "agent-a", saw: null },
    });
  });
});
