// Property: an agent step's side effect runs at most once across a
// mid-step crash. The runtime flushes `StepStarted` durably before it
// invokes the agent, so a crash mid-invocation leaves a durable marker
// with no `StepCompleted`. On resume, the runtime settles that residual
// in-flight step as a terminal `StepFailed` (the agent is NOT
// re-invoked) and the run settles `RunFailed` with the crash reason,
// rather than silently re-invoking the agent or stalling with no
// terminal event.

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

describe("resume mid-agent-step", () => {
  test("a crash after StepStarted but before StepCompleted settles RunFailed without re-invoking the agent", async () => {
    const def = defineWorkflow({
      id: "midstep-resume",
      trigger: { type: "manual" },
      steps: {
        s: step({ agent: makeAgent("solo") }),
      },
    });

    let invocations = 0;
    const invokeStep: StepInvoker = async ({ input }) => {
      invocations += 1;
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
    expect(invocations).toBe(1);

    // Truncate the first run's log at the agent step's own StepStarted,
    // inclusive, to simulate a crash mid-invocation: the durable marker
    // is present but StepCompleted never landed.
    const trimmed: WorkflowEvent[] = [];
    for (const e of result1.events) {
      trimmed.push(e);
      if (e.kind === "StepStarted" && e.stepId === "s") {
        break;
      }
    }

    // Resume against a FRESH repo store but the SAME blob substrate so
    // the seed log's blob refs resolve.
    const repoStore2 = createInMemoryRepoStore();
    const env2: WorkflowRuntimeEnv = {
      ...env1,
      repoStore: repoStore2,
      blobs,
      scheduler: createInMemoryScheduler({ repoStore: repoStore2, clock }),
      signalChannel: createInMemorySignalChannel(),
    };

    const result2 = await runtimeRun(def, env2, {
      runId: result1.runId,
      resumeFromEvents: trimmed,
    }).complete;

    // The agent is invoked at most once: the resume did not re-invoke it.
    expect(invocations).toBe(1);
    expect(result2.terminalStatus).toBe("failed");
    expect(result2.events.some((e) => e.kind === "RunFailed")).toBe(true);

    const failures = result2.events.filter((e) => e.kind === "StepFailed");
    expect(failures.length).toBeGreaterThan(0);
    for (const f of failures) {
      if (f.kind !== "StepFailed") throw new Error("unreachable");
      expect(f.error.code).toBe("crash-mid-invocation");
    }
  });

  test("a crashed mid-invocation step settles terminal while a still-runnable dependent runs to completion on resume", async () => {
    // Two agent steps: `b` depends on `a`. `a` crashes mid-invocation
    // (its StepStarted is durable, StepCompleted never landed) and `b`
    // has not started yet. On resume, `a` settles terminal (StepFailed,
    // NOT re-invoked); because a failed dependency counts as resolved,
    // `b` is genuinely schedulable and runs to completion. The run still
    // settles RunFailed because `a` failed.
    // `b` names `a` in `after` (so it starts only after `a` settles) but
    // takes an explicit literal input rather than the default-input
    // convention's `{ from: "steps.a.output" }`: `a` fails with no
    // output, so a `b` that read `a`'s output could never run. The
    // literal input keeps `b` genuinely schedulable once `a` reaches a
    // terminal (failed) phase.
    const def = defineWorkflow({
      id: "midstep-resume-dependent",
      trigger: { type: "manual" },
      steps: {
        a: step({ agent: makeAgent("agent-a") }),
        b: step({
          agent: makeAgent("agent-b"),
          after: ["a"],
          input: { literal: { seed: 1 } },
        }),
      },
    });

    // Key the counter off the agent identity, not any request field the
    // env does not carry: `StepInvokeRequest` has no top-level `stepId`,
    // so keying on `agent.id` is what distinguishes the two agents'
    // invocations. This makes the "`a` not re-invoked" assertion
    // watertight rather than incidentally true.
    const invocationsByAgent = new Map<string, number>();
    const invokeStep: StepInvoker = async ({ agent, input }) => {
      invocationsByAgent.set(
        agent.id,
        (invocationsByAgent.get(agent.id) ?? 0) + 1,
      );
      return { output: { processed: input, by: agent.id } };
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
    expect(invocationsByAgent.get("agent-a")).toBe(1);
    expect(invocationsByAgent.get("agent-b")).toBe(1);

    // Truncate at `a`'s StepStarted, inclusive: `a`'s StepCompleted is
    // dropped (crash mid-invocation) and `b` has no StepStarted yet
    // (it only starts after `a` completes, which never happened).
    const trimmed: WorkflowEvent[] = [];
    for (const e of result1.events) {
      trimmed.push(e);
      if (e.kind === "StepStarted" && e.stepId === "a") {
        break;
      }
    }
    expect(
      trimmed.some((e) => e.kind === "StepCompleted" && e.stepId === "a"),
    ).toBe(false);
    expect(
      trimmed.some((e) => e.kind === "StepStarted" && e.stepId === "b"),
    ).toBe(false);

    // Fresh counters for the resume so the assertions observe only what
    // the resumed run invokes.
    const resumeInvocations = new Map<string, number>();
    const resumeInvokeStep: StepInvoker = async ({ agent, input }) => {
      resumeInvocations.set(
        agent.id,
        (resumeInvocations.get(agent.id) ?? 0) + 1,
      );
      return { output: { processed: input, by: agent.id } };
    };
    const repoStore2 = createInMemoryRepoStore();
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
      resumeFromEvents: trimmed,
    }).complete;

    // `a` is NOT re-invoked on resume; `b` runs exactly once.
    expect(resumeInvocations.get("agent-a")).toBeUndefined();
    expect(resumeInvocations.get("agent-b")).toBe(1);

    // `b` reached completion on resume.
    expect(
      result2.events.some(
        (e) => e.kind === "StepCompleted" && e.stepId === "b",
      ),
    ).toBe(true);

    // The run settles RunFailed because `a` failed.
    expect(result2.terminalStatus).toBe("failed");
    expect(result2.events.some((e) => e.kind === "RunFailed")).toBe(true);

    // The StepFailed for `a` carries the crash reason.
    const aFailure = result2.events.find(
      (e) => e.kind === "StepFailed" && e.stepId === "a",
    );
    if (aFailure?.kind !== "StepFailed") throw new Error("unreachable");
    expect(aFailure.error.code).toBe("crash-mid-invocation");
  });
});
