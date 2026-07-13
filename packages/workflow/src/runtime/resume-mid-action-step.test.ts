// Property: an action step's side effect runs at most once across a
// mid-invocation crash. `runAction` flushes `StepStarted` durably before
// it invokes the action handler, so the durable log at the instant of
// invocation already carries `StepStarted` with no `StepCompleted`. A
// crash there leaves that residual, which resume settles as a terminal
// `StepFailed` (the action is NOT re-invoked) and the run settles
// `RunFailed`.
//
// The crash is modelled by capturing the durable log at the first line of
// `invokeAction` -- the exact state a power-loss at that instant would
// leave -- and resuming from it. This is faithful where truncating a
// completed run's log is not: a completed run flushes its whole buffer at
// the terminal boundary, so its log carries the action's `StepStarted`
// regardless of whether `runAction` made it durable at invoke time.
// Without the barrier a lone action's `StepStarted` is buffered, unflushed
// at invoke time, and absent from the captured snapshot -- so the resume
// has no residual to settle and the property does not hold.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import {
  action,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  runtimeRun,
  type ActionInvoker,
  type StepInvoker,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

describe("resume mid-action-step", () => {
  test("a lone action flushes StepStarted durably before invoking, so a mid-invocation crash settles RunFailed without re-invoking the action", async () => {
    const def = defineWorkflow({
      id: "midaction-resume",
      trigger: { type: "manual" },
      steps: {
        act: action({ handler: "noop" }),
      },
    });

    const runId = "midaction-run-fixed";
    let invocations = 0;
    // Capture the durable log at the instant the action is invoked: this
    // is the true mid-invocation crash boundary. The barrier's effect is
    // that this snapshot already carries the action's `StepStarted`.
    let snapshotAtInvoke: readonly WorkflowEvent[] = [];
    const invokeAction: ActionInvoker = async ({ input }) => {
      invocations += 1;
      // Copy to freeze the point-in-time view: the in-memory store's
      // `read` returns its live array, which later appends mutate in
      // place, so an un-copied reference would grow to the full log.
      snapshotAtInvoke = [...(await env1.repoStore.read(runId))];
      return { output: { processed: input } };
    };
    // Never invoked: the workflow has no agent step. Fail loudly if the
    // runtime ever routes here, rather than masking a mis-wired test.
    const invokeStep: StepInvoker = async () => {
      throw new Error("invokeStep must not run for an action-only workflow");
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
      invokeAction,
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
    };

    await runtimeRun(def, env1, { runId }).complete;
    expect(invocations).toBe(1);

    // Load-bearing assertion: the barrier flushed `StepStarted` durably
    // BEFORE the handler ran. Without the barrier the snapshot is empty
    // (both `RunStarted` and the buffered `StepStarted` are unflushed at
    // invoke time), and this fails -- which is what proves the test
    // exercises the barrier rather than the kind-agnostic settle path.
    expect(
      snapshotAtInvoke.some(
        (e) => e.kind === "StepStarted" && e.stepId === "act",
      ),
    ).toBe(true);

    // Resume from the durable state as of the crash instant, against a
    // FRESH repo store but the SAME blob substrate so the snapshot's blob
    // refs resolve.
    const repoStore2 = createInMemoryRepoStore();
    const env2: WorkflowRuntimeEnv = {
      ...env1,
      repoStore: repoStore2,
      blobs,
      scheduler: createInMemoryScheduler({ repoStore: repoStore2, clock }),
      signalChannel: createInMemorySignalChannel(),
    };

    const result2 = await runtimeRun(def, env2, {
      runId,
      resumeFromEvents: [...snapshotAtInvoke],
    }).complete;

    // The action is invoked at most once: the resume did not re-invoke it.
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
});
