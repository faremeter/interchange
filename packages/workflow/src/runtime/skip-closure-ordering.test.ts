// emitSkipClosure completes a branch-prune closure leaf-first, so a skipped
// step never becomes terminal while a skipped dependent is still offerable.
// Without leaf-first ordering, a skipped intermediate that completed before
// its skipped dependent was settled would unblock that dependent, and the
// drive loop -- woken by an unrelated sibling settling mid-prune -- would
// schedule a step that was supposed to be pruned. These tests drive a gate
// whose not-selected branch is a depth-2 chain (ordinary, then with a sleep)
// alongside an independent sibling forced to settle inside the prune window.
//
// "Ran" vs "pruned" is distinguished by whether `invokeStep` was called for a
// step's agent, and by the run completing rather than hanging on a pruned
// sleep's timer.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  defineWorkflow,
  gate,
  runtimeRun,
  sleep,
  step,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

function agent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
}

function buildEnv(
  def: WorkflowDefinition,
  invokeStep: StepInvoker,
  drain: WorkflowRuntimeEnv["drain"] = createNoopDrainController(def),
): {
  env: WorkflowRuntimeEnv;
  repoStore: ReturnType<typeof createInMemoryRepoStore>;
} {
  const clock = (): Date => new Date();
  const repoStore = createInMemoryRepoStore();
  const env: WorkflowRuntimeEnv = {
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
    invokeStep,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain,
  };
  return { env, repoStore };
}

// An already-aborted drain reporting "cancel": the production window after
// drain fires but before the supervisor commits CancelRequested, so the run
// phase is still `running`. A prune must complete in this window -- the step's
// abort is set, but bailing would leave the not-selected branch live.
function abortedDrain(): WorkflowRuntimeEnv["drain"] {
  const controller = new AbortController();
  controller.abort();
  return { signal: controller.signal, behaviorFor: () => "cancel" };
}

// A step whose invocation settles only after several microtasks, so it wakes
// the drive loop's Promise.race in the middle of a concurrent prune.
async function settleLate(): Promise<{ output: null }> {
  for (let i = 0; i < 8; i += 1) await Promise.resolve();
  return { output: null };
}

describe("emitSkipClosure leaf-first ordering", () => {
  test("a gate prunes a depth-2 ordinary else-branch under a mid-prune sibling", async () => {
    const def = defineWorkflow({
      id: "gate-chain",
      trigger: { type: "manual" },
      steps: {
        g: gate({
          when: { from: "trigger.payload" },
          then: "keep",
          else: "e1",
        }),
        keep: step({ agent: agent("keep"), after: ["g"] }),
        e1: step({ agent: agent("e1"), after: ["g"] }),
        e2: step({ agent: agent("e2"), after: ["e1"] }),
        sib: step({ agent: agent("sib") }),
      },
    });
    const invoked: string[] = [];
    const { env } = buildEnv(def, async (req) => {
      invoked.push(req.agent.id);
      if (req.agent.id === "sib") return settleLate();
      return { output: null };
    });
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: true });
    const res = await run.complete;

    expect(res.terminalStatus).toBe("completed");
    // The whole not-selected chain is pruned; neither depth is ever invoked.
    expect(invoked).toContain("keep");
    expect(invoked).toContain("sib");
    expect(invoked).not.toContain("e1");
    expect(invoked).not.toContain("e2");
  });

  test("a gate prunes an else-branch containing a sleep under a mid-prune sibling", async () => {
    const def = defineWorkflow({
      id: "gate-sleep",
      trigger: { type: "manual" },
      steps: {
        g: gate({
          when: { from: "trigger.payload" },
          then: "keep",
          else: "e1",
        }),
        keep: step({ agent: agent("keep"), after: ["g"] }),
        e1: step({ agent: agent("e1"), after: ["g"] }),
        nap: sleep({ duration: 9_999_999, after: ["e1"] }),
        sib: step({ agent: agent("sib") }),
      },
    });
    const invoked: string[] = [];
    const { env } = buildEnv(def, async (req) => {
      invoked.push(req.agent.id);
      if (req.agent.id === "sib") return settleLate();
      return { output: null };
    });
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: true });
    const res = await run.complete;

    // If the pruned sleep were scheduled it would arm a ~long timer and the
    // run would not complete; completion proves the sleep stayed pruned.
    expect(res.terminalStatus).toBe("completed");
    expect(invoked).toContain("keep");
    expect(invoked).not.toContain("e1");
  });

  test("a gate prunes its else-branch even when the step is drained", async () => {
    // A drain fires the step's abort while the run is still `running`. The
    // prune must complete anyway; bailing would leave the not-selected chain
    // live, and its `after`-dep (the gate) becoming terminal would schedule it.
    const def = defineWorkflow({
      id: "gate-drain",
      trigger: { type: "manual" },
      steps: {
        g: gate({
          when: { from: "trigger.payload" },
          then: "keep",
          else: "e1",
        }),
        keep: step({ agent: agent("keep"), after: ["g"] }),
        e1: step({ agent: agent("e1"), after: ["g"] }),
        e2: step({ agent: agent("e2"), after: ["e1"] }),
      },
    });
    const invoked: string[] = [];
    const { env } = buildEnv(
      def,
      async (req) => {
        invoked.push(req.agent.id);
        return { output: null };
      },
      abortedDrain(),
    );
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: true });
    const res = await run.complete;

    expect(res.terminalStatus).toBe("completed");
    expect(invoked).not.toContain("e1");
    expect(invoked).not.toContain("e2");
  });
});
