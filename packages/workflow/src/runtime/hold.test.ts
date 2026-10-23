// A host hold keeps a run from starting a step the log has not started, while
// a step already running goes on to its end. Lifting the hold starts the
// held step.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  defineWorkflow,
  resolveDrainBehavior,
  runtimeRun,
  step,
  type DrainController,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { createTerminalChildHandle } from "./suspendable-child-handle";

function agent(id: string) {
  return defineAgent({
    id,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
}

// A drain controller the test holds and lifts, reporting each time the run
// reads the hold so the test can wait for the run to decide.
function createHoldableDrain(definition: WorkflowDefinition) {
  let hold: PromiseWithResolvers<undefined> | null = null;
  const reads: (() => void)[] = [];
  const drain: DrainController = {
    signal: new AbortController().signal,
    behaviorFor: (stepId) => resolveDrainBehavior(definition, stepId),
    get hold() {
      for (const wake of reads.splice(0)) wake();
      return hold === null ? null : hold.promise;
    },
  };
  return {
    drain,
    holdSteps() {
      hold = Promise.withResolvers<undefined>();
    },
    liftHold() {
      const lifted = hold;
      hold = null;
      lifted?.resolve(undefined);
    },
    nextRead(): Promise<void> {
      return new Promise((resolve) => reads.push(resolve));
    },
  };
}

describe("a host hold", () => {
  test("starts no new step until it is lifted, and lets the running step finish", async () => {
    const def = defineWorkflow({
      id: "held",
      trigger: { type: "manual" },
      steps: {
        first: step({ agent: agent("first") }),
        second: step({ agent: agent("second"), after: ["first"] }),
      },
    });
    const holdable = createHoldableDrain(def);
    const invoked: string[] = [];
    let holding = false;
    let startedWhileHeld = false;
    const firstStarted = Promise.withResolvers<undefined>();
    const finishFirst = Promise.withResolvers<undefined>();
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
      invokeStep: async (req) => {
        invoked.push(req.agent.id);
        if (req.agent.id === "second" && holding) startedWhileHeld = true;
        if (req.agent.id === "first") {
          firstStarted.resolve(undefined);
          await finishFirst.promise;
        }
        return { output: null };
      },
      spawnChild: async () => createTerminalChildHandle("completed"),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: holdable.drain,
      hasUpstreamSignalResolver: true,
    };

    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    await firstStarted.promise;
    holdable.holdSteps();
    holding = true;
    const decided = holdable.nextRead();
    finishFirst.resolve(undefined);
    // The run reads the hold once `first` has settled and `second` is ready.
    await decided;
    for (let turn = 0; turn < 5; turn += 1) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    expect(invoked).toEqual(["first"]);
    const log = await repoStore.read("r");
    expect(
      log.some((e) => e.kind === "StepStarted" && e.stepId === "second"),
    ).toBe(false);

    holding = false;
    holdable.liftHold();
    const result = await run.complete;
    expect(result.terminalStatus).toBe("completed");
    expect(invoked).toEqual(["first", "second"]);
    expect(startedWhileHeld).toBe(false);
  });

  test("makes what the run buffered durable before it waits", async () => {
    const def = defineWorkflow({
      id: "held-at-start",
      trigger: { type: "manual" },
      steps: { only: step({ agent: agent("only") }) },
    });
    const holdable = createHoldableDrain(def);
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
      invokeStep: async () => ({ output: null }),
      spawnChild: async () => createTerminalChildHandle("completed"),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: holdable.drain,
      hasUpstreamSignalResolver: true,
    };

    holdable.holdSteps();
    const decided = holdable.nextRead();
    const run = runtimeRun(def, env, { runId: "r", triggerPayload: null });
    await decided;
    for (let turn = 0; turn < 5; turn += 1) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    // A host that stops the held run now keeps its trigger.
    expect((await repoStore.read("r")).map((e) => e.kind)).toEqual([
      "RunStarted",
    ]);

    holdable.liftHold();
    expect((await run.complete).terminalStatus).toBe("completed");
  });
});
