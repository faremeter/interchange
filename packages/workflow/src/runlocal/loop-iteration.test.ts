// Loop-iteration host seam: shared-store idempotency and blob-spilled
// output resolution.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import { action, defineWorkflow, map, step } from "../definition/index";
import { createEffectContext } from "../runtime/effect-context";
import { createNoopDrainController } from "../runtime/drain";
import type {
  ActionInvoker,
  EffectLedger,
  WorkflowRuntimeEnv,
} from "../runtime/env";
import type { WorkflowAuthorizeFn } from "../authorize-context";
import { createInMemoryBlobSubstrate } from "./blob-substrate";
import { createLoopIteration } from "./loop-iteration";
import { createInMemoryRepoStore } from "./repo-store";
import { createInMemoryScheduler } from "./scheduler";
import { createInMemorySignalChannel } from "./signal-channel";

function inMemoryLedger(): EffectLedger {
  const store = new Map<string, { output: unknown }>();
  return {
    async lookup(effectKey) {
      return store.get(effectKey);
    },
    async record(effectKey, output) {
      store.set(effectKey, { output });
    },
  };
}

// A base env whose effect ledger and stores are shared across iterations
// (as the loop's shared-store contract requires). The single action's
// effect calls `runEffect`, so a test can count real executions.
function buildBaseEnv(runEffect: () => Promise<unknown>): WorkflowRuntimeEnv {
  const clock = () => new Date();
  const repoStore = createInMemoryRepoStore();
  const effects = inMemoryLedger();
  const authorize: WorkflowAuthorizeFn = async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
  const invokeAction: ActionInvoker = async ({
    input,
    requires,
    authzContext,
  }) => {
    const ctx = createEffectContext({
      authorize,
      effects,
      requires,
      authzContext,
      input,
    });
    const output = await ctx.perform({
      effectId: "write",
      capability: "fs:write",
      run: runEffect,
    });
    return { output };
  };
  const trivial = defineWorkflow({
    id: "parent",
    trigger: { type: "manual" },
    steps: {
      act: action({ handler: "h", effect: { requires: ["fs:write"] } }),
    },
  });
  const env: WorkflowRuntimeEnv = {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs: createInMemoryBlobSubstrate(),
    directors: createDefaultDirectorRegistry(),
    authorize,
    invokeStep: async () => ({ output: null }),
    invokeAction,
    effects,
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
    drain: createNoopDrainController(trivial),
  };
  env.runLoopIteration = createLoopIteration(env);
  return env;
}

const loopBody = defineWorkflow({
  id: "body",
  trigger: { type: "manual" },
  steps: {
    act: action({ handler: "h", effect: { requires: ["fs:write"] } }),
  },
});

function iterationInput(childRunId: string) {
  return {
    bodyDefinition: loopBody,
    childRunId,
    input: { n: 1 },
    parentRunId: "parent-run",
    parentStepId: "loop",
    signal: new AbortController().signal,
  };
}

describe("createLoopIteration", () => {
  test("runs a body iteration and returns its resolved outputs", async () => {
    let runs = 0;
    const env = buildBaseEnv(async () => {
      runs += 1;
      return `ran-${String(runs)}`;
    });
    const iterate = env.runLoopIteration;
    if (iterate === undefined) throw new Error("runLoopIteration not wired");

    const result = await iterate(iterationInput("loop-0"));
    expect(result.terminalStatus).toBe("completed");
    expect(result.output.act).toBe("ran-1");
    expect(runs).toBe(1);
  });

  test("re-running the same child id is idempotent against the shared store", async () => {
    let runs = 0;
    const env = buildBaseEnv(async () => {
      runs += 1;
      return `ran-${String(runs)}`;
    });
    const iterate = env.runLoopIteration;
    if (iterate === undefined) throw new Error("runLoopIteration not wired");

    const first = await iterate(iterationInput("loop-0"));
    expect(first.output.act).toBe("ran-1");
    expect(runs).toBe(1);

    // Same child id: adopt the persisted terminal log, do not re-run.
    const second = await iterate(iterationInput("loop-0"));
    expect(second.terminalStatus).toBe("completed");
    expect(second.output.act).toBe("ran-1");
    expect(runs).toBe(1);
  });

  test("fresh and idempotent-replay return the same output shape", async () => {
    // A map body's log carries scoped inner-step StepCompleted events;
    // both paths hydrate outputs from the same log, so an iteration
    // returns the identical shape whether freshly run or replayed.
    const env = buildBaseEnv(async () => "unused");
    const iterate = env.runLoopIteration;
    if (iterate === undefined) throw new Error("runLoopIteration not wired");

    const mapBody = defineWorkflow({
      id: "map-body",
      trigger: { type: "manual" },
      steps: {
        fan: map({
          over: { from: "trigger.payload" },
          step: step({
            agent: defineAgent({
              id: "worker",
              systemPrompt: "worker",
              tools: [],
              capabilities: [],
              inference: { sources: [{ provider: "fake", model: "fake" }] },
            }),
          }),
        }),
      },
    });
    const call = {
      bodyDefinition: mapBody,
      childRunId: "loop-map",
      input: [1, 2, 3],
      parentRunId: "parent-run",
      parentStepId: "loop",
      signal: new AbortController().signal,
    };

    const first = await iterate(call);
    const second = await iterate(call);
    expect(second.output).toEqual(first.output);
  });

  test("adopts a failed iteration's terminal log without re-running", async () => {
    let runs = 0;
    const env = buildBaseEnv(async () => {
      runs += 1;
      throw new Error("iteration effect boom");
    });
    const iterate = env.runLoopIteration;
    if (iterate === undefined) throw new Error("runLoopIteration not wired");

    const first = await iterate(iterationInput("loop-fail"));
    expect(first.terminalStatus).toBe("failed");
    expect(runs).toBe(1);

    // The failed log is terminal; replay adopts it without re-running.
    const second = await iterate(iterationInput("loop-fail"));
    expect(second.terminalStatus).toBe("failed");
    expect(runs).toBe(1);
  });

  test("resolves a blob-spilled output on idempotent replay", async () => {
    // A large output spills past the inline threshold to a blob ref;
    // idempotent replay must resolve it against the shared blob
    // substrate that recorded it.
    const big = "x".repeat(2_000_000);
    let runs = 0;
    const env = buildBaseEnv(async () => {
      runs += 1;
      return big;
    });
    const iterate = env.runLoopIteration;
    if (iterate === undefined) throw new Error("runLoopIteration not wired");

    const first = await iterate(iterationInput("loop-big"));
    expect(first.output.act).toBe(big);
    expect(runs).toBe(1);

    const second = await iterate(iterationInput("loop-big"));
    expect(second.output.act).toBe(big);
    expect(runs).toBe(1);
  });
});
