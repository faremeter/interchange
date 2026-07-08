// Action primitive: deterministic effect nodes with capability-checked,
// ledger-deduplicated effects.

import { describe, test, expect } from "bun:test";

import {
  action,
  createEffectContext,
  defineWorkflow,
  runLocal,
  type ActionHandler,
  type ActionInvoker,
  type EffectLedger,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

function allowAll(): WorkflowAuthorizeFn {
  return async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
}

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

function invokerFor(
  handler: ActionHandler,
  effects: EffectLedger,
  authorize: WorkflowAuthorizeFn,
): ActionInvoker {
  return async ({ input, requires, authzContext, signal }) => {
    const ctx = createEffectContext({
      authorize,
      effects,
      requires,
      authzContext,
      input,
    });
    return { output: await handler(input, ctx, signal) };
  };
}

describe("action primitive", () => {
  test("runs its handler and surfaces the output in the run outputs", async () => {
    const handler: ActionHandler = async (input) => ({ echoed: input });
    const def = defineWorkflow({
      id: "act-happy",
      trigger: { type: "manual" },
      steps: {
        act: action({ handler: "echo", input: { from: "trigger.payload" } }),
      },
    });
    const result = await runLocal(def, {
      triggerPayload: { n: 1 },
      actionResolver: () => handler,
    }).complete;
    expect(result.terminalStatus).toBe("completed");
    expect(result.outputs.act).toEqual({ echoed: { n: 1 } });
  });

  test("performs a granted effect through the EffectContext", async () => {
    let effectRan = 0;
    const handler: ActionHandler = async (_input, ctx) =>
      ctx.perform({
        effectId: "write",
        capability: "fs:write",
        run: async () => {
          effectRan += 1;
          return "written";
        },
      });
    const def = defineWorkflow({
      id: "act-effect",
      trigger: { type: "manual" },
      steps: {
        act: action({ handler: "w", effect: { requires: ["fs:write"] } }),
      },
    });
    const result = await runLocal(def, {
      actionResolver: () => handler,
    }).complete;
    expect(result.terminalStatus).toBe("completed");
    expect(effectRan).toBe(1);
    expect(result.outputs.act).toBe("written");
  });

  test("fails when an effect uses a capability the action did not declare", async () => {
    const handler: ActionHandler = async (_input, ctx) =>
      ctx.perform({
        effectId: "x",
        capability: "shell:run",
        run: async () => "ran",
      });
    const def = defineWorkflow({
      id: "act-ungranted",
      trigger: { type: "manual" },
      steps: {
        act: action({ handler: "h", effect: { requires: ["fs:write"] } }),
      },
    });
    const result = await runLocal(def, {
      actionResolver: () => handler,
    }).complete;
    expect(result.terminalStatus).toBe("failed");
  });

  test("fails when authorize denies a declared effect", async () => {
    const deny: WorkflowAuthorizeFn = async () => ({
      effect: "deny",
      matchingGrants: [],
      resolvedBy: null,
    });
    const handler: ActionHandler = async (_input, ctx) =>
      ctx.perform({
        effectId: "x",
        capability: "fs:write",
        run: async () => "ran",
      });
    const def = defineWorkflow({
      id: "act-denied",
      trigger: { type: "manual" },
      steps: {
        act: action({ handler: "h", effect: { requires: ["fs:write"] } }),
      },
    });
    const result = await runLocal(def, {
      authorize: deny,
      actionResolver: () => handler,
    }).complete;
    expect(result.terminalStatus).toBe("failed");
  });

  test("re-driving an action dedups its effect against the durable ledger", async () => {
    // Models the crash window "effect recorded, StepCompleted lost": the
    // ledger is durable on write, so a re-drive (fresh run-log, same
    // runId, shared ledger) replays the handler but the per-effect ledger
    // hit skips the real effect and reconstructs the same output. A crash
    // BETWEEN the effect and the record is the handler's to close via
    // idempotency (the R2b contract); this test pins the ledger-hit path.
    const effects = inMemoryLedger();
    const authorize = allowAll();
    let effectRuns = 0;
    const handler: ActionHandler = async (_input, ctx) =>
      ctx.perform({
        effectId: "commit",
        capability: "git:commit",
        run: async () => {
          effectRuns += 1;
          return `sha-${String(effectRuns)}`;
        },
      });
    const invokeAction = invokerFor(handler, effects, authorize);
    const def = defineWorkflow({
      id: "act-dedup",
      trigger: { type: "manual" },
      steps: {
        act: action({
          handler: "commit",
          effect: { requires: ["git:commit"] },
        }),
      },
    });
    const runId = "run-dedup-fixed";

    const first = await runLocal(def, { invokeAction, runId }).complete;
    expect(first.terminalStatus).toBe("completed");
    expect(effectRuns).toBe(1);
    expect(first.outputs.act).toBe("sha-1");

    const second = await runLocal(def, { invokeAction, runId }).complete;
    expect(second.terminalStatus).toBe("completed");
    expect(effectRuns).toBe(1);
    expect(second.outputs.act).toBe("sha-1");
  });
});
