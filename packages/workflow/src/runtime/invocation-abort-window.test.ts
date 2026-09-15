// The window between a step's durable start commit and the bridge that
// carries the outer abort into the per-invocation controller.
//
// A cancel reaches this window on the ordinary control-plane path, not only
// under a synthetic teardown: cancelling a run aborts the controller that
// every per-primitive abort wraps, and the cancel's own durable write is
// queued on the same per-run commit chain as the step's start, so it lands
// directly behind it.
//
// A bridge that subscribes to the abort edge alone misses one already raised,
// leaving the invoker a signal that can never fire. Each case below covers one
// invocation runner, so reverting either guard fails only its own test.

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";

import {
  action,
  defineWorkflow,
  runtimeRun,
  step,
  type RepoStore,
  type WorkflowDefinition,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";

import {
  abortOnDurableEvent,
  buildAbortWindowEnv,
  settlesWithin,
} from "./abort-window.test-helpers";

const agent = defineAgent({
  id: "a",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});

const oneStep = defineWorkflow({
  id: "one-step",
  trigger: { type: "manual" },
  steps: { s: step({ agent }) },
});

const oneAction = defineWorkflow({
  id: "one-action",
  trigger: { type: "manual" },
  steps: { a: action({ handler: "handle" }) },
});

/**
 * Resolve only when `signal` aborts, checking the level before subscribing to
 * the edge. A real invoker guards this way, so a mock that does not would
 * hang on a correctly delivered abort and hide the very fix under test.
 */
async function blockUntilAborted(
  signal: AbortSignal | undefined,
): Promise<never> {
  if (signal?.aborted === true) throw new Error("aborted");
  return new Promise<never>((_resolve, reject) => {
    signal?.addEventListener(
      "abort",
      () => {
        reject(new Error("aborted"));
      },
      { once: true },
    );
  });
}

/**
 * The shared abort-window env, plus whichever invoker the case under test
 * needs to observe the signal it is handed.
 */
function buildEnv(
  def: WorkflowDefinition,
  opts: {
    repoStore: RepoStore;
    invokeStep?: WorkflowRuntimeEnv["invokeStep"];
    invokeAction?: WorkflowRuntimeEnv["invokeAction"];
  },
): WorkflowRuntimeEnv {
  const env = buildAbortWindowEnv(def, { repoStore: opts.repoStore });
  if (opts.invokeStep !== undefined) env.invokeStep = opts.invokeStep;
  if (opts.invokeAction !== undefined) env.invokeAction = opts.invokeAction;
  return env;
}

describe("an abort landing during the StepStarted commit", () => {
  test("reaches the step invoker instead of leaving it a live signal", async () => {
    const teardown = new AbortController();
    const observed: { aborted: boolean | null } = { aborted: null };
    const env = buildEnv(oneStep, {
      repoStore: abortOnDurableEvent(teardown, "StepStarted"),
      invokeStep: async ({ signal }) => {
        observed.aborted = signal?.aborted ?? null;
        return blockUntilAborted(signal);
      },
    });

    const run = runtimeRun(oneStep, env, {
      runId: "run-step-abort-window",
      localAbort: teardown.signal,
    });

    expect(await settlesWithin(run.complete, 2000)).toBe("settled");
    expect((await run.complete).terminalStatus).toBe("failed");
    expect(observed.aborted).toBe(true);
  });

  test("reaches the action invoker instead of leaving it a live signal", async () => {
    const teardown = new AbortController();
    const observed: { aborted: boolean | null } = { aborted: null };
    const env = buildEnv(oneAction, {
      repoStore: abortOnDurableEvent(teardown, "StepStarted"),
      invokeAction: async ({ signal }) => {
        observed.aborted = signal?.aborted ?? null;
        return blockUntilAborted(signal);
      },
    });

    const run = runtimeRun(oneAction, env, {
      runId: "run-action-abort-window",
      localAbort: teardown.signal,
    });

    expect(await settlesWithin(run.complete, 2000)).toBe("settled");
    expect((await run.complete).terminalStatus).toBe("failed");
    expect(observed.aborted).toBe(true);
  });
});
