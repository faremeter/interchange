// The action invoker must not start a handler for a run it already knows is
// cancelled.
//
// An action is single-attempt and its side effects are observable, so starting
// one after cancellation is not recoverable by anything downstream: the ledger
// dedups only effects routed through `perform`, and there is no retry to
// reconsider the decision. The step invoker already refuses a pre-aborted
// signal at entry; this is the same obligation on the action path, and without
// it the same cancel stops an agent step while still letting a charge through.
//
// This covers the runtime's half of the contract only. Whether a handler stops
// once it can observe the abort is the handler's own obligation, which the
// runtime hands over with the signal and cannot enforce.

import { describe, test, expect } from "bun:test";

import { defineWorkflow, action } from "../definition/index";
import type { WorkflowAuthorizeFn } from "../authorize-context";
import { runtimeRun } from "../runtime/run";
import {
  createDefaultActionInvoker,
  createInMemoryEffectLedger,
} from "./run-local";
import {
  abortOnDurableEvent,
  buildAbortWindowEnv,
  settlesWithin,
} from "../runtime/abort-window.test-helpers";

const allow: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const oneAction = defineWorkflow({
  id: "one-action",
  trigger: { type: "manual" },
  steps: { charge: action({ handler: "charge" }) },
});

describe("the default action invoker", () => {
  test("refuses a pre-aborted signal without resolving or running the handler", async () => {
    let resolverCalls = 0;
    let handlerCalls = 0;
    const invoke = createDefaultActionInvoker(
      allow,
      createInMemoryEffectLedger(),
      () => {
        resolverCalls += 1;
        return async () => {
          handlerCalls += 1;
          return null;
        };
      },
    );

    const ctrl = new AbortController();
    ctrl.abort();

    await expect(
      invoke({
        handler: "charge",
        input: null,
        requires: [],
        authzContext: { stepId: "charge", attempt: 1, runId: "run-1" },
        signal: ctrl.signal,
      }),
    ).rejects.toThrow();

    // Refused before construction, so neither the resolver nor the ledger-backed
    // context was built.
    expect(resolverCalls).toBe(0);
    expect(handlerCalls).toBe(0);
  });

  test("leaves the side effect undone when the run is cancelled mid-commit", async () => {
    const teardown = new AbortController();
    const sideEffects: string[] = [];
    const env = buildAbortWindowEnv(oneAction, {
      repoStore: abortOnDurableEvent(teardown, "StepStarted"),
    });
    // The real invoker, not a mock: the point is that the production path
    // refuses, not that a stand-in can be written to.
    env.invokeAction = createDefaultActionInvoker(
      allow,
      createInMemoryEffectLedger(),
      () => async () => {
        sideEffects.push("charged the customer");
        return null;
      },
    );

    const run = runtimeRun(oneAction, env, {
      runId: "run-action-cancelled-mid-commit",
      localAbort: teardown.signal,
    });

    expect(await settlesWithin(run.complete, 5000)).toBe("settled");
    expect((await run.complete).terminalStatus).not.toBe("completed");
    expect(sideEffects).toEqual([]);
  });
});
