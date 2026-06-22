// Regression test for C5: cancel() in the early-lifecycle window
// before the runtime body commits `RunStarted` must resolve the
// `complete` promise to a `cancelled` terminal status. Prior to the
// fix, the state machine rejected `CancelRequested` from
// `phase=pending` with a `TransitionError(code="phase")`, leaving the
// caller staring at an unhandled rejection.
//
// The fix has three load-bearing pieces, each of which this test
// pins by observable behavior:
//
//   1. `handleCancelRequested` in `state-machine/transition.ts` admits
//      `phase=pending` in addition to `phase=running`. (Without this,
//      the cancel commit lands a TransitionError, which the cancel
//      caller surfaces.)
//   2. `commit-chain.ts` validates the transition before appending so
//      the early `CancelRequested` reaches `cancelling` cleanly and
//      the subsequent body-side `RunStarted` is rejected with
//      `code=phase` rather than appended out of order.
//   3. `executeRunBody` in `runtime/run.ts` tolerates the
//      `TransitionError(code="phase")` thrown when its `RunStarted`
//      commit races a cancel that already transitioned the run to
//      `cancelling`; it reloads and falls into the cancellation
//      cleanup branch instead of crashing the run-body promise.
//
// The test forces the production-shaped race by gating the body's
// first `repoStore.read` so the cancel's `CancelRequested` commit
// reaches the chain BEFORE the body's `RunStarted` commit. Without
// this gate the body's first await tends to resolve first and the
// commit chain processes RunStarted before CancelRequested -- a
// legitimate ordering, but one that lets the bug under test hide
// because cancel then admits cleanly from `phase=running`. Pinning
// the production-shaped race is the whole point of the regression.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import {
  defineWorkflow,
  step,
  type WorkflowDefinition,
} from "../definition/index";
import { createInMemoryBlobSubstrate } from "../runlocal/blob-substrate";
import { createInMemoryRepoStore } from "../runlocal/repo-store";
import { createInMemoryScheduler } from "../runlocal/scheduler";
import { createInMemorySignalChannel } from "../runlocal/signal-channel";
import { createNoopDrainController } from "./drain";
import type { RepoStore, StepInvoker, WorkflowRuntimeEnv } from "./env";
import { runtimeRun } from "./run";
import { TransitionError } from "../state-machine/index";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: `you are ${id}`,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

function singleStepWorkflow(): WorkflowDefinition {
  return defineWorkflow({
    id: "early-cancel",
    trigger: { type: "manual" },
    steps: {
      s: step({ agent: makeAgent("a") }),
    },
  });
}

/**
 * Wrap an in-memory repo store so the body's first `read` is gated
 * behind an external `release` callback. The cancel path's own
 * `read` (which also goes through the wrapper) is allowed to proceed
 * because we only gate the FIRST caller; the test releases the gate
 * after the cancel has completed its `CancelRequested` commit.
 *
 * This deterministically pins the race the bug lived in: cancel
 * lands first, transitioning the run to `cancelling`; the body's
 * subsequent `RunStarted` commit hits the state machine's
 * `RunStarted in phase cancelling` rejection (code=phase).
 */
function gatedRepoStore(): {
  store: RepoStore;
  releaseBodyFirstRead: () => void;
} {
  const inner = createInMemoryRepoStore();
  let firstReadHit = false;
  let releaseFirst!: () => void;
  const firstReadGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const store: RepoStore = {
    async read(runId) {
      if (!firstReadHit) {
        firstReadHit = true;
        await firstReadGate;
      }
      return inner.read(runId);
    },
    append: inner.append.bind(inner),
    appendBatch: inner.appendBatch.bind(inner),
    subscribe: inner.subscribe.bind(inner),
  };
  return { store, releaseBodyFirstRead: releaseFirst };
}

function buildEnv(
  repoStore: RepoStore,
  invokeStep: StepInvoker,
): WorkflowRuntimeEnv {
  const clock = () => new Date();
  const def = singleStepWorkflow();
  return {
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
    drain: createNoopDrainController(def),
  };
}

describe("C5 regression: cancel before first StepStarted", () => {
  test("cancel that wins the chain race against RunStarted resolves to cancelled", async () => {
    const def = singleStepWorkflow();
    let invokerCalled = false;
    const invokeStep: StepInvoker = async () => {
      invokerCalled = true;
      return { output: null };
    };
    const { store, releaseBodyFirstRead } = gatedRepoStore();
    const env = buildEnv(store, invokeStep);

    const run = runtimeRun(def, env);
    // The body's first `read` (inside executeRunBody's seed restore)
    // is now waiting on the gate. Issue cancel; the cancel's
    // `reloadState` calls store.read but the gate only blocks the
    // FIRST read, which the body already consumed. So cancel's read
    // resolves immediately and its CancelRequested commit reaches
    // the per-runId chain first.
    const cancelPromise = run.cancel("supervisor-operator", "early cancel");
    // Yield a couple of microtasks so cancel's reload + commit are
    // queued before we release the body.
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    // Now release the body. Its RunStarted commit will reach the
    // chain after CancelRequested has already landed; the chain
    // rejects it with `code=phase` and executeRunBody's catch absorbs
    // the rejection and reloads into the cancellation cleanup branch.
    releaseBodyFirstRead();

    let result;
    let thrown: unknown;
    try {
      result = await run.complete;
    } catch (cause) {
      thrown = cause;
    }
    await cancelPromise;

    // The cancel must not surface a TransitionError to the awaiter.
    // The bug this pins manifested as the complete promise rejecting
    // with `TransitionError(code="phase")` because (a) cancel itself
    // rejected from `pending`, or (b) the body's RunStarted threw
    // and was not caught.
    expect(thrown).toBeUndefined();
    if (thrown instanceof TransitionError) {
      throw new Error(
        `complete promise rejected with TransitionError(code=${thrown.code}); the C5 fix is not in effect`,
      );
    }
    expect(result).toBeDefined();
    expect(result?.terminalStatus).toBe("cancelled");

    // The invoker must not have been called -- cancellation landed
    // before the scheduler reached the first StepStarted.
    expect(invokerCalled).toBe(false);

    // The log carries CancelRequested (committed by cancel ahead of
    // the body's RunStarted) and RunCancelled (committed by the body
    // in its cleanup branch). The body's RunStarted commit was
    // rejected by the chain so it does NOT appear in the log.
    // No StepStarted or StepFailed for the single step.
    if (result === undefined) {
      throw new Error("body returned without resolving via complete");
    }
    const events = result.events;
    const kinds = events.map((e) => e.kind);
    expect(kinds).toContain("CancelRequested");
    expect(kinds).toContain("RunCancelled");
    expect(kinds).not.toContain("RunStarted");
    expect(kinds).not.toContain("StepStarted");
    expect(kinds).not.toContain("StepFailed");

    // Ordering: CancelRequested precedes RunCancelled. (RunStarted is
    // absent so we do not assert a relation against it.)
    const cancelRequestedIdx = kinds.indexOf("CancelRequested");
    const runCancelledIdx = kinds.indexOf("RunCancelled");
    expect(cancelRequestedIdx).toBeGreaterThanOrEqual(0);
    expect(runCancelledIdx).toBeGreaterThan(cancelRequestedIdx);
  });
});
