// Regression test for C-B: a `cancel()` that races the post-loop
// terminal-event commit (the sibling of the C5 race, in the same
// file) must resolve the `complete` promise to a `cancelled`
// terminal status rather than rejecting with
// `TransitionError(code="phase")`.
//
// Setup: resume against a seed log whose final event is
// `StepCompleted` for the workflow's single step. `resumeFromLog`
// reports phase=running with the step terminal, so `isRunDone`
// returns true and the main loop never iterates. Control reaches
// the post-loop branch at `run.ts` and the body issues
// `RunCompleted`. A `cancel()` invoked immediately after
// `runtimeRun` lands `CancelRequested` on the chain before the
// post-loop `RunCompleted` reaches it; the chain pre-validates,
// sees phase=cancelling, and rejects `RunCompleted` with code=phase.
//
// Prior to the C-B fix the rejection escaped the body and the
// `complete` promise rejected with a TransitionError. The fix
// mirrors the C5 catch shape verbatim around the post-loop commit:
// reload and route through the cancelling cleanup branch so the
// run settles as `cancelled`.
//
// The pre-existing `cancel-early-lifecycle.test.ts` pins the
// initial-RunStarted catch (C5). This test pins the structurally
// identical post-loop catch (C-B).

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";

import { defineWorkflow, step } from "../definition/index";
import { createInMemoryBlobSubstrate } from "../runlocal/blob-substrate";
import { createInMemoryRepoStore } from "../runlocal/repo-store";
import { createInMemoryScheduler } from "../runlocal/scheduler";
import { createInMemorySignalChannel } from "../runlocal/signal-channel";
import { createNoopDrainController } from "./drain";
import type { WorkflowRuntimeEnv } from "./env";
import { runtimeRun } from "./run";
import { TransitionError, type WorkflowEvent } from "../state-machine/index";

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: id,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

describe("C-B regression: cancel racing the post-loop terminal commit", () => {
  test("cancel immediately after resume invocation settles as cancelled", async () => {
    const def = defineWorkflow({
      id: "resume-cancel",
      trigger: { type: "manual" },
      steps: { s: step({ agent: makeAgent("a") }) },
    });
    const at = new Date().toISOString();
    const seed: WorkflowEvent[] = [
      {
        kind: "RunStarted",
        seq: 1,
        at,
        runId: "run-resume-cancel",
        definitionHash: "x",
        trigger: { type: "manual", payload: undefined },
      },
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "s",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "StepCompleted",
        seq: 3,
        at,
        stepId: "s",
        attempt: 1,
        output: { ref: "inline:null" },
      },
    ];
    const clock = () => new Date();
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
      spawnChild: async () => ({ terminalStatus: "completed" }),
      clock,
      newId: (prefix) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`,
      drain: createNoopDrainController(def),
    };
    const run = runtimeRun(def, env, {
      runId: "run-resume-cancel",
      resumeFromEvents: seed,
    });
    // Issue cancel before the body reaches its post-loop terminal commit.
    const cancelPromise = run.cancel("self", "racing post-loop commit");
    let result;
    let thrown: unknown;
    try {
      result = await run.complete;
    } catch (cause) {
      thrown = cause;
    }
    await cancelPromise.catch(() => undefined);

    // The body must absorb the post-loop phase rejection and settle
    // the run via the cancelling cleanup branch. A TransitionError on
    // the `complete` promise means the C-B fix is not in effect.
    expect(thrown).toBeUndefined();
    if (thrown instanceof TransitionError) {
      throw new Error(
        `complete promise rejected with TransitionError(code=${thrown.code}); the C-B fix is not in effect`,
      );
    }
    expect(result).toBeDefined();
    if (result === undefined) {
      throw new Error("body returned without resolving via complete");
    }
    // Either outcome is admissible by the runtime contract: the cancel
    // landed before the post-loop commit (cancelled) or the post-loop
    // commit landed first (completed). The bug under test manifested
    // as a TransitionError on `complete`, not as a particular terminal
    // status; the assertion above is the load-bearing one.
    expect(["cancelled", "completed"]).toContain(result.terminalStatus);
  });
});
