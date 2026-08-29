// Contract test for the suspendable-loop executor's handle.
//
// A loop iteration runs its body through the SAME `SuspendableChildHandle`
// contract onTrigger bodies use, including `resumeFromEvents`. `runLoop` does
// not send `resumeFromEvents` today (each iteration spawns fresh; crash-resume
// of a parked iteration is a later commit), so this drives that arm of the
// contract directly: a handle spawned with a terminal child log must short-
// circuit to `terminal` via `runtimeRun`'s buildResultFromLog, resolving the
// recorded outputs from the shared blob substrate WITHOUT re-running the body.

import { describe, test, expect } from "bun:test";

import { createDefaultDirectorRegistry } from "@intx/agent";

import { action, defineWorkflow } from "../definition/index";
import type { ActionHandler } from "../runlocal/index";
import {
  createDefaultActionInvoker,
  createInMemoryBlobSubstrate,
  createInMemoryEffectLedger,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
} from "../runlocal/index";
import type { BlobSubstrate, WorkflowRuntimeEnv } from "./env";
import { createNoopDrainController } from "./drain";
import { createLoopIterationHandle } from "./loop-iteration-handle";
import { runtimeRun } from "./run";

const body = defineWorkflow({
  id: "handle-body",
  trigger: { type: "manual" },
  steps: {
    touch: action({ handler: "touch", input: { from: "trigger.payload" } }),
  },
});

function buildEnv(
  blobs: BlobSubstrate,
  actionResolver: (ref: string) => ActionHandler,
): WorkflowRuntimeEnv {
  const repoStore = createInMemoryRepoStore();
  const clock = (): Date => new Date(0);
  const authorize: WorkflowRuntimeEnv["authorize"] = async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });
  return {
    repoStore,
    scheduler: createInMemoryScheduler({ repoStore, clock }),
    signalChannel: createInMemorySignalChannel(),
    blobs,
    directors: createDefaultDirectorRegistry(),
    authorize,
    invokeStep: async () => ({ output: null }),
    invokeAction: createDefaultActionInvoker(
      authorize,
      createInMemoryEffectLedger(),
      actionResolver,
    ),
    effects: createInMemoryEffectLedger(),
    spawnChild: async () => ({ terminalStatus: "completed" }),
    clock,
    newId: (prefix) => `${prefix}-0`,
    drain: createNoopDrainController(body),
  };
}

describe("createLoopIterationHandle", () => {
  test("resumes from a terminal child log without re-running the body", async () => {
    let runs = 0;
    const resolver = (): ActionHandler => async (input) => {
      runs += 1;
      return { echoed: input };
    };

    // Share the blob substrate so the recorded StepCompleted output ref
    // resolves on resume; use a SEPARATE fresh store for the resume so the
    // terminal log arrives ONLY via resumeFromEvents (the contract arm).
    const blobs = createInMemoryBlobSubstrate();
    const first = await runtimeRun(body, buildEnv(blobs, resolver), {
      runId: "child-0",
      triggerPayload: 7,
    }).complete;
    expect(first.terminalStatus).toBe("completed");
    expect(runs).toBe(1);

    const handle = createLoopIterationHandle(buildEnv(blobs, resolver), {
      definition: body,
      childRunId: "child-0",
      input: 7,
      depth: 0,
      maxChildSpawnDepth: 32,
      resumeFromEvents: first.events,
      signal: new AbortController().signal,
      signalChannel: createInMemorySignalChannel(),
    });

    const event = await handle.next();
    expect(event).toEqual({ kind: "terminal", terminalStatus: "completed" });
    // The body short-circuited on the terminal log; the action did NOT re-run.
    expect(runs).toBe(1);
  });
});
