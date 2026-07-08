// Loop-iteration host seam for runLocal.
//
// Runs one loop iteration's body as a child run against the SHARED store
// (the parent's repoStore + blobs + effects), with idempotency: a
// childRunId whose durable log is already terminal returns its recorded
// outputs without re-running. Because the body-ban forbids a loop body
// from suspending (awaitSignal/sleep/childWorkflow), a persisted child
// log is always terminal -- a mid-iteration crash drops the whole
// buffered segment, leaving an empty log this re-runs fresh. Sharing the
// blob substrate is load-bearing: a blob-spilled child output is only
// resolvable from the substrate that recorded it.

import { createNoopDrainController } from "../runtime/drain";
import type {
  BlobSubstrate,
  RunLoopIteration,
  WorkflowRuntimeEnv,
} from "../runtime/env";
import { runtimeRun } from "../runtime/run";
import {
  isTerminalRunPhase,
  resumeFromLog,
  type RunPhase,
  type WorkflowEvent,
} from "../state-machine/index";

export function createLoopIteration(
  baseEnv: WorkflowRuntimeEnv,
): RunLoopIteration {
  return async ({ bodyDefinition, childRunId, input, signal }) => {
    const persisted = await baseEnv.repoStore.read(childRunId);
    if (persisted.length === 0) {
      // Fresh iteration: run the body against the shared store. On a
      // ledger miss the effects run; the child's terminal log lands in
      // the shared repoStore. (An already-persisted log is an idempotent
      // replay: skip re-running and adopt its outputs below.)
      const childEnv: WorkflowRuntimeEnv = {
        ...baseEnv,
        drain: createNoopDrainController(bodyDefinition),
      };
      const child = runtimeRun(bodyDefinition, childEnv, {
        runId: childRunId,
        triggerPayload: input,
      });
      const onAbort = (): void => {
        void child.cancel("supervisor-operator", "parent cancelled");
      };
      signal.addEventListener("abort", onAbort);
      try {
        await child.complete;
      } finally {
        signal.removeEventListener("abort", onAbort);
      }
    }

    // Both the fresh and idempotent-replay paths resolve outputs from the
    // now-durable child log the same way, so an iteration returns the
    // same shape whether it just ran or was adopted from a prior run.
    const log = [...(await baseEnv.repoStore.read(childRunId))];
    const state = resumeFromLog(childRunId, log);
    if (!isTerminalRunPhase(state.phase)) {
      throw new Error(
        `loop iteration ${childRunId} ended in non-terminal phase ` +
          `${state.phase}; a loop body cannot suspend, so its log must ` +
          `be terminal`,
      );
    }
    const output = await hydrateOutputs(log, baseEnv.blobs);
    return { terminalStatus: terminalStatusOf(state.phase), output };
  };
}

/**
 * Resolve every StepCompleted output in a terminal child log to a value.
 * Mirrors executeRunBody's own resume hydration, so the idempotent
 * replay path yields the same outputs shape a resume would.
 */
async function hydrateOutputs(
  events: readonly WorkflowEvent[],
  blobs: BlobSubstrate,
): Promise<Record<string, unknown>> {
  const outputs: Record<string, unknown> = {};
  for (const event of events) {
    if (event.kind === "StepCompleted") {
      outputs[event.stepId] = await blobs.resolveRef(event.output.ref);
    }
  }
  return outputs;
}

function terminalStatusOf(
  phase: RunPhase,
): "completed" | "failed" | "cancelled" {
  if (phase === "completed" || phase === "failed" || phase === "cancelled") {
    return phase;
  }
  throw new Error(`loop iteration ended in non-terminal phase ${phase}`);
}
