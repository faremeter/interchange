// Loop-iteration host seam (shared by runLocal and the deployed child host).
//
// Runs one loop iteration's body as a child run against the SHARED store
// (the parent's repoStore + blobs + effects). It resolves the child log to
// one of three states:
//   - empty        -> run the body fresh;
//   - terminal     -> idempotent replay: return the recorded outputs without
//                     re-running;
//   - non-terminal -> throw (the caller fails the iteration).
//
// The body-ban forbids a loop body from SUSPENDING (awaitSignal / sleep /
// childWorkflow), so a body of purely-buffered steps flushes nothing until its
// terminal boundary: a mid-iteration crash drops the whole buffered segment,
// leaving an empty log this re-runs fresh. An ACTION body is the exception that
// makes the non-terminal case reachable: `runAction` flushes its `StepStarted`
// durably BEFORE invoking the handler, so a crash between the effect and the
// action's `StepCompleted` leaves a non-empty, non-terminal child log. On resume
// this hits the non-terminal throw -- the iteration fails loud and the handler
// is NOT re-invoked -- which is what gives an action-in-loop at-most-once
// semantics without a durable effect ledger (it rests on the store keeping the
// child's flushed `StepStarted` durable, i.e. child appends being as durable as
// the parent's -- an invariant the isogit store provides).
//
// Sharing the blob substrate is load-bearing: a blob-spilled child output is
// only resolvable from the substrate that recorded it.

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
