// Shared park-aware drive for a suspendable child body.
//
// Runs a body sub-DAG through `runtimeRun` and returns a `SuspendableChildHandle`
// the caller drives across the body's approval / author-`awaitSignal` parks.
// This is the SINGLE handle implementation behind the contract: every
// suspendable-child seam (an onTrigger section body, a loop iteration) builds
// its own host-shaped env and hands it here, so both share identical
// park/resume/signal semantics by construction.
//
// The helper OWNS `onPark`/`onSignalPark` -- it installs its own sinks to feed
// the FIFO the caller drains via `next()`. A caller-set sink would be silently
// overridden, so this fails loud instead.

import { signalName } from "@intx/types";

import type { WorkflowDefinition } from "../definition/index";
import type {
  SuspendableChildHandle,
  SuspendableChildPark,
  WorkflowRuntimeEnv,
} from "./env";
import { bridgeAbort } from "./abort-bridge";
import { runtimeRun } from "./run";
import type { WorkflowEvent } from "../state-machine/index";

export function createSuspendableChildHandle(
  env: WorkflowRuntimeEnv,
  args: {
    definition: WorkflowDefinition;
    childRunId: string;
    input: unknown;
    /**
     * The depth the body runs at (the container's own, unchanged) and the
     * tree-wide ceiling.
     */
    depth: number;
    maxChildSpawnDepth: number;
    resumeFromEvents?: readonly WorkflowEvent[];
    signal: AbortSignal;
    /**
     * Channel teardown, run once the body settles; `stop()` is not on the
     * `SignalChannel` interface, so the caller that created the channel
     * supplies it.
     */
    cleanup?: () => void | Promise<void>;
  },
): SuspendableChildHandle {
  const {
    definition,
    childRunId,
    input,
    depth,
    maxChildSpawnDepth,
    resumeFromEvents,
    signal,
    cleanup,
  } = args;

  if (env.onPark !== undefined || env.onSignalPark !== undefined) {
    throw new Error(
      `createSuspendableChildHandle: the env for ${childRunId} already wired ` +
        `onPark/onSignalPark; this helper owns the park sinks, so a caller-set ` +
        `sink would be silently overridden`,
    );
  }

  // FIFO the caller drains via `next()`: approval parks, signal parks, or a
  // fatal illegal-park error.
  type BodyEvent =
    | { kind: "park"; park: SuspendableChildPark }
    | { kind: "signal-park"; name: string }
    | { kind: "error"; error: Error };
  const events: BodyEvent[] = [];
  let wake: (() => void) | null = null;
  const notify = (): void => {
    if (wake !== null) {
      const resolve = wake;
      wake = null;
      resolve();
    }
  };

  const runEnv: WorkflowRuntimeEnv = {
    ...env,
    onPark: (park) => {
      if (park.parkKind === "approval") {
        events.push({
          kind: "park",
          park: {
            correlationId: park.correlationId,
            ...(park.approvalSnapshot !== undefined
              ? { approvalSnapshot: park.approvalSnapshot }
              : {}),
          },
        });
      } else {
        events.push({
          kind: "error",
          error: new Error(
            `suspendable body ${childRunId} parked on a control-plane input ` +
              `channel (${park.correlationId}); a suspendable body may not ` +
              `re-arm an input park -- it has no upstream resolver`,
          ),
        });
      }
      notify();
    },
    // A body `awaitSignal` gate on an author name: surface it so the container
    // proxies it up and relays the resolved signal back via `deliverSignal`.
    onSignalPark: (park) => {
      events.push({ kind: "signal-park", name: park.name });
      notify();
    },
  };

  // A suspendable body runs in-process under the child's own principal, which
  // cannot sign a control-plane `CancelRequested` (that needs supervisor
  // authority). So the body never self-cancels; teardown -- a parent-abort
  // cascade or an illegal input re-arm -- aborts the run's own cancel
  // controller via `runtimeRun`'s `localAbort`, failing the parked step to
  // `StepFailed` with no durable cancel to sign. One controller unifies both
  // teardown triggers.
  const localTeardown = new AbortController();
  const onParentAbort = (): void => {
    localTeardown.abort();
  };
  const detachParentAbort = bridgeAbort(signal, onParentAbort);

  // On resume, drive the run from its durable log (the body step re-parks
  // silently); on a fresh spawn, seed it with the trigger payload.
  const baseOptions =
    resumeFromEvents !== undefined
      ? { runId: childRunId, resumeFromEvents, depth, maxChildSpawnDepth }
      : { runId: childRunId, triggerPayload: input, depth, maxChildSpawnDepth };
  const handle = runtimeRun(definition, runEnv, {
    ...baseOptions,
    localAbort: localTeardown.signal,
  });

  let settled: {
    terminalStatus: "completed" | "failed" | "cancelled";
  } | null = null;
  let failure: Error | null = null;
  void handle.complete
    .then((result) => {
      settled = { terminalStatus: result.terminalStatus };
    })
    .catch((cause) => {
      failure = cause instanceof Error ? cause : new Error(String(cause));
    })
    .finally(() => {
      detachParentAbort();
      if (cleanup !== undefined) void cleanup();
      notify();
    });

  return {
    next: async () => {
      for (;;) {
        const event = events.shift();
        if (event !== undefined) {
          if (event.kind === "error") {
            // The body re-armed an input park nothing will resolve. Tear the
            // child down locally (an in-process body cannot sign a control-plane
            // cancel), then surface the error; the throw lands the container
            // run's terminal via `runPrimitiveSafe`.
            localTeardown.abort();
            throw event.error;
          }
          if (event.kind === "signal-park") {
            return { kind: "signal-park", name: event.name };
          }
          return { kind: "park", park: event.park };
        }
        if (failure !== null) throw failure;
        if (settled !== null) {
          return { kind: "terminal", terminalStatus: settled.terminalStatus };
        }
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
    },
    resume: async (correlationId, decision) => {
      await env.signalChannel.deliver(signalName(correlationId), decision);
    },
    deliverSignal: async (name, payload, signalId) => {
      await env.signalChannel.deliver(name, payload, signalId);
    },
  };
}
