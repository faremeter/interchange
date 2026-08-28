// Shared park-aware drive for a suspendable child body.
//
// Runs a body sub-DAG through `runtimeRun` and returns a `SuspendableChildHandle`
// the caller drives across the body's approval / author-`awaitSignal` parks
// (rather than awaiting a terminal). This is the SINGLE handle implementation
// behind the `SuspendableChildHandle` contract: every suspendable-child seam --
// an onTrigger section body and a loop iteration -- builds its own host-shaped
// env (a fresh capped env for a section, the inherited parent env for a loop)
// and then hands it here, so the two share identical park/resume/signal
// semantics by construction.
//
// The helper OWNS `onPark` / `onSignalPark`: it spreads the caller's env and
// installs its own sinks to feed the FIFO the caller drains via `next()`. The
// caller must therefore pass an env that has not wired its own park sinks; a
// silent override would drop them, so this fails loud instead.

import { signalName } from "@intx/types";

import type { WorkflowDefinition } from "../definition/index";
import type {
  SuspendableChildHandle,
  SuspendableChildPark,
  WorkflowRuntimeEnv,
} from "./env";
import { runtimeRun } from "./run";
import type { WorkflowEvent } from "../state-machine/index";

export function createSuspendableChildHandle(
  env: WorkflowRuntimeEnv,
  args: {
    definition: WorkflowDefinition;
    childRunId: string;
    input: unknown;
    resumeFromEvents?: readonly WorkflowEvent[];
    signal: AbortSignal;
    /**
     * Channel teardown, run once the body settles. `signalChannel.stop()` is a
     * concrete-channel method (not on the `SignalChannel` interface), so the
     * caller that created the child's channel supplies its teardown here rather
     * than the helper reaching for it on `env.signalChannel`.
     */
    cleanup?: () => void | Promise<void>;
  },
): SuspendableChildHandle {
  const { definition, childRunId, input, resumeFromEvents, signal, cleanup } =
    args;

  if (env.onPark !== undefined || env.onSignalPark !== undefined) {
    throw new Error(
      `createSuspendableChildHandle: the env for ${childRunId} already wired ` +
        `onPark/onSignalPark; this helper owns the park sinks, so a caller-set ` +
        `sink would be silently overridden`,
    );
  }

  // FIFO the caller drains via `next()`: each entry is either an approval park
  // to proxy up or a fatal illegal-park error. A single waiter slot suffices
  // because `next()` has exactly one consumer driving it sequentially,
  // mirroring the signal channel's single-consumer shape.
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
    // proxies it up as a signal-relay await and relays the resolved signal back
    // via `deliverSignal`. Without this the body would park on the signal
    // channel with nothing upstream to route a delivery to it.
    onSignalPark: (park) => {
      events.push({ kind: "signal-park", name: park.name });
      notify();
    },
  };

  // On resume, drive the run from its durable log; the body step re-parks
  // silently (a re-park does not re-fire onPark), and the caller relays the
  // grant via resume on the correlation it recovered from its own log. On a
  // fresh spawn, seed the run with the event's trigger payload.
  const handle = runtimeRun(
    definition,
    runEnv,
    resumeFromEvents !== undefined
      ? { runId: childRunId, resumeFromEvents }
      : { runId: childRunId, triggerPayload: input },
  );

  const cancelOnAbort = (): void => {
    void handle.cancel("supervisor-operator", "parent cancelled");
  };
  if (signal.aborted) {
    cancelOnAbort();
  } else {
    signal.addEventListener("abort", cancelOnAbort, { once: true });
  }

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
      signal.removeEventListener("abort", cancelOnAbort);
      if (cleanup !== undefined) void cleanup();
      notify();
    });

  return {
    next: async () => {
      for (;;) {
        const event = events.shift();
        if (event !== undefined) {
          if (event.kind === "error") {
            // The body re-armed an input park nothing will resolve. Cancel the
            // child so its terminal (and the channel teardown tied to it)
            // fires, then surface the error: the throw lands the container
            // run's terminal via `runPrimitiveSafe`.
            void handle.cancel(
              "supervisor-operator",
              "suspendable body re-armed an unsupported input park",
            );
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
