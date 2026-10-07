// Loop-iteration env shaping for the suspendable-child seam.
//
// A loop iteration runs its body through the same `SuspendableChildHandle`
// contract an onTrigger section body uses, but INHERITS the parent run's
// env (real tools, action invoker, grants, durable store) -- a loop is the
// parent's own bounded rework, not a fresh capped section body. Two things
// are not inherited:
//   - a fresh, iteration-OWNED signalChannel (sharing the parent's would
//     race the body's `awaitSignal` awaits with the container's relay awaits
//     on one FIFO name);
//   - `onPark`/`onSignalPark`, which `createSuspendableChildHandle` owns.
// `hasUpstreamSignalResolver` is restated explicitly rather than left to the
// spread so a future change to the spread cannot silently drop it.

import type { WorkflowDefinition } from "../definition/index";
import { createNoopDrainController } from "./drain";
import type {
  SignalChannel,
  SuspendableChildHandle,
  WorkflowRuntimeEnv,
} from "./env";
import { createSuspendableChildHandle } from "./suspendable-child-handle";
import type { WorkflowEvent } from "../state-machine/index";

export function createLoopIterationHandle(
  baseEnv: WorkflowRuntimeEnv,
  args: {
    definition: WorkflowDefinition;
    childRunId: string;
    input: unknown;
    /**
     * The depth the iteration body runs at (the container's own) and the
     * tree-wide ceiling.
     */
    depth: number;
    maxChildSpawnDepth: number;
    /**
     * A durable child log to re-adopt instead of a fresh spawn (crash-resume
     * re-link).
     */
    resumeFromEvents?: readonly WorkflowEvent[];
    signal: AbortSignal;
    /** The iteration-owned signal channel (host-created per childRunId). */
    signalChannel: SignalChannel;
    /** Teardown for the owned channel, run once the iteration settles. */
    cleanup?: () => void | Promise<void>;
  },
): SuspendableChildHandle {
  // Destructure the park sinks out so the inherited env carries neither;
  // the shared handle owns them.
  const { onPark, onSignalPark, ...inherited } = baseEnv;
  const childEnv: WorkflowRuntimeEnv = {
    ...inherited,
    signalChannel: args.signalChannel,
    drain: createNoopDrainController(args.definition),
    // Restated rather than left to the spread: an iteration is answerable
    // exactly when its container is, and `parkOnSignalResult` refuses on it.
    hasUpstreamSignalResolver: baseEnv.hasUpstreamSignalResolver,
  };
  return createSuspendableChildHandle(childEnv, {
    definition: args.definition,
    childRunId: args.childRunId,
    input: args.input,
    depth: args.depth,
    maxChildSpawnDepth: args.maxChildSpawnDepth,
    ...(args.resumeFromEvents !== undefined
      ? { resumeFromEvents: args.resumeFromEvents }
      : {}),
    signal: args.signal,
    ...(args.cleanup !== undefined ? { cleanup: args.cleanup } : {}),
  });
}
