// Loop-iteration env shaping for the suspendable-child seam.
//
// A loop iteration runs its body through the SAME `SuspendableChildHandle`
// contract an onTrigger section body uses, but with a different env: it INHERITS
// the parent run's env -- the real `invokeStep` (tools), `invokeAction`,
// `authorize` (parent grants), `effects`, and the durable shared
// `repoStore`/`blobs` -- so tool, action, grant, and inference-source behaviour
// matches the in-process iteration this replaces. An onTrigger body, by
// contrast, runs in a fresh capped/toolless env; a loop is the parent's own
// bounded rework, so parent-env inheritance is what it IS.
//
// Two things are NOT inherited:
//   - a fresh, iteration-OWNED signalChannel (supplied by the host, in-memory
//     locally / substrate-backed on the deployed path). Sharing the parent's
//     channel would put the body's `awaitSignal` awaits and the container's
//     relay awaits on one FIFO name, racing delivery.
//   - `onPark`/`onSignalPark`, which `createSuspendableChildHandle` owns: the
//     body's parks feed the handle's queue for the container to proxy up, not
//     the parent env's supervisor-facing `onPark`.
// Everything else -- including `readParkedApprovalOps`, the child's own
// mid-park crash-recovery read hook -- is inherited unchanged.

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
     * The depth the iteration body runs at (the loop container's own depth,
     * unchanged) and the tree-wide ceiling, forwarded so a `childWorkflow`
     * inside the body counts against the same bound as the top-level run.
     */
    depth: number;
    maxChildSpawnDepth: number;
    /**
     * A durable child log to re-adopt instead of a fresh spawn. `runLoop` sends
     * it when re-linking a parked iteration on crash-resume (planLoopResume);
     * the handle-contract test drives it directly.
     */
    resumeFromEvents?: readonly WorkflowEvent[];
    signal: AbortSignal;
    /** The iteration-owned signal channel (host-created per childRunId). */
    signalChannel: SignalChannel;
    /** Teardown for the owned channel, run once the iteration settles. */
    cleanup?: () => void | Promise<void>;
  },
): SuspendableChildHandle {
  // `onPark`/`onSignalPark` are destructured out (and left unread) so the
  // inherited env carries neither: the shared handle owns those sinks, and the
  // body's parks must feed its queue, not the parent's supervisor-facing onPark.
  const { onPark, onSignalPark, ...inherited } = baseEnv;
  const childEnv: WorkflowRuntimeEnv = {
    ...inherited,
    signalChannel: args.signalChannel,
    drain: createNoopDrainController(args.definition),
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
