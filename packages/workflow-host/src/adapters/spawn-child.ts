// Production `WorkflowRuntimeEnv.SpawnChildWorkflow` adapter.
//
// The runtime body sees the spawn callback shape: given a `definitionRef`
// (the internal ref the deploy step assigned when it lifted the authored
// inline child), a parent-allocated `childRunId`, the materialized child
// input, and parent attribution, settle once the child run reaches a terminal
// phase. The adapter itself does not execute the child workflow -- it resolves
// the `definitionRef` into a concrete `WorkflowDefinition` and delegates the
// spawn to a runtime-supplied `runChild` callback. The supervisor wires the
// callback against a child `WorkflowRuntimeEnv` and `runtimeRun`.
//
// Two spawn types with DIFFERENT trust structures resolve here, so they
// take different resolution paths -- not one shared resolver that pretends
// they are the same:
//
//   - onTrigger BODY (the suspendable adapter): a body is a section
//     extracted from the PARENT's own approved definition. Source-ref is the
//     only deploy lineage, so the body is resolved in-memory from the parent's
//     re-evaluated closure (`createInMemorySpawnSuspendableChild`), already
//     covered by the parent's re-verify -- no separate on-disk read and no
//     separate per-body re-verify.
//
//   - childWorkflow (the terminal adapter): an owned import embedded inline in
//     the parent's definition. It is lifted to an internal `{ ref }` at child
//     boot and resolved in-memory from the parent's closure map
//     (`createInMemorySpawnChild`) -- exactly like a source-ref onTrigger
//     body, with NO on-disk asset and NO separate per-child re-verify (the
//     parent's re-verify already covers it, since the inline child rides the
//     parent's hashed projection). The terminal-only drive (await the child's
//     terminal, no park) is the only thing that distinguishes it from the
//     suspendable body adapter.
//
// Drain coordination is handled by the supervisor's drain primitive
// (`packages/workflow-host/src/supervisor`), not by this adapter. The
// spawn path ships the basic shape the runtime body needs and leaves
// same-deployment vs cross-deployment drain semantics to the caller.
//
// Abort handling: if `signal` is already aborted on entry, the adapter
// short-circuits with a DOMException-shaped `AbortError`. The signal
// is propagated to the `runChild` callback so the child runtime can
// honor a parent-initiated cancellation. The adapter does not wrap
// the signal -- the same `AbortSignal` flows through so the abort
// reason attribution is unchanged across the boundary.
//
// Sub-namespace scoping (in-process recursion): the adapter is the
// resolution point that gives the runtime-supplied `runChild` callback
// a concrete `WorkflowDefinition` paired with the parent's allocated
// `childRunId`. The callback is expected to construct a child
// `WorkflowRuntimeEnv` whose `repoStore`/`blobs`/`signalChannel` route
// every per-run write through the SAME workflow-run repo the parent
// runs in, with the runtime body's per-call `runId` argument being the
// child's allocated `childRunId`. The workflow-run substrate's path
// shape is `runs/<runId>/events/<seq>.json` (and `runs/<runId>/blobs/`
// for the blob substrate), so feeding `childRunId` into the child env
// at the call boundary lands every child event under
// `runs/<childRunId>/...` of the parent's deployment workflow-run
// repo, sibling to the parent's own `runs/<parentRunId>/...` subtree.
// The adapter itself does not construct that env -- the supervisor's
// `runChild` does -- but the callback's input shape (`{ definition,
// childRunId, ... }`) is the seam that makes the scoping unambiguous
// at the boundary.

import type { InferenceEvent } from "@intx/types/runtime";
import type {
  SpawnChildWorkflow,
  SpawnSuspendableChild,
  SuspendableChildHandle,
  WorkflowDefinition,
  WorkflowEvent,
} from "@intx/workflow";

import type { CredentialMaterialRef } from "../child/run-child";

/**
 * The terminal-status shape the runtime body expects back from a
 * spawn. Mirrored from `SpawnChildWorkflow`'s return type so the
 * `runChild` callback's signature is symmetric with the adapter's.
 */
export type ChildTerminalStatus = "completed" | "failed" | "cancelled";

/**
 * Runtime-supplied child execution callback. The supervisor owns the
 * child `WorkflowRuntimeEnv` construction (per-deployment substrate,
 * per-run blob substrate, child director registry) and the
 * `runtimeRun` invocation; the adapter is the single resolution
 * point that hands the supervisor a concrete `WorkflowDefinition`
 * alongside the parent attribution the runtime body produced.
 *
 * The callback receives the same `AbortSignal` the parent runtime
 * passed into the adapter so a parent-initiated cancellation
 * propagates to the child without an intermediate wrapper. It also
 * receives the parent run's live `onEvent` sink so the child's agent
 * steps emit inference events up the same channel (mirroring
 * {@link RunSuspendableChild}), and the spawn `depth` / ceiling so the
 * child run's own spawns keep counting against the tree-wide bound.
 *
 * The callback also receives the parent run's live credential-material cell
 * (the same reference the top-level step invoker reads live), so the child's
 * inference resolves its source secret by `credentialId` against the run's
 * current delivery -- a rotation the parent applies reaches the child through
 * the shared reference. A non-sidecar executor that carries no credential
 * material omits it.
 */
export type RunChildWorkflow = (
  input: {
    definition: WorkflowDefinition;
    definitionRef: string;
    childRunId: string;
    input: unknown;
    parentRunId: string;
    parentStepId: string;
    signal: AbortSignal;
    depth: number;
    maxChildSpawnDepth: number;
  },
  onEvent: (event: InferenceEvent) => void,
  credentialMaterial?: CredentialMaterialRef,
) => Promise<{ terminalStatus: ChildTerminalStatus }>;

/**
 * Construct the terminal `WorkflowRuntimeEnv.SpawnChildWorkflow` adapter for an
 * owned childWorkflow import. The child re-evaluated the whole pinned closure
 * and lifted every inline child to an internal `{ ref }`, so the child
 * definitions are in hand and already covered by the parent's re-verify.
 * Resolve each `definitionRef`
 * from the in-memory `bodies` map and delegate to the runtime-supplied
 * `runChild`, with NO on-disk round-trip and NO separate per-child re-verify:
 * materializing the child back out and re-fingerprinting it would round-trip
 * trusted-in-hand data for no gain, and the closure re-eval on restart
 * re-derives the same bodies durably. Mirrors
 * {@link createInMemorySpawnSuspendableChild} but drives the child terminal-only
 * (await its terminal status) rather than across approval parks.
 */
/**
 * Host-side widening of the runtime {@link SpawnChildWorkflow} contract: the
 * same input plus the per-run `onEvent` sink the host injects. The runtime env
 * carries the narrow `SpawnChildWorkflow` (no event slot); the caller wraps this
 * with its run's `onEvent`, mirroring {@link HostSpawnSuspendableChild}. The
 * sink is a call argument (not closed over at construction) because the resolver
 * is selected once per deployment while `onEvent` is built per run. The run's
 * live credential-material cell rides the same seam, so the child's inference
 * reads the parent's current credential delivery.
 */
export type HostSpawnChild = (
  input: Parameters<SpawnChildWorkflow>[0],
  onEvent: (event: InferenceEvent) => void,
  credentialMaterial?: CredentialMaterialRef,
) => ReturnType<SpawnChildWorkflow>;

export function createInMemorySpawnChild(opts: {
  bodies: ReadonlyMap<string, WorkflowDefinition>;
  runChild: RunChildWorkflow;
}): HostSpawnChild {
  return async (
    {
      definitionRef,
      childRunId,
      input,
      parentRunId,
      parentStepId,
      signal,
      depth,
      maxChildSpawnDepth,
    },
    onEvent,
    credentialMaterial,
  ) => {
    if (signal.aborted) {
      throw abortError(signal);
    }

    const definition = opts.bodies.get(definitionRef);
    if (definition === undefined) {
      throw new Error(
        `workflow-runtime: spawn-child has no in-memory childWorkflow ` +
          `definition for ${JSON.stringify(definitionRef)}; the parent's ` +
          `closure should have lifted every inline child`,
      );
    }

    // Re-check the abort signal after the resolution await. The
    // caller can fire `signal.abort()` between the entry-time check
    // and here; without this re-check the child callback would be
    // invoked with an already-aborted signal and the parent's audit
    // log would carry a spawn the adapter could have short-circuited
    // before it ever reached the supervisor.
    if (signal.aborted) {
      throw abortError(signal);
    }

    const result = await opts.runChild(
      {
        definition,
        definitionRef,
        childRunId,
        input,
        parentRunId,
        parentStepId,
        signal,
        depth,
        maxChildSpawnDepth,
      },
      onEvent,
      credentialMaterial,
    );
    return { terminalStatus: result.terminalStatus };
  };
}

/**
 * Runtime-supplied suspendable child execution callback. The park-aware
 * analog of {@link RunChildWorkflow}: the supervisor owns the child
 * `WorkflowRuntimeEnv` construction and the `runtimeRun` invocation and
 * returns a live `SuspendableChildHandle` the caller drives across the
 * body's approval parks, rather than awaiting a terminal. The adapter is
 * the single resolution point that hands the supervisor a concrete
 * `WorkflowDefinition` alongside the parent attribution the runtime body
 * produced.
 */
export type RunSuspendableChild = (
  input: {
    definition: WorkflowDefinition;
    definitionRef: string;
    childRunId: string;
    input: unknown;
    parentRunId: string;
    parentStepId: string;
    signal: AbortSignal;
    depth: number;
    maxChildSpawnDepth: number;
    resumeFromEvents?: readonly WorkflowEvent[];
  },
  /**
   * Live inference-event sink for the child's agent steps. Threaded from the
   * host's per-run funnel (the parent run's event-channel closure) so the
   * body's inference events reach the hub's live stream instead of being
   * silently dropped. Per-run durable attribution is unaffected -- the child
   * runtime commits its events under `runs/<childRunId>/events/` regardless.
   */
  onEvent: (event: InferenceEvent) => void,
  /**
   * The parent run's live credential-material cell. Threaded so the body's
   * inference resolves its source secret by `credentialId` against the run's
   * current delivery, reached live through the shared reference on a rotation.
   * A non-sidecar executor that carries no credential material omits it.
   */
  credentialMaterial?: CredentialMaterialRef,
) => Promise<SuspendableChildHandle>;

/**
 * Host-side widening of the runtime {@link SpawnSuspendableChild} contract: the
 * same input plus the per-run `onEvent` sink the host injects. The runtime
 * calls the narrow `SpawnSuspendableChild` (no event slot); the host binding
 * wired into the runtime env closes over the run's funnel and forwards it here,
 * mirroring how `ChildStepInvoker` widens the runtime `StepInvoker` with
 * `onEvent`. The runtime contract in `@intx/workflow` stays untouched. The
 * run's live credential-material cell rides the same seam, so the body's
 * inference reads the parent's current credential delivery.
 */
export type HostSpawnSuspendableChild = (
  input: Parameters<SpawnSuspendableChild>[0],
  onEvent: (event: InferenceEvent) => void,
  credentialMaterial?: CredentialMaterialRef,
) => ReturnType<SpawnSuspendableChild>;

/**
 * Construct the `WorkflowRuntimeEnv.SpawnSuspendableChild` adapter for the
 * source-ref (code-sourced) path -- the only deploy lineage. The parent child
 * re-evaluated the whole pinned closure in one sandbox and re-verified it
 * against the approved hash -- which already covers every inline onTrigger body
 * -- so the body definitions are in hand and already proven. Resolve each
 * `definitionRef` from that in-memory `bodies` map and run it in-process, with
 * NO disk round-trip and NO separate per-body re-verify: materializing the body
 * back out and re-fingerprinting it would round-trip trusted-in-hand data for no
 * gain, and the closure re-eval on restart re-derives the same bodies durably.
 * The body still runs in the parent's sandbox (in-process today; a stricter
 * per-body boundary is the deferred, opt-in SandboxBoundary case).
 */
export function createInMemorySpawnSuspendableChild(opts: {
  bodies: ReadonlyMap<string, WorkflowDefinition>;
  runSuspendableChild: RunSuspendableChild;
}): HostSpawnSuspendableChild {
  return async (
    {
      definitionRef,
      childRunId,
      input,
      parentRunId,
      parentStepId,
      signal,
      depth,
      maxChildSpawnDepth,
      resumeFromEvents,
    },
    onEvent,
    credentialMaterial,
  ) => {
    if (signal.aborted) {
      throw abortError(signal);
    }

    const definition = opts.bodies.get(definitionRef);
    if (definition === undefined) {
      throw new Error(
        `workflow-runtime: source-ref spawn-child has no in-memory onTrigger ` +
          `body for ${JSON.stringify(definitionRef)}; the parent's closure ` +
          `re-eval should have extracted every inline body`,
      );
    }

    if (signal.aborted) {
      throw abortError(signal);
    }

    return opts.runSuspendableChild(
      {
        definition,
        definitionRef,
        childRunId,
        input,
        parentRunId,
        parentStepId,
        signal,
        depth,
        maxChildSpawnDepth,
        ...(resumeFromEvents !== undefined ? { resumeFromEvents } : {}),
      },
      onEvent,
      credentialMaterial,
    );
  };
}

/**
 * Construct the rejection used when `signal.aborted` short-circuits.
 * Mirrors the abort-error shape the sibling step-invoker adapter
 * emits so consumers can `instanceof DOMException` /
 * `name === "AbortError"` against a stable shape across the runtime.
 */
function abortError(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  return new DOMException("aborted", "AbortError");
}
