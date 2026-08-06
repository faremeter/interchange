// Production `WorkflowRuntimeEnv.SpawnChildWorkflow` adapter.
//
// The runtime body sees the spawn callback shape: given a
// `definitionRef` (a workflow asset's repo id), a parent-allocated
// `childRunId`, the materialized child input, and parent attribution,
// settle once the child run reaches a terminal phase. The adapter
// itself does not execute the child workflow -- it resolves the
// `definitionRef` into a concrete `WorkflowDefinition` from the
// workflow repo's deploy ref, then delegates the spawn to a
// runtime-supplied `runChild` callback. The supervisor wires the
// callback against a child `WorkflowRuntimeEnv` and `runtimeRun`.
//
// Resolution path:
//   1. Look up the `definitionRef`'s hub-approved wire hash in the
//      per-body `referencedDefinitionHashes` map (sourced from
//      `SpawnTimeEnv.referencedDefinitionHashes`). A ref with no entry
//      has no hub authority to verify against, so the resolver refuses
//      to load it -- fail closed.
//   2. Route the load through the shared `loadVerifiedWorkflowDefinition`
//      re-verify barrier against `RepoId { kind: "workflow", id:
//      definitionRef }`. The barrier reads `workflow.json` from the
//      deploy working tree at `getRepoDir(repoId)` (the deploy-time
//      `writeTree` materializes the file there, so a flat `fs.readFile`
//      gives the envelope without a git object-database read), validates
//      the envelope, recomputes the wire hash over the validated
//      projection, and fails closed when the recompute differs from the
//      approved hash. The state-machine-narrowed primitives are validated
//      by the runtime body downstream. Both this adapter and the
//      top-level run child route through the same loader, so the barrier
//      lives in exactly one layer.
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

import type { Principal, RepoStore } from "@intx/hub-sessions/substrate";
import type { InferenceEvent } from "@intx/types/runtime";
import type {
  SpawnChildWorkflow,
  SpawnSuspendableChild,
  SuspendableChildHandle,
  WorkflowDefinition,
  WorkflowEvent,
} from "@intx/workflow";

import { loadVerifiedWorkflowDefinition } from "../child/verified-definition-loader";

const WORKFLOW_JSON_PATH = "workflow.json";

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
 * propagates to the child without an intermediate wrapper.
 */
export type RunChildWorkflow = (input: {
  definition: WorkflowDefinition;
  definitionRef: string;
  childRunId: string;
  input: unknown;
  parentRunId: string;
  parentStepId: string;
  signal: AbortSignal;
}) => Promise<{ terminalStatus: ChildTerminalStatus }>;

export interface WorkflowSpawnChildOpts {
  /**
   * Substrate the deploy orchestrator wrote the workflow asset into.
   * The adapter reads the workflow envelope through
   * `substrate.getRepoDir` -- the deploy-time `writeTree` already
   * materialized the file under the returned directory and a flat
   * `fs.readFile` does not need to walk the git object database.
   */
  substrate: RepoStore;
  /**
   * Principal the adapter presents to the substrate for any future
   * authorize-gated read path. The current implementation does not
   * gate `getRepoDir` (the substrate documents it as a pure path
   * computation), but holding the principal in closure keeps the
   * adapter symmetric with the sibling production adapters and ready
   * for a future API that surfaces an authorize gate on the same
   * read path.
   */
  principal: Principal;
  /**
   * Ref under the workflow asset's repo whose tree holds the
   * deployed `workflow.json`. Callers typically supply
   * `"refs/heads/main"` -- the workflow-kind handler enforces the
   * envelope's structural shape at push time so a deploy ref read
   * here either yields a valid envelope or surfaces a targeted
   * parse/validation error.
   */
  deployRef: string;
  /**
   * Runtime-supplied child execution callback. The adapter delegates
   * here once the `WorkflowDefinition` is resolved; the supervisor
   * owns the child `WorkflowRuntimeEnv` and the `runtimeRun`
   * invocation.
   */
  runChild: RunChildWorkflow;
  /**
   * Hub-approved wire hash per referenced onTrigger body id, sourced from
   * `SpawnTimeEnv.referencedDefinitionHashes`. The resolver looks up the
   * `definitionRef` here and routes the load through
   * `loadVerifiedWorkflowDefinition`, which fails closed when the
   * recomputed hash differs. A body with no entry has no hub authority to
   * verify against, so the resolver refuses to load it. Absent (the
   * default `{}`) means no referenced body can be resolved -- fail closed
   * -- until the host wires the per-body hashes.
   */
  referencedDefinitionHashes?: Record<string, string>;
}

/**
 * Construct the production `WorkflowRuntimeEnv.SpawnChildWorkflow`
 * adapter. The substrate handle, the principal, the deploy ref, and
 * the runtime-supplied child callback live in closure; the returned
 * callable satisfies the runtime-env interface.
 */
export function createWorkflowSpawnChild(
  opts: WorkflowSpawnChildOpts,
): SpawnChildWorkflow {
  return async ({
    definitionRef,
    childRunId,
    input,
    parentRunId,
    parentStepId,
    signal,
  }) => {
    if (signal.aborted) {
      throw abortError(signal);
    }

    const definition = await resolveDefinition(
      {
        substrate: opts.substrate,
        deployRef: opts.deployRef,
        referencedDefinitionHashes: opts.referencedDefinitionHashes ?? {},
      },
      definitionRef,
    );

    // Re-check the abort signal after the resolution await. The
    // caller can fire `signal.abort()` between the entry-time check
    // and here; without this re-check the child callback would be
    // invoked with an already-aborted signal and the parent's audit
    // log would carry a spawn the adapter could have short-circuited
    // before it ever reached the supervisor.
    if (signal.aborted) {
      throw abortError(signal);
    }

    const result = await opts.runChild({
      definition,
      definitionRef,
      childRunId,
      input,
      parentRunId,
      parentStepId,
      signal,
    });
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
) => Promise<SuspendableChildHandle>;

/**
 * Host-side widening of the runtime {@link SpawnSuspendableChild} contract: the
 * same input plus the per-run `onEvent` sink the host injects. The runtime
 * calls the narrow `SpawnSuspendableChild` (no event slot); the host binding
 * wired into the runtime env closes over the run's funnel and forwards it here,
 * mirroring how `ChildStepInvoker` widens the runtime `StepInvoker` with
 * `onEvent`. The runtime contract in `@intx/workflow` stays untouched.
 */
export type HostSpawnSuspendableChild = (
  input: Parameters<SpawnSuspendableChild>[0],
  onEvent: (event: InferenceEvent) => void,
) => ReturnType<SpawnSuspendableChild>;

export interface WorkflowSpawnSuspendableChildOpts {
  /**
   * Substrate the deploy orchestrator wrote the workflow asset into.
   * Read through `substrate.getRepoDir` exactly as the terminal-only
   * adapter resolves its definition.
   */
  substrate: RepoStore;
  /**
   * Principal held in closure for symmetry with the terminal-only adapter
   * and a future authorize-gated read path; `getRepoDir` resolution does
   * not gate on it today.
   */
  principal: Principal;
  /**
   * Ref under the workflow asset's repo whose tree holds the deployed
   * `workflow.json`.
   */
  deployRef: string;
  /**
   * Runtime-supplied suspendable child execution callback. The adapter
   * delegates here once the `WorkflowDefinition` is resolved; the
   * supervisor owns the child `WorkflowRuntimeEnv`, the `runtimeRun`
   * invocation, and the returned handle.
   */
  runSuspendableChild: RunSuspendableChild;
  /**
   * Hub-approved wire hash per referenced onTrigger body id, sourced from
   * `SpawnTimeEnv.referencedDefinitionHashes`. Threaded into the shared
   * `loadVerifiedWorkflowDefinition` re-verify barrier exactly as the
   * terminal-only adapter threads it. Absent (the default `{}`) fails
   * closed: no referenced body resolves until the host wires the hashes.
   */
  referencedDefinitionHashes?: Record<string, string>;
}

/**
 * Construct the production `WorkflowRuntimeEnv.SpawnSuspendableChild`
 * adapter. Mirrors {@link createWorkflowSpawnChild}: it resolves the
 * `definitionRef` to a concrete `WorkflowDefinition` from the deploy ref
 * and delegates to the runtime-supplied `runSuspendableChild`, which
 * returns the live handle `runOnTrigger` drives across the body's
 * approval parks. Sharing `resolveDefinition` keeps definitionRef
 * resolution owned by this layer for both the terminal-only and
 * park-aware spawn paths.
 */
export function createWorkflowSpawnSuspendableChild(
  opts: WorkflowSpawnSuspendableChildOpts,
): HostSpawnSuspendableChild {
  return async (
    {
      definitionRef,
      childRunId,
      input,
      parentRunId,
      parentStepId,
      signal,
      resumeFromEvents,
    },
    onEvent,
  ) => {
    if (signal.aborted) {
      throw abortError(signal);
    }

    const definition = await resolveDefinition(
      {
        substrate: opts.substrate,
        deployRef: opts.deployRef,
        referencedDefinitionHashes: opts.referencedDefinitionHashes ?? {},
      },
      definitionRef,
    );

    // Re-check the abort signal after the resolution await, mirroring the
    // terminal-only adapter: a caller can fire `signal.abort()` between the
    // entry-time check and here, and the child callback must not spin up a
    // run against an already-aborted signal.
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
        ...(resumeFromEvents !== undefined ? { resumeFromEvents } : {}),
      },
      onEvent,
    );
  };
}

/**
 * Resolve a referenced onTrigger body's `definitionRef` to a verified
 * `WorkflowDefinition`. The load routes through the shared
 * `loadVerifiedWorkflowDefinition` re-verify barrier: it reads
 * `workflow.json` from the deploy working tree, validates the envelope,
 * recomputes the wire hash, and fails closed if the recompute differs
 * from the hub-approved hash for this body. A `definitionRef` with no
 * approved-hash entry has no hub authority to verify against, so the
 * resolver refuses to load it rather than resolving an unverified body.
 */
async function resolveDefinition(
  opts: {
    substrate: RepoStore;
    deployRef: string;
    referencedDefinitionHashes: Record<string, string>;
  },
  definitionRef: string,
): Promise<WorkflowDefinition> {
  const approvedHash = opts.referencedDefinitionHashes[definitionRef];
  if (approvedHash === undefined) {
    throw new Error(
      `workflow-runtime: spawn-child has no hub-approved wire hash for definitionRef ${JSON.stringify(definitionRef)} on ${opts.deployRef}; refusing to load an unverified referenced body`,
    );
  }
  return loadVerifiedWorkflowDefinition({
    substrate: opts.substrate,
    repoId: { kind: "workflow", id: definitionRef },
    workflowPath: WORKFLOW_JSON_PATH,
    approvedHash,
  });
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
