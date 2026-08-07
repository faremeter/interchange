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
// Two spawn types with DIFFERENT trust structures resolve here, so they
// take different resolution paths -- not one shared resolver that pretends
// they are the same:
//
//   - onTrigger BODY (the suspendable adapter): a body is a section
//     extracted from the PARENT's own approved definition, so the parent's
//     approval already carries the body's `approvedWireHash` on the signed
//     deploy frame (surfaced here as `referencedDefinitionHashes[bodyId]`).
//     That hash arrives OUT-OF-BAND from the on-disk bytes, so the body path
//     routes through the `loadVerifiedWorkflowDefinition` re-verify barrier
//     and fails closed on mismatch (or on a body with no frame-carried hash,
//     which is a misconfigured deploy). This is where re-verify is
//     load-bearing.
//
//   - childWorkflow (the terminal adapter): a `childWorkflow{definitionRef}`
//     references a SEPARATELY-approved workflow asset by id. The parent's
//     approval has no authority over that asset and carries no hash for it,
//     so there is no out-of-band pin to verify against -- a gate here could
//     only fail-closed-always. This path reads + envelope-validates the
//     asset directly (`readWorkflowDefinitionEnvelope`). Its integrity rests
//     on the workflow-kind repo's hub-writes / sidecar-reads authorization
//     plus push-time envelope validation; the child asset's own content hash
//     is re-verified when the child is itself deployed, not from a parent it
//     is merely referenced by.
//
// Both paths read `workflow.json` from the deploy working tree at
// `getRepoDir(repoId)` (the deploy-time `writeTree` materializes the file
// there, so a flat `fs.readFile` gives the envelope without a git
// object-database read) and share that read+validate step; only the terminal
// re-verify gate differs.
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

import {
  loadVerifiedWorkflowDefinition,
  readWorkflowDefinitionEnvelope,
} from "../child/verified-definition-loader";

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

    // childWorkflow spawn: resolve a SEPARATELY-approved workflow asset by
    // id. The parent's approval carries no hash for it (there is no
    // out-of-band pin), so this reads + envelope-validates the asset without
    // a re-verify gate -- gating here could only fail-closed-always. The
    // asset's integrity rests on the workflow-kind repo's hub-writes /
    // sidecar-reads authorization plus push-time envelope validation; the
    // asset re-verifies against its OWN approved hash when it is deployed,
    // not from a parent that merely references it.
    const definition = await readWorkflowDefinitionEnvelope({
      substrate: opts.substrate,
      repoId: { kind: "workflow", id: definitionRef },
      workflowPath: WORKFLOW_JSON_PATH,
    });

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
   * `SpawnTimeEnv.referencedDefinitionHashes` (the parent's signed deploy
   * frame). REQUIRED, not optional: a body is part of the parent's approval,
   * so its hash is an out-of-band pin the body path re-verifies against. A
   * `definitionRef` with no entry here is a misconfigured deploy and fails
   * closed at resolution. The map may be empty (a deployment with no bodies),
   * but the host must pass it explicitly rather than defaulting it away.
   */
  referencedDefinitionHashes: Record<string, string>;
}

/**
 * Construct the production `WorkflowRuntimeEnv.SpawnSuspendableChild`
 * adapter. Mirrors {@link createWorkflowSpawnChild} in shape -- resolve the
 * `definitionRef` to a concrete `WorkflowDefinition` and delegate to the
 * runtime-supplied `runSuspendableChild`, which returns the live handle
 * `runOnTrigger` drives across the body's approval parks -- but a body is
 * part of the parent's approval, so this path RE-VERIFIES the resolved
 * definition against the parent's frame-carried body hash
 * (`resolveVerifiedBody`), where the terminal childWorkflow adapter reads a
 * separately-approved asset with no such pin.
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

    // onTrigger body spawn: the body's approved hash is intrinsic to the
    // parent's approval and rides the signed frame, so this path re-verifies
    // against that out-of-band pin.
    const definition = await resolveVerifiedBody(
      {
        substrate: opts.substrate,
        deployRef: opts.deployRef,
        referencedDefinitionHashes: opts.referencedDefinitionHashes,
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
 * Resolve a referenced onTrigger body's `definitionRef` to a re-verified
 * `WorkflowDefinition`. The body's `approvedWireHash` is intrinsic to the
 * parent's approval and rides the signed deploy frame, so the load routes
 * through the `loadVerifiedWorkflowDefinition` re-verify barrier: read
 * `workflow.json`, validate the envelope, recompute the wire hash, and fail
 * closed if it differs from the frame-carried hash for this body. A
 * `definitionRef` with no frame-carried hash is a misconfigured deploy (the
 * parent's approval should have carried every body's hash), so the resolver
 * refuses to load it rather than resolving an unverified body.
 */
async function resolveVerifiedBody(
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
      `workflow-runtime: spawn-child has no hub-approved wire hash for onTrigger body ${JSON.stringify(definitionRef)} on ${opts.deployRef}; the parent's approval should carry every body's hash -- refusing to load an unverified body`,
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
