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
//   1. Build `RepoId { kind: "workflow", id: definitionRef }` against
//      the substrate the deploy orchestrator wrote the workflow asset
//      into.
//   2. Read `workflow.json` from the deploy ref's working tree at
//      `getRepoDir(repoId)`. The deploy-time `writeTree` materializes
//      the file on disk under the same path, so a flat `fs.readFile`
//      against the substrate's repo dir gives the workflow envelope
//      without dragging in a git object-database read for this commit.
//      The sibling repo-store and blob-substrate adapters use the same
//      working-tree-read pattern.
//   3. Parse as JSON, validate the envelope shape via
//      `workflowDefinitionEnvelopeSchema`, and surface the parsed
//      object as a `WorkflowDefinition`. The state-machine-narrowed
//      primitives are validated by the runtime body downstream; the
//      adapter does the structural-shape check the workflow-kind
//      handler already enforces at push time so a tampered-on-disk
//      tree still surfaces a clear error here rather than crashing
//      deep inside the runtime.
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

import { type } from "arktype";

import type { Principal, RepoStore } from "@intx/hub-sessions";
import { workflowDefinitionEnvelopeSchema } from "@intx/hub-sessions";
import type { SpawnChildWorkflow, WorkflowDefinition } from "@intx/workflow";

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

    const definition = await resolveDefinition(opts, definitionRef);

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

async function resolveDefinition(
  opts: WorkflowSpawnChildOpts,
  definitionRef: string,
): Promise<WorkflowDefinition> {
  const repoId = { kind: "workflow" as const, id: definitionRef };
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = opts.substrate.getRepoDir(repoId);
  const workflowPath = path.join(dir, WORKFLOW_JSON_PATH);
  let raw: string;
  try {
    raw = await fs.readFile(workflowPath, "utf8");
  } catch (cause) {
    if (isErrnoNotFound(cause)) {
      throw new Error(
        `workflow-runtime: spawn-child cannot resolve definitionRef ${JSON.stringify(definitionRef)}: ${WORKFLOW_JSON_PATH} not present under ${repoId.kind}/${repoId.id} on ${opts.deployRef}`,
        { cause },
      );
    }
    throw cause;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      `workflow-runtime: spawn-child read ${WORKFLOW_JSON_PATH} for ${repoId.kind}/${repoId.id} on ${opts.deployRef} is not valid JSON`,
      { cause },
    );
  }
  const validated = workflowDefinitionEnvelopeSchema(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `workflow-runtime: spawn-child ${WORKFLOW_JSON_PATH} for ${repoId.kind}/${repoId.id} on ${opts.deployRef} failed envelope validation: ${validated.summary}`,
    );
  }
  // The envelope schema enforces the structural shape the workflow
  // body and state machine consume; the discriminated narrow over
  // every `Primitive` variant lives downstream (the runtime body
  // walks the steps and dispatches per-kind). Re-deriving the
  // primitive narrow here would duplicate `defineWorkflow`'s
  // validation, and the workflow-kind handler already enforced the
  // same envelope at push time.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- WorkflowDefinition's primitive union is narrowed downstream by the runtime body; the envelope schema enforces the structural shape this adapter cares about
  return validated as unknown as WorkflowDefinition;
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

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}
