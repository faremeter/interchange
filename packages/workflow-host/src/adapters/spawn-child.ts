// Production `WorkflowRuntimeEnv.SpawnChildWorkflow` adapters.
//
// Resolves a `definitionRef` into a concrete `WorkflowDefinition` from
// the parent's in-memory closure map and delegates execution to a
// runtime-supplied callback; the supervisor wires the callback against
// a child `WorkflowRuntimeEnv` and `runtimeRun`. Two spawn types share
// the resolution path but differ in drive:
//
//   - onTrigger BODY (`createInMemorySpawnSuspendableChild`): a section
//     extracted from the parent's own approved definition, resolved
//     in-memory and driven across approval parks.
//   - childWorkflow (`createInMemorySpawnChild`): an owned import
//     lifted to an internal `{ ref }` at child boot, driven
//     terminal-only.
//
// Both are already covered by the parent's re-verify, so no on-disk
// read and no separate per-child re-verify. Abort: a pre-aborted signal
// short-circuits with a DOMException-shaped `AbortError`; the signal is
// propagated to the callback unchanged. The callback's `childRunId`
// routes the child's writes under `runs/<childRunId>/...` in the
// parent's workflow-run repo.

import type { InferenceEvent } from "@intx/types/runtime";
import type {
  SpawnChildWorkflow,
  SpawnSuspendableChild,
  SuspendableChildHandle,
  WorkflowDefinition,
  WorkflowEvent,
} from "@intx/workflow";

import type { CredentialMaterialRef } from "../child/run-child";

/** Terminal status the runtime body expects back from a spawn. */
export type ChildTerminalStatus = "completed" | "failed" | "cancelled";

/**
 * Runtime-supplied child execution callback. The supervisor owns the
 * child env construction and `runtimeRun`; the adapter resolves the
 * definition and hands it over. The callback receives the parent's
 * `AbortSignal` unchanged, the parent run's live `onEvent` sink, the
 * spawn `depth`/ceiling, and the parent run's live credential-material
 * cell so a rotation reaches the child through the shared reference.
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
 * Terminal `SpawnChildWorkflow` adapter for an owned childWorkflow
 * import: resolve `definitionRef` from the in-memory `bodies` map (the
 * closure re-eval already lifted every inline child and the parent's
 * re-verify covers it) and delegate to `runChild` with no on-disk
 * round-trip. Mirrors
 * {@link createInMemorySpawnSuspendableChild} but drives the child
 * terminal-only rather than across approval parks.
 */
/**
 * Host-side widening of the runtime {@link SpawnChildWorkflow} contract:
 * the same input plus the per-run `onEvent` sink the host injects. The
 * sink is a call argument (not closed over) because the resolver is
 * selected once per deployment while `onEvent` is built per run.
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

    // Re-check after the resolution await; the caller can fire the
    // abort between the entry-time check and here.
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
 * analog of {@link RunChildWorkflow}: the supervisor returns a live
 * `SuspendableChildHandle` the caller drives across the body's approval
 * parks rather than awaiting a terminal.
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
  /** Live inference-event sink for the child's agent steps. */
  onEvent: (event: InferenceEvent) => void,
  /**
   * The parent run's live credential-material cell, read live on a rotation.
   */
  credentialMaterial?: CredentialMaterialRef,
) => Promise<SuspendableChildHandle>;

/**
 * Host-side widening of the runtime {@link SpawnSuspendableChild} contract:
 * the same input plus the per-run `onEvent` sink the host injects. The
 * runtime contract in `@intx/workflow` stays untouched.
 */
export type HostSpawnSuspendableChild = (
  input: Parameters<SpawnSuspendableChild>[0],
  onEvent: (event: InferenceEvent) => void,
  credentialMaterial?: CredentialMaterialRef,
) => ReturnType<SpawnSuspendableChild>;

/**
 * `SpawnSuspendableChild` adapter for the source-ref (code-sourced)
 * path: resolve `definitionRef` from the in-memory `bodies` map (the
 * parent's re-eval + re-verify already covers every inline body) and
 * run it in-process with no disk round-trip and no separate per-body
 * re-verify.
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

/** Abort rejection, mirroring the step-invoker adapter's stable shape. */
function abortError(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  return new DOMException("aborted", "AbortError");
}
