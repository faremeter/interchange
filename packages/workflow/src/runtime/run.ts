// The single runtime body, invoked by `runLocal` and the future child-process
// entry point. Switches on env keys, never on host process (`run.test.ts`
// enforces that), so env implementations swap without re-validating.

import { correlationIdFromSignalName, signalName } from "@intx/types";
import type { ApprovalSnapshot, ControlParkKind } from "@intx/types/runtime";

import type {
  ActionPrimitive,
  AwaitSignalPrimitive,
  BodyFailurePolicy,
  ChildWorkflowPrimitive,
  EscalationPrimitive,
  GatePrimitive,
  LoopPrimitive,
  MapPrimitive,
  OnTriggerPrimitive,
  Primitive,
  SleepPrimitive,
  StepPrimitive,
  WorkflowDefinition,
} from "../definition/index";
import {
  downstreamClosure,
  hashDefinition,
  RUN_ID_PATTERN,
  stepTriggerBudget,
  validateRetryTriggerCombination,
} from "../definition/index";
import { evaluate, type SelectorContext } from "./selectors";
import {
  hasFailedStep,
  isCrashedInvocationStep,
  isResumableAwaitingSignalStep,
  isResumableLoopStep,
  isResumableOnTriggerStep,
  isResumableReceivedAwaitSignalStep,
  isResumableSleepStep,
  isRunDone,
  nextSchedulable,
} from "./dag";
import {
  commit as commitDurableToChain,
  commitBuffered as commitBufferedToChain,
  dropChain,
  flushChain,
  reloadState as reloadStateInChain,
} from "./commit-chain";
import type {
  RunResult,
  RuntimeWorkflowRun,
  SpawnSuspendableChild,
  SuspendableChildHandle,
  WorkflowPark,
  WorkflowRuntimeEnv,
} from "./env";
import { shouldAbortForDrain } from "./drain";
import { bridgeAbort } from "./abort-bridge";
import {
  assertSpawnDepthWithinLimit,
  resolveMaxChildSpawnDepth,
} from "./child-depth";
import { RuntimeResumeUnsupportedError } from "./errors";
import { loopBodyRunId, scopedStepId } from "./step-scope";
import { inlineBodyRef } from "../ontrigger-bodies";
import {
  controlParkKindOf,
  decideTerminalRunFlip,
  isTerminalRunPhase,
  resumeFromLog,
  TransitionError,
  type RunState,
  type WorkflowEvent,
} from "../state-machine/index";

export interface RuntimeRunOptions {
  triggerPayload?: unknown;
  consumedMessageId?: string;
  runId?: string;
  /**
   * Pre-existing event log to resume from. Accepted: complete-or-cancelled,
   * step-boundary-aligned, and resumable carve-out logs; an
   * invocation-boundary step (`step`/`action`) left `in-flight` settles
   * `StepFailed` (at-most-once), other residuals surface
   * `RuntimeResumeUnsupportedError`. When omitted, the durable log for
   * `runId` is reduced: empty starts fresh, non-terminal is adopted,
   * terminal is returned as-is.
   */
  resumeFromEvents?: readonly WorkflowEvent[];
  /**
   * Nesting depth in the childWorkflow spawn chain (top-level is 0), carried
   * on the spawn seam, not in the durable log. Default 0.
   */
  depth?: number;
  /**
   * Ceiling on child spawn depth for this run tree. An injected value can
   * only lower it below `MAX_CHILD_SPAWN_DEPTH`, never raise it. Threaded to
   * every spawned child. Default `MAX_CHILD_SPAWN_DEPTH`.
   */
  maxChildSpawnDepth?: number;
  /**
   * A parent-supplied abort that tears this run down locally: it aborts the
   * run's own cancel controller, so an in-flight or parked step settles
   * `StepFailed` and the run settles `RunFailed`. Unlike `WorkflowRun.cancel`,
   * it writes no durable `CancelRequested` and never enters `cancelling`.
   * Used for an in-process suspendable child (loop iteration, onTrigger body)
   * whose principal cannot sign a control-plane cancel.
   */
  localAbort?: AbortSignal;
}

/**
 * Run a workflow against a `WorkflowRuntimeEnv`. Recovery runs against
 * canonical state seeded via `options.resumeFromEvents` or adopted from the
 * durable log; see `resumeFromEvents` for the accepted log shapes. A seed's
 * blob: refs must resolve via the `BlobSubstrate` that minted them (the
 * seed-contract guard in the body enforces this).
 */
export function runtimeRun(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  options: RuntimeRunOptions = {},
): RuntimeWorkflowRun {
  const runId = options.runId ?? env.newId("run");
  // A run id is a durable-store path segment and a mail-address local part, so
  // an unconstrained caller-supplied id is a path-escape hazard.
  if (!RUN_ID_PATTERN.test(runId)) {
    throw new Error(
      `run id ${JSON.stringify(runId)} must match ${RUN_ID_PATTERN.source}`,
    );
  }
  const cancelController = new AbortController();
  // Local teardown: abort the run's own controller directly -- no durable
  // `CancelRequested`, so the run fails rather than cancels.
  if (options.localAbort !== undefined) {
    bridgeAbort(options.localAbort, () => {
      cancelController.abort();
    });
  }
  const completePromise = executeRun(
    definition,
    env,
    runId,
    cancelController,
    options,
  );
  return {
    runId,
    complete: completePromise,
    async cancel(origin, reason) {
      // Route through `commit` so a cancel racing in-flight primitive commits
      // cannot collide on seq numbers; the catch absorbs the narrow race.
      const live = await reloadState(env, runId);
      if (isTerminalRunPhase(live.phase)) return;
      const event: WorkflowEvent = {
        kind: "CancelRequested",
        seq: live.lastSeq + 1,
        at: env.clock().toISOString(),
        reason,
        origin,
      };
      try {
        // Out-of-band vs the segment buffer: persist immediately so a crash
        // mid-cancel does not lose the request; `commitDurable` flushes first.
        await commitDurable(env, runId, event);
        // Emit ChildCancelRequested for live children before the abort listener
        // fires, so the parent's log records the ask alongside the child's
        // terminal. Idempotent: later passes skip already-flagged children.
        const afterCancel = await reloadState(env, runId);
        await emitChildCancelCascade(env, runId, afterCancel);
      } catch (cause) {
        if (
          cause instanceof TransitionError &&
          cause.code === "terminal-phase"
        ) {
          return;
        }
        throw cause;
      }
      cancelController.abort();
    },
    async applyCommittedCancellation() {
      try {
        await emitChildCancelCascade(env, runId, await reloadState(env, runId));
      } catch (cause) {
        if (
          !(cause instanceof TransitionError) ||
          cause.code !== "terminal-phase"
        ) {
          throw cause;
        }
      } finally {
        // A parked body waits on this abort; a failed read or cascade must not
        // withhold it.
        cancelController.abort();
      }
    },
    async signal(name, payload, signalId) {
      await env.signalChannel.deliver(name, payload, signalId);
    },
  };
}

async function executeRun(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  cancelController: AbortController,
  options: RuntimeRunOptions,
): Promise<RunResult> {
  try {
    return await executeRunBody(
      definition,
      env,
      runId,
      cancelController,
      options,
    );
  } finally {
    // Drop the per-runId commit chain entry even on a thrown body, so
    // long-running processes do not accumulate dead promise chains.
    dropChain(runId);
  }
}

// Intra-segment commit: validate the transition and assign the seq in
// memory; the durable write lands in the per-runId buffer.
function commit(
  env: WorkflowRuntimeEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<ReturnType<typeof resumeFromLog>> {
  return commitBufferedToChain(env, runId, event);
}

// Segment-boundary commit: buffer the event, then flush the whole pending
// buffer in one durable `appendBatch`. Used for terminal events, the
// control-plane `cancel`, and the agent-invoke barrier (see `runStep`); the
// terminal event last keeps the terminal-lock satisfied.
function commitDurable(
  env: WorkflowRuntimeEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<ReturnType<typeof resumeFromLog>> {
  return commitDurableToChain(env, runId, event);
}

// Flush the pending buffer in one `appendBatch` AFTER buffering the
// suspension marker, so the marker is durable before the run parks; the
// out-of-process scheduler tails the durable `TimerSet`.
async function flush(env: WorkflowRuntimeEnv, runId: string): Promise<void> {
  await flushChain(env, runId);
}

async function reloadState(
  env: WorkflowRuntimeEnv,
  runId: string,
): Promise<ReturnType<typeof resumeFromLog>> {
  return reloadStateInChain(env, runId);
}

async function executeRunBody(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  cancelController: AbortController,
  options: RuntimeRunOptions,
): Promise<RunResult> {
  const initialEvents = options.resumeFromEvents ?? [];

  // Resolve the child-spawn depth guard once at this run edge: this run's own
  // depth (0 at the top level) and the tree-wide ceiling (an override can only
  // tighten it). Both thread to the childWorkflow spawn site.
  const depth = options.depth ?? 0;
  const maxChildSpawnDepth = resolveMaxChildSpawnDepth(
    options.maxChildSpawnDepth,
  );

  // Restore prior events to the repo store on resume so a downstream read
  // sees the historical log alongside newly-appended events. Seeds are
  // written verbatim with their original seqs (the commit-lock path would
  // reassign seq and corrupt the replay invariant).
  const existing = await env.repoStore.read(runId);
  const existingBySeq = new Map(existing.map((e) => [e.seq, e]));
  for (const event of initialEvents) {
    const already = existingBySeq.get(event.seq);
    if (already !== undefined) {
      // Same-seq seeds are idempotent only when structurally identical; a
      // divergent seed corrupts the replay invariant and must throw.
      if (!eventsStructurallyEqual(already, event)) {
        throw new Error(
          `resume seed conflicts with store at seq ${String(event.seq)}: store holds ${already.kind}, seed carries ${event.kind} (or a different payload)`,
        );
      }
      continue;
    }
    await env.repoStore.append(runId, event);
  }

  // Seed-contract guard, before any blob resolution: a seeded resume whose
  // `resumeFromEvents` carries blob: refs needs the BlobSubstrate that
  // recorded them; the runLocal in-memory substrate is ephemeral, so fail
  // with a targeted error rather than a deep `resolveRef` miss. Runs before
  // the terminal short-circuit and the hydration below (both call
  // `resolveRef`); keyed on `initialEvents`, so a seedless resume skips it.
  const seedBlobRefs = initialEvents.filter(
    (e): e is typeof e & { kind: "StepCompleted" } =>
      e.kind === "StepCompleted" && e.output.ref.startsWith("blob:"),
  );
  if (seedBlobRefs.length > 0 && env.blobs.ephemeral) {
    throw new Error(
      `resume requires the BlobSubstrate that recorded the seed log's blob refs (${String(seedBlobRefs.length)} blob output(s) present); the runLocal in-memory substrate is ephemeral and starts empty. Pass the originating env, or use a durable substrate.`,
    );
  }

  // Establish canonical state from the durable log, not the seed array; every
  // decision below keys on the reduced log.
  let state = await reloadState(env, runId);

  // Terminal short-circuit: a recovery call against an already-terminal log
  // must not emit a fresh `RunStarted` (that throws `terminal-phase`); return
  // the existing result reconstructed from the durable log.
  if (isTerminalRunPhase(state.phase)) {
    return buildResultFromLog(env, runId, state);
  }

  // Classify residual steps the canonical log leaves in a non-terminal phase
  // the runtime body cannot re-arm: `nextSchedulable` skips any step already
  // in `state.steps`, so such a step would stall the main loop. Cancellation
  // paths are exempt -- the cleanup branch owns `cancelling` steps.
  //
  // A residual `in-flight` step at an invocation boundary (`step`/`action`)
  // is a crash mid-invocation: its `StepStarted` is durable, no
  // `StepCompleted` landed, and the invoked primitive is non-deterministic,
  // so it cannot be replayed exactly-once; settle it as a terminal
  // `StepFailed`. Every other non-terminal residual (a mid-`map`/
  // `childWorkflow` container, an awaiting-signal/timer step) surfaces
  // `RuntimeResumeUnsupportedError` -- the host owns re-arming those.
  const crashedInFlight: { stepId: string; attempt: number }[] = [];
  const recoverableParks: {
    stepId: string;
    correlationId: string;
    timeoutAtMs?: number;
  }[] = [];
  if (state.phase === "running") {
    for (const [stepId, stepState] of state.steps) {
      // Resumable carve-outs, each re-offered by `nextSchedulable` on the same
      // predicate: a mid-loop container (runLoop re-derives its cursor), an
      // `awaitSignal` still `awaiting-signal` (re-parks) or `in-flight` from a
      // mover (reconstructs the outcome from the log), and a `sleep`
      // `awaiting-timer`/`in-flight` (re-adopts or completes).
      if (
        isResumableLoopStep(definition, stepId, stepState.phase) ||
        isResumableAwaitingSignalStep(definition, stepId, stepState.phase) ||
        isResumableReceivedAwaitSignalStep(
          definition,
          stepId,
          stepState.phase,
        ) ||
        isResumableOnTriggerStep(definition, stepId, stepState.phase) ||
        isResumableSleepStep(definition, stepId, stepState.phase)
      ) {
        // The onTrigger container never self-completes; `runOnTrigger`
        // re-derives its cursor. Leave it for `nextSchedulable`.
        continue;
      }
      // A crashed-mid-invocation step (agent step or action). Before settling
      // it terminal, check the crash-mid-park window: the reactor durably
      // recorded an approval suspension whose `SignalAwaited` was buffered
      // but never flushed, so the reduced phase is `in-flight`, not
      // `awaiting-signal`. Recoverable -- the loop below re-commits the
      // missing `SignalAwaited` from the pending-op store -- otherwise it is
      // a genuine crash mid-agent-turn and settles terminal. Both settlings
      // happen AFTER this loop; committing inline would leave `state` stale.
      if (isCrashedInvocationStep(definition, stepId, stepState.phase)) {
        const parkedOps =
          env.readParkedApprovalOps !== undefined
            ? await env.readParkedApprovalOps({
                runId,
                stepId,
                attempt: stepState.currentAttempt,
              })
            : [];
        // A step-attempt parks on at most one control-plane suspension; more
        // durable pending ops mean a corrupt store -- fail loud.
        if (parkedOps.length > 1) {
          throw new Error(
            `crashed step ${stepId} (attempt ${String(stepState.currentAttempt)}) has ${String(parkedOps.length)} durable pending approval operations; a step-attempt parks on at most one control-plane suspension`,
          );
        }
        const parked = parkedOps[0];
        if (parked !== undefined) {
          recoverableParks.push({
            stepId,
            correlationId: parked.correlationId,
            ...(parked.timeoutAtMs !== undefined
              ? { timeoutAtMs: parked.timeoutAtMs }
              : {}),
          });
        } else {
          crashedInFlight.push({
            stepId,
            attempt: stepState.currentAttempt,
          });
        }
        continue;
      }
      // Every other non-terminal residual keeps declining: the host owns the
      // recovery decision.
      if (
        stepState.phase === "in-flight" ||
        stepState.phase === "awaiting-signal" ||
        stepState.phase === "awaiting-timer"
      ) {
        throw new RuntimeResumeUnsupportedError(
          stepId,
          stepState.phase,
          `durable log leaves step ${stepId} in phase ${stepState.phase} with no schedulable primitive on the DAG`,
        );
      }
    }
  }

  // Recover each crash-mid-park approval step by committing the `SignalAwaited`
  // the crash prevented from flushing, from the reactor's durable pending op.
  // This advances the step to `awaiting-signal`, the ordinary crash-after-park
  // case: `nextSchedulable` re-offers it and `runStep` re-parks on the
  // recovered channel with the original correlationId; already
  // `awaiting-signal`, it does not re-fire `onPark`, so the correlation
  // registers once and the agent turn is never re-invoked (at-most-once).
  // `timeoutAt` is stored as epoch ms; `SignalAwaited` carries ISO, so
  // convert, and omit the field for the indefinite-hold norm.
  //
  // APPROVAL-only: an "input" park has no durable pending-op, so a crash in
  // its pre-flush window settles as a terminal StepFailed below.
  for (const { stepId, correlationId, timeoutAtMs } of recoverableParks) {
    const awaited: WorkflowEvent = {
      kind: "SignalAwaited",
      seq: state.lastSeq + 1,
      at: env.clock().toISOString(),
      stepId,
      signalName: signalName(correlationId),
      ...(timeoutAtMs !== undefined
        ? { timeoutAt: new Date(timeoutAtMs).toISOString() }
        : {}),
    };
    state = await commitDurable(env, runId, awaited);
  }

  // Settle each crashed-mid-invocation step as a terminal `StepFailed`
  // (`retriesExhausted: true`), so `nextSchedulable` will not re-schedule it
  // (at-most-once). A unit carrying `onFailure` routes rather than going
  // fatal:
  // the resume reconciliation below observes the `routed` phase, prunes its
  // normal dependents, and reconstructs its sentinel; without one it stays a
  // bare fatal `StepFailed` and the post-loop `hasFailedStep` path commits
  // `RunFailed`. (`isCrashedInvocationStep` matches only `step`/`action`; a
  // crashed `childWorkflow` is host-owned.)
  for (const { stepId, attempt } of crashedInFlight) {
    const primitive = definition.steps[stepId];
    const onFailure =
      primitive !== undefined &&
      (primitive.kind === "step" || primitive.kind === "action")
        ? primitive.onFailure
        : undefined;
    const failed: WorkflowEvent = {
      kind: "StepFailed",
      seq: state.lastSeq + 1,
      at: env.clock().toISOString(),
      stepId,
      attempt,
      error: {
        message: `step ${stepId} crashed mid-invocation; the invoked primitive is non-deterministic and unrecorded, so it is not re-invoked (at-most-once)`,
        code: "crash-mid-invocation",
      },
      retriesExhausted: true,
      ...(onFailure !== undefined ? { routedTo: onFailure } : {}),
    };
    state = await commitDurable(env, runId, failed);
  }

  if (state.phase === "pending") {
    const event: WorkflowEvent = {
      kind: "RunStarted",
      seq: state.lastSeq + 1,
      at: env.clock().toISOString(),
      runId,
      definitionHash: bytesToHex(hashDefinition(definition)),
      trigger: triggerSnapshot(definition, options.triggerPayload),
      ...(options.consumedMessageId !== undefined
        ? { consumedMessageId: options.consumedMessageId }
        : {}),
    };
    try {
      state = await commit(env, runId, event);
    } catch (cause) {
      // Reached only when canonical state was `pending`, so seedless recovery
      // (whose canonical log already carries `RunStarted`) never lands here.
      // The one race that still rejects with `code: "phase"` is a
      // `cancel("self", ...)` beating this first `RunStarted` commit:
      // `CancelRequested` is legal from `pending`, so the chain reloads, sees
      // phase=cancelling, and rejects. Reload and continue -- proceeding
      // routes through the cancellation cleanup branch and emits
      // `RunCancelled`. Any other rejection is a real error and must surface.
      if (cause instanceof TransitionError && cause.code === "phase") {
        state = await reloadState(env, runId);
      } else {
        throw cause;
      }
    }
  }

  const inFlight = new Set<string>();
  const stepOutputs: Record<string, unknown> = {};
  // Hydrate stepOutputs from the canonical log's StepCompleted events so
  // downstream steps can resolve `{ from: "steps.<id>.output" }` selectors
  // against work completed before this process took over (seed or adopted
  // log); without it, any such selector throws as a spurious StepFailed.
  const canonicalLog = await env.repoStore.read(runId);
  for (const event of canonicalLog) {
    if (event.kind !== "StepCompleted") continue;
    stepOutputs[event.stepId] = await env.blobs.resolveRef(event.output.ref);
  }

  // onFailure resume reconciliation, after the log hydration above and before
  // the first `nextSchedulable`. A routed unit records no StepCompleted, so
  // its live failure sentinel -- held only in the in-process `stepOutputs`
  // map -- is lost on resume; rebuild it from the routed StepFailed's reduced
  // error, and complete the branch prune for each routed or completed
  // onFailure unit (idempotent: each route emits its prune before its
  // terminal). Skipped unless the run is `running`: a cancelling/terminal
  // resume is owned by the drive loop's settlement, and a skip StepStarted
  // would throw.
  if (state.phase === "running") {
    for (const [stepId, primitive] of Object.entries(definition.steps)) {
      const onFailure =
        primitive.kind === "step" ||
        primitive.kind === "action" ||
        primitive.kind === "childWorkflow"
          ? primitive.onFailure
          : undefined;
      if (onFailure === undefined) continue;
      const phase = state.steps.get(stepId)?.phase;
      if (phase === "completed") {
        await pruneAroundRoute(
          definition,
          env,
          runId,
          stepId,
          onFailure,
          "completed",
          cancelController.signal,
        );
      } else if (phase === "routed") {
        await pruneAroundRoute(
          definition,
          env,
          runId,
          stepId,
          onFailure,
          "routed",
          cancelController.signal,
        );
        const lastError = state.steps.get(stepId)?.lastError;
        if (lastError === undefined) {
          throw new Error(
            `routed unit ${stepId} has no lastError to reconstruct its onFailure sentinel`,
          );
        }
        stepOutputs[stepId] = {
          failed: true,
          stepId,
          error: { message: lastError.message },
        };
      }
    }
    state = await reloadState(env, runId);
  }

  const stepPromises = new Map<string, Promise<void>>();
  const justSettled = new Set<string>();
  // Per-step local abort controllers. Each scheduled primitive gets one; the
  // controller fires when the outer cancelController aborts, or when
  // drain.signal aborts AND the step's behavior is `"cancel"`.
  const stepAborts = new Map<string, AbortController>();

  // Tick loop: schedule everything ready, await any in-flight to settle,
  // repeat until done. Cancellation aborts every in-flight executor; we still
  // loop to commit `CancelPropagated` and the terminal `RunCancelled`.
  while (!isRunDone(definition, state)) {
    if (cancelController.signal.aborted && state.phase !== "cancelling") {
      state = await reloadState(env, runId);
    }

    // Drain observation point #1: main loop entry. If drain has fired, abort
    // every in-flight step whose declared behavior is `"cancel"`.
    if (env.drain.signal.aborted) {
      for (const stepId of inFlight) {
        if (shouldAbortForDrain(env.drain, stepId)) {
          const ac = stepAborts.get(stepId);
          if (ac !== undefined && !ac.signal.aborted) ac.abort();
        }
      }
    }

    const ready = nextSchedulable(definition, state, inFlight);
    for (const primitive of ready) {
      inFlight.add(primitive.id);
      const ctx: SelectorContext = {
        trigger: { payload: options.triggerPayload },
        steps: Object.fromEntries(
          Object.entries(stepOutputs).map(([id, output]) => [id, { output }]),
        ),
      };
      const stepLocalAbort = createStepAbort(
        primitive.id,
        cancelController.signal,
        env.drain,
      );
      stepAborts.set(primitive.id, stepLocalAbort);
      const promise = runPrimitiveSafe(
        definition,
        env,
        runId,
        primitive,
        ctx,
        stepLocalAbort.signal,
        depth,
        maxChildSpawnDepth,
      )
        .then((output) => {
          stepOutputs[primitive.id] = output;
        })
        .catch(() => {
          // Errors are committed as StepFailed inside the primitive runner;
          // the main loop notices the failed phase on the next reload.
        })
        .finally(() => {
          inFlight.delete(primitive.id);
          justSettled.add(primitive.id);
          stepAborts.delete(primitive.id);
        });
      stepPromises.set(primitive.id, promise);
    }

    if (state.phase === "cancelling") {
      state = await reloadState(env, runId);
      for (const [stepId, stepState] of state.steps) {
        if (
          stepState.phase !== "in-flight" &&
          stepState.phase !== "awaiting-signal" &&
          stepState.phase !== "awaiting-timer"
        ) {
          continue;
        }
        const propagate: WorkflowEvent = {
          kind: "CancelPropagated",
          seq: state.lastSeq + 1,
          at: env.clock().toISOString(),
          stepId,
        };
        state = await commit(env, runId, propagate);
      }
      state = await emitChildCancelCascade(env, runId, state);
      await Promise.allSettled(stepPromises.values());
      const cancelled: WorkflowEvent = {
        kind: "RunCancelled",
        seq: state.lastSeq + 1,
        at: env.clock().toISOString(),
      };
      state = await commitDurable(env, runId, cancelled);
      break;
    }

    if (stepPromises.size === 0) {
      if (ready.length === 0) {
        throw new Error(
          `workflow ${definition.id} run ${runId} stalled with no schedulable primitives`,
        );
      }
      // Promises were scheduled this tick but already completed
      // synchronously; reload state and continue.
      state = await reloadState(env, runId);
      continue;
    }

    // Wait for at least one in-flight primitive to settle. Each runner
    // swallows its own errors into StepFailed events, so the race resolves
    // cleanly.
    await Promise.race(
      Array.from(stepPromises.values()).map((p) => p.catch(() => undefined)),
    );
    state = await reloadState(env, runId);
    for (const stepId of justSettled) {
      stepPromises.delete(stepId);
    }
    justSettled.clear();
  }

  // If we exited the loop without a terminal phase, settle it. The
  // `cancelling` branch also lands here when the cancel-vs-completion race
  // makes `isRunDone` return true via the all-steps-terminal path before the
  // cancellation block ran. Every run must reach a terminal event.
  if (state.phase === "cancelling") {
    state = await settleCancelling(env, runId);
  } else if (state.phase === "running") {
    const terminal: WorkflowEvent = hasFailedStep(state)
      ? {
          kind: "RunFailed",
          seq: state.lastSeq + 1,
          at: env.clock().toISOString(),
          error: { message: "one or more steps failed" },
        }
      : {
          kind: "RunCompleted",
          seq: state.lastSeq + 1,
          at: env.clock().toISOString(),
        };
    try {
      state = await commitDurable(env, runId, terminal);
    } catch (cause) {
      // A `cancel()` racing the post-loop terminal commit can land
      // `CancelRequested` first (legal from `phase=running`); the chain then
      // reloads, sees phase=cancelling, and rejects the terminal commit with
      // `code: "phase"` -- the post-loop sibling of the RunStarted race
      // above. Reload, confirm the live phase is cancelling (or already
      // terminal), and route through the cancelling cleanup branch so the run
      // settles `cancelled`.
      if (cause instanceof TransitionError && cause.code === "phase") {
        state = await reloadState(env, runId);
        if (state.phase === "cancelling") {
          state = await settleCancelling(env, runId);
        } else if (!isTerminalRunPhase(state.phase)) {
          throw cause;
        }
      } else {
        throw cause;
      }
    }
  }

  const events = await env.repoStore.read(runId);
  const terminalStatus = decideTerminalRunFlip(state.phase);
  return {
    runId,
    terminalStatus,
    outputs: stepOutputs,
    events,
  };
}

/**
 * Reconstruct the terminal `RunResult` for a run whose canonical log is
 * already terminal, without re-driving it (the terminal short-circuit).
 * Shape matches the live terminal path: `terminalStatus` from the terminal
 * phase, `events` from the durable log, `outputs` hydrated from its
 * `StepCompleted` refs.
 */
async function buildResultFromLog(
  env: WorkflowRuntimeEnv,
  runId: string,
  state: ReturnType<typeof resumeFromLog>,
): Promise<RunResult> {
  const events = await env.repoStore.read(runId);
  const outputs: Record<string, unknown> = {};
  for (const event of events) {
    if (event.kind !== "StepCompleted") continue;
    outputs[event.stepId] = await env.blobs.resolveRef(event.output.ref);
  }
  const terminalStatus = decideTerminalRunFlip(state.phase);
  return { runId, terminalStatus, outputs, events };
}

/**
 * Structural equality for two events at the same seq. Events are plain
 * JSON-serializable objects by the state-machine contract; a canonical-JSON
 * comparison ignores key order and absent-vs-undefined field differences.
 */
function eventsStructurallyEqual(a: WorkflowEvent, b: WorkflowEvent): boolean {
  return canonicalEventJSON(a) === canonicalEventJSON(b);
}

function canonicalEventJSON(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? "null";
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalEventJSON).join(",")}]`;
  }
  const entries = Object.entries(value)
    .filter(([, v]) => v !== undefined)
    .sort(([l], [r]) => (l < r ? -1 : l > r ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalEventJSON(v)}`).join(",")}}`;
}

/**
 * Post-loop cancellation cleanup: reload, cascade `ChildCancelRequested` to
 * any live children, and commit `RunCancelled`. Shared between the natural
 * cancelling exit and the post-loop catch that absorbs a phase rejection on
 * the terminal commit when a concurrent cancel won the chain race.
 */
async function settleCancelling(
  env: WorkflowRuntimeEnv,
  runId: string,
): Promise<ReturnType<typeof resumeFromLog>> {
  let state = await reloadState(env, runId);
  state = await emitChildCancelCascade(env, runId, state);
  const cancelled: WorkflowEvent = {
    kind: "RunCancelled",
    seq: state.lastSeq + 1,
    at: env.clock().toISOString(),
  };
  return commitDurable(env, runId, cancelled);
}

/**
 * Emit `ChildCancelRequested` for every tracked child whose cancellation has
 * not been issued and which has not already reached a terminal status. The
 * state machine's resume invariant documents the runtime's responsibility
 * for this cascade: without it, a resuming process cannot rebuild the cancel
 * chain from the log alone.
 */
async function emitChildCancelCascade(
  env: WorkflowRuntimeEnv,
  runId: string,
  state: ReturnType<typeof resumeFromLog>,
): Promise<ReturnType<typeof resumeFromLog>> {
  let current = state;
  for (const [childRunId, childState] of current.children) {
    if (childState.cancelRequested) continue;
    if (childState.terminalStatus !== undefined) continue;
    const event: WorkflowEvent = {
      kind: "ChildCancelRequested",
      seq: current.lastSeq + 1,
      at: env.clock().toISOString(),
      childRunId,
    };
    current = await commit(env, runId, event);
  }
  return current;
}

function triggerSnapshot(
  definition: WorkflowDefinition,
  payload: unknown,
): { type: string; payload: unknown } {
  const first = definition.triggers[0];
  if (!first) {
    return { type: "manual", payload };
  }
  return { type: first.type, payload };
}

function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) {
    out += b.toString(16).padStart(2, "0");
  }
  return out;
}

/**
 * Build the per-step local AbortController used as the `abort` argument to
 * `runPrimitiveSafe`. Aborts when the outer cancelController.signal aborts,
 * or when drain.signal aborts and the step's drainBehavior is `"cancel"`;
 * a `"wait"`-behavior step ignores drain entirely.
 */
function createStepAbort(
  stepId: string,
  outerSignal: AbortSignal,
  drain: import("./drain").DrainController,
): AbortController {
  const ac = new AbortController();
  if (outerSignal.aborted) {
    ac.abort();
    return ac;
  }
  if (shouldAbortForDrain(drain, stepId)) {
    ac.abort();
    return ac;
  }
  const onOuter = (): void => {
    ac.abort();
  };
  outerSignal.addEventListener("abort", onOuter, { once: true });
  const onDrain = (): void => {
    if (shouldAbortForDrain(drain, stepId)) {
      ac.abort();
    }
  };
  drain.signal.addEventListener("abort", onDrain, { once: true });
  return ac;
}

// =========================================================================
// Per-primitive execution
// =========================================================================

async function runPrimitive(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: Primitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
  depth: number,
  maxChildSpawnDepth: number,
): Promise<unknown> {
  switch (primitive.kind) {
    case "step":
      return runStep(definition, env, runId, primitive, selectorCtx, abort);
    case "action":
      return runAction(definition, env, runId, primitive, selectorCtx, abort);
    case "loop":
      return runLoop(
        definition,
        env,
        runId,
        primitive,
        selectorCtx,
        abort,
        depth,
        maxChildSpawnDepth,
      );
    case "onTrigger":
      return runOnTrigger(
        env,
        runId,
        primitive,
        selectorCtx,
        abort,
        depth,
        maxChildSpawnDepth,
      );
    case "map":
      return runMap(definition, env, runId, primitive, selectorCtx, abort);
    case "gate":
      return runGate(definition, env, runId, primitive, selectorCtx, abort);
    case "awaitSignal":
      return runAwaitSignal(definition, env, runId, primitive, abort);
    case "sleep":
      return runSleep(env, runId, primitive, abort);
    case "childWorkflow":
      return runChildWorkflow(
        definition,
        env,
        runId,
        primitive,
        selectorCtx,
        abort,
        depth,
        maxChildSpawnDepth,
      );
    case "escalation":
      return runEscalation(env, runId, primitive, selectorCtx);
  }
}

/**
 * Wrap the per-primitive runner so an uncaught throw always lands a terminal
 * step-phase event in the log. Each runner already commits its own normal
 * completion and most failure paths; this is the safety net for awaited
 * promises rejecting outside the runner's own try/finally (e.g. signal
 * abort).
 */
async function runPrimitiveSafe(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: Primitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
  depth: number,
  maxChildSpawnDepth: number,
): Promise<unknown> {
  try {
    return await runPrimitive(
      definition,
      env,
      runId,
      primitive,
      selectorCtx,
      abort,
      depth,
      maxChildSpawnDepth,
    );
  } catch (cause) {
    let state = await reloadState(env, runId);
    const stepState = state.steps.get(primitive.id);
    if (!stepState) {
      // The step never reached `StepStarted`. If the run is cancelling (or
      // already terminal), the body's cleanup path owns the terminal events;
      // synthetic step events would be rejected. Surface the original cause
      // and leave the log untouched.
      if (state.phase === "cancelling" || isTerminalRunPhase(state.phase)) {
        throw cause;
      }
      // No StepStarted was committed (e.g. the input selector threw). Emit a
      // synthetic StepStarted + StepFailed so the scheduler sees the step as
      // terminal.
      const message = cause instanceof Error ? cause.message : String(cause);
      const syntheticStarted: WorkflowEvent = {
        kind: "StepStarted",
        seq: state.lastSeq + 1,
        at: env.clock().toISOString(),
        stepId: primitive.id,
        attempt: 1,
        input: { ref: "(error)" },
      };
      state = await commit(env, runId, syntheticStarted);
      const syntheticFailed: WorkflowEvent = {
        kind: "StepFailed",
        seq: state.lastSeq + 1,
        at: env.clock().toISOString(),
        stepId: primitive.id,
        attempt: 1,
        error: { message },
        retriesExhausted: true,
      };
      state = await commit(env, runId, syntheticFailed);
      void state;
      throw cause;
    }
    const stillRunning =
      stepState.phase === "in-flight" ||
      stepState.phase === "awaiting-signal" ||
      stepState.phase === "awaiting-timer";
    if (stillRunning) {
      // Cancellation wins over failure: a step mid-flight when cancellation
      // reached it should end up `cancelled`, not `failed`; the step-level
      // guarantee lives here.
      if (state.phase === "cancelling") {
        const propagated: WorkflowEvent = {
          kind: "CancelPropagated",
          seq: state.lastSeq + 1,
          at: env.clock().toISOString(),
          stepId: primitive.id,
        };
        state = await commit(env, runId, propagated);
      } else {
        const message = cause instanceof Error ? cause.message : String(cause);
        // action/childWorkflow route their permanent failure to an onFailure
        // handler HERE -- this arm commits their terminal StepFailed (unlike
        // `step`, which routes inside runStep; it is NOT routed here, so gate
        // on the invocation kinds explicitly). A cancelled child keeps its
        // disposition (bare StepFailed, no routedTo): routing it would fire
        // the fallback handler on an operator's intentional stop.
        const unit = definition.steps[primitive.id];
        const onFailure =
          unit !== undefined &&
          (unit.kind === "action" || unit.kind === "childWorkflow")
            ? unit.onFailure
            : undefined;
        const cancelledChild =
          cause instanceof ChildWorkflowFailedError &&
          cause.childTerminalStatus === "cancelled";
        // A SuccessTerminalizationError means the unit's WORK succeeded but
        // landing its terminal failed; it must NOT route (that would fire the
        // handler on a success) and NOT retry. Falls through to the bare
        // failure below.
        if (
          onFailure !== undefined &&
          !cancelledChild &&
          !(cause instanceof SuccessTerminalizationError)
        ) {
          await pruneAroundRoute(
            definition,
            env,
            runId,
            primitive.id,
            onFailure,
            "routed",
            abort,
          );
          state = await reloadState(env, runId);
          // Cancellation wins over routing: a cancel that landed across the
          // prune's awaits settles the unit `cancelled` rather than routed.
          if (state.phase === "cancelling") {
            const propagated: WorkflowEvent = {
              kind: "CancelPropagated",
              seq: state.lastSeq + 1,
              at: env.clock().toISOString(),
              stepId: primitive.id,
            };
            state = await commit(env, runId, propagated);
            void state;
            throw cause;
          }
          const routed: WorkflowEvent = {
            kind: "StepFailed",
            seq: state.lastSeq + 1,
            at: env.clock().toISOString(),
            stepId: primitive.id,
            attempt: stepState.currentAttempt,
            error: { message },
            retriesExhausted: true,
            routedTo: onFailure,
          };
          state = await commit(env, runId, routed);
          void state;
          return { failed: true, stepId: primitive.id, error: { message } };
        }
        const failed: WorkflowEvent = {
          kind: "StepFailed",
          seq: state.lastSeq + 1,
          at: env.clock().toISOString(),
          stepId: primitive.id,
          attempt: stepState.currentAttempt,
          error: { message },
          retriesExhausted: true,
        };
        state = await commit(env, runId, failed);
      }
    }
    void state;
    throw cause;
  }
}

/**
 * Run an agent step (the agent path; `runAction` is the separate action
 * path).
 *
 * Agent-invoke durability barrier: the step's `StepStarted` is flushed
 * durably via `commitDurable` BEFORE `env.invokeStep` is called, so a crash
 * mid-invocation leaves a durable `StepStarted` with no `StepCompleted`,
 * which recovery settles as a terminal failure rather than re-invoking the
 * agent (at-most-once).
 */
async function runStep(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  step: StepPrimitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
): Promise<unknown> {
  // Re-apply the retry/budget cross-field guard here as a defensive re-check
  // at the runtime's single read point for both fields, rather than trust
  // that every definition reached it through `step()`.
  validateRetryTriggerCombination(step);
  let attempt = 1;
  const maxAttempts = step.retry?.maxAttempts ?? 1;
  // StepStarted is committed exactly once per step -- the entry to the first
  // attempt. Subsequent attempts re-enter via the AttemptScheduled +
  // TimerFired pair, which moves the step from awaiting-timer back to
  // in-flight without a fresh StepStarted.
  let stepStartedEmitted = false;

  // Crash-resume re-entry. A run re-driving the durable log re-offers a
  // `step` left `awaiting-signal` via `isResumableAwaitingSignalStep`. The
  // agent already parked on a reactor gate before the crash; re-invoking it with
  // the original input would start a NEW turn and re-run the suspended work.
  // Instead recover the channel from the reduced state (the
  // `signalName(correlationId)` lives only on the durable `SignalAwaited`),
  // RE-PARK on it -- `parkOnSignal` skips re-emitting since the step is
  // already `awaiting-signal` -- and seed `resume` so the first `invokeStep`
  // re-invokes the agent against the delivered decision.
  const entryState = await reloadState(env, runId);
  const entryStepState = entryState.steps.get(step.id);
  let resumeFromPark:
    | { signalName: string; correlationId: string; parkKind: ControlParkKind }
    | undefined;
  if (entryStepState?.phase === "awaiting-signal") {
    const parkedSignalName = findAwaitedSignalNameForStep(entryState, step.id);
    if (parkedSignalName === undefined) {
      throw new Error(
        `runStep resume: step ${step.id} is awaiting-signal but no awaited signal name is in the reduced state`,
      );
    }
    const correlationId = correlationIdFromSignalName(parkedSignalName);
    if (correlationId === undefined) {
      throw new Error(
        `runStep resume: step ${step.id} is parked on ${parkedSignalName}, which is not a reserved control-plane signal name; an agent step suspends only on a signalName(correlationId) channel`,
      );
    }
    // Recover the park kind from the durable reduced state so the resume
    // synthesizes the right inbound after a respawn. `controlParkKindOf` is
    // the single point that maps the optional kind to a definite one.
    const awaited = entryStepState.awaitingSignal;
    if (awaited === undefined) {
      throw new Error(
        `runStep resume: step ${step.id} is awaiting-signal but has no awaitingSignal in the reduced state`,
      );
    }
    resumeFromPark = {
      signalName: parkedSignalName,
      correlationId,
      parkKind: controlParkKindOf(awaited),
    };
    // Recover the attempt the step suspended on: the suspend committed its
    // pending-op + turns under the cold-path ContextStore keyed by this
    // attempt (`stepStorageRoot({runId, stepId, attempt})`), so the resume
    // re-invoke must reopen the SAME store. A step that RETRIED before
    // suspending reduces to currentAttempt >= 2; leaving `attempt` at 1
    // would reopen the wrong store and hang.
    attempt = entryStepState.currentAttempt;
    // The durable log already carries this step's StepStarted, so the
    // fresh-attempt emit below must be skipped: re-emitting throws.
    stepStartedEmitted = true;
  }

  while (true) {
    // Materialize the input first so the StepStarted event carries the
    // substrate-resolvable ref the audit reader expects. Selector throws
    // land the synthetic StepFailed in `runPrimitiveSafe`.
    const rawInput =
      step.input !== undefined ? evaluate(step.input, selectorCtx) : null;
    // Canonicalize `undefined` to `null` once here so the audit blob and the
    // invoker see the same value: a default-input convention resolving to
    // `undefined` is stored as `null` so the audit ref stays round-trippable.
    const input = rawInput === undefined ? null : rawInput;
    if (!stepStartedEmitted) {
      const { ref: inputRef } = await env.blobs.recordOutput(
        `${step.id}.input`,
        attempt,
        input,
      );
      let state = await reloadState(env, runId);
      const started: WorkflowEvent = {
        kind: "StepStarted",
        seq: state.lastSeq + 1,
        at: env.clock().toISOString(),
        stepId: step.id,
        attempt,
        input: { ref: inputRef },
      };
      state = await commitDurable(env, runId, started);
      void state;
      stepStartedEmitted = true;
    }

    // Build per-step abort: timeout AND outer cancellation both abort. The
    // durable commit above is an await, so check the outer signal's level
    // before subscribing; an abort raised during that commit would otherwise
    // never reach the invoker.
    const stepAbort = new AbortController();
    const onOuter = () => {
      stepAbort.abort();
    };
    bridgeAbort(abort, onOuter);
    let timer: ReturnType<typeof setTimeout> | undefined;
    if (step.timeout !== undefined) {
      timer = setTimeout(() => {
        stepAbort.abort();
      }, step.timeout);
    }

    try {
      // Suspend/resume bridge. The first invocation drives a plain agent
      // send; if the reactor parks on a gate, `invokeStep` returns
      // `{ suspend: { correlationId } }` instead of an output. The step then
      // parks durably on the reserved `signalName(correlationId)` channel;
      // when the decision arrives it is re-invoked with `resume`, so the
      // invoker re-dispatches the tool and the real reply -- not the raw
      // signal payload -- is the step output.
      let output: unknown;
      let resume:
        | { correlationId: string; decision: unknown; kind: ControlParkKind }
        | undefined;
      // Re-park FIRST (per the entry comment): `resumeFromPark` is consumed
      // once; a resume that suspends again re-parks via `{ suspend }` below.
      if (resumeFromPark !== undefined) {
        const parkState = await reloadState(env, runId);
        const decision = await parkOnSignal(
          env,
          runId,
          {
            stepId: step.id,
            signalName: resumeFromPark.signalName,
          },
          parkState,
          stepAbort.signal,
        );
        resume = {
          correlationId: resumeFromPark.correlationId,
          decision,
          kind: resumeFromPark.parkKind,
        };
        resumeFromPark = undefined;
      }
      // Trigger budget: how many triggers this step services before it
      // completes (`stepTriggerBudget` owns the absent-means-1 default). A
      // batch step (1) completes on its first output; an `"unbounded"` step
      // re-arms after every output and never self-completes. For a finite
      // budget > 1 the serviced count must survive a respawn, so it is
      // seeded from the durable log -- the number of input-park
      // `SignalAwaited`s the step emitted equals its serviced turns, since
      // the re-arm below is the ONLY minter of input parks. Budgets of 1
      // and "unbounded" never re-arm, so both skip the log read.
      const triggerBudget = stepTriggerBudget(step);
      let servicedTriggers = 0;
      if (triggerBudget !== "unbounded" && triggerBudget > 1) {
        const priorEvents = await env.repoStore.read(runId);
        servicedTriggers = priorEvents.filter(
          (e) =>
            e.kind === "SignalAwaited" &&
            e.stepId === step.id &&
            e.parkKind === "input",
        ).length;
      }
      while (true) {
        const result = await env.invokeStep({
          agent: step.agent,
          input,
          authzContext: {
            stepId: step.id,
            attempt,
            runId,
          },
          signal: stepAbort.signal,
          ...(resume !== undefined ? { resume } : {}),
        });
        if ("output" in result) {
          output = result.output;
          servicedTriggers += 1;
          // Budget spent -> complete; budget remaining -> re-arm: park the
          // step on a fresh input control-plane channel awaiting its next
          // trigger, which becomes the next turn's input. The park is
          // snapshot-less (`kind: "input"`) and fires no host notify;
          // `env.newId` mints a unique channel per turn so deliveries never
          // collide, and the owner discovers the current channel from the
          // reduced `awaitingSignal.name`.
          const hasMoreTriggers =
            triggerBudget === "unbounded" || servicedTriggers < triggerBudget;
          if (!hasMoreTriggers) break;
          const inputCorrelationId = env.newId("corr");
          const rearmState = await reloadState(env, runId);
          const decision = await parkOnSignal(
            env,
            runId,
            {
              stepId: step.id,
              signalName: signalName(inputCorrelationId),
              parkKind: "input",
            },
            rearmState,
            stepAbort.signal,
          );
          resume = {
            correlationId: inputCorrelationId,
            decision,
            kind: "input",
          };
          continue;
        }
        // The reactor parked. Park the step on the reserved signal channel
        // for this correlation. Unlike runAwaitSignal, the step already
        // emitted its own `StepStarted` on runStep entry, so the reduced
        // state reads `in-flight`, and the re-park guard emits a fresh
        // `SignalAwaited` rather than re-parking an already-awaiting gate.
        const parkState = await reloadState(env, runId);
        const decision = await parkOnSignal(
          env,
          runId,
          {
            stepId: step.id,
            signalName: signalName(result.suspend.correlationId),
            // An invoker can only suspend as an approval (the input park is
            // minted by the trigger-budget re-arm above, never by an
            // invoker), and the approval arm carries a mandatory snapshot.
            parkKind: result.suspend.kind,
            approvalSnapshot: result.suspend.approvalSnapshot,
          },
          parkState,
          stepAbort.signal,
        );
        resume = {
          correlationId: result.suspend.correlationId,
          decision,
          kind: result.suspend.kind,
        };
      }
      // Wrap the success terminalization -- recording the output, pruning the
      // handler branch, and committing StepCompleted -- so a durable-store
      // failure here is distinguishable from an invocation failure in the
      // catch below: the work already succeeded, so it must not be routed or
      // retried; the catch lands a bare failure instead.
      try {
        const outputRef = (
          await env.blobs.recordOutput(step.id, attempt, output)
        ).ref;
        // A unit carrying onFailure routes on BOTH outcomes: on success the
        // handler branch must be pruned, or the scheduler would offer the
        // failure handler off its `after: [unit]` once the unit is terminal.
        // Prune while the unit is still in-flight, before the StepCompleted
        // below, so no sibling settling can schedule the handler mid-prune.
        if (step.onFailure !== undefined) {
          await pruneAroundRoute(
            definition,
            env,
            runId,
            step.id,
            step.onFailure,
            "completed",
            abort,
          );
        }
        let after = await reloadState(env, runId);
        const completed: WorkflowEvent = {
          kind: "StepCompleted",
          seq: after.lastSeq + 1,
          at: env.clock().toISOString(),
          stepId: step.id,
          attempt,
          output: { ref: outputRef },
        };
        after = await commit(env, runId, completed);
        void after;
      } catch (termCause) {
        throw new SuccessTerminalizationError(termCause);
      }
      return output;
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      const exhausted = attempt >= maxAttempts;
      let after = await reloadState(env, runId);
      // Cancellation wins over step-level failure: if the run is cancelling,
      // the catch landed because the step's abort fired, so the audit log
      // records a cancellation rather than a runtime-attributed failure.
      if (after.phase === "cancelling") {
        const propagated: WorkflowEvent = {
          kind: "CancelPropagated",
          seq: after.lastSeq + 1,
          at: env.clock().toISOString(),
          stepId: step.id,
        };
        after = await commit(env, runId, propagated);
        void after;
        throw cause;
      }
      if (cause instanceof SuccessTerminalizationError) {
        // The step's work succeeded but landing its terminal failed. Do not
        // route and do not retry -- both would act on a step that already did
        // its work (a retry would re-invoke the agent, an at-most-once
        // violation). Land a bare failure so the run fails loudly via the
        // verdict.
        const failed: WorkflowEvent = {
          kind: "StepFailed",
          seq: after.lastSeq + 1,
          at: env.clock().toISOString(),
          stepId: step.id,
          attempt,
          error: { message },
          retriesExhausted: true,
        };
        after = await commit(env, runId, failed);
        void after;
        throw cause;
      }
      if (exhausted && step.onFailure !== undefined) {
        // Route the permanent failure to the handler instead of failing the
        // run. Prune the unit's normal dependents FIRST -- while the unit is
        // still in-flight -- then land the unit `routed` with a single
        // StepFailed{routedTo}. The returned sentinel becomes the unit's own
        // output in stepOutputs, so the handler reads
        // steps.<unit>.output.error.message (live-path only: no StepCompleted
        // for a routed unit).
        await pruneAroundRoute(
          definition,
          env,
          runId,
          step.id,
          step.onFailure,
          "routed",
          abort,
        );
        after = await reloadState(env, runId);
        // Cancellation wins over routing, as in the cancelling guard above.
        if (after.phase === "cancelling") {
          const propagated: WorkflowEvent = {
            kind: "CancelPropagated",
            seq: after.lastSeq + 1,
            at: env.clock().toISOString(),
            stepId: step.id,
          };
          after = await commit(env, runId, propagated);
          void after;
          throw cause;
        }
        const routed: WorkflowEvent = {
          kind: "StepFailed",
          seq: after.lastSeq + 1,
          at: env.clock().toISOString(),
          stepId: step.id,
          attempt,
          error: { message },
          retriesExhausted: true,
          routedTo: step.onFailure,
        };
        after = await commit(env, runId, routed);
        void after;
        return { failed: true, stepId: step.id, error: { message } };
      }
      const failed: WorkflowEvent = {
        kind: "StepFailed",
        seq: after.lastSeq + 1,
        at: env.clock().toISOString(),
        stepId: step.id,
        attempt,
        error: { message },
        retriesExhausted: exhausted,
      };
      after = await commit(env, runId, failed);
      if (exhausted) {
        throw cause;
      }
      // Schedule the next attempt: emit TimerSet then AttemptScheduled.
      const backoff = computeBackoff(step.retry, attempt);
      const timerId = env.newId("timer");
      const fireAtDate = new Date(env.clock().getTime() + backoff);
      const fireAt = fireAtDate.toISOString();
      const timerSet: WorkflowEvent = {
        kind: "TimerSet",
        seq: after.lastSeq + 1,
        at: env.clock().toISOString(),
        timerId,
        fireAt,
        stepId: step.id,
      };
      after = await commit(env, runId, timerSet);
      const nextAttempt = attempt + 1;
      const scheduled: WorkflowEvent = {
        kind: "AttemptScheduled",
        seq: after.lastSeq + 1,
        at: env.clock().toISOString(),
        stepId: step.id,
        nextAttempt,
        timerId,
        fireAt,
      };
      after = await commit(env, runId, scheduled);
      // Wait for the scheduler to commit TimerFired before looping into the
      // next attempt; the step stays awaiting-timer through the wait.
      await waitForTimer(
        env,
        runId,
        timerId,
        fireAtDate,
        abort,
        env.drain,
        step.id,
      );
      // Drain observation point #2: retry-between-attempts in runStep. If
      // drain has fired and the step's behavior is `"cancel"`, abort before
      // launching the next attempt. The outer `abort` already fires on drain
      // via `createStepAbort`; this explicit second site covers a drain
      // landing between waitForTimer settling and the next invokeStep.
      if (shouldAbortForDrain(env.drain, step.id)) {
        throw new Error("aborted: drain requested");
      }
      attempt = nextAttempt;
    } finally {
      abort.removeEventListener("abort", onOuter);
      if (timer !== undefined) clearTimeout(timer);
    }
  }
}

/**
 * Execute a deterministic effect node -- the action invocation boundary.
 * `StepStarted` is flushed durably before `env.invokeAction` runs
 * (at-most-once on a crash mid-invocation). No retry loop: an action is
 * single-attempt, so a thrown effect lands `StepFailed` through
 * `runPrimitiveSafe` like every other non-step runner. Cancellation: the
 * invoker refuses a pre-aborted signal at entry, so a handler is never
 * STARTED for a run already known to be cancelled; once started, stopping is
 * the handler's half.
 */
async function runAction(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: ActionPrimitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
): Promise<unknown> {
  const invokeAction = env.invokeAction;
  if (invokeAction === undefined) {
    throw new Error(
      `action ${primitive.id} requires an invokeAction on the env; this host does not support action primitives`,
    );
  }
  const rawInput =
    primitive.input !== undefined
      ? evaluate(primitive.input, selectorCtx)
      : null;
  const input = rawInput === undefined ? null : rawInput;
  // Inline like `runStep` (not the buffered `emitStepStartedWithValue` the
  // coordination runners share); single-attempt, so `attempt` is 1.
  const { ref: inputRef } = await env.blobs.recordOutput(
    `${primitive.id}.input`,
    1,
    input,
  );
  let started = await reloadState(env, runId);
  const startedEvent: WorkflowEvent = {
    kind: "StepStarted",
    seq: started.lastSeq + 1,
    at: env.clock().toISOString(),
    stepId: primitive.id,
    attempt: 1,
    input: { ref: inputRef },
  };
  started = await commitDurable(env, runId, startedEvent);
  void started;

  // As in `runStep`: check the outer signal's level after the await above,
  // so an abort raised during the commit still reaches the handler.
  const actionAbort = new AbortController();
  const onOuter = (): void => {
    actionAbort.abort();
  };
  bridgeAbort(abort, onOuter);
  let timer: ReturnType<typeof setTimeout> | undefined;
  if (primitive.timeout !== undefined) {
    timer = setTimeout(() => {
      actionAbort.abort();
    }, primitive.timeout);
  }

  try {
    const result = await invokeAction({
      handler: primitive.handler,
      input,
      requires: primitive.effect?.requires ?? [],
      authzContext: { stepId: primitive.id, attempt: 1, runId },
      signal: actionAbort.signal,
    });
    try {
      if (primitive.onFailure !== undefined) {
        await pruneAroundRoute(
          definition,
          env,
          runId,
          primitive.id,
          primitive.onFailure,
          "completed",
          abort,
        );
      }
      await emitStepCompletedWithValue(env, runId, primitive.id, result.output);
    } catch (termCause) {
      throw new SuccessTerminalizationError(termCause);
    }
    return result.output;
  } finally {
    abort.removeEventListener("abort", onOuter);
    if (timer !== undefined) clearTimeout(timer);
  }
}

/**
 * Bounded rework loop. Each iteration is a separate child run of the body
 * against the shared store (via `env.spawnLoopIteration`), scoped
 * `<loopId>[<index>]` at the step level (mirroring `runMap`) with a path-safe
 * child run id `<runId>__<loopId>__<index>` (`loopBodyRunId`). The `while`
 * predicate decides whether to continue on each iteration's output; `carry`
 * threads the next iteration's input. On convergence (`while` false) the loop
 * routes to its normal `after`-dependents; on hitting `maxIterations` with
 * `while` still true it routes to `onExhausted` -- a gate-style
 * mutually-exclusive branch, so the not-taken side is pruned with skip
 * sentinels before the loop's own StepCompleted lands.
 */
async function runLoop(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: LoopPrimitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
  depth: number,
  maxChildSpawnDepth: number,
): Promise<unknown> {
  const spawnLoopIteration = env.spawnLoopIteration;
  const loopFns = env.loopFns;
  if (spawnLoopIteration === undefined || loopFns === undefined) {
    throw new Error(
      `loop ${primitive.id} requires spawnLoopIteration and loopFns on the env; this host does not support loops`,
    );
  }
  const bodyRef = inlineBodyRef(definition.id, primitive.id);
  const whileFn = loopFns(primitive.while);
  const carryFn = loopFns(primitive.carry);

  // Read the log once for cursor re-derivation and input reconstruction.
  // reloadState reflects durable + this-process buffer, so every "already
  // emitted?" check below sees everything committed so far; a re-emit is
  // refused by the state machine.
  const log = await env.repoStore.read(runId);
  let state = await reloadState(env, runId);

  // The loop container is in state.steps only on resume; a fresh run emits its
  // StepStarted, a resumed run must not (re-emit throws).
  if (!state.steps.has(primitive.id)) {
    await emitStepStartedWithValue(env, runId, primitive.id, {
      while: primitive.while,
      carry: primitive.carry,
      maxIterations: primitive.maxIterations,
    });
  }

  // Replay fully-done iterations (child terminal AND step completed) from the
  // log to re-derive the cursor, the threaded input, and whether the loop
  // already reached its outcome before the crash (the post-routing window).
  // while/carry are pure, so replaying them over recorded inputs and outputs
  // reproduces the pre-crash decisions.
  const iterationZeroInput =
    primitive.input !== undefined
      ? evaluate(primitive.input, selectorCtx)
      : null;
  let currentInput: unknown =
    iterationZeroInput === undefined ? null : iterationZeroInput;
  let iteration = 0;
  let terminated = false;
  let outcome: "converged" | "exhausted" = "exhausted";
  // The most recent iteration's output, boxed so a legitimately `undefined`
  // output stays distinguishable from "no iteration has run yet". The settle
  // path needs it: the loop breaks the moment `while` goes false, BEFORE
  // `carry` runs, so `currentInput` is the converging iteration's input and
  // this is its output.
  let lastIteration: { output: unknown } | undefined;
  while (isIterationDone(state, runId, primitive.id, iteration)) {
    const doneStepId = scopedStepId(primitive.id, iteration);
    const doneInput = await resolveIterationInput(env, log, doneStepId);
    const doneOutput = await resolveIterationOutput(env, log, doneStepId);
    lastIteration = { output: doneOutput };
    iteration += 1;
    if (!whileFn(doneOutput, doneInput)) {
      outcome = "converged";
      terminated = true;
      break;
    }
    if (iteration >= primitive.maxIterations) {
      outcome = "exhausted";
      terminated = true;
      break;
    }
    currentInput = carryFn(doneOutput, doneInput);
  }

  // Prefer the resume iteration's own recorded input (an in-flight iteration
  // whose StepStarted is durable) over the carry recomputation.
  if (!terminated) {
    const resumeInputRef = findStepInputRef(
      log,
      scopedStepId(primitive.id, iteration),
    );
    if (resumeInputRef !== undefined) {
      currentInput = await env.blobs.resolveRef(resumeInputRef);
    }
  }

  // A crash-recovered iteration whose body was parked or mid-relay needs a
  // re-link token on its FIRST drive; `planLoopResume` yields it (or
  // undefined when the forward drive can re-adopt from the child log).
  // Cleared after the recovered iteration so later ones spawn fresh.
  let occurrenceResume = terminated
    ? undefined
    : await planLoopResume(env, primitive, runId, state, log, iteration);

  let iterations = iteration;
  for (let i = iteration; !terminated && i < primitive.maxIterations; i += 1) {
    iterations = i + 1;
    const stepId = scopedStepId(primitive.id, i);
    const childRunId = loopBodyRunId(runId, primitive.id, i);

    state = await reloadState(env, runId);
    if (!state.steps.has(stepId)) {
      await emitStepStartedWithValue(env, runId, stepId, currentInput);
    }
    // Drive the iteration body through the suspendable-child seam: commits
    // ChildSpawned on the loop container step, spawns the body under the
    // inherited-env executor, proxies any body park up on the container, and
    // commits ChildCompleted. `occurrenceResume` is set only on the
    // recovered iteration of a crash resume; cleared after this drive.
    const { terminalStatus } = await driveSuspendableOccurrence(
      env,
      runId,
      primitive.id,
      {
        childRunId,
        bodyRef,
        input: currentInput,
        resume: occurrenceResume,
        spawnSuspendableChild: spawnLoopIteration,
        depth,
        maxChildSpawnDepth,
        abort,
      },
    );
    occurrenceResume = undefined;

    // The drive returns only the terminal status; hydrate the iteration's
    // step outputs from the child's durable log (the scoped StepCompleted
    // records them; while/carry read them).
    const output = await hydrateChildOutputs(env, childRunId);
    lastIteration = { output };

    const after = await reloadState(env, runId);
    if (after.steps.get(stepId)?.phase !== "completed") {
      await emitStepCompletedWithValue(env, runId, stepId, output);
    }
    await flush(env, runId);

    if (terminalStatus !== "completed") {
      // A failed or cancelled iteration is a real failure, not an exhaustion.
      // Throw so runPrimitiveSafe lands StepFailed (or CancelPropagated when
      // the run is cancelling) on the loop node. Throwing skips
      // routeLoopOutcome, so neither branch is pruned; both the normal
      // dependents and onExhausted then run before the run settles failed.
      // The mutually-exclusive routing holds only on the success path.
      throw new Error(
        `loop ${primitive.id} iteration ${String(i)} ended ${terminalStatus}`,
      );
    }

    if (!whileFn(output, currentInput)) {
      outcome = "converged";
      break;
    }
    if (i + 1 >= primitive.maxIterations) {
      outcome = "exhausted";
      break;
    }
    currentInput = carryFn(output, currentInput);
  }

  if (lastIteration === undefined) {
    // Unreachable through `loop()`, which rejects a non-positive
    // maxIterations, so every settle follows at least one iteration --
    // replayed or driven. Fail loud rather than publish a `final` the loop
    // never produced. Checked BEFORE the route so the throw cannot leave one
    // branch of the loop's dependents pruned.
    throw new Error(
      `loop ${primitive.id} settled ${outcome} without running an iteration`,
    );
  }
  await routeLoopOutcome(definition, env, runId, primitive, outcome, abort);
  const output = {
    outcome,
    iterations,
    carry: currentInput,
    final: lastIteration.output,
  };
  await emitStepCompletedWithValue(env, runId, primitive.id, output);
  return output;
}

/**
 * Re-link state for a crash-recovered occurrence whose body is parked. The
 * resume planner yields at most one of these -- an approval park or a
 * signal-relay park, never both -- so a single discriminated union encodes
 * the mutual exclusion the two former locals maintained by discipline.
 */
type SuspendableOccurrenceResume =
  | { kind: "approval"; corr: string; relay: boolean; decision?: unknown }
  | { kind: "signal-relay-reestablish"; name: string; awaitSeq: number }
  | {
      kind: "signal-relay-relay";
      name: string;
      payload: unknown;
      signalId: string;
    }
  // The body is parked on an author `awaitSignal` gate but the container
  // never emitted its relay await before the crash (in-flight, no durable
  // relay). The re-adopted body re-parks silently, so drive the container
  // relay FRESH over the recovered name instead of re-establishing an await
  // that does not exist.
  | { kind: "signal-relay-drive-fresh"; name: string };

/**
 * Drive one occurrence's suspendable-child body to terminal, durably: commit
 * `ChildSpawned`, spawn the body, re-link a crash-recovered park, proxy each
 * of the body's parks up on THIS run's own park machinery over
 * `containerStepId` (approval -> `parkOnSignal`/`resume`; author
 * `awaitSignal` -> signal-relay), then commit `ChildCompleted`. Returns only
 * the terminal status; a body's step outputs live in the child log, so a
 * caller that needs them hydrates them from there.
 *
 * The drive is agnostic to where an occurrence's input comes from: `input` is
 * supplied by the caller, and everything occurrence-divergent stays there too
 * (the terminal-is-final vs tolerate policy and the re-arm that produces the
 * next occurrence's input). `runOnTrigger` is the caller, one occurrence per
 * trigger event. `containerStepId` is both the proxy-park step and the
 * child's `parentStepId`.
 */
async function driveSuspendableOccurrence(
  env: WorkflowRuntimeEnv,
  runId: string,
  containerStepId: string,
  args: {
    childRunId: string;
    bodyRef: string;
    input: unknown;
    resume: SuspendableOccurrenceResume | undefined;
    spawnSuspendableChild: SpawnSuspendableChild;
    depth: number;
    maxChildSpawnDepth: number;
    abort: AbortSignal;
  },
): Promise<{ terminalStatus: "completed" | "failed" | "cancelled" }> {
  const {
    childRunId,
    bodyRef,
    input,
    resume,
    spawnSuspendableChild,
    depth,
    maxChildSpawnDepth,
    abort,
  } = args;

  let before = await reloadState(env, runId);
  if (!before.children.has(childRunId)) {
    const spawned: WorkflowEvent = {
      kind: "ChildSpawned",
      seq: before.lastSeq + 1,
      at: env.clock().toISOString(),
      stepId: containerStepId,
      childRunId,
      childDefinitionRef: bodyRef,
    };
    before = await commit(env, runId, spawned);
    void before;
  }
  // Flush the spawn record durable before the child runs so a resumed parent
  // log records the spawn ahead of any child-side work.
  await flush(env, runId);

  const child = await spawnSuspendableChild({
    definitionRef: bodyRef,
    childRunId,
    input,
    parentRunId: runId,
    parentStepId: containerStepId,
    signal: abort,
    depth,
    maxChildSpawnDepth,
    ...(resume !== undefined
      ? { resumeFromEvents: await env.repoStore.read(childRunId) }
      : {}),
  });

  let terminalStatus: "completed" | "failed" | "cancelled";
  // `pending` carries a body event already pulled by a signal-relay drive
  // below (its `next()` raced the signal), so the loop consumes it rather
  // than calling `next()` a second time and dropping it.
  let pending: Awaited<ReturnType<typeof child.next>> | undefined;

  if (resume?.kind === "approval") {
    // Re-link the parent to a body re-spawned from its log and parked on the
    // shared correlation. A re-park does not re-fire onPark, so the park is
    // not surfaced via next(); drive the resume directly from the recovered
    // correlation. A grant already delivered to the parent log is relayed
    // as-is; otherwise re-park the container and await the grant.
    let decision: unknown;
    if (resume.relay) {
      decision = resume.decision;
    } else {
      const parkRearm = await reloadState(env, runId);
      decision = await parkOnSignal(
        env,
        runId,
        {
          stepId: containerStepId,
          signalName: signalName(resume.corr),
          parkKind: "approval",
        },
        parkRearm,
        abort,
      );
    }
    await child.resume(resume.corr, decision);
  } else if (resume?.kind === "signal-relay-relay") {
    // A signal delivered before the crash but not relayed: deliver it (with
    // its original id, so the body's dedup makes it idempotent) to unblock
    // the body's gate; the loop then drives the body's next event.
    await child.deliverSignal(resume.name, resume.payload, resume.signalId);
  } else if (resume?.kind === "signal-relay-reestablish") {
    // The container's signal-relay await is durable; the re-spawned body
    // re-parks on the name silently, so re-drive the race from the recovered
    // await seq (no re-emit) and continue with the body event it yields.
    pending = await raceContainerSignalRelay(
      env,
      runId,
      containerStepId,
      child,
      resume.name,
      resume.awaitSeq,
      abort,
    );
  } else if (resume?.kind === "signal-relay-drive-fresh") {
    // The container never emitted its relay await before the crash, so there
    // is nothing to reestablish and the re-adopted body re-parks silently (no
    // onSignalPark). Drive the container relay FRESH over the recovered name
    // -- emit the relay await and race -- so the container ends up awaiting
    // the signal that will arrive.
    pending = await driveContainerSignalRelay(
      env,
      runId,
      containerStepId,
      child,
      resume.name,
      abort,
    );
  }

  for (;;) {
    const bodyEvent = pending ?? (await child.next());
    pending = undefined;
    if (bodyEvent.kind === "terminal") {
      terminalStatus = bodyEvent.terminalStatus;
      break;
    }
    if (bodyEvent.kind === "park") {
      // A body step parked on an approval. Proxy it up on the SAME
      // correlation via THIS run's own park machinery, so the whole
      // deployment-runId approval path (registerSuspension/hub/deliver) is
      // reused unchanged and the approver sees the body step's real snapshot;
      // then relay the granted decision back into the child so the body
      // continues.
      const parkRearm = await reloadState(env, runId);
      const decision = await parkOnSignal(
        env,
        runId,
        {
          stepId: containerStepId,
          signalName: signalName(bodyEvent.park.correlationId),
          parkKind: "approval",
          ...(bodyEvent.park.approvalSnapshot !== undefined
            ? { approvalSnapshot: bodyEvent.park.approvalSnapshot }
            : {}),
        },
        parkRearm,
        abort,
      );
      await child.resume(bodyEvent.park.correlationId, decision);
      continue;
    }
    // A body step parked on an author `awaitSignal` gate. Proxy it up as a
    // signal-relay await on THIS container run over the SAME author name and
    // relay the resolved signal back into the body. The drive returns the
    // body's next event (its `next()` was consumed in the race), so continue
    // the loop with it.
    pending = await driveContainerSignalRelay(
      env,
      runId,
      containerStepId,
      child,
      bodyEvent.name,
      abort,
    );
  }

  let after = await reloadState(env, runId);
  if (after.children.get(childRunId)?.terminalStatus === undefined) {
    const completed: WorkflowEvent = {
      kind: "ChildCompleted",
      seq: after.lastSeq + 1,
      at: env.clock().toISOString(),
      childRunId,
      terminalStatus,
      // Durably record whether a `failed` terminal is a parent-abort
      // teardown (an in-process body cannot self-cancel, so it settles
      // `failed` locally when the container aborts) rather than a genuine
      // body failure; `planOnTriggerResume` keys the section's end-vs-re-arm
      // decision on this.
      ...(terminalStatus === "failed" && abort.aborted
        ? { abortedTeardown: true }
        : {}),
    };
    after = await commit(env, runId, completed);
    void after;
  }
  await flush(env, runId);

  return { terminalStatus };
}

/**
 * Run a long-lived onTrigger section. The section services each occurrence of
 * its trigger as an EVENT: it spawns the body as a child run resolved by the
 * deployed `bodyRef`, awaits the body's terminal, then re-arms on a
 * snapshot-less input park to await the next occurrence. The container never
 * self-completes -- it settles only when a body run ends non-`completed`
 * (terminal-is-final) or the run is cancelled/aborted.
 *
 * Each event's body is a full sub-run under `runs/<sectionId>__<index>/`, so
 * per-event detail lives in its own log; the parent log carries only the
 * container `StepStarted`, a `ChildSpawned`/`ChildCompleted` pair per event,
 * and the input-park re-arm -- all existing event kinds, so the state machine
 * is untouched. A body that suspends on an approval park is serviced by
 * proxying the park up on the shared correlation via this run's own park
 * machinery. On crash-recovery the driver reconstructs its position from the
 * reduced state and durable log -- which event is current, whether its body
 * is parked mid-approval (and whether the grant already landed), or idle
 * between events -- and re-links the parked body rather than re-running from
 * event 0.
 */
async function runOnTrigger(
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: OnTriggerPrimitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
  depth: number,
  maxChildSpawnDepth: number,
): Promise<unknown> {
  if (!("ref" in primitive.body)) {
    throw new Error(
      `onTrigger ${primitive.id} reached the runtime with an inline body; ` +
        `the deploy step must materialize the body to a workflow-asset ref`,
    );
  }
  const bodyRef = primitive.body.ref;
  const spawnSuspendableChild = env.spawnSuspendableChild;
  if (spawnSuspendableChild === undefined) {
    throw new Error(
      `onTrigger ${primitive.id}: this host does not support onTrigger ` +
        `sections (spawnSuspendableChild is not wired)`,
    );
  }

  const initial = await reloadState(env, runId);
  let eventIndex: number;
  let currentInput: unknown;
  // Set only on a crash-recovered iteration whose body is parked mid-approval:
  // it re-links the parent to that body on the shared correlation before the
  // drive loop, then clears so every later iteration is a fresh spawn.
  let resumeApproval:
    | { corr: string; relay: boolean; decision?: unknown }
    | undefined;
  // The signal-relay sibling of `resumeApproval`: set on a crash-recovered
  // iteration whose body is parked mid-signal-relay. `reestablish` re-drives
  // the durable await's race; `relay` delivers a signal that landed but was
  // not relayed before the crash. Cleared after the recovered iteration is
  // re-linked.
  let resumeSignalRelay:
    | { kind: "reestablish"; name: string; awaitSeq: number }
    | { kind: "relay"; name: string; payload: unknown; signalId: string }
    | undefined;

  if (!initial.steps.has(primitive.id)) {
    await emitStepStartedWithValue(env, runId, primitive.id, {
      on: primitive.on,
      bodyRef,
    });
    // Event 0's input is the run's firing trigger payload; each later event's
    // input arrives on the input park below.
    eventIndex = 0;
    currentInput = evaluate({ from: "trigger.payload" }, selectorCtx);
  } else {
    // A durable container `StepStarted` means this section is being re-driven
    // after a crash. Reconstruct the drive position from the reduced state and
    // the log rather than re-running from event 0.
    const log = await env.repoStore.read(runId);
    const plan = await planOnTriggerResume(env, primitive, initial, log);
    switch (plan.kind) {
      case "fresh":
        eventIndex = 0;
        currentInput = evaluate({ from: "trigger.payload" }, selectorCtx);
        break;
      case "terminal-is-final":
        // The body already ended non-`completed` before the crash; end the
        // section the same way the steady-state loop does.
        throw new Error(
          `onTrigger ${primitive.id} body run ${primitive.id}__${String(plan.eventIndex)} ended ${plan.terminalStatus}`,
        );
      case "reestablish-approval":
        eventIndex = plan.eventIndex;
        currentInput = undefined;
        resumeApproval = { corr: plan.corr, relay: false };
        break;
      case "relay-grant":
        eventIndex = plan.eventIndex;
        currentInput = undefined;
        resumeApproval = {
          corr: plan.corr,
          relay: true,
          decision: plan.decision,
        };
        break;
      case "reestablish-signal-relay":
        eventIndex = plan.eventIndex;
        currentInput = undefined;
        resumeSignalRelay = {
          kind: "reestablish",
          name: plan.name,
          awaitSeq: plan.awaitSeq,
        };
        break;
      case "relay-signal-grant":
        eventIndex = plan.eventIndex;
        currentInput = undefined;
        resumeSignalRelay = {
          kind: "relay",
          name: plan.name,
          payload: plan.payload,
          signalId: plan.signalId,
        };
        break;
      case "advance-with-input":
        // The next event's trigger already arrived durably on the input
        // channel -- its SignalReceived consumed the re-arm, moving the
        // container off `awaiting-signal` -- but the body spawn had not yet
        // committed. Advance to that event with the delivered input WITHOUT
        // re-parking, so the trigger is not dropped.
        currentInput = plan.input;
        eventIndex = plan.eventIndex + 1;
        break;
      case "readopt-in-flight-body":
        // The body child is in flight with no container park (a bare sleep
        // parked entirely inside the child). Re-adopt this SAME event: re-spawn
        // the body from its own durable log and await its terminal.
        // `currentInput` is unused on a re-adopt (the body has a durable
        // RunStarted), and `resume` stays `undefined` -- the "re-adopt from
        // the child log" sentinel the loop body path also uses.
        eventIndex = plan.eventIndex;
        currentInput = undefined;
        break;
      case "reawait-input": {
        // The current event's body completed; the section is idle on its input
        // re-arm. Re-adopt the durable input park or mint a fresh one, await
        // the next event's input, then advance to the next event.
        const inputSignalName =
          plan.existingSignalName ?? signalName(env.newId("corr"));
        const rearm = await reloadState(env, runId);
        currentInput = await parkOnSignal(
          env,
          runId,
          {
            stepId: primitive.id,
            signalName: inputSignalName,
            parkKind: "input",
          },
          rearm,
          abort,
        );
        eventIndex = plan.eventIndex + 1;
        break;
      }
    }
  }

  while (true) {
    const childRunId = `${primitive.id}__${String(eventIndex)}`;
    let resume: SuspendableOccurrenceResume | undefined;
    if (resumeApproval !== undefined) {
      resume = {
        kind: "approval",
        corr: resumeApproval.corr,
        relay: resumeApproval.relay,
        ...(resumeApproval.decision !== undefined
          ? { decision: resumeApproval.decision }
          : {}),
      };
      resumeApproval = undefined;
    } else if (resumeSignalRelay !== undefined) {
      resume =
        resumeSignalRelay.kind === "relay"
          ? {
              kind: "signal-relay-relay",
              name: resumeSignalRelay.name,
              payload: resumeSignalRelay.payload,
              signalId: resumeSignalRelay.signalId,
            }
          : {
              kind: "signal-relay-reestablish",
              name: resumeSignalRelay.name,
              awaitSeq: resumeSignalRelay.awaitSeq,
            };
      resumeSignalRelay = undefined;
    }

    const { terminalStatus } = await driveSuspendableOccurrence(
      env,
      runId,
      primitive.id,
      {
        childRunId,
        bodyRef,
        input: currentInput,
        resume,
        spawnSuspendableChild,
        depth,
        maxChildSpawnDepth,
        abort,
      },
    );

    // Terminal-is-final unless the section tolerates a body failure. Two
    // cases always end the section, `tolerate` or not: a `cancelled` body,
    // and ANY body terminal reached while the container itself is aborting
    // (`abort` -- a drain or operator cancel tearing the section down). An
    // in-process body's parent-abort teardown surfaces as a `failed`
    // terminal, not `cancelled`, so the abort check is what ends a torn-down
    // `tolerate` section rather than re-arming into a park whose signal
    // never resolves. A `failed` body absent that abort ends the section
    // only under the default `end` policy; a tolerated failure re-arms.
    if (
      terminalStatus === "cancelled" ||
      (terminalStatus === "failed" &&
        (bodyFailurePolicyOf(primitive) !== "tolerate" || abort.aborted))
    ) {
      // Throwing lands the parent terminal via `runPrimitiveSafe`; the run
      // does not relaunch.
      throw new Error(
        `onTrigger ${primitive.id} body run ${childRunId} ended ` +
          `${terminalStatus}`,
      );
    }

    // Re-arm: park on a fresh input channel for the next event. The park is
    // snapshot-less (`kind: "input"`); the run's owner delivers the next
    // event's payload on this channel and `parkOnSignal` returns it.
    const correlationId = env.newId("corr");
    const rearm = await reloadState(env, runId);
    currentInput = await parkOnSignal(
      env,
      runId,
      {
        stepId: primitive.id,
        signalName: signalName(correlationId),
        parkKind: "input",
      },
      rearm,
      abort,
    );
    eventIndex += 1;
  }
}

/**
 * Drive one body `signal-relay` await: proxy the body's author `awaitSignal`
 * gate up as a signal-relay await on the container over the SAME name, then
 * relay the resolved signal back into the body. Returns the body's NEXT event
 * (its `next()` is consumed here) for the caller's loop to continue with.
 *
 * BUFFER-FIFO (operator ruling): a signal is RELAYED, never dropped. The
 * container's `SignalAwaited` emit runs the reducer's FIFO pairing: (a) it
 * pre-consumes a signal queued before it (container reduces to in-flight) --
 * bind it from the log's pairing and relay, no await; or (b) the container
 * is left awaiting -- race the signal's arrival against the body producing
 * its next event (a gate the body timed out itself) and against abort.
 * Signal-first relays it directly; body-first retires the now-stale relay
 * await (`SignalAwaitAbandoned`) unless a signal landed and was consumed
 * during the race, in which case it is relayed idempotently.
 */
async function driveContainerSignalRelay(
  env: WorkflowRuntimeEnv,
  runId: string,
  containerStepId: string,
  child: SuspendableChildHandle,
  name: string,
  abort: AbortSignal,
): Promise<Awaited<ReturnType<SuspendableChildHandle["next"]>>> {
  const state = await reloadState(env, runId);

  // Fail-loud guard on the one residual divergence from the reducer's
  // pairing: `boundSignalForContainerAwait` is container-scoped and faithful
  // ONLY while THIS container is the sole step awaiting `name`. If ANY other
  // step awaits the same author name -- another section's signal-relay proxy
  // OR a plain author `awaitSignal` gate (which reduces with no parkKind) --
  // the reducer consumes a delivery by `state.steps` Map-insertion order
  // across ALL steps, not this container's seq order, so the helper could
  // mis-bind a payload. Refuse loudly. The parkKind is NOT filtered: a
  // plain-gate sibling reduces to `"approval"` and would slip a
  // signal-relay-only check (parity with `hasForeignSameNameAwaiter`).
  for (const [otherStepId, otherStep] of state.steps) {
    if (
      otherStepId !== containerStepId &&
      otherStep.phase === "awaiting-signal" &&
      otherStep.awaitingSignal?.name === name
    ) {
      throw new Error(
        `onTrigger ${containerStepId} signal-relay: step ${otherStepId} is ` +
          `already awaiting the same signal name ${name}; two steps ` +
          `awaiting the same signal name concurrently is not supported`,
      );
    }
  }

  // Emit the container's signal-relay await over the body's author name; the
  // commit's reducer pass decides (a) vs (b): a signal queued before it
  // pre-consumes it (in-flight); otherwise the container is left awaiting.
  const awaited: WorkflowEvent = {
    kind: "SignalAwaited",
    seq: state.lastSeq + 1,
    at: env.clock().toISOString(),
    stepId: containerStepId,
    signalName: name,
    parkKind: "signal-relay",
  };
  const awaitSeq = awaited.seq;
  await commit(env, runId, awaited);
  await flush(env, runId);
  const afterEmit = await reloadState(env, runId);

  if (afterEmit.steps.get(containerStepId)?.phase === "in-flight") {
    // (a) PRE-CONSUME: the emit consumed a signal queued before it. Bind it
    // from the log's FIFO pairing and relay -- the body's gate is already
    // satisfiable, so there is nothing to await.
    const log = await env.repoStore.read(runId);
    const bound = boundSignalForContainerAwait(
      log,
      name,
      containerStepId,
      awaitSeq,
    );
    if (bound === undefined) {
      throw new Error(
        `onTrigger ${containerStepId} signal-relay: the emit pre-consumed a ` +
          `queued ${name} signal but no paired SignalReceived is in the log`,
      );
    }
    await child.deliverSignal(name, bound.payload, bound.signalId);
    return child.next();
  }

  // (b) The container is awaiting. If THIS container is itself a suspendable
  // child (its env carries an `onSignalPark` sink), surface the relay await up
  // to its parent so the parent relays a delivery down into this container's
  // owned channel -- the same way the body's leaf gate surfaced up to here.
  // Nesting composes one layer at a time until the run whose channel has a
  // real upstream. An unset sink means this run owns the outermost channel:
  // an addressable top-level run (caller delivers) or a terminal
  // `childWorkflow` child (nothing can address it; `parkOnSignalResult`
  // already refuses the body's untimed gate there). Fired on the fresh drive
  // only; the reestablish path re-drives from its own durable await.
  if (env.onSignalPark !== undefined) {
    env.onSignalPark({ runId, name });
  }

  // Race the signal's arrival against the body advancing on its own.
  return raceContainerSignalRelay(
    env,
    runId,
    containerStepId,
    child,
    name,
    awaitSeq,
    abort,
  );
}

/**
 * The awaiting arm of a container signal-relay: the container's
 * `SignalAwaited(name, "signal-relay")` at `awaitSeq` is durable and the body
 * is parked on `name`; race the signal's live arrival against the body
 * producing its next event (a gate the body timed out itself) and against
 * abort, then relay or retire. Shared by the fresh drive
 * ({@link driveContainerSignalRelay}, which just emitted the await) and by
 * crash-recovery (`reestablish-signal-relay`, whose re-spawned body re-parks
 * on `name` silently, so the await is re-driven from its durable seq without
 * a re-emit). Returns the body's NEXT event for the caller's loop.
 */
async function raceContainerSignalRelay(
  env: WorkflowRuntimeEnv,
  runId: string,
  containerStepId: string,
  child: SuspendableChildHandle,
  name: string,
  awaitSeq: number,
  abort: AbortSignal,
): Promise<Awaited<ReturnType<SuspendableChildHandle["next"]>>> {
  // `awaitNext` rejects on `raceAbort`; `next()` returns a terminal when the
  // body is cancelled, so abort is threaded through the awaitNext leg.
  const raceAbort = new AbortController();
  const onOuterAbort = (): void => {
    raceAbort.abort();
  };
  bridgeAbort(abort, onOuterAbort);
  const pSignal = env.signalChannel
    .awaitNext(name, raceAbort.signal)
    .then((r) => ({ tag: "signal" as const, r }));
  const pNext = child.next().then((ev) => ({ tag: "next" as const, ev }));
  let outcome:
    | { tag: "signal"; r: { payload: unknown; signalId: string } }
    | { tag: "next"; ev: Awaited<ReturnType<SuspendableChildHandle["next"]>> };
  try {
    outcome = await Promise.race([pSignal, pNext]);
  } catch (cause) {
    // awaitNext rejected: the outer abort fired (the run is tearing down).
    // Drain the still-pending next() so it does not leak, then propagate.
    raceAbort.abort();
    await pNext.catch(() => undefined);
    throw cause;
  } finally {
    abort.removeEventListener("abort", onOuterAbort);
  }

  if (outcome.tag === "signal") {
    // The awaited signal arrived live. Commit its SignalReceived -- mirroring
    // parkOnSignal's awaiter-commits contract, so the container log carries
    // it in every host (a production delivery that also committed it dedups
    // by signalId) -- then relay it into the body and take its next event.
    const before = await reloadState(env, runId);
    const received: WorkflowEvent = {
      kind: "SignalReceived",
      seq: before.lastSeq + 1,
      at: env.clock().toISOString(),
      signalName: name,
      signalId: outcome.r.signalId,
      payload: outcome.r.payload,
    };
    await commit(env, runId, received);
    await flush(env, runId);
    await child.deliverSignal(name, outcome.r.payload, outcome.r.signalId);
    return (await pNext).ev;
  }

  // The body produced its next event before the signal arrived: its gate
  // timed out and it moved on, so the container's relay await is stale. Stop
  // the outstanding awaitNext, then check whether a signal landed and was
  // consumed during the race.
  raceAbort.abort();
  await pSignal.catch(() => undefined);
  const afterRace = await reloadState(env, runId);
  const container = afterRace.steps.get(containerStepId);
  if (
    container?.phase === "awaiting-signal" &&
    container.awaitingSignal?.name === name
  ) {
    // No signal was consumed: retire the stale relay await so the reducer stops
    // treating the container as awaiting this name.
    const retire = await reloadState(env, runId);
    const abandoned: WorkflowEvent = {
      kind: "SignalAwaitAbandoned",
      seq: retire.lastSeq + 1,
      at: env.clock().toISOString(),
      stepId: containerStepId,
      signalName: name,
    };
    await commit(env, runId, abandoned);
    await flush(env, runId);
  } else {
    // A signal landed and was consumed during the race (its SignalReceived
    // reduced the container off awaiting-signal). Relay it idempotently: the
    // body has moved past its gate, but its run-lifetime dedup on the ORIGINAL
    // signalId absorbs a relay it no longer needs. Do NOT abandon.
    const log = await env.repoStore.read(runId);
    const bound = boundSignalForContainerAwait(
      log,
      name,
      containerStepId,
      awaitSeq,
    );
    if (bound !== undefined) {
      await child.deliverSignal(name, bound.payload, bound.signalId);
    }
  }
  return outcome.ev;
}

/**
 * Discriminated resume plan for a crash-recovered onTrigger container, derived
 * purely from the container's reduced `state` plus its durable `log`. The
 * `children` map locates the current event index and the container step's
 * phase locates its lifecycle point; the log recovers a
 * delivered-but-unrelayed approval grant, whose `SignalReceived` reduces the
 * container step to `in-flight` and strips its `awaitingSignal` (so the
 * correlation and decision are no longer in the reduced state).
 */
type OnTriggerResumePlan =
  | { kind: "fresh" }
  | { kind: "reestablish-approval"; eventIndex: number; corr: string }
  | { kind: "relay-grant"; eventIndex: number; corr: string; decision: unknown }
  | {
      kind: "reestablish-signal-relay";
      eventIndex: number;
      name: string;
      awaitSeq: number;
    }
  | {
      kind: "relay-signal-grant";
      eventIndex: number;
      name: string;
      payload: unknown;
      signalId: string;
    }
  | { kind: "reawait-input"; eventIndex: number; existingSignalName?: string }
  | { kind: "advance-with-input"; eventIndex: number; input: unknown }
  | { kind: "readopt-in-flight-body"; eventIndex: number }
  | {
      kind: "terminal-is-final";
      eventIndex: number;
      terminalStatus: "failed" | "cancelled";
    };

/**
 * The section's body-failure policy, defaulting an absent field to `"end"`
 * (terminal-is-final). Single source for the default so the steady-state drive
 * loop and the resume planner cannot drift.
 */
function bodyFailurePolicyOf(primitive: OnTriggerPrimitive): BodyFailurePolicy {
  return primitive.onBodyFailure ?? "end";
}

/**
 * The `awaitSignal` gate names a body child is parked on in its reduced
 * state, split by channel: `author` names are author-chosen
 * (`correlationIdFromSignalName` undefined); `controlPlane` names are
 * reserved approval/relay channels. Both signal a crash window where the
 * body's leaf `SignalAwaited` flushed but the container's proxy await did
 * not, so a naive re-adopt re-parks the body on a signal the container never
 * relays. The loop planner keys its author-gate drive-fresh on `author`; the
 * onTrigger planner refuses on either (no fresh-relay drive exists).
 */
function bodyParkedSignals(childState: RunState): {
  author: string[];
  controlPlane: string[];
} {
  const author: string[] = [];
  const controlPlane: string[] = [];
  for (const step of childState.steps.values()) {
    if (step.phase !== "awaiting-signal" || step.awaitingSignal === undefined) {
      continue;
    }
    const name = step.awaitingSignal.name;
    if (correlationIdFromSignalName(name) === undefined) {
      author.push(name);
    } else {
      controlPlane.push(name);
    }
  }
  return { author, controlPlane };
}

async function planOnTriggerResume(
  env: WorkflowRuntimeEnv,
  primitive: OnTriggerPrimitive,
  state: RunState,
  log: readonly WorkflowEvent[],
): Promise<OnTriggerResumePlan> {
  const prefix = `${primitive.id}__`;
  let eventIndex = -1;
  for (const childRunId of state.children.keys()) {
    if (!childRunId.startsWith(prefix)) continue;
    const parsed = Number.parseInt(childRunId.slice(prefix.length), 10);
    if (Number.isInteger(parsed) && parsed > eventIndex) eventIndex = parsed;
  }
  if (eventIndex === -1) {
    // The container `StepStarted` is durable but no body was ever spawned.
    return { kind: "fresh" };
  }
  const childRunId = `${prefix}${String(eventIndex)}`;
  const child = state.children.get(childRunId);
  if (child === undefined) {
    throw new Error(
      `onTrigger ${primitive.id} resume: event ${String(eventIndex)} has no child state`,
    );
  }
  // ORDERING IS LOAD-BEARING: the body-TERMINAL checks (terminal-is-final
  // for failed/cancelled, reawait-input for completed) MUST precede the
  // container-in-flight throw below. This is what lets the signal-relay
  // abandon path own no distinct resume arm: after a body's timed gate
  // abandons, the container drops to the ordinary in-flight driving state,
  // and a body that then completed is caught HERE (reawait-input), not by
  // the in-flight throw.
  //
  // Terminal-is-final unless the section tolerates a GENUINE body failure. A
  // cancelled body always ends; so does an abort-teardown `failed` body
  // (`abortedTeardown`, the durable record of the live `abort.aborted` guard
  // in the drive loop). A failed body absent that abort ends only under the
  // default `end` policy; a tolerated genuine failure falls through to the
  // completed block below, which re-adopts the SAME input park a completed
  // body does.
  if (
    child.terminalStatus === "cancelled" ||
    (child.terminalStatus === "failed" &&
      (bodyFailurePolicyOf(primitive) !== "tolerate" ||
        child.abortedTeardown === true))
  ) {
    return {
      kind: "terminal-is-final",
      eventIndex,
      terminalStatus: child.terminalStatus,
    };
  }
  const container = state.steps.get(primitive.id);
  if (
    child.terminalStatus === "completed" ||
    (child.terminalStatus === "failed" &&
      bodyFailurePolicyOf(primitive) === "tolerate")
  ) {
    // The event's body finished (completed, or failed under a `tolerate`
    // policy); the section is idle on its input re-arm. Re-adopt the durable
    // input park if it was committed, else re-arm fresh.
    if (
      container !== undefined &&
      container.phase === "awaiting-signal" &&
      container.awaitingSignal !== undefined &&
      controlParkKindOf(container.awaitingSignal) === "input"
    ) {
      return {
        kind: "reawait-input",
        eventIndex,
        existingSignalName: container.awaitingSignal.name,
      };
    }
    // The re-arm's next trigger may have been DELIVERED before the crash
    // spawned its body: the input `SignalReceived` moved the container to
    // in-flight (awaitingSignal stripped), so it is not caught above. Advance
    // to that event with the delivered payload rather than re-parking and
    // dropping it -- the input sibling of the delivered-approval/relay windows
    // on the body-in-flight side.
    const delivered = recoverDeliveredInput(primitive.id, log);
    if (delivered !== undefined) {
      return {
        kind: "advance-with-input",
        eventIndex,
        input: delivered.payload,
      };
    }
    return { kind: "reawait-input", eventIndex };
  }
  // The body was mid-flight at crash.
  if (container === undefined) {
    throw new Error(
      `onTrigger ${primitive.id} resume: body child ${childRunId} is in flight but the container step has no reduced state`,
    );
  }
  if (
    container.phase === "awaiting-signal" &&
    container.awaitingSignal !== undefined
  ) {
    const parkKind = controlParkKindOf(container.awaitingSignal);
    if (parkKind === "signal-relay") {
      // The container is proxy-parked on the body's author `awaitSignal` gate.
      // Recover the durable await's seq (the FIFO binding key) so the resume
      // re-drives the race over the same await without re-emitting it.
      const name = container.awaitingSignal.name;
      const recovered = lastSignalRelayAwait(primitive.id, log, name);
      if (recovered === undefined) {
        throw new Error(
          `onTrigger ${primitive.id} resume: container awaits signal-relay ${name} but no matching SignalAwaited is in the log`,
        );
      }
      return {
        kind: "reestablish-signal-relay",
        eventIndex,
        name,
        awaitSeq: recovered.seq,
      };
    }
    if (parkKind !== "approval") {
      throw new Error(
        `onTrigger ${primitive.id} resume: container is parked on an input channel while body child ${childRunId} is still in flight`,
      );
    }
    const corr = correlationIdFromSignalName(container.awaitingSignal.name);
    if (corr === undefined) {
      throw new Error(
        `onTrigger ${primitive.id} resume: container awaiting-signal ${container.awaitingSignal.name} is not a reserved control-plane channel`,
      );
    }
    return { kind: "reestablish-approval", eventIndex, corr };
  }
  // The container is not parked but the body is still in flight: the approval
  // grant was delivered (its SignalReceived moved the container to in-flight)
  // but not yet relayed into the child. Recover the correlation and decision
  // from the log.
  const grant = recoverDeliveredApprovalGrant(primitive.id, log);
  if (grant !== undefined) {
    return {
      kind: "relay-grant",
      eventIndex,
      corr: grant.corr,
      decision: grant.decision,
    };
  }
  // Signal-relay sibling of the delivered-grant window: a signal delivered to
  // the container's last signal-relay await consumed it (moving the container
  // to in-flight) but was not relayed into the body before the crash. Recover
  // it from the log's FIFO pairing and relay on resume.
  const relaySignal = recoverDeliveredSignalRelay(primitive.id, log);
  if (relaySignal !== undefined) {
    return {
      kind: "relay-signal-grant",
      eventIndex,
      name: relaySignal.name,
      payload: relaySignal.payload,
      signalId: relaySignal.signalId,
    };
  }
  // The body is in flight with nothing delivered to the container. Inspect
  // the body's OWN reduced state. A bare `sleep` parks entirely inside the
  // child and surfaces no container park, so re-adopt the in-flight body and
  // await its terminal: the re-spawned body re-drives its own durable log
  // and re-adopts its sleep timer via `isResumableSleepStep`.
  //
  // The shape we must NOT re-adopt is a body still parked on a signal the
  // container has not proxied -- author `awaitSignal` OR a reserved
  // approval/relay channel -- because its leaf `SignalAwaited` flushed but
  // the container's proxy await did not: re-adopting re-parks the body
  // silently (no `onPark`) on a signal the container never relays, hanging
  // both sides. onTrigger has no fresh-relay drive, so keep failing loud.
  // Any body NOT awaiting a signal re-adopts and its OWN resume classifier
  // decides its terminal: a `sleep` resumes; a mid-flight
  // `childWorkflow`/`map` REJECTS (loud section failure); a crashed agent
  // step SETTLES a terminal `StepFailed` -- under `tolerate` the section
  // absorbs the crashed body and re-arms.
  const childState = await reloadState(env, childRunId);
  const { author, controlPlane } = bodyParkedSignals(childState);
  const parkedSignals = [...author, ...controlPlane];
  if (parkedSignals.length > 0) {
    throw new Error(
      `onTrigger ${primitive.id} resume: body child ${childRunId} is parked ` +
        `on an un-relayed signal (${parkedSignals.join(", ")}) with no ` +
        `container proxy await; onTrigger cannot re-establish the relay fresh`,
    );
  }
  return { kind: "readopt-in-flight-body", eventIndex };
}

/**
 * The resume token for a crash-recovered loop iteration whose body was
 * parked or mid-relay at the crash, derived purely from the container's
 * reduced `state` plus its durable `log` for the given `iteration`. It
 * yields ONLY the active re-link the body needs on its next drive:
 * re-establish the container's signal-relay race, relay a grant/signal
 * delivered but not relayed before the crash, or re-adopt an approval park.
 *
 * `undefined` means "nothing to re-link": `runLoop`'s forward drive re-adopts
 * the iteration from the child's own durable log, and (unlike
 * `planOnTriggerResume`) there is no `fresh` or `terminal-is-final` arm --
 * `runLoop` owns the iteration cursor.
 */
async function planLoopResume(
  env: WorkflowRuntimeEnv,
  primitive: LoopPrimitive,
  runId: string,
  state: RunState,
  log: readonly WorkflowEvent[],
  iteration: number,
): Promise<SuspendableOccurrenceResume | undefined> {
  const childRunId = loopBodyRunId(runId, primitive.id, iteration);
  const child = state.children.get(childRunId);
  if (child === undefined || child.terminalStatus !== undefined) {
    return undefined;
  }
  const container = state.steps.get(primitive.id);
  if (container === undefined) {
    throw new Error(
      `loop ${primitive.id} resume: body child ${childRunId} is in flight but the container step has no reduced state`,
    );
  }
  if (
    container.phase === "awaiting-signal" &&
    container.awaitingSignal !== undefined
  ) {
    const parkKind = controlParkKindOf(container.awaitingSignal);
    if (parkKind === "signal-relay") {
      // As in `planOnTriggerResume`: re-drive the race over the durable
      // await's seq without re-emitting it.
      const name = container.awaitingSignal.name;
      const recovered = lastSignalRelayAwait(primitive.id, log, name);
      if (recovered === undefined) {
        throw new Error(
          `loop ${primitive.id} resume: container awaits signal-relay ${name} but no matching SignalAwaited is in the log`,
        );
      }
      return {
        kind: "signal-relay-reestablish",
        name,
        awaitSeq: recovered.seq,
      };
    }
    if (parkKind !== "approval") {
      throw new Error(
        `loop ${primitive.id} resume: container is parked on an input channel while body child ${childRunId} is still in flight`,
      );
    }
    const corr = correlationIdFromSignalName(container.awaitingSignal.name);
    if (corr === undefined) {
      throw new Error(
        `loop ${primitive.id} resume: container awaiting-signal ${container.awaitingSignal.name} is not a reserved control-plane channel`,
      );
    }
    return { kind: "approval", corr, relay: false };
  }
  // The container is not parked but the body is still in flight: a grant or
  // signal was DELIVERED (its SignalReceived moved the container to in-flight)
  // but not relayed into the body before the crash. Relay it into the
  // re-adopted body, else the body's silently re-parked gate would wait
  // forever for a signal already consumed.
  const grant = recoverDeliveredApprovalGrant(primitive.id, log);
  if (grant !== undefined) {
    return {
      kind: "approval",
      corr: grant.corr,
      relay: true,
      ...(grant.decision !== undefined ? { decision: grant.decision } : {}),
    };
  }
  const relaySignal = recoverDeliveredSignalRelay(primitive.id, log);
  if (relaySignal !== undefined) {
    return {
      kind: "signal-relay-relay",
      name: relaySignal.name,
      payload: relaySignal.payload,
      signalId: relaySignal.signalId,
    };
  }
  // Otherwise the body is in flight with nothing delivered to the container.
  // On a consistent store the body's own durable log may show it parked on an
  // author `awaitSignal` gate the container never relayed -- the crash landed
  // after the body's leaf `SignalAwaited` flushed but before the container's
  // relay `SignalAwaited` flushed. The re-adopted body re-parks silently, so
  // the forward drive would never surface the gate and the container would
  // block forever. Recover the parked author name from the body's reduced
  // state and drive the container relay FRESH. (On an inconsistent store the
  // child log is gone, so the forward drive re-runs the iteration.)
  const childState = await reloadState(env, childRunId);
  const { author: authorAwaits } = bodyParkedSignals(childState);
  if (authorAwaits.length > 1) {
    throw new Error(
      `loop ${primitive.id} resume: body child ${childRunId} is parked on ` +
        `multiple concurrent author signals (${authorAwaits.join(", ")}); ` +
        `re-establishing more than one un-relayed gate is not supported`,
    );
  }
  const parkedName = authorAwaits[0];
  if (parkedName !== undefined) {
    return { kind: "signal-relay-drive-fresh", name: parkedName };
  }
  return undefined;
}

/**
 * Recover the correlation and delivered decision of the container's most
 * recent approval park from the durable log. Used only for the narrow crash
 * window where the grant's `SignalReceived` landed before the driver relayed
 * it into the body -- the container step reduces to `in-flight` with its
 * `awaitingSignal` stripped, so the correlation lives only in the log.
 */
function recoverDeliveredApprovalGrant(
  stepId: string,
  log: readonly WorkflowEvent[],
): { corr: string; decision: unknown } | undefined {
  let corr: string | undefined;
  let parkKind: ControlParkKind | undefined;
  for (const event of log) {
    if (event.kind === "SignalAwaited" && event.stepId === stepId) {
      const candidate = correlationIdFromSignalName(event.signalName);
      if (candidate !== undefined) {
        corr = candidate;
        parkKind = controlParkKindOf(event);
      }
    }
  }
  if (corr === undefined || parkKind !== "approval") return undefined;
  const reserved = signalName(corr);
  let decision: unknown;
  let found = false;
  for (const event of log) {
    if (event.kind === "SignalReceived" && event.signalName === reserved) {
      decision = event.payload;
      found = true;
    }
  }
  if (!found) return undefined;
  return { corr, decision };
}

/**
 * Recover an input re-arm whose next trigger was delivered but whose body
 * spawn had not yet committed at the crash -- the input sibling of
 * `recoverDeliveredApprovalGrant`. The delivery's `SignalReceived` moved the
 * container to `in-flight` and stripped its `awaitingSignal`, so the
 * delivered trigger survives only in the log. Return its payload so the
 * resume advances to the next event instead of re-parking and dropping it.
 *
 * The seq DISCRIMINATOR is load-bearing: the input re-arm await must be
 * NEWER than the highest `ChildSpawned` for this container. An input await
 * OLDER than that belongs to an already-spawned event, and advancing on its
 * already-consumed trigger would double-spawn the next event.
 */
function recoverDeliveredInput(
  stepId: string,
  log: readonly WorkflowEvent[],
): { payload: unknown } | undefined {
  let maxChildSpawnedSeq = -1;
  for (const event of log) {
    if (
      event.kind === "ChildSpawned" &&
      event.stepId === stepId &&
      event.seq > maxChildSpawnedSeq
    ) {
      maxChildSpawnedSeq = event.seq;
    }
  }
  let lastInputAwait: { name: string; seq: number } | undefined;
  for (const event of log) {
    if (
      event.kind === "SignalAwaited" &&
      event.stepId === stepId &&
      controlParkKindOf(event) === "input"
    ) {
      lastInputAwait = { name: event.signalName, seq: event.seq };
    }
  }
  if (lastInputAwait === undefined) return undefined;
  if (lastInputAwait.seq <= maxChildSpawnedSeq) return undefined;
  let payload: unknown;
  let found = false;
  for (const event of log) {
    if (
      event.kind === "SignalReceived" &&
      event.signalName === lastInputAwait.name
    ) {
      payload = event.payload;
      found = true;
    }
  }
  if (!found) return undefined;
  return { payload };
}

/**
 * The name + seq of the container's last `SignalAwaited(_, "signal-relay")` --
 * optionally filtered to `name` -- or undefined if it has none. The last such
 * await is the one a crash-recovery re-drives (`reestablish-signal-relay`) or
 * binds a delivered signal against (`recoverDeliveredSignalRelay`).
 */
function lastSignalRelayAwait(
  containerStepId: string,
  log: readonly WorkflowEvent[],
  name?: string,
): { name: string; seq: number } | undefined {
  let last: { name: string; seq: number } | undefined;
  for (const event of log) {
    if (
      event.kind === "SignalAwaited" &&
      event.stepId === containerStepId &&
      controlParkKindOf(event) === "signal-relay" &&
      (name === undefined || event.signalName === name)
    ) {
      last = { name: event.signalName, seq: event.seq };
    }
  }
  return last;
}

/**
 * Recover a signal delivered to the container's last signal-relay await but
 * not yet relayed into the body -- the signal-relay sibling of
 * `recoverDeliveredApprovalGrant`. The delivery's `SignalReceived` consumed
 * the container's await (moving it to `in-flight`), so the payload lives
 * only in the log; bind it by replaying the reducer's FIFO pairing. Returns
 * undefined when the container has no signal-relay await, or its last one
 * carries no paired signal (an await abandoned rather than consumed).
 */
function recoverDeliveredSignalRelay(
  containerStepId: string,
  log: readonly WorkflowEvent[],
): { name: string; payload: unknown; signalId: string } | undefined {
  const last = lastSignalRelayAwait(containerStepId, log);
  if (last === undefined) return undefined;
  const bound = boundSignalForContainerAwait(
    log,
    last.name,
    containerStepId,
    last.seq,
  );
  if (bound === undefined) return undefined;
  return { name: last.name, payload: bound.payload, signalId: bound.signalId };
}

/**
 * Bind the signal a container `signal-relay` await consumed, by REPLAYING the
 * reducer's FIFO signal pairing over the container's durable log. The reducer
 * owns the queue -- `handleSignalReceived` queues a signal with no awaiter and
 * `handleSignalAwaited` consumes the queue head -- so recovering WHICH signal
 * paired with the await at `awaitSeq` means replaying that exact pairing, not
 * re-heuristicking it (a newest-observed heuristic returns the NEWEST
 * `SignalReceived`, whereas the reducer consumes the OLDEST queued one, so
 * under multiple queued signals it binds the wrong payload).
 *
 * Two reducer behaviors the pairing depends on, both replayed here: dedup by
 * `signalId` (a redelivered `SignalReceived` is a reducer no-op yet still
 * lands in the log, so the replay skips an already-seen id) and retiring an
 * abandoned awaiter (`SignalAwaitAbandoned` drops the waiter, so the replay
 * pops it to match).
 *
 * PRECONDITION: `log` is the COMPLETE parent-run log from seq 1; a windowed
 * suffix would start mid-stream and mis-pair, so a non-full log fails loud.
 *
 * ASSUMPTION: a UNIQUE container awaiter per name in the parent run, enforced
 * by `driveContainerSignalRelay`, which refuses a two-step topology.
 */
// Exported for direct unit tests: the reducer-FIFO-replay binding is the
// correctness crux of the signal-relay pass; its correction cases (dedup,
// FIFO-oldest, abandon-retire) are reachable only on the pre-consume/race-
// landed log-read paths, so they are proven against constructed logs.
export function boundSignalForContainerAwait(
  log: readonly WorkflowEvent[],
  name: string,
  containerStepId: string,
  awaitSeq: number,
): { payload: unknown; signalId: string } | undefined {
  if (log.length > 0 && log[0]?.seq !== 1) {
    throw new Error(
      `boundSignalForContainerAwait requires the full run log from seq 1; got ` +
        `a log starting at seq ${String(log[0]?.seq)} (a windowed suffix would ` +
        `mis-pair the reducer's FIFO)`,
    );
  }
  const queue: { payload: unknown; signalId: string }[] = [];
  const waiters: number[] = [];
  const pairing = new Map<number, { payload: unknown; signalId: string }>();
  const observed = new Set<string>();
  for (const event of log) {
    if (event.kind === "SignalReceived" && event.signalName === name) {
      if (observed.has(event.signalId)) continue;
      observed.add(event.signalId);
      const waiter = waiters.shift();
      if (waiter !== undefined) {
        pairing.set(waiter, {
          payload: event.payload,
          signalId: event.signalId,
        });
      } else {
        queue.push({ payload: event.payload, signalId: event.signalId });
      }
    } else if (
      event.kind === "SignalAwaited" &&
      event.signalName === name &&
      event.stepId === containerStepId
    ) {
      const head = queue.shift();
      if (head !== undefined) {
        pairing.set(event.seq, head);
      } else {
        waiters.push(event.seq);
      }
    } else if (
      event.kind === "SignalAwaitAbandoned" &&
      event.signalName === name &&
      event.stepId === containerStepId
    ) {
      waiters.pop();
    }
  }
  return pairing.get(awaitSeq);
}

function isIterationDone(
  state: RunState,
  runId: string,
  loopId: string,
  iteration: number,
): boolean {
  const child = state.children.get(loopBodyRunId(runId, loopId, iteration));
  const step = state.steps.get(scopedStepId(loopId, iteration));
  return child?.terminalStatus !== undefined && step?.phase === "completed";
}

function findStepInputRef(
  log: readonly WorkflowEvent[],
  stepId: string,
): string | undefined {
  for (const event of log) {
    if (event.kind === "StepStarted" && event.stepId === stepId) {
      return event.input.ref;
    }
  }
  return undefined;
}

async function resolveIterationInput(
  env: WorkflowRuntimeEnv,
  log: readonly WorkflowEvent[],
  stepId: string,
): Promise<unknown> {
  const ref = findStepInputRef(log, stepId);
  if (ref === undefined) {
    throw new Error(`loop resume: no StepStarted input for ${stepId}`);
  }
  return env.blobs.resolveRef(ref);
}

async function resolveIterationOutput(
  env: WorkflowRuntimeEnv,
  log: readonly WorkflowEvent[],
  stepId: string,
): Promise<unknown> {
  for (const event of log) {
    if (event.kind === "StepCompleted" && event.stepId === stepId) {
      return env.blobs.resolveRef(event.output.ref);
    }
  }
  throw new Error(`loop resume: no StepCompleted output for ${stepId}`);
}

/**
 * Resolve every `StepCompleted` output in a loop iteration's child log to a
 * value, keyed by the body step id. The suspendable-child drive returns only
 * a terminal status, so the loop rebuilds the iteration's step outputs from
 * its own durable child log (the scoped records `while`/`carry` consume).
 */
async function hydrateChildOutputs(
  env: WorkflowRuntimeEnv,
  childRunId: string,
): Promise<Record<string, unknown>> {
  const outputs: Record<string, unknown> = {};
  const log = await env.repoStore.read(childRunId);
  for (const event of log) {
    if (event.kind === "StepCompleted") {
      outputs[event.stepId] = await env.blobs.resolveRef(event.output.ref);
    }
  }
  return outputs;
}

/**
 * Prune the not-taken branch of a completed loop with skip sentinels, BEFORE
 * the loop's own StepCompleted lands, so the scheduler only ever hands back
 * the live side. Converged -> the normal `after`-dependents run and
 * `onExhausted` is pruned; exhausted -> the reverse. `onExhausted` names the
 * loop in its own `after` (enforced at definition time), so it is excluded
 * from the normal-dependent set here.
 */
async function routeLoopOutcome(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: LoopPrimitive,
  outcome: "converged" | "exhausted",
  abort: AbortSignal,
): Promise<void> {
  const normalDependents = Object.entries(definition.steps)
    .filter(
      ([id, p]) =>
        id !== primitive.onExhausted &&
        (p.after?.includes(primitive.id) ?? false),
    )
    .map(([id]) => id);
  const onExhausted = [primitive.onExhausted];
  const notSelected = outcome === "converged" ? onExhausted : normalDependents;
  const selected = outcome === "converged" ? normalDependents : onExhausted;

  const toSkip = collectBranchClosure(definition, notSelected, selected);
  const sentinel = { skipped: true, loopId: primitive.id, outcome };
  await emitSkipClosure(env, runId, definition, toSkip, sentinel, abort);
}

/**
 * Prune around an onFailure route, the mirror of `routeLoopOutcome`. A unit
 * carrying `onFailure` settles on both outcomes, so both prune the not-taken
 * side: a routed failure prunes the unit's normal after-dependents and spares
 * the handler branch; a success does the reverse. The caller runs this
 * BEFORE it commits the unit's terminal event, so the unit is still in-flight
 * while the skips land -- the scheduler offers none of its direct dependents
 * until it is terminal.
 */
async function pruneAroundRoute(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  unitId: string,
  handlerId: string,
  settled: "routed" | "completed",
  abort: AbortSignal,
): Promise<void> {
  const normalDependents = Object.entries(definition.steps)
    .filter(
      ([id, p]) => id !== handlerId && (p.after?.includes(unitId) ?? false),
    )
    .map(([id]) => id);
  const handler = [handlerId];
  const notSelected = settled === "routed" ? normalDependents : handler;
  const selected = settled === "routed" ? handler : normalDependents;
  const toSkip = collectBranchClosure(definition, notSelected, selected);
  const sentinel = { skipped: true, onFailureStepId: unitId, settled };
  await emitSkipClosure(env, runId, definition, toSkip, sentinel, abort);
}

/**
 * Event-sourced timer wait: tells the scheduler to commit
 * `TimerFired{timerId}` at `fireAt`, then subscribes to the run's log tail
 * and resolves on the matching `TimerFired`.
 *
 * The scheduler is the single writer of `TimerFired`; the runtime body never
 * commits it itself. Disposing the scheduler entry on abort cancels the
 * pending `TimerFired` commit so a stale one does not land after the awaiter
 * has settled on a sibling event.
 *
 * The replay base is `state.lastSeq + 1`: the scheduler may commit
 * `TimerFired` before the subscriber's `for await` reaches the first
 * iteration, so the subscription starts from the seq immediately after the
 * caller's last-observed event rather than from `"head"`.
 */
async function waitForTimer(
  env: WorkflowRuntimeEnv,
  runId: string,
  timerId: string,
  fireAt: Date,
  abort: AbortSignal,
  drain: import("./drain").DrainController,
  stepId: string,
): Promise<void> {
  // Segment boundary: the run parks here, tailing the durable log for the
  // scheduler-committed `TimerFired`. Flush the buffered segment (the
  // `TimerSet` -- and, on the retry path, the preceding
  // `StepFailed`/`AttemptScheduled`) BEFORE subscribing, so the
  // out-of-process scheduler can tail the durable `TimerSet` and a
  // crash-while-waiting leaves a resumable pre-suspension log.
  await flush(env, runId);
  const subscribeFromSeq = (await reloadState(env, runId)).lastSeq + 1;
  const ac = new AbortController();
  const onOuterAbort = (): void => {
    ac.abort();
  };
  if (abort.aborted) {
    throw new Error("aborted");
  }
  // Drain observation point #3: waitForTimer entry. If drain is already
  // aborted and the step's behavior is `"cancel"`, bail immediately without
  // arming the subscription.
  if (shouldAbortForDrain(drain, stepId)) {
    throw new Error("aborted: drain requested");
  }
  abort.addEventListener("abort", onOuterAbort, { once: true });
  // Listen for drain transitions that land mid-wait. A drain that fires after
  // the subscription has armed must abort the local controller so the
  // `for await` ends cleanly.
  const onDrain = (): void => {
    if (shouldAbortForDrain(drain, stepId)) {
      ac.abort();
    }
  };
  drain.signal.addEventListener("abort", onDrain, { once: true });
  const dispose = env.scheduler.scheduleIn(runId, timerId, fireAt);
  try {
    for await (const { event } of env.repoStore.subscribe(runId, {
      signal: ac.signal,
      from: { seq: subscribeFromSeq },
    })) {
      if (event.kind === "TimerFired" && event.timerId === timerId) {
        return;
      }
    }
    if (abort.aborted) throw new Error("aborted");
    if (shouldAbortForDrain(drain, stepId)) {
      throw new Error("aborted: drain requested");
    }
    // The subscription ended without a matching TimerFired and the outer
    // abort did not fire. The only ways to get here are an explicit
    // consumer-side `return()` (we are the consumer; this does not happen) or
    // the substrate closing the stream unexpectedly. Either is a
    // substrate-level invariant violation.
    throw new Error(
      `waitForTimer ${timerId} on run ${runId}: subscription ended without matching TimerFired`,
    );
  } finally {
    dispose();
    abort.removeEventListener("abort", onOuterAbort);
    drain.signal.removeEventListener("abort", onDrain);
  }
}

function computeBackoff(
  retry: { initialBackoffMs: number; maxBackoffMs?: number } | undefined,
  attempt: number,
): number {
  if (!retry) return 0;
  const cap = retry.maxBackoffMs ?? Number.MAX_SAFE_INTEGER;
  return Math.min(retry.initialBackoffMs * 2 ** (attempt - 1), cap);
}

async function runMap(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: MapPrimitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
): Promise<unknown> {
  const over = evaluate(primitive.over, selectorCtx);
  if (!Array.isArray(over)) {
    throw new Error(`map.over for ${primitive.id} did not resolve to an array`);
  }
  await emitStepStartedWithValue(env, runId, primitive.id, over);
  // v1 runs the inner steps sequentially. A parallel fan-out would need
  // per-item commit serialization against the same run log beyond what the
  // existing commit chain offers, plus a parallelism bound on the env. The
  // spec does not commit to either semantic; sequential keeps the event log
  // readable and the runtime simple.
  const inner = primitive.step;
  const outputs: unknown[] = [];
  for (let i = 0; i < over.length; i += 1) {
    const item = over[i];
    const itemCtx: SelectorContext = {
      ...selectorCtx,
      trigger: { payload: item },
    };
    const scopedStep: StepPrimitive = {
      ...inner,
      id: scopedStepId(primitive.id, i),
      // The outer map's retry policy applies as the fan-out-level default when
      // the inner step does not declare its own; the spread below only fills
      // in from the map when the inner is missing one.
      ...(inner.retry === undefined && primitive.retry !== undefined
        ? { retry: primitive.retry }
        : {}),
    };
    const output = await runStep(
      definition,
      env,
      runId,
      scopedStep,
      itemCtx,
      abort,
    );
    outputs.push(output);
  }
  await emitStepCompletedWithValue(env, runId, primitive.id, outputs);
  return outputs;
}

async function runGate(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: GatePrimitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
): Promise<unknown> {
  const value = evaluate(primitive.when, selectorCtx);
  const selected = value ? primitive.then : primitive.else;
  const notSelected = value ? primitive.else : primitive.then;
  await emitStepStartedWithValue(env, runId, primitive.id, {
    when: value,
    then: primitive.then,
    else: primitive.else,
  });
  // Skip the not-selected branch's downstream closure BEFORE the gate's own
  // StepCompleted lands, so the scheduler treats it as resolved without
  // invoking its bodies. The sentinel names the gate and branch head so a
  // diamond-join reading both branches can branch on `skipped` without
  // ambiguity against a legitimate `null`.
  const toSkip = collectBranchClosure(definition, [notSelected], [selected]);
  const sentinel = { skipped: true, gateId: primitive.id, branch: notSelected };
  await emitSkipClosure(env, runId, definition, toSkip, sentinel, abort);
  const output = { branch: selected, value };
  await emitStepCompletedWithValue(env, runId, primitive.id, output);
  return output;
}

/**
 * Compute the steps to skip when the `notSelected` branch roots are
 * suppressed in favor of the `selected` roots: the transitive downstream
 * closure of the not-selected roots, MINUS any step also reachable from the
 * selected roots. A diamond-join step listing both a selected and a
 * not-selected root in its `after` stays live. Both sides are sets: a `gate`
 * calls this with singleton roots (`then`/`else`), a `loop` with
 * `onExhausted` against the loop's normal dependents.
 */
function collectBranchClosure(
  definition: WorkflowDefinition,
  notSelected: readonly string[],
  selected: readonly string[],
): readonly string[] {
  const selectedSet = new Set(selected);
  const reachableFromSelected = downstreamClosure(definition.steps, selected);
  const skip = new Set<string>();
  const queue: string[] = notSelected.filter((id) => id in definition.steps);
  while (queue.length > 0) {
    const id = queue.shift();
    if (id === undefined) break;
    if (skip.has(id)) continue;
    if (selectedSet.has(id)) continue;
    if (reachableFromSelected.has(id)) continue;
    skip.add(id);
    for (const [otherId, primitive] of Object.entries(definition.steps)) {
      const after = primitive.after;
      if (after === undefined) continue;
      if (
        after.includes(id) &&
        !skip.has(otherId) &&
        !selectedSet.has(otherId) &&
        !reachableFromSelected.has(otherId)
      ) {
        queue.push(otherId);
      }
    }
  }
  return [...skip];
}

/**
 * Order the members of a skip closure so a member is completed before any
 * member it `after`-depends on -- leaf-first over the induced subgraph (edges
 * = `after` restricted to closure members). `collectBranchClosure` returns
 * BFS-from-roots order, which is not topological across a diamond, so this
 * recomputes the order rather than reversing that output.
 */
function leafFirstOrder(
  definition: WorkflowDefinition,
  closure: readonly string[],
): readonly string[] {
  const members = new Set(closure);
  const visited = new Set<string>();
  const depsFirst: string[] = [];
  const visit = (node: string): void => {
    if (visited.has(node)) return;
    visited.add(node);
    for (const dep of definition.steps[node]?.after ?? []) {
      if (members.has(dep)) visit(dep);
    }
    depsFirst.push(node);
  };
  for (const node of closure) visit(node);
  // depsFirst puts a dependency before its dependents; reverse so a dependent
  // is completed first.
  return depsFirst.reverse();
}

/**
 * Emit the skip sentinels for a branch-prune closure, the shared body of
 * every route-to-handler prune (gate, loop, onFailure). Completes the closure
 * LEAF-FIRST: a skipped step is completed only after every skipped step that
 * depends on it. This closes a scheduling race -- while the container/unit is
 * in-flight the scheduler offers none of its direct dependents, but a skipped
 * INTERMEDIATE that completed before its skipped dependent was settled would
 * unblock that dependent, and the drive loop (woken by an unrelated sibling
 * settling) could schedule it. Leaf-first means a skipped step's dependents
 * are already terminal when it completes, so `areDepsResolved` never offers
 * it while the step is in-flight.
 *
 * A step already in the log is not re-started (idempotent replay); a step
 * left in-flight by a crash mid-prune is re-completed. The prune bails only
 * for a run that is no longer `running` (a cancelling/terminal run, whose
 * cancel sweep settles the closure via CancelPropagated); it MUST complete
 * for a drained-but-still-running run, since the caller commits the unit's
 * terminal right after and a half-pruned branch would leave the not-taken
 * side live.
 */
async function emitSkipClosure(
  env: WorkflowRuntimeEnv,
  runId: string,
  definition: WorkflowDefinition,
  toSkip: readonly string[],
  sentinel: unknown,
  abort: AbortSignal,
): Promise<void> {
  const snapshot = await reloadState(env, runId);
  // `abort` fires on drain too, so `abort.aborted` alone cannot decide; the
  // phase check is the discriminator (see the JSDoc's bail rule).
  if (abort.aborted && snapshot.phase !== "running") return;
  for (const skipId of leafFirstOrder(definition, toSkip)) {
    // No per-iteration abort bail: if the run turns cancelling mid-prune the
    // next StepStarted throws (the reducer requires `running`) and propagates
    // to the cancel sweep; a step already started still completes
    // (StepCompleted has no run-phase guard), so no skip is left half-emitted.
    const phase = snapshot.steps.get(skipId)?.phase;
    if (phase !== undefined && phase !== "in-flight") continue;
    if (phase === undefined) {
      await emitStepStartedWithValue(env, runId, skipId, sentinel);
    }
    await emitStepCompletedWithValue(env, runId, skipId, sentinel);
  }
}

/**
 * Symmetric to `emitStepCompletedWithValue`: route a primitive's semantic
 * input through the substrate so the committed `StepStarted.input.ref` is
 * round-trippable through `env.blobs.resolveRef`.
 */
async function emitStepStartedWithValue(
  env: WorkflowRuntimeEnv,
  runId: string,
  stepId: string,
  value: unknown,
): Promise<void> {
  const { ref } = await env.blobs.recordOutput(`${stepId}.input`, 1, value);
  await emitStepStarted(env, runId, stepId, ref);
}

async function emitStepStarted(
  env: WorkflowRuntimeEnv,
  runId: string,
  stepId: string,
  ref: string,
): Promise<void> {
  let state = await reloadState(env, runId);
  const started: WorkflowEvent = {
    kind: "StepStarted",
    seq: state.lastSeq + 1,
    at: env.clock().toISOString(),
    stepId,
    attempt: 1,
    input: { ref },
  };
  state = await commit(env, runId, started);
  void state;
}

async function emitStepCompleted(
  env: WorkflowRuntimeEnv,
  runId: string,
  stepId: string,
  ref: string,
): Promise<void> {
  let state = await reloadState(env, runId);
  const completed: WorkflowEvent = {
    kind: "StepCompleted",
    seq: state.lastSeq + 1,
    at: env.clock().toISOString(),
    stepId,
    attempt: 1,
    output: { ref },
  };
  state = await commit(env, runId, completed);
  void state;
}

/**
 * Commit a `StepCompleted` event whose output is a real value the runtime
 * materialized (`runMap`, `runGate`, `runChildWorkflow`, `runAwaitSignal`,
 * `runEscalation`). Routing through `env.blobs.recordOutput` lets resume
 * rehydrate the output via the standard substrate path -- without this,
 * downstream selectors targeting a non-`step` primitive's output crash on
 * resume because the hydration loop only resolves substrate-readable refs.
 */
async function emitStepCompletedWithValue(
  env: WorkflowRuntimeEnv,
  runId: string,
  stepId: string,
  value: unknown,
): Promise<void> {
  const { ref } = await env.blobs.recordOutput(stepId, 1, value);
  await emitStepCompleted(env, runId, stepId, ref);
}

type GateOutcome =
  | { timedOut: false; payload: unknown; signalId: string }
  | { timedOut: true };

/**
 * Reconstruct how a single admitted `awaitSignal` gate left
 * `awaiting-signal`, by replaying the reducer's signal FIFO over the full run
 * log and folding the gate's own `TimerFired` as a competing mover. Returns
 * whether a delivered signal moved the gate (with the bound payload and its
 * `signalId`), the gate's timer fired first, or `undefined` when the log
 * shows nothing moved it (a corrupt in-flight residual surfaced loudly).
 *
 * The caller guarantees, via {@link hasForeignSameNameAwaiter}, that
 * `selfStepId` is the SOLE awaiter of `signalName`, so the reducer's global
 * "first awaiting step for this name" scan can only ever resolve to this
 * gate; a per-gate replay therefore reproduces the reduction faithfully. It
 * mirrors the two reducer rules that decide the binding: a delivery arriving
 * with no awaiter present queues, and the gate's `SignalAwaited` drains the
 * queue HEAD (oldest-first), while a redelivered `signalId` is a dedup no-op.
 * When `selfTimerId` is given, a `TimerFired` for it moves the gate off
 * `awaiting-signal` exactly as `handleTimerFired` does, so whichever of the
 * delivered signal or the fired timer moves the gate first wins the race.
 *
 * NOT merged with {@link boundSignalForContainerAwait}: that binder honors
 * `SignalAwaitAbandoned`; this one folds a timer mover for a plain gate.
 */
export function reconstructGateOutcome(
  log: readonly WorkflowEvent[],
  signalName: string,
  selfStepId: string,
  selfTimerId?: string,
): GateOutcome | undefined {
  const queue: { payload: unknown; signalId: string }[] = [];
  const observed = new Set<string>();
  let awaiting = false;
  for (const event of log) {
    if (event.kind === "SignalReceived" && event.signalName === signalName) {
      if (observed.has(event.signalId)) continue;
      observed.add(event.signalId);
      if (awaiting) {
        return {
          timedOut: false,
          payload: event.payload,
          signalId: event.signalId,
        };
      }
      queue.push({ payload: event.payload, signalId: event.signalId });
    } else if (
      event.kind === "SignalAwaited" &&
      event.signalName === signalName &&
      event.stepId === selfStepId
    ) {
      const head = queue.shift();
      if (head !== undefined) {
        return {
          timedOut: false,
          payload: head.payload,
          signalId: head.signalId,
        };
      }
      awaiting = true;
    } else if (
      selfTimerId !== undefined &&
      event.kind === "TimerFired" &&
      event.timerId === selfTimerId &&
      awaiting
    ) {
      return { timedOut: true };
    }
  }
  return undefined;
}

/**
 * The timer id a timed `awaitSignal` gate armed, read from its durable
 * `TimerSet`. `reconstructGateOutcome` folds a `TimerFired` for this id as the
 * mover that competes with a delivered signal; a no-timeout gate has no
 * `TimerSet`, so this returns `undefined`.
 */
function gateTimerId(
  log: readonly WorkflowEvent[],
  stepId: string,
): string | undefined {
  for (const event of log) {
    if (event.kind === "TimerSet" && event.stepId === stepId) {
      return event.timerId;
    }
  }
  return undefined;
}

/**
 * Whether any `awaitSignal` gate OTHER than `selfStepId` awaited `signalName`
 * anywhere in the run. `reconstructGateOutcome` replays the reducer FIFO
 * scoped to a SINGLE awaiter of the name, so a second same-name awaiter --
 * even one that already COMPLETED -- breaks that assumption: the log can no
 * longer say which gate consumed which delivery. The in-flight short-circuit
 * refuses that topology rather than bind a payload to the wrong gate; the
 * predicate keys on the durable `SignalAwaited` marker, which outlives a
 * completed sibling (the case a phase-scoped in-flight count would miss).
 */
function hasForeignSameNameAwaiter(
  log: readonly WorkflowEvent[],
  signalName: string,
  selfStepId: string,
): boolean {
  for (const event of log) {
    if (
      event.kind === "SignalAwaited" &&
      event.signalName === signalName &&
      event.stepId !== selfStepId
    ) {
      return true;
    }
  }
  return false;
}

/**
 * Find an unfired pending timer bound to `stepId` in the reduced state. On a
 * re-park resume of an awaiting-signal step with a timeout, its original
 * `TimerSet` is still pending (a fired timeout would have moved the step to
 * `in-flight`); re-arming re-adopts it rather than minting a duplicate.
 */
function findUnfiredTimerForStep(
  state: RunState,
  stepId: string,
): { timerId: string; fireAt: string } | undefined {
  for (const pending of state.pendingTimers.values()) {
    if (pending.stepId === stepId) {
      return { timerId: pending.timerId, fireAt: pending.fireAt };
    }
  }
  return undefined;
}

/**
 * Recover the signal name a step is parked on from the reduced state. A step
 * in `awaiting-signal` carries its channel in `awaitingSignal.name`, reduced
 * from the step's durable `SignalAwaited`. On a crash-resume the agent
 * `step`-suspend arm re-enters `runStep` with only the definition's
 * `StepPrimitive`, which does NOT carry the runtime-minted
 * `signalName(correlationId)` channel (that name lives only on the log); this
 * recovers it so the resume can re-park on the same channel.
 */
function findAwaitedSignalNameForStep(
  state: RunState,
  stepId: string,
): string | undefined {
  const step = state.steps.get(stepId);
  if (step?.phase !== "awaiting-signal") return undefined;
  return step.awaitingSignal?.name;
}

/**
 * Signal-park core, shared by `runAwaitSignal` and the step-suspend arm in
 * `runStep`. Given a step already marked started, it emits the `SignalAwaited`
 * marker (unless the step is already `awaiting-signal` on a re-park resume),
 * arms the optional timeout timer, flushes the segment, parks on the signal
 * channel via `awaitNext`, and on a received signal commits `SignalReceived`
 * and returns the payload WITHOUT completing the step; the completion seam
 * and resume-idempotency pieces above the call stay with the caller.
 *
 * Returns a discriminated result rather than throwing on timeout: only the
 * caller knows whether a fired timer routes onward (`awaitSignal.onTimeout`)
 * or fails the step. A park with no `timeout` never yields `timedOut: true`.
 */
type ParkResult = { timedOut: false; payload: unknown } | { timedOut: true };

/**
 * Unwrap a park that the caller KNOWS cannot time out (every control-plane
 * park -- approval, input, signal-relay -- carries no `timeout`). A timeout
 * here is a wiring bug, so surface it loudly rather than mis-route a control
 * decision.
 */
function requireSignalPayload(result: ParkResult): unknown {
  if (result.timedOut) {
    throw new Error(
      "parkOnSignal: a control-plane park reported a timeout, but only a " +
        "timed awaitSignal gate sets a timeout",
    );
  }
  return result.payload;
}

async function parkOnSignalResult(
  env: WorkflowRuntimeEnv,
  runId: string,
  opts: {
    stepId: string;
    signalName: string;
    timeout?: number;
    approvalSnapshot?: ApprovalSnapshot;
    /**
     * The control-plane park kind for a reserved-channel suspend. `"approval"`
     * requires a snapshot and notifies the host; `"input"` is snapshot-less
     * and does not notify (the run's owner delivers the next input on this
     * channel). Absent for a plain `awaitSignal` gate on an author-chosen name
     * (not a control-plane suspension) and on a re-park resume, which re-adopts
     * the durable `SignalAwaited` without re-deriving the kind.
     */
    parkKind?: ControlParkKind;
  },
  state: ReturnType<typeof resumeFromLog>,
  abort: AbortSignal,
): Promise<ParkResult> {
  // An untimed park waits for a signal from outside this run, so it can only
  // be answered where something upstream can deliver one. A terminal child
  // has no address of its own and no container relaying decisions down to it,
  // so the wait would never end and an approval nobody can see holds the
  // whole tree (a nested body inherits the same answer). Refuse before
  // anything durable is written, so the run fails at the step that asked for
  // the impossible and no suspension is recorded. A timed gate is exempt --
  // its own timer resolves it in process.
  if (!env.hasUpstreamSignalResolver && opts.timeout === undefined) {
    throw new Error(
      `step ${opts.stepId} waits on ${opts.signalName} with no timeout, but ` +
        `nothing outside this run can deliver it. A childWorkflow child is ` +
        `run to its terminal rather than driven across parks, so no gate ` +
        `beneath that boundary can be answered, at any depth. Give the gate ` +
        `a timeout, or move it into a run the control plane can address`,
    );
  }
  // Re-emit `SignalAwaited` only when the gate is not already awaiting it. On
  // a re-park resume the gate is already `awaiting-signal` (StepStarted +
  // SignalAwaited durable), so this is skipped and the tail re-parks on the
  // signal channel for a signal that has not yet arrived.
  //
  // A fresh control-plane suspension also captures the host notify, deferred
  // until the flush below (see that site); captured only on the fresh-emit
  // branch so a re-park resume does not re-notify.
  let parkToNotify: WorkflowPark | undefined;
  // Author `awaitSignal` gate (non-reserved name) captured on the fresh park
  // so the suspendable-child seam's `onSignalPark` sink fires after the flush,
  // mirroring the `parkToNotify` deferral. Set only on the author branch.
  let signalParkToNotify: string | undefined;
  if (state.steps.get(opts.stepId)?.phase !== "awaiting-signal") {
    const awaited: WorkflowEvent = {
      kind: "SignalAwaited",
      seq: state.lastSeq + 1,
      at: env.clock().toISOString(),
      stepId: opts.stepId,
      signalName: opts.signalName,
      ...(opts.timeout !== undefined
        ? {
            timeoutAt: new Date(
              env.clock().getTime() + opts.timeout,
            ).toISOString(),
          }
        : {}),
      // Record the park kind so recovery can distinguish an input park from
      // an approval one after a crash/reconnect (see parked-correlations).
      ...(opts.parkKind !== undefined ? { parkKind: opts.parkKind } : {}),
    };
    state = await commit(env, runId, awaited);
    // Only a park on a reserved `signalName(correlationId)` channel (the
    // agent-step suspend arm) carries a correlation the resolver routes a
    // decision back on; a plain `awaitSignal` gate parked on an author-chosen
    // name is not a control-plane suspension, so no notify fires.
    const correlationId = correlationIdFromSignalName(opts.signalName);
    if (correlationId !== undefined) {
      if (opts.parkKind === "input") {
        // An input park carries no snapshot and does NOT notify the hub. The
        // supervisor (not the hub) owns the next input delivery, so the child
        // forwards the correlationId upstream so the supervisor can cache it
        // for signal delivery.
        parkToNotify = {
          runId,
          correlationId,
          parkKind: "input",
        };
      } else {
        // A reserved control-plane channel is an APPROVAL park (a legacy
        // reserved-channel park with no recorded kind is an approval by
        // construction and falls here too).
        //
        // An approval park REQUIRES its snapshot (the sidecar->hub co-write
        // treats it as mandatory). This layer owns the park, so it enforces
        // the invariant: a correlated suspend that carries no snapshot fails
        // loud here rather than mislabelling a snapshot-less park and
        // crashing the co-write downstream. Failing before the flush is
        // correct: nothing has been transmitted yet.
        if (opts.approvalSnapshot === undefined) {
          throw new Error(
            `control-plane approval park for ${correlationId} carries no ` +
              `approval snapshot; a snapshot-less correlated suspend (e.g. a ` +
              `director caps.suspend) is not a supported approval`,
          );
        }
        parkToNotify = {
          runId,
          correlationId,
          parkKind: "approval",
          approvalSnapshot: opts.approvalSnapshot,
        };
      }
    } else {
      // A plain author `awaitSignal` gate. Not a control-plane suspension the
      // hub registers, but a suspendable-child body surfaces it up so the
      // section proxies + relays it; capture it to fire after the flush.
      signalParkToNotify = opts.signalName;
    }
  }
  // The per-step timeout commits TimerSet before asking the scheduler to
  // fire, so the pairing with the scheduler-committed `TimerFired` is
  // explicit in the log: without it, a scheduler re-arming unfired timers at
  // startup could not see signal-await timeouts -- the deadline would be
  // silently lost across a crash.
  let timerId: string | undefined;
  let fireAtDate: Date | undefined;
  let subscribeFromSeq: number | undefined;
  if (opts.timeout !== undefined) {
    let beforeTimer = await reloadState(env, runId);
    // On a re-park resume the durable log already carries this step's TimerSet
    // with its original id in `pendingTimers` (unfired -- a fired timeout
    // would have moved the step to `in-flight` and been refused). Re-adopt
    // it rather than minting a second one: a duplicate TimerSet would
    // double-count the deadline and leave two scheduler entries racing to
    // fire.
    const existing = findUnfiredTimerForStep(beforeTimer, opts.stepId);
    if (existing !== undefined) {
      timerId = existing.timerId;
      fireAtDate = new Date(existing.fireAt);
      subscribeFromSeq = beforeTimer.lastSeq + 1;
    } else {
      timerId = env.newId("timer");
      fireAtDate = new Date(env.clock().getTime() + opts.timeout);
      const timerSet: WorkflowEvent = {
        kind: "TimerSet",
        seq: beforeTimer.lastSeq + 1,
        at: env.clock().toISOString(),
        timerId,
        fireAt: fireAtDate.toISOString(),
        stepId: opts.stepId,
      };
      beforeTimer = await commit(env, runId, timerSet);
      subscribeFromSeq = beforeTimer.lastSeq + 1;
    }
  }
  void state;

  // Segment boundary: the run is about to park on the signal channel (and,
  // when a timeout is set, tail the durable log for the scheduler-committed
  // `TimerFired`). Flush the buffered `SignalAwaited` (+ `TimerSet`) BEFORE
  // parking so (a) the out-of-process scheduler can tail the durable
  // `TimerSet` and arm the timeout, (b) a crash-while-suspended leaves a
  // complete pre-suspension log, and (c) the control-plane suspension is
  // durable before the host is notified below. `subscribeFromSeq` was
  // computed from the in-memory tip; the flush makes the durable tip match,
  // so the timer-watch subscription starts exactly past the flushed markers.
  await flush(env, runId);

  // Notify the host of the fresh control-plane suspension only now that it is
  // durable in the log. Transmitting the correlationId after the flush closes
  // the crash-across-park boundary: a crash before this point leaves no hub
  // row and no `SignalAwaited`, so resume re-parks cleanly; a crash after it
  // leaves both, and the re-emit from durable state reconciles idempotently.
  if (parkToNotify !== undefined) {
    env.onPark?.(parkToNotify);
  }
  // The author-signal sibling notify: a suspendable-child body's `awaitSignal`
  // gate is surfaced up so the section can proxy it. Fired after the flush
  // like `onPark` so the `SignalAwaited` is durable before the section
  // observes it.
  if (signalParkToNotify !== undefined) {
    env.onSignalPark?.({ runId, name: signalParkToNotify });
  }

  // Drain observation point #4: signal-park entry. If drain has fired and the
  // step's behavior is `"cancel"` (an awaitSignal whose author opted in to
  // cancel-on-drain), abort immediately. `awaitSignal` defaults to `"wait"`
  // so the typical human-in-the-loop pause sits through drain untouched.
  if (shouldAbortForDrain(env.drain, opts.stepId)) {
    throw new Error("aborted: drain requested");
  }
  const combinedAbort = new AbortController();
  const onOuterAbort = (): void => {
    combinedAbort.abort();
  };
  // The durable flush above is an await, so the outer signal may already have
  // aborted by the time this bridge is built. An abort is an edge, not a
  // level: a listener attached after the fact never fires, and the park below
  // would then wait on a signal nothing will send.
  //
  // Abort the combined controller rather than throwing, as `waitForTimer`
  // does from the same position: `awaitNext` consults its pre-delivery queue
  // before the abort signal, so a signal that arrived before the park is
  // still consumed as durable progress; throwing would discard it.
  bridgeAbort(abort, onOuterAbort);
  // Listen for drain transitions that land mid-await.
  const onDrain = (): void => {
    if (shouldAbortForDrain(env.drain, opts.stepId)) {
      combinedAbort.abort();
    }
  };
  env.drain.signal.addEventListener("abort", onDrain, { once: true });
  let timerDispose: (() => void) | undefined;
  let timerFired = false;
  let timerWaitAbort: AbortController | undefined;
  let timerWatch: Promise<void> | undefined;
  if (
    opts.timeout !== undefined &&
    timerId !== undefined &&
    fireAtDate !== undefined &&
    subscribeFromSeq !== undefined
  ) {
    timerDispose = env.scheduler.scheduleIn(runId, timerId, fireAtDate);
    timerWaitAbort = new AbortController();
    const watchedTimerId = timerId;
    const watchedFromSeq = subscribeFromSeq;
    const watchAbort = timerWaitAbort;
    timerWatch = (async (): Promise<void> => {
      for await (const { event } of env.repoStore.subscribe(runId, {
        signal: watchAbort.signal,
        from: { seq: watchedFromSeq },
      })) {
        if (event.kind === "TimerFired" && event.timerId === watchedTimerId) {
          timerFired = true;
          combinedAbort.abort();
          return;
        }
      }
    })();
  }
  try {
    const received = await env.signalChannel.awaitNext(
      opts.signalName,
      combinedAbort.signal,
    );
    let next = await reloadState(env, runId);
    const signalReceived: WorkflowEvent = {
      kind: "SignalReceived",
      seq: next.lastSeq + 1,
      at: env.clock().toISOString(),
      signalName: opts.signalName,
      signalId: received.signalId,
      payload: received.payload,
    };
    next = await commit(env, runId, signalReceived);
    void next;
    return { timedOut: false, payload: received.payload };
  } catch (cause) {
    // Distinguish timeout from outer cancellation: a timeout that fires while
    // the run is still `running` is not a failure here -- it is a routing
    // decision the caller owns (route onward via onTimeout, or fail the
    // step), so return the discriminated timeout rather than throwing. The
    // scheduler already committed TimerFired when `timerFired` was set; the
    // runtime body MUST NOT commit a second (single-writer invariant).
    if (timerFired) {
      return { timedOut: true };
    }
    throw cause;
  } finally {
    abort.removeEventListener("abort", onOuterAbort);
    env.drain.signal.removeEventListener("abort", onDrain);
    if (timerDispose !== undefined) timerDispose();
    if (timerWaitAbort !== undefined) timerWaitAbort.abort();
    if (timerWatch !== undefined) {
      await timerWatch.catch(() => undefined);
    }
  }
}

/**
 * Park on a signal that CANNOT time out and return the delivered payload
 * directly. Every control-plane park (approval, input, signal-relay) carries
 * no `timeout`, so it never yields the discriminated timeout; a timeout here
 * is a wiring bug that {@link requireSignalPayload} surfaces loudly. Only the
 * `awaitSignal` gate calls `parkOnSignalResult` directly.
 */
async function parkOnSignal(
  env: WorkflowRuntimeEnv,
  runId: string,
  opts: Parameters<typeof parkOnSignalResult>[2],
  state: ReturnType<typeof resumeFromLog>,
  abort: AbortSignal,
): Promise<unknown> {
  return requireSignalPayload(
    await parkOnSignalResult(env, runId, opts, state, abort),
  );
}

async function runAwaitSignal(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: AwaitSignalPrimitive,
  abort: AbortSignal,
): Promise<unknown> {
  // Read the log once for resume idempotency. On a run re-driving the durable
  // log, the gate's `StepStarted`/`SignalAwaited`/`TimerSet` are already
  // committed; re-emitting any of them throws in the state machine, so each
  // marker below is emitted only when absent (mirroring runLoop).
  let state = await reloadState(env, runId);
  const resumed = state.steps.has(primitive.id);

  // Short-circuit resume: an `awaitSignal` step found `in-flight` means a
  // mover already took it off `awaiting-signal` -- a `SignalReceived` (or a
  // pre-await queued signal consumed by `SignalAwaited`), or, for a timed
  // gate, a `TimerFired`. The step only lacks its `StepCompleted` (or, on
  // timeout, its routing/failure) -- the crash-after-move-before-StepCompleted
  // window (`isResumableReceivedAwaitSignalStep`). The reduced `StepState`
  // records neither which mover won nor the payload, so reconstruct both from
  // the durable log and complete without parking.
  if (resumed && state.steps.get(primitive.id)?.phase === "in-flight") {
    const log = await env.repoStore.read(runId);
    // The replay below binds by signal name, faithful only while this gate is
    // the sole awaiter (see `hasForeignSameNameAwaiter`).
    if (hasForeignSameNameAwaiter(log, primitive.name, primitive.id)) {
      throw new RuntimeResumeUnsupportedError(
        primitive.id,
        "in-flight",
        `another awaitSignal gate for ${primitive.name} awaited the signal on a different step, so the consumed signal cannot be unambiguously bound to step ${primitive.id}`,
      );
    }
    // Reconstruct which mover took the gate off `awaiting-signal`: a
    // delivered signal (recovering its payload) or, for a timed gate, its own
    // `TimerFired` (the gate's durable `TimerSet`; a no-timeout gate has none
    // to fold). Fail loud if the log shows nothing moved the gate.
    const selfTimerId =
      primitive.timeout !== undefined
        ? gateTimerId(log, primitive.id)
        : undefined;
    const outcome = reconstructGateOutcome(
      log,
      primitive.name,
      primitive.id,
      selfTimerId,
    );
    if (outcome === undefined) {
      throw new Error(
        `runAwaitSignal resume: step ${primitive.id} is in-flight but the log shows no mover (signal or timeout) for ${primitive.name}`,
      );
    }
    return completeAwaitSignalOutcome(
      definition,
      env,
      runId,
      primitive,
      outcome,
      abort,
    );
  }

  if (!resumed) {
    await emitStepStartedWithValue(env, runId, primitive.id, {
      name: primitive.name,
      ...(primitive.timeout !== undefined
        ? { timeout: primitive.timeout }
        : {}),
      ...(primitive.onTimeout !== undefined
        ? { onTimeout: primitive.onTimeout }
        : {}),
      ...(primitive.drainBehavior !== undefined
        ? { drainBehavior: primitive.drainBehavior }
        : {}),
    });
    state = await reloadState(env, runId);
  }
  // The SignalAwaited emit, timeout plumbing, flush, and awaitNext/resolve
  // block live in the shared `parkOnSignal` core. The two resume idempotency
  // pieces ABOVE this call -- the in-flight-received short-circuit and the
  // `StepStarted` emit -- stay here because runAwaitSignal owns its gate's
  // `StepStarted` (the step-suspend arm emits its own via runStep) and
  // recovers the crash-window payload by binding it to an awaitSignal gate by
  // name. The completion seam is owned here too: the gate completes with the
  // raw delivered payload, whereas the step-suspend arm re-invokes and
  // completes with a reply.
  const result = await parkOnSignalResult(
    env,
    runId,
    {
      stepId: primitive.id,
      signalName: primitive.name,
      ...(primitive.timeout !== undefined
        ? { timeout: primitive.timeout }
        : {}),
    },
    state,
    abort,
  );

  return completeAwaitSignalOutcome(
    definition,
    env,
    runId,
    primitive,
    result,
    abort,
  );
}

/**
 * Settle an `awaitSignal` gate from its resolved {@link ParkResult}.
 */
async function completeAwaitSignalOutcome(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: AwaitSignalPrimitive,
  result: ParkResult,
  abort: AbortSignal,
): Promise<unknown> {
  // No onTimeout: preserve the prior behavior -- a delivered signal completes
  // the gate with its payload; a fired timer fails the step.
  if (primitive.onTimeout === undefined) {
    if (result.timedOut) {
      throw new Error(
        `signal-await on ${primitive.name} timed out after ${String(primitive.timeout)}ms`,
      );
    }
    await emitStepCompletedWithValue(env, runId, primitive.id, result.payload);
    return result.payload;
  }

  // onTimeout set: route conditionally via the gate mechanism (prune the
  // not-taken branch with skip-sentinels, complete the gate, let the taken
  // branch schedule off its `after`), exactly as `routeLoopOutcome` does for
  // a loop's onExhausted. A fired timer routes to the onTimeout target and
  // prunes the normal successors; a delivered signal takes the normal
  // successors and prunes the onTimeout branch. The gate completes either
  // way.
  const onTimeoutTarget = primitive.onTimeout;
  const normalDependents = Object.entries(definition.steps)
    .filter(
      ([id, p]) =>
        id !== onTimeoutTarget && (p.after?.includes(primitive.id) ?? false),
    )
    .map(([id]) => id);
  const notSelected = result.timedOut ? normalDependents : [onTimeoutTarget];
  const selected = result.timedOut ? [onTimeoutTarget] : normalDependents;
  const toSkip = collectBranchClosure(definition, notSelected, selected);
  const skipState = await reloadState(env, runId);
  for (const skipId of toSkip) {
    if (abort.aborted) break;
    // A resumed routing pass skips a sentinel already committed (mirrors
    // routeLoopOutcome); a timed gate crashed mid-routing is otherwise the
    // pre-existing RuntimeResumeUnsupportedError window, inherited unchanged.
    if (skipState.steps.has(skipId)) continue;
    const sentinel = {
      skipped: true,
      gateId: primitive.id,
      timedOut: result.timedOut,
    };
    await emitStepStartedWithValue(env, runId, skipId, sentinel);
    await emitStepCompletedWithValue(env, runId, skipId, sentinel);
  }
  const output = result.timedOut ? { timedOut: true } : result.payload;
  await emitStepCompletedWithValue(env, runId, primitive.id, output);
  return output;
}

async function runSleep(
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: SleepPrimitive,
  abort: AbortSignal,
): Promise<unknown> {
  // Read the log once for resume idempotency. A fresh sleep has no state
  // entry and mints its `StepStarted`/`TimerSet`; a re-driving run re-adopts
  // the durable timer instead (mirroring runAwaitSignal).
  let state = await reloadState(env, runId);
  const resumed = state.steps.has(primitive.id);

  // Short-circuit resume: a `sleep` found `in-flight` means its `TimerFired`
  // already landed and only its `StepCompleted` is missing -- the
  // crash-after-TimerFired-before-StepCompleted window. `TimerFired` is the
  // sole mover off `awaiting-timer` for a sleep (no signal, no payload), and
  // `StepStarted`+`TimerSet` flush together at `waitForTimer`, so a durable
  // in-flight sleep always carries a fired timer. Complete with `null`
  // without re-parking.
  if (resumed && state.steps.get(primitive.id)?.phase === "in-flight") {
    // handleTimerFired clears the fired timer from `pendingTimers`; a
    // lingering pending timer on an in-flight sleep would mean the reducer
    // contract broke.
    if (findUnfiredTimerForStep(state, primitive.id) !== undefined) {
      throw new Error(
        `runSleep resume: step ${primitive.id} is in-flight but still has a pending timer; a fired sleep timer must be cleared from pendingTimers`,
      );
    }
    await emitStepCompletedWithValue(env, runId, primitive.id, null);
    return null;
  }

  let timerId: string;
  let fireAtDate: Date;
  if (resumed) {
    // Re-park resume (phase `awaiting-timer`): re-adopt the durable unfired
    // `TimerSet` (the parkOnSignal re-adopt rule) -- re-minting from a
    // recomputed delay would restart the clock and discard the elapsed sleep.
    const existing = findUnfiredTimerForStep(state, primitive.id);
    if (existing === undefined) {
      throw new Error(
        `runSleep resume: step ${primitive.id} is awaiting-timer but has no pending timer to re-adopt`,
      );
    }
    timerId = existing.timerId;
    fireAtDate = new Date(existing.fireAt);
  } else {
    const delay =
      primitive.duration ?? computeDelayToUntil(primitive.until, env);
    await emitStepStartedWithValue(env, runId, primitive.id, {
      ...(primitive.duration !== undefined
        ? { duration: primitive.duration }
        : {}),
      ...(primitive.until !== undefined ? { until: primitive.until } : {}),
      ...(primitive.drainBehavior !== undefined
        ? { drainBehavior: primitive.drainBehavior }
        : {}),
    });
    state = await reloadState(env, runId);
    timerId = env.newId("timer");
    fireAtDate = new Date(env.clock().getTime() + delay);
    const timerSet: WorkflowEvent = {
      kind: "TimerSet",
      seq: state.lastSeq + 1,
      at: env.clock().toISOString(),
      timerId,
      fireAt: fireAtDate.toISOString(),
      stepId: primitive.id,
    };
    state = await commit(env, runId, timerSet);
  }
  void state;
  await waitForTimer(
    env,
    runId,
    timerId,
    fireAtDate,
    abort,
    env.drain,
    primitive.id,
  );
  await emitStepCompletedWithValue(env, runId, primitive.id, null);
  return null;
}

function computeDelayToUntil(
  until: string | undefined,
  env: WorkflowRuntimeEnv,
): number {
  if (until === undefined) {
    throw new Error("sleep requires either `duration` or `until`");
  }
  const fireAt = new Date(until).getTime();
  const now = env.clock().getTime();
  return Math.max(0, fireAt - now);
}

async function runChildWorkflow(
  parent: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  parentRunId: string,
  primitive: ChildWorkflowPrimitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
  depth: number,
  maxChildSpawnDepth: number,
): Promise<unknown> {
  // Bound the spawn chain BEFORE committing StepStarted/ChildSpawned. A
  // reject here lands a clean StepFailed on this spawn step (runPrimitiveSafe
  // synthesizes it) and never writes a phantom child-run log. The child runs
  // one rung deeper; the ceiling is tree-wide (threaded from this run).
  const childDepth = depth + 1;
  assertSpawnDepthWithinLimit(childDepth, primitive.id, maxChildSpawnDepth);
  // Post-extraction the child definition is the internal `{ ref }` handle:
  // the deploy step lifts the authored inline child to a standalone
  // definition and the host resolves it from an in-memory closure map keyed
  // by this ref. An inline child reaching the runtime is a deploy-step bug --
  // the same contract `runOnTrigger` enforces on its body.
  if (!("ref" in primitive.definition)) {
    throw new Error(
      `childWorkflow ${primitive.id} reached the runtime with an inline ` +
        `definition; the deploy step must lift the child to an internal ref`,
    );
  }
  const definitionRef = primitive.definition.ref;
  const childInput =
    primitive.input !== undefined
      ? evaluate(primitive.input, selectorCtx)
      : null;
  // Allocate the child run-id locally and commit StepStarted + ChildSpawned
  // *before* invoking the spawn callback so the parent audit log records the
  // spawn ahead of any child-side work: a crash between the spawn-launch and
  // the post-await commit would otherwise leave the parent log with no record
  // the child was spawned, and a concurrent cancel sweep iterating
  // state.children would not find the child to cascade against.
  const childRunId = env.newId("run");
  await emitStepStartedWithValue(env, parentRunId, primitive.id, {
    definitionRef,
    input: childInput,
    ...(primitive.drainBehavior !== undefined
      ? { drainBehavior: primitive.drainBehavior }
      : {}),
  });
  let state = await reloadState(env, parentRunId);
  const spawned: WorkflowEvent = {
    kind: "ChildSpawned",
    seq: state.lastSeq + 1,
    at: env.clock().toISOString(),
    stepId: primitive.id,
    childRunId,
    childDefinitionRef: definitionRef,
  };
  state = await commit(env, parentRunId, spawned);
  // Segment boundary: the parent is about to hand off to and AWAIT a sub-run
  // (which commits its own events to the same workflow-run repo while this
  // await blocks). Flush the parent's buffered pre-spawn events BEFORE the
  // child runs, so the audit log records the spawn ahead of any child-side
  // work and a concurrent cancel sweep finds the child to cascade against --
  // the same invariant the ChildSpawned-before-spawn ordering upholds.
  await flush(env, parentRunId);
  // Wrap the spawn callback so a throw still lands a closing ChildCompleted
  // "failed" for the orphan: without it, ChildSpawned would persist with
  // `terminalStatus: undefined` and a future resume would treat the child as
  // live to cascade cancellation to. Rethrows so runPrimitiveSafe lands
  // StepFailed on the parent's spawn step.
  let child: { terminalStatus: "completed" | "failed" | "cancelled" };
  try {
    child = await env.spawnChild({
      definitionRef,
      childRunId,
      input: childInput,
      parentRunId,
      parentStepId: primitive.id,
      signal: abort,
      depth: childDepth,
      maxChildSpawnDepth,
    });
  } catch (cause) {
    let afterThrow = await reloadState(env, parentRunId);
    const childFailed: WorkflowEvent = {
      kind: "ChildCompleted",
      seq: afterThrow.lastSeq + 1,
      at: env.clock().toISOString(),
      childRunId,
      terminalStatus: "failed",
    };
    afterThrow = await commit(env, parentRunId, childFailed);
    void afterThrow;
    throw cause;
  }
  state = await reloadState(env, parentRunId);
  const childCompleted: WorkflowEvent = {
    kind: "ChildCompleted",
    seq: state.lastSeq + 1,
    at: env.clock().toISOString(),
    childRunId,
    terminalStatus: child.terminalStatus,
  };
  state = await commit(env, parentRunId, childCompleted);
  void state;
  if (child.terminalStatus !== "completed") {
    // A child run that ended `failed` or `cancelled` propagates to the parent
    // step as a failure. The runtime is the layer with enough information to
    // know the child did not succeed; pushing the decision to a downstream
    // gate makes the gating mandatory and silent-if-forgotten.
    // runPrimitiveSafe's catch lands the StepFailed when the throw bubbles
    // out of this runner.
    throw new ChildWorkflowFailedError(
      `child run ${childRunId} (${definitionRef}) ended ${child.terminalStatus}`,
      child.terminalStatus,
    );
  }
  const output = { childRunId, terminalStatus: child.terminalStatus };
  // Wrap the success terminalization so a durable-store failure while pruning
  // the handler branch or committing StepCompleted is distinguishable from a
  // child failure in runPrimitiveSafe -- the child already completed, so it
  // must land a bare failure, not route or retry.
  try {
    if (primitive.onFailure !== undefined) {
      await pruneAroundRoute(
        parent,
        env,
        parentRunId,
        primitive.id,
        primitive.onFailure,
        "completed",
        abort,
      );
    }
    await emitStepCompletedWithValue(env, parentRunId, primitive.id, output);
  } catch (termCause) {
    throw new SuccessTerminalizationError(termCause);
  }
  return output;
}

/**
 * Sentinel error type the `childWorkflow` primitive throws when the spawned
 * child run ends in a non-success terminal phase. The runtime body's
 * safe-runner catches it and commits `StepFailed` on the parent's spawn step;
 * downstream parent steps then see the parent step as failed rather than
 * `completed` with a hidden `terminalStatus` payload.
 */
class ChildWorkflowFailedError extends Error {
  readonly childTerminalStatus: "failed" | "cancelled";
  constructor(message: string, childTerminalStatus: "failed" | "cancelled") {
    super(message);
    this.name = "ChildWorkflowFailedError";
    this.childTerminalStatus = childTerminalStatus;
  }
}

// A unit's work succeeded but landing its terminal -- pruning the handler
// branch or committing StepCompleted -- threw (e.g. a transient durable-store
// failure). Distinct from an invocation failure so the runner catches land a
// bare failure rather than routing to the onFailure handler or retrying: the
// work is already done, so a route would invert a success into a fired
// handler and a retry would re-invoke it.
class SuccessTerminalizationError extends Error {
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = "SuccessTerminalizationError";
  }
}

async function runEscalation(
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: EscalationPrimitive,
  selectorCtx: SelectorContext,
): Promise<unknown> {
  const payload =
    primitive.data !== undefined ? evaluate(primitive.data, selectorCtx) : null;
  await emitStepStartedWithValue(env, runId, primitive.id, {
    to: primitive.to,
    data: payload,
  });
  const output = { escalatedTo: primitive.to, payload };
  await emitStepCompletedWithValue(env, runId, primitive.id, output);
  return output;
}
