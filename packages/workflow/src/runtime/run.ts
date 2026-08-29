// The single runtime body.
//
// `runtimeRun` is the only entry point both `runLocal` and the
// future child-process entry point invoke. The body switches on env
// keys -- never on which host process it is running in. An explicit
// source-level test in `run.test.ts` asserts that this file contains
// no environment-shaped discriminator references; the discipline lets
// us swap the env implementations underneath without re-validating the
// body.

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
  hashDefinition,
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
  SpawnSuspendableChild,
  SuspendableChildHandle,
  WorkflowPark,
  WorkflowRun,
  WorkflowRuntimeEnv,
} from "./env";
import { shouldAbortForDrain } from "./drain";
import {
  assertSpawnDepthWithinLimit,
  resolveMaxChildSpawnDepth,
} from "./child-depth";
import { RuntimeResumeUnsupportedError } from "./errors";
import { scopedStepId } from "./step-scope";
import { inlineBodyRef } from "../ontrigger-bodies";
import {
  controlParkKindOf,
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
   * Pre-existing event log to resume from. The runtime re-applies the
   * log via `resumeFromLog` and continues from the resulting state.
   *
   * Resume is supported for seed logs that are either
   * complete-or-cancelled or aligned on step boundaries -- every
   * non-terminal entry in `state.steps` must be schedulable by the
   * DAG's `nextSchedulable` set. The resumable carve-outs on top of the
   * step-boundary base are: an in-flight `loop` container (runLoop
   * re-derives its cursor), an `awaitSignal` step still `awaiting-signal`
   * (runAwaitSignal re-parks on the signal channel for a signal delivered
   * later), and an `awaitSignal` step left `in-flight` by a mover -- a
   * received signal or, for a timed gate, a fired timeout -- which
   * runAwaitSignal reconstructs from the log to complete (or, on timeout,
   * route or fail) without parking (the crash-after-move-before-StepCompleted
   * window). A seed log whose tail leaves an invocation-boundary step -- an
   * agent `step` or a deterministic `action` -- `in-flight` is a crash
   * mid-invocation: the runtime settles it as a terminal `StepFailed`
   * (at-most-once refusal) instead of re-invoking it. A step left
   * `awaiting-timer`, mid-`map`, or otherwise `in-flight` (a
   * `childWorkflow`) stays unsupported and surfaces as
   * `RuntimeResumeUnsupportedError`.
   *
   * When omitted, the runtime reduces canonical state from the durable
   * log for `runId`: an empty log starts fresh and emits `RunStarted`;
   * a non-terminal log is adopted and driven to terminal -- the same
   * recovery the seed path performs, for a seedless recovery call; an
   * already-terminal log is returned as-is without
   * re-driving.
   */
  resumeFromEvents?: readonly WorkflowEvent[];
  /**
   * This run's nesting depth in the childWorkflow spawn chain. The
   * top-level triggered run is depth 0; a `childWorkflow` child is one
   * deeper than the run that spawned it. Carried on the spawn seam, not in
   * the durable log, so a resumed non-root child that re-enters at depth 0
   * does not re-check depth -- resume of an in-flight `childWorkflow` is
   * `RuntimeResumeUnsupportedError` anyway. Default 0.
   */
  depth?: number;
  /**
   * Ceiling on child spawn depth for this run tree, resolved once at the
   * `runtimeRun` edge (see `resolveMaxChildSpawnDepth`). An injected value
   * can only lower the ceiling below `MAX_CHILD_SPAWN_DEPTH`, never raise
   * it. Threaded to each spawned child so the whole tree shares one
   * ceiling. Default `MAX_CHILD_SPAWN_DEPTH`.
   */
  maxChildSpawnDepth?: number;
}

/**
 * Run a workflow against a `WorkflowRuntimeEnv`. The function is the
 * single runtime body invoked by both `runLocal` and the (future)
 * child-process entry point.
 *
 * Resume contract: recovery runs against canonical state, whether it
 * arrived as an `options.resumeFromEvents` seed or was reduced from a
 * durable log this call adopts (a seedless recovery call). A supplied seed log
 * must satisfy two constraints:
 *
 *   1. The env's `BlobSubstrate` either holds the blob: refs the seed
 *      log references, or is durable enough that the refs are
 *      resolvable by the same substrate that minted them. The
 *      `runLocal` in-memory substrate is `ephemeral` and starts empty
 *      per instance; resume against a fresh one with a seed log that
 *      contains blob: refs fails fast with a targeted error.
 *   2. The seed log is either complete-or-cancelled, aligned on step
 *      boundaries, or left in one of the resumable carve-outs: an
 *      in-flight `loop` container, or an `awaitSignal` step (still
 *      `awaiting-signal`, or `in-flight` from a received signal or a
 *      fired timeout). An invocation-boundary step -- an agent `step`
 *      or a deterministic `action` -- left `in-flight` is a crash
 *      mid-invocation and settles as a terminal `StepFailed` rather
 *      than re-invoking. A step left `awaiting-timer`, mid-`map`, or
 *      otherwise `in-flight` (a `childWorkflow`) is unsupported: the
 *      runtime body has no surface for re-arming the timer scheduler
 *      entry or the inner-map iteration state from the log alone, so it
 *      surfaces as `RuntimeResumeUnsupportedError` and the host
 *      (supervisor) owns the recovery decision.
 */
export function runtimeRun(
  definition: WorkflowDefinition,
  env: WorkflowRuntimeEnv,
  options: RuntimeRunOptions = {},
): WorkflowRun {
  const runId = options.runId ?? env.newId("run");
  const cancelController = new AbortController();
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
      // Route through `commit` so cancel races against in-flight
      // primitive commits cannot collide on seq numbers. The
      // pre-lock check returns immediately when the run is already
      // terminal; the under-lock try/catch absorbs the narrow race
      // where the run terminates between the pre-lock read and the
      // commit so a late cancel surfaces as a no-op regardless of
      // which side of the lock the terminal transition landed on.
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
        // The control-plane cancel is out-of-band relative to the run
        // body's segment buffer; persist it immediately so the run
        // body's loop observes a durable `CancelRequested` and a crash
        // mid-cancel does not lose the request. `commitDurable` flushes
        // any pending run-body buffer first, keeping the durable tip
        // contiguous.
        await commitDurable(env, runId, event);
        // Emit ChildCancelRequested for any live children before the
        // abort listener fires. Without this here, the parent's main
        // loop might settle the spawn step (the child terminates
        // first via its own cancellation cascade) before the
        // ChildCancelRequested event for the *parent* log is
        // emitted, leaving the log without a record that the parent
        // ever asked the child to cancel. The cascade is idempotent
        // -- a second pass in the cancelling block or post-loop
        // skips children whose cancelRequested flag is set.
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
    // Always drop the per-runId commit chain entry, even on a thrown
    // body, so long-running processes accumulating many workflows do
    // not hold dead promise chains for runs that crashed during
    // resume seeding or a stall guard.
    dropChain(runId);
  }
}

// Intra-segment commit: validates the transition and assigns the seq
// in memory (unchanged from before batching) but DEFERS the durable
// write into the per-runId buffer. The buffer is flushed in one
// `appendBatch` at the next segment boundary -- a suspension (`flush`
// before the run parks) or completion (`commitDurable` on the terminal
// event). A synchronous single-step run buffers RunStarted ->
// StepStarted -> StepCompleted and flushes all of them together with
// the terminal RunCompleted in ONE commit.
function commit(
  env: WorkflowRuntimeEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<ReturnType<typeof resumeFromLog>> {
  return commitBufferedToChain(env, runId, event);
}

// Segment-boundary commit: buffers the event then flushes the whole
// pending buffer (this event LAST) in one durable `appendBatch`. Used
// for the terminal events (RunCompleted/RunFailed/RunCancelled) so the
// terminal is on disk -- and the supervisor's terminal-write sniff
// fires -- the moment the commit resolves, for the control-plane
// `cancel` whose `CancelRequested` must persist immediately, and for
// the agent-invoke barrier in `runStep` (the agent step's `StepStarted`
// is flushed durably before `env.invokeStep` runs, so a crash
// mid-invocation leaves a durable marker the recovery path settles as a
// terminal failure rather than re-invoking the non-idempotent agent).
// The terminal event being the last blob in the merge keeps the
// workflow-run kind handler's terminal-lock satisfied.
function commitDurable(
  env: WorkflowRuntimeEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<ReturnType<typeof resumeFromLog>> {
  return commitDurableToChain(env, runId, event);
}

// Flush the pending buffer to durable storage in one `appendBatch`.
// Called at a suspension boundary AFTER buffering the suspension
// marker (`SignalAwaited`/`TimerSet`) so the marker -- and everything
// before it in the segment -- is durable BEFORE the run parks. The
// out-of-process scheduler tails the durable `TimerSet`; resume after
// a crash-while-suspended reconstructs the awaiting state from the
// durable log. A buffered suspension marker that never flushed would
// be lost on a park, so this flush is load-bearing, not an
// optimisation knob.
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

  // Resolve the child-spawn depth guard once at this run edge: this run's
  // own depth (0 at the top level) and the tree-wide ceiling (clamped so an
  // injected override can only tighten it). Both thread down to the
  // childWorkflow spawn site, which checks the child's depth before it
  // commits anything and passes the incremented depth to the child run.
  const depth = options.depth ?? 0;
  const maxChildSpawnDepth = resolveMaxChildSpawnDepth(
    options.maxChildSpawnDepth,
  );

  // Restore prior events to the repo store on resume so a downstream
  // read sees the historical log alongside any newly-appended events.
  // The seeds carry their original seqs from the historical log and
  // must be written verbatim (the commit-lock path reassigns seq,
  // which would corrupt the replay invariant). Resume is therefore a
  // single-owner operation: the caller owns the runId and guarantees
  // no concurrent runtime is writing the same log during the seed
  // phase. Events the store already holds at the same seq are
  // idempotent seeds; conflicting kinds at the same seq throw rather
  // than being silently swallowed.
  const existing = await env.repoStore.read(runId);
  const existingBySeq = new Map(existing.map((e) => [e.seq, e]));
  for (const event of initialEvents) {
    const already = existingBySeq.get(event.seq);
    if (already !== undefined) {
      // Same-seq seeds are idempotent only when the payload is
      // structurally identical to what the store already holds.
      // A divergent seed (different kind or different content at the
      // same seq) corrupts the replay invariant and must surface as
      // an error rather than be silently dropped.
      if (!eventsStructurallyEqual(already, event)) {
        throw new Error(
          `resume seed conflicts with store at seq ${String(event.seq)}: store holds ${already.kind}, seed carries ${event.kind} (or a different payload)`,
        );
      }
      continue;
    }
    await env.repoStore.append(runId, event);
  }

  // Seed-contract guard, before any blob resolution. A resume that
  // supplied `resumeFromEvents` referencing blob: refs requires the
  // BlobSubstrate that recorded them; the runLocal in-memory substrate
  // is ephemeral and starts empty, so it cannot serve them. Fail with a
  // targeted error naming the contract rather than a deep
  // `resolveRef`-miss surfacing downstream. This must run BEFORE the
  // terminal short-circuit's `buildResultFromLog` and the canonical-log
  // hydration below -- both call `resolveRef`, and either would otherwise
  // hit the raw "unknown blob ref" failure first. Keyed on
  // `initialEvents` because it is the seed contract: it fires for a
  // seeded resume whether the seeded log is terminal or not, and is a
  // no-op for a seedless recovery call.
  const seedBlobRefs = initialEvents.filter(
    (e): e is typeof e & { kind: "StepCompleted" } =>
      e.kind === "StepCompleted" && e.output.ref.startsWith("blob:"),
  );
  if (seedBlobRefs.length > 0 && env.blobs.ephemeral) {
    throw new Error(
      `resume requires the BlobSubstrate that recorded the seed log's blob refs (${String(seedBlobRefs.length)} blob output(s) present); the runLocal in-memory substrate is ephemeral and starts empty. Pass the originating env, or use a durable substrate.`,
    );
  }

  // Establish canonical state from the durable log itself, not from the
  // seed array. The seed may have arrived two ways -- as a
  // `resumeFromEvents` array this process just wrote, or as a log a
  // prior (crashed) process left on disk that this process adopts without an
  // explicit `resumeFromEvents` seed.
  // Reducing the durable
  // log answers the only question that matters for recovery -- "does the
  // canonical log carry residual work?" -- identically for both, so
  // every decision below (terminal short-circuit, crashed-in-flight
  // settling, the RunStarted emit) keys on canonical state rather than
  // on how the events reached this process.
  let state = await reloadState(env, runId);

  // Terminal short-circuit. A recovery call whose canonical log is already
  // terminal (`completed`/`failed`/`cancelled`) must NOT emit a fresh
  // `RunStarted` -- that throws `TransitionError("terminal-phase")`,
  // uncaught, and rejects the run. Return the existing terminal result
  // reconstructed from the durable log, matching the shape the live
  // terminal path below produces (`emitTerminalEvent` and the child
  // entry point walk `events` for the terminal event and read
  // `terminalStatus`).
  if (isTerminalRunPhase(state.phase)) {
    return buildResultFromLog(env, runId, state);
  }

  // Classify residual steps the canonical log leaves in a non-terminal
  // phase the runtime body cannot re-arm. The DAG's `nextSchedulable`
  // skips any step that already appears in `state.steps` (the safe-runner
  // needs a fresh `StepStarted` to advance one), so a step left
  // `in-flight`, `awaiting-signal`, or `awaiting-timer` would stall the
  // main loop with no schedulable primitive. Cancellation paths are
  // exempt -- the cleanup branch owns settling steps whose phase is
  // `cancelling`.
  //
  // A residual `in-flight` step whose primitive is an invocation
  // boundary (`isCrashedInvocationStep`: an agent `step` or an `action`)
  // is a crash mid-invocation: its `StepStarted` is durable but no
  // `StepCompleted` landed, and the invoked primitive is
  // non-deterministic and unrecorded, so it cannot be replayed
  // exactly-once. Rather than re-invoke it (at-most-once refusal), settle
  // it as a terminal `StepFailed`. Every OTHER non-terminal residual --
  // an `in-flight` coordination container (mid-`map`, `childWorkflow`),
  // or an `awaiting-signal`/`awaiting-timer` step -- still surfaces
  // `RuntimeResumeUnsupportedError`: those have a live re-arming surface
  // the host owns (rebuild the map state, re-park on a later signal), so
  // declining honestly is correct there.
  //
  // The pass runs whenever canonical state is `running`, whether the
  // residual arrived via a `resumeFromEvents` seed OR was adopted from a
  // durable log this process is adopting without a seed. Keying on
  // `state.phase === "running"` (not on whether a seed was supplied) is
  // what settles a crashed step under seedless recovery; the
  // RunStarted emit below is skipped in that case because the canonical
  // phase is already `running`.
  const crashedInFlight: { stepId: string; attempt: number }[] = [];
  const recoverableParks: {
    stepId: string;
    correlationId: string;
    timeoutAtMs?: number;
  }[] = [];
  if (state.phase === "running") {
    for (const [stepId, stepState] of state.steps) {
      // Resumable carve-outs, each re-offered by `nextSchedulable` on the
      // SAME predicate:
      //   - a mid-loop container (or its in-flight synthetic iteration
      //     step): runLoop re-derives its cursor from the log;
      //   - an `awaitSignal` step still `awaiting-signal`: runAwaitSignal
      //     skips its already-emitted markers and re-parks on the signal
      //     channel, holding a live awaiter for a later signal;
      //   - an `awaitSignal` step left `in-flight` by a mover -- a received
      //     signal or, for a timed gate, a fired timeout: runAwaitSignal
      //     reconstructs the outcome from the log and short-circuits to
      //     completion (or, on timeout, routes or fails) without parking
      //     (the crash-after-move-before-StepCompleted window).
      if (
        isResumableLoopStep(definition, stepId, stepState.phase) ||
        isResumableAwaitingSignalStep(definition, stepId, stepState.phase) ||
        isResumableReceivedAwaitSignalStep(
          definition,
          stepId,
          stepState.phase,
        ) ||
        isResumableOnTriggerStep(definition, stepId, stepState.phase)
      ) {
        // The onTrigger container carries no crash-mid-invocation risk (it never
        // self-completes); `runOnTrigger` re-derives its cursor from the log and
        // re-links a body parked mid-approval. Leave it for `nextSchedulable` to
        // re-offer, the same as the other coordination carve-outs.
        continue;
      }
      // A crashed-mid-invocation step (agent step or action). Before settling
      // it as a terminal failure, check whether the reactor durably recorded an
      // approval suspension for this step-attempt that never reached the log as
      // a flushed `SignalAwaited` -- the crash-mid-park window: `StepStarted`
      // durable, `SignalAwaited` buffered but not flushed, so the reduced phase
      // is `in-flight` rather than `awaiting-signal`. If so the step is
      // recoverable: the suspension decision is durable in the reactor's
      // pending-operation store even though the workflow log lost it, so the
      // reconstruction loop below re-commits the missing `SignalAwaited` rather
      // than failing the run. Otherwise (no binding, or no pending approval op)
      // it is a genuine crash mid-agent-turn and settles terminal, the
      // pre-recovery behavior. Both settlings happen AFTER this loop --
      // committing inline would leave `state` stale and merely relocate the
      // stall to the main loop.
      if (isCrashedInvocationStep(definition, stepId, stepState.phase)) {
        const parkedOps =
          env.readParkedApprovalOps !== undefined
            ? await env.readParkedApprovalOps({
                runId,
                stepId,
                attempt: stepState.currentAttempt,
              })
            : [];
        // A step-attempt parks on at most one control-plane suspension. More
        // than one durable pending approval op is a corrupt store, not a state
        // the runtime can reconcile by picking one -- fail loud.
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
      // Every other non-terminal residual keeps declining: the host owns
      // the recovery decision (crash, alert, or redeploy).
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
  // the crash prevented from flushing, reconstructed from the reactor's durable
  // pending operation. This advances the step to `awaiting-signal`, converting
  // the crash-mid-park residual into the ordinary crash-after-park residual the
  // resume machinery already handles: `nextSchedulable` re-offers the
  // awaiting-signal step, `runStep`'s resume-from-park re-parks on the recovered
  // channel reusing the original correlationId, and -- because the re-park finds
  // the step already `awaiting-signal` -- it does not re-fire `onPark`; the
  // correlation re-registers exactly once through the durable-state re-emit. The
  // agent turn is never re-invoked, preserving at-most-once. The reactor stores
  // `timeoutAt` as epoch ms; `SignalAwaited` carries it as an ISO string, so
  // convert, and omit the field entirely for the indefinite-hold parks that are
  // the norm.
  //
  // This recovery is APPROVAL-only: `recoverableParks` come from the reactor's
  // durable approval pending-op store, which reconstructs an approval park. An
  // "input" park has no equivalent durable pending-op yet, so a crash in its
  // pre-flush window (StepStarted flushed, SignalAwaited not) leaves it in
  // `crashedInFlight` and it settles as a terminal StepFailed below rather than
  // re-parking. The durable input substrate that closes this window is future
  // work; input-park crash-safety today holds only for the post-flush window.
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
  // (`retriesExhausted: true`), advancing `state`/`seq` per commit. This
  // moves the step to phase `failed`, so `nextSchedulable` will not
  // re-schedule it and the agent is never re-invoked; the post-loop
  // `hasFailedStep` path is left to commit `RunFailed` and settle the run.
  for (const { stepId, attempt } of crashedInFlight) {
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
    };
    state = await commitDurable(env, runId, failed);
  }

  // Issue RunStarted only if the state machine has not seen it.
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
      // This block is reached only when canonical state was `pending`
      // (an empty or RunStarted-less log), so seedless recovery
      // -- whose canonical log already carries `RunStarted` -- never
      // lands here: reload-at-entry sees its `running`/terminal phase and
      // skips this emit entirely. The one race that still lands a
      // `code: "phase"` rejection is a `cancel("self", ...)` that beats
      // this very first `RunStarted` commit: `CancelRequested` is legal
      // from `pending` (the state machine admits early-lifecycle
      // cancellation), so the chain reloads, sees phase=cancelling, and
      // rejects `RunStarted`. Reload and continue -- proceeding routes
      // through the cancellation cleanup branch and emits `RunCancelled`.
      // Any other rejection is a real error and must surface.
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
  // downstream steps can resolve `{ from: "steps.<id>.output" }`
  // selectors against work that completed before this process took over
  // -- whether that work arrived as a `resumeFromEvents` seed or was
  // adopted from a durable log this process recovered without a seed (the
  // adopt-by-skip frontier). Without hydration, the runtime starts with
  // an empty stepOutputs and any selector referencing a
  // previously-completed step's output throws, landing as a spurious
  // StepFailed. The ephemeral-substrate seed-contract guard that
  // protects these `resolveRef` calls runs up front, before any blob
  // resolution.
  const canonicalLog = await env.repoStore.read(runId);
  for (const event of canonicalLog) {
    if (event.kind !== "StepCompleted") continue;
    stepOutputs[event.stepId] = await env.blobs.resolveRef(event.output.ref);
  }
  const stepPromises = new Map<string, Promise<void>>();
  const justSettled = new Set<string>();
  // Per-step local abort controllers. Each scheduled primitive gets
  // one of these; the controller fires when (a) the outer
  // cancelController aborts (explicit cancel), or (b) drain.signal
  // aborts AND the drain controller declares the step is
  // `"cancel"`-behavior. The main loop entry observation point reads
  // this map to abort in-flight cancel-mode steps when drain fires
  // after the step was already scheduled.
  const stepAborts = new Map<string, AbortController>();

  // Tick loop: schedule everything ready, await any in-flight to
  // settle, repeat until done. Cancellation aborts every in-flight
  // executor; we still loop to commit `CancelPropagated` and the
  // terminal `RunCancelled`.
  while (!isRunDone(definition, state)) {
    if (cancelController.signal.aborted && state.phase !== "cancelling") {
      state = await reloadState(env, runId);
    }

    // Drain observation point #1: main loop entry. If drain has
    // fired, abort every in-flight step whose declared behavior is
    // `"cancel"`. The supervisor's drainTimeout accumulator on the
    // host side ticks against these aborts; on expiry it commits a
    // signed `CancelRequested{origin: "supervisor-drain"}` which
    // the runtime body picks up via the existing cancel cascade.
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
          // Errors are committed as StepFailed inside the primitive
          // runner; the main loop notices the failed phase on the
          // next state reload.
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

    // Wait for at least one in-flight primitive to settle. Each
    // primitive's runner already swallows its own errors into
    // StepFailed events so the race resolves cleanly.
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
  // `cancelling` branch also lands here when the cancel-vs-completion
  // race makes `isRunDone` return true via the all-steps-terminal
  // path before the cancellation block ran. The log invariant
  // requires every run reach a terminal event; the runtime body owns
  // emitting one.
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
      // `CancelRequested` first (legal from `phase=running`). The
      // chain then reloads, sees phase=cancelling, and rejects the
      // RunCompleted/RunFailed commit with `code: "phase"`. The
      // structurally identical race for the initial RunStarted commit
      // is handled at the top of executeRunBody (C5); this is its
      // post-loop sibling (C-B). Reload, confirm the live phase is
      // cancelling (or already terminal), and route through the
      // cancelling cleanup branch so the run settles as `cancelled`.
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
  const terminalStatus =
    state.phase === "completed"
      ? "completed"
      : state.phase === "failed"
        ? "failed"
        : "cancelled";
  return {
    runId,
    terminalStatus,
    outputs: stepOutputs,
    events,
  };
}

/**
 * Reconstruct the terminal `RunResult` for a run whose canonical log is
 * already terminal, without re-driving it. Used by the seedless-recovery
 * terminal short-circuit: a recovery call against an already-terminal durable
 * log must return the existing result rather than emit a fresh
 * `RunStarted` (which would throw `terminal-phase`).
 *
 * The shape matches the live terminal path at the tail of
 * `executeRunBody` byte-for-byte: `terminalStatus` derived from the
 * (already terminal) `state.phase`, `events` read from the durable log
 * (so it carries the terminal event `emitTerminalEvent` and the child
 * entry point walk for), and `outputs` hydrated from the log's
 * `StepCompleted` refs (the live path threads in-process `stepOutputs`,
 * which for a run driven end to end holds exactly those completed-step
 * outputs).
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
  const terminalStatus =
    state.phase === "completed"
      ? "completed"
      : state.phase === "failed"
        ? "failed"
        : "cancelled";
  return { runId, terminalStatus, outputs, events };
}

/**
 * Structural equality for two events at the same seq. The events are
 * plain JSON-serializable objects by the state-machine contract; a
 * canonical-JSON comparison ignores key order and absent-vs-undefined
 * field differences.
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
 * Run the post-loop cancellation cleanup: reload, cascade
 * `ChildCancelRequested` to any live children, and commit
 * `RunCancelled`. Shared between the natural cancelling exit and the
 * post-loop catch that absorbs a phase rejection on the terminal
 * commit when a concurrent cancel won the chain race.
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
 * Emit `ChildCancelRequested` for every tracked child whose
 * cancellation has not been issued and which has not already
 * reached a terminal status. The state machine's resume invariant
 * documents the runtime's responsibility for this cascade: without
 * it, a resuming process cannot rebuild the cancel chain from the
 * log alone.
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
 * Build the per-step local AbortController used as the `abort`
 * argument to `runPrimitiveSafe`. Aborts when (a) the outer
 * cancelController.signal aborts, or (b) drain.signal aborts and the
 * step's declared drainBehavior is `"cancel"`. A `"wait"`-behavior
 * step ignores drain entirely; only the explicit cancel path can
 * abort it.
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
      return runStep(env, runId, primitive, selectorCtx, abort);
    case "action":
      return runAction(env, runId, primitive, selectorCtx, abort);
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
      return runMap(env, runId, primitive, selectorCtx, abort);
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
 * Wrap the per-primitive runner so an uncaught throw always lands a
 * terminal step-phase event in the log. Each runner already commits
 * its own normal-path completion and most failure paths; this is the
 * safety net that catches awaited promises rejecting outside the
 * runner's own try/finally (e.g. signal abort during awaitSignal).
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
      // The step never reached `StepStarted`. If the run is being
      // cancelled (or already cancelled/terminal), the body's cleanup
      // path owns the terminal events; emitting synthetic step events
      // here would be rejected by the state machine because
      // `StepStarted` requires `running`. Surface the original cause
      // so the awaiter sees a coherent failure shape, but leave the
      // log untouched.
      if (state.phase === "cancelling" || isTerminalRunPhase(state.phase)) {
        throw cause;
      }
      // No StepStarted was committed (e.g., the input-materialization
      // selector threw before runStep emitted StepStarted). Emit a
      // synthetic StepStarted + StepFailed so the scheduler sees the
      // step as terminal and does not busy-loop on it.
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
      // If the run is being cancelled, propagate cancellation
      // directly rather than landing StepFailed -- a step that was
      // mid-flight when cancellation reached it should end up
      // `cancelled`, not `failed`. The state machine documents this
      // ordering as "cancellation wins over failure" but only at
      // the run-level; the step-level guarantee lives here.
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
 * durably via `commitDurable` BEFORE `env.invokeStep` is called, not
 * left in the run-body buffer. The agent invocation is a
 * non-deterministic, potentially non-idempotent side effect that the
 * runtime cannot record exactly-once; flushing the marker first means a
 * crash mid-invocation leaves a durable `StepStarted` with no
 * `StepCompleted`, which the recovery path in `executeRunBody` settles
 * as a terminal failure rather than silently re-invoking the agent
 * (at-most-once). On a fresh single-step run this flush carries the
 * still-buffered `RunStarted` to durable storage together with the
 * `StepStarted` in one batch, so both are on disk before the agent
 * runs. `StepStarted` is emitted exactly once per step (the
 * `stepStartedEmitted` guard), so retry attempts re-enter without a
 * second flush.
 */
async function runStep(
  env: WorkflowRuntimeEnv,
  runId: string,
  step: StepPrimitive,
  selectorCtx: SelectorContext,
  abort: AbortSignal,
): Promise<unknown> {
  // Re-apply the retry/budget cross-field guard here as a defensive
  // re-check, at the runtime's single read point for both fields, rather
  // than trust that every definition reached it through `step()`.
  validateRetryTriggerCombination(step);
  let attempt = 1;
  const maxAttempts = step.retry?.maxAttempts ?? 1;
  // StepStarted is committed exactly once per step -- the entry to
  // the first attempt. Subsequent attempts re-enter via the
  // AttemptScheduled + TimerFired pair, which moves the step from
  // awaiting-timer back to in-flight without a fresh StepStarted.
  // The state machine's handleStepStarted rejects a re-emit, so the
  // runtime mirrors the invariant here.
  let stepStartedEmitted = false;

  // Crash-resume re-entry. A run re-driving the durable log re-offers a
  // `step` left `awaiting-signal` (its `StepStarted` + `SignalAwaited` are
  // durable, no `StepCompleted`) via `isResumableAwaitingSignalStep`. The
  // agent already parked on a reactor gate before the crash; re-invoking
  // it with the original input here would build a fresh agent and start a
  // NEW turn, silently re-running the suspended work. Instead recover the
  // channel it parked on from the reduced state (the runtime-minted
  // `signalName(correlationId)` lives only on the durable `SignalAwaited`,
  // not in the definition), RE-PARK on it -- `parkOnSignal`'s guard skips
  // re-emitting `SignalAwaited` since the step is already
  // `awaiting-signal` -- and, once the signal arrives, seed the
  // suspend/resume bridge with the recovered `resume` so the first
  // `invokeStep` re-invokes the agent against the delivered decision.
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
    // synthesizes the right inbound after a respawn. The awaiting-signal phase
    // guarantees `awaitingSignal` is set -- read it definitely and fail loud on
    // the invariant rather than papering over a missing field; `controlParkKindOf`
    // is the single point that maps its optional kind to a definite one.
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
    // Recover the attempt the step suspended on. The suspend committed its
    // pending-op + turns under the cold-path ContextStore keyed by this
    // attempt (`stepStorageRoot({runId, stepId, attempt})`); the resume
    // re-invoke must reopen that SAME `attempt-N` store so `rehydrateGates`
    // finds the pending-op and the delivered decision correlates. A step
    // that suspended on its first attempt reduces `currentAttempt` to 1 (the
    // hardcoded default was correct only by coincidence there); a step that
    // RETRIED before suspending reduces to >=2, and leaving `attempt` at 1
    // would reopen the wrong `attempt-1` store, rehydrate no gate, and hang
    // on a decision that correlates nothing. Recovering it also continues
    // the outer retry lineage from the suspended attempt: a resumed step
    // that later fails schedules attempt N+1, not attempt 2.
    attempt = entryStepState.currentAttempt;
    // The durable log already carries this step's StepStarted, so the
    // fresh-attempt emit below must be skipped: re-emitting throws.
    stepStartedEmitted = true;
  }

  while (true) {
    // Materialize the input first so the StepStarted event carries
    // the substrate-resolvable ref the audit reader expects.
    // Selector throws propagate to runPrimitiveSafe; the safe-runner
    // detects the missing step state and emits a synthetic
    // StepStarted+StepFailed so the scheduler sees the step as
    // terminal instead of busy-looping.
    const rawInput =
      step.input !== undefined ? evaluate(step.input, selectorCtx) : null;
    // Canonicalize `undefined` to `null` once here so the audit blob
    // and the invoker see the same value. The substrate rejects
    // non-serializable values; an input that resolved to `undefined`
    // (e.g. the default-input convention's `trigger.payload` against
    // a caller that did not supply one) is stored as `null` so the
    // audit ref stays round-trippable, and the invoker observes the
    // same `null` so an audit reader cannot diverge from the agent's
    // actual input.
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

    // Build per-step abort: timeout AND outer cancellation both abort.
    const stepAbort = new AbortController();
    const onOuter = () => {
      stepAbort.abort();
    };
    abort.addEventListener("abort", onOuter, { once: true });
    let timer: ReturnType<typeof setTimeout> | undefined;
    if (step.timeout !== undefined) {
      timer = setTimeout(() => {
        stepAbort.abort();
      }, step.timeout);
    }

    try {
      // Suspend/resume bridge. The first invocation drives a plain agent
      // send; if the reactor parks on a gate, `invokeStep` returns
      // `{ suspend: { correlationId } }` instead of an output. The step
      // then becomes a durable `awaiting-signal` step parked under the
      // reserved `signalName(correlationId)` channel via `parkOnSignal`
      // (which emits SignalAwaited + parks and returns the delivered
      // decision). When the decision arrives, the step is re-invoked with
      // `resume`, so the invoker re-dispatches the tool and drives the
      // reactor to a real reply -- that reply, not the raw signal payload,
      // is the step output. A resume that parks again re-parks, mirroring
      // runAwaitSignal's re-park.
      let output: unknown;
      let resume:
        | { correlationId: string; decision: unknown; kind: ControlParkKind }
        | undefined;
      // A crash-resume re-entry re-parks on the recovered channel FIRST,
      // before any `invokeStep`, so the agent is never re-sent the original
      // input. The delivered decision seeds `resume` so the bridge loop's
      // first `invokeStep` re-invokes the agent against it, exactly as a
      // same-process resume would. `resumeFromPark` is consumed once per
      // step; a resume that suspends AGAIN re-parks through the normal
      // `{ suspend }` arm below.
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
      // budget > 1 the count of triggers already serviced must survive a
      // respawn, so it is seeded from the durable log -- the number of
      // input-park `SignalAwaited`s the step has emitted equals the turns it
      // has serviced, because the runtime's re-arm below is the ONLY minter
      // of input parks (`StepInvokeResult` offers invokers no input arm), and
      // it emits exactly one per completed turn. A budget of 1 never re-arms
      // (its count is 0 by construction) and unbounded never counts, so both
      // skip the log read -- the batch default pays nothing.
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
          // Budget spent -> complete (the batch case: one trigger, one turn,
          // done). Budget remaining -> re-arm: park the step on a fresh input
          // control-plane channel awaiting its next trigger, which becomes the
          // next turn's input. The park is snapshot-less (`kind: "input"`) and
          // fires no host notify -- the run's owner delivers the next trigger
          // on this channel. `env.newId` mints a unique channel per turn so
          // deliveries never collide; the run's owner discovers the current
          // channel from the reduced `awaitingSignal.name`.
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
        // for this correlation. Unlike runAwaitSignal, the agent step
        // already emitted its own `StepStarted` on runStep entry, so the
        // reduced state passed to `parkOnSignal` reads the step as
        // `in-flight`, and its re-park guard emits a fresh `SignalAwaited`
        // rather than treating this as a re-park of an already-awaiting
        // gate.
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
      const outputRef = (await env.blobs.recordOutput(step.id, attempt, output))
        .ref;
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
      return output;
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      const exhausted = attempt >= maxAttempts;
      let after = await reloadState(env, runId);
      // Cancellation wins over step-level failure: if the run is
      // cancelling, the catch landed because the step's abort fired,
      // and the audit log should record this as a step cancellation
      // rather than a runtime-attributed failure. CancelPropagated
      // moves the step to `cancelled` so the main loop sees it as
      // terminal and the post-loop cancellation branch settles the
      // run.
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
      // Wait for the scheduler to commit TimerFired before looping
      // into the next attempt. The step itself stays awaiting-timer
      // through the wait.
      await waitForTimer(
        env,
        runId,
        timerId,
        fireAtDate,
        abort,
        env.drain,
        step.id,
      );
      // Drain observation point #2: retry-between-attempts in
      // runStep. If drain has fired and the step's behavior is
      // `"cancel"`, abort before launching the next attempt. The
      // outer `abort` was wired through `createStepAbort` to fire on
      // drain already; this guard is the explicit second site so a
      // drain that lands during the brief window between
      // waitForTimer settling and the next attempt's invokeStep call
      // does not stall behind a live invokeStep that the supervisor
      // is waiting on to wind down.
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
 * Like `runStep`, the action's `StepStarted` is flushed durably via
 * `commitDurable` BEFORE `env.invokeAction` runs, not left in the run-body
 * buffer. An action handler can perform observable side effects the
 * runtime cannot record exactly-once: the EffectContext ledger dedups
 * only effects routed through `perform`, which is an author obligation the
 * runtime cannot enforce. Flushing the marker first means a crash
 * mid-invocation leaves a durable `StepStarted` with no `StepCompleted`,
 * which the recovery path in `executeRunBody` settles as a terminal
 * failure rather than re-invoking the handler (at-most-once). On a fresh
 * single-action run this flush carries the still-buffered `RunStarted` to
 * durable storage together with the `StepStarted` in one batch.
 *
 * No retry loop: an action is single-attempt, so a thrown effect lands
 * `StepFailed` through `runPrimitiveSafe` like every other non-step
 * runner. The per-effect ledger is a deeper exactly-once line of defense
 * for effects routed through `perform`; the barrier here is what makes the
 * action non-re-invocable at the runtime layer.
 */
async function runAction(
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
  // Action-invoke durability barrier: flush `StepStarted` durably before
  // `invokeAction` runs, inline like `runStep` rather than through the
  // buffered `emitStepStartedWithValue` the coordination runners share.
  // An action is single-attempt, so `attempt` is 1.
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

  const actionAbort = new AbortController();
  const onOuter = (): void => {
    actionAbort.abort();
  };
  abort.addEventListener("abort", onOuter, { once: true });
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
    await emitStepCompletedWithValue(env, runId, primitive.id, result.output);
    return result.output;
  } finally {
    abort.removeEventListener("abort", onOuter);
    if (timer !== undefined) clearTimeout(timer);
  }
}

/**
 * Bounded rework loop. Each iteration is a separate child run of the
 * body against the shared store (via `env.runLoopIteration`), scoped
 * `<loopId>[<index>]` at the step level (mirroring `runMap`) with a
 * path-safe child run id `<loopId>__<index>`. The registered `while`
 * predicate decides whether to continue on each iteration's output; the
 * registered `carry` threads the next iteration's input. On convergence
 * (`while` false) the loop routes to its normal `after`-dependents; on
 * hitting `maxIterations` with `while` still true it routes to
 * `onExhausted` -- a gate-style mutually-exclusive branch, so the
 * not-taken side is pruned with skip sentinels before the loop's own
 * StepCompleted lands.
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
  // emitted?" check below is against everything committed so far; a
  // re-emit of anything reflected is refused by the state machine.
  const log = await env.repoStore.read(runId);
  let state = await reloadState(env, runId);

  // The loop container is in state.steps only on resume; a fresh run
  // emits its StepStarted, a resumed run must not (re-emit throws).
  if (!state.steps.has(primitive.id)) {
    await emitStepStartedWithValue(env, runId, primitive.id, {
      while: primitive.while,
      carry: primitive.carry,
      maxIterations: primitive.maxIterations,
    });
  }

  // Replay fully-done iterations (child terminal AND step completed) from
  // the log to re-derive the cursor, the threaded input, and whether the
  // loop already reached its outcome before the crash (the post-routing
  // window). while/carry are pure, so replaying them over recorded inputs
  // and outputs reproduces the pre-crash decisions.
  const iterationZeroInput =
    primitive.input !== undefined
      ? evaluate(primitive.input, selectorCtx)
      : null;
  let currentInput: unknown =
    iterationZeroInput === undefined ? null : iterationZeroInput;
  let iteration = 0;
  let terminated = false;
  let outcome: "converged" | "exhausted" = "exhausted";
  while (isIterationDone(state, primitive.id, iteration)) {
    const doneStepId = scopedStepId(primitive.id, iteration);
    const doneInput = await resolveIterationInput(env, log, doneStepId);
    const doneOutput = await resolveIterationOutput(env, log, doneStepId);
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

  // Prefer the resume iteration's own recorded input (an in-flight
  // iteration whose StepStarted is durable) over the carry recomputation.
  if (!terminated) {
    const resumeInputRef = findStepInputRef(
      log,
      scopedStepId(primitive.id, iteration),
    );
    if (resumeInputRef !== undefined) {
      currentInput = await env.blobs.resolveRef(resumeInputRef);
    }
  }

  // A crash-recovered iteration whose body was parked or mid-relay needs an
  // active re-link on its FIRST drive below. planLoopResume yields that token
  // (or undefined when the forward drive can re-adopt the iteration from the
  // child's own durable log). Cleared after the recovered iteration is driven
  // so every later iteration spawns fresh.
  let occurrenceResume = terminated
    ? undefined
    : planLoopResume(primitive, state, log, iteration);

  let iterations = iteration;
  for (let i = iteration; !terminated && i < primitive.maxIterations; i += 1) {
    iterations = i + 1;
    const stepId = scopedStepId(primitive.id, i);
    const childRunId = `${primitive.id}__${String(i)}`;

    state = await reloadState(env, runId);
    if (!state.steps.has(stepId)) {
      await emitStepStartedWithValue(env, runId, stepId, currentInput);
    }
    // Drive the iteration body through the suspendable-child seam: this commits
    // ChildSpawned on the loop container step, spawns the body under the
    // inherited-env executor, proxies any body park up on the container, and
    // commits ChildCompleted. A non-suspending body drives straight to
    // terminal; a body that parks in-process (e.g. an agent step on an approval
    // gate, or an author `awaitSignal`) is serviced by the park proxy above
    // without the body reaching a terminal. `occurrenceResume` is non-undefined
    // only on the recovered iteration of a crash resume, where it re-links a
    // body parked or mid-relay at the crash; it is cleared after this drive so
    // every later iteration spawns fresh.
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

    // The drive returns only the terminal status; the iteration's step outputs
    // live in the child's durable log, so hydrate them here -- the scoped
    // StepCompleted records them, and while/carry read them.
    const output = await hydrateChildOutputs(env, childRunId);

    const after = await reloadState(env, runId);
    if (after.steps.get(stepId)?.phase !== "completed") {
      await emitStepCompletedWithValue(env, runId, stepId, output);
    }
    await flush(env, runId);

    if (terminalStatus !== "completed") {
      // A failed or cancelled iteration is a real failure, not an
      // exhaustion. Throw so runPrimitiveSafe lands StepFailed (or
      // CancelPropagated when the run is cancelling) on the loop node.
      // Note: throwing skips routeLoopOutcome, so neither branch is
      // pruned; both the normal dependents and onExhausted then run
      // before the run settles failed, per the engine's "a failed
      // dependency is resolved" scheduling (the same as a failed gate).
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

  await routeLoopOutcome(definition, env, runId, primitive, outcome, abort);
  const output = { outcome, iterations, carry: currentInput };
  await emitStepCompletedWithValue(env, runId, primitive.id, output);
  return output;
}

/**
 * Re-link state for a crash-recovered occurrence whose body is parked. The
 * resume planner yields at most one of these -- an approval park or a
 * signal-relay park, never both -- so a single discriminated union encodes the
 * mutual exclusion the two former locals maintained by discipline.
 */
type SuspendableOccurrenceResume =
  | { kind: "approval"; corr: string; relay: boolean; decision?: unknown }
  | { kind: "signal-relay-reestablish"; name: string; awaitSeq: number }
  | {
      kind: "signal-relay-relay";
      name: string;
      payload: unknown;
      signalId: string;
    };

/**
 * Drive one occurrence's suspendable-child body to terminal, durably: commit
 * `ChildSpawned`, spawn the body, re-link a crash-recovered park, proxy each of
 * the body's parks up on THIS run's own park machinery over `containerStepId`
 * (approval -> `parkOnSignal`/`resume`; author `awaitSignal` -> signal-relay),
 * then commit `ChildCompleted`. Returns only the terminal status; a body's step
 * outputs live in the child log, so a caller that needs them hydrates them from
 * there and this seam stays free of occurrence-divergent concerns.
 *
 * The drive is agnostic to where an occurrence's input comes from: `input` is
 * supplied by the caller, and everything occurrence-divergent stays there too --
 * the terminal-is-final vs tolerate policy and the re-arm that produces the next
 * occurrence's input. `runOnTrigger` is the caller, one occurrence per trigger
 * event. `containerStepId` is both the proxy-park step and the child's
 * `parentStepId`; a caller that proxy-parks on a step other than the child's
 * parent would need a second knob.
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
  // Flush the spawn record durable before the child runs so a resumed
  // parent log records the spawn ahead of any child-side work.
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
    // correlation. A grant already delivered to the parent log (its
    // SignalReceived consumed the container's park) is relayed as-is;
    // otherwise re-park the container and await the grant as the steady-state
    // loop would.
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
      // reused unchanged and the approver sees the body step's real
      // snapshot; then relay the granted decision back into the child so the
      // body continues.
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
    };
    after = await commit(env, runId, completed);
    void after;
  }
  await flush(env, runId);

  return { terminalStatus };
}

/**
 * Run a long-lived onTrigger section. The section services each occurrence
 * of its trigger as an EVENT: it spawns the body as a child run resolved by
 * the deployed `bodyRef`, awaits the body's terminal, then re-arms on a
 * snapshot-less input park to await the next occurrence. The container never
 * self-completes -- the run stays alive between events and settles only when
 * a body run ends non-`completed` (terminal-is-final) or the run is
 * cancelled/aborted.
 *
 * Each event's body is a full sub-run under `runs/<sectionId>__<index>/`, so
 * per-event detail lives in its own log; the parent log carries only the
 * container `StepStarted`, a `ChildSpawned`/`ChildCompleted` pair per event,
 * and the input-park `SignalAwaited`/`SignalReceived` re-arm -- all existing
 * event kinds, so the state machine is untouched.
 *
 * A body that suspends mid-run on an approval park is serviced by proxying the
 * park up on the shared correlation via this run's own park machinery. On
 * crash-recovery the driver reconstructs its position from the container's
 * reduced state and durable log -- which event is current, whether its body is
 * parked mid-approval (and whether the grant already landed), or whether the
 * section is idle between events -- and re-links the parked body rather than
 * re-running from event 0.
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
  // iteration whose body is parked mid-signal-relay. `reestablish` re-drives the
  // durable await's race; `relay` delivers a signal that landed but was not
  // relayed before the crash. Cleared after the recovered iteration is re-linked.
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
    const plan = planOnTriggerResume(primitive, initial, log);
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
        // The next event's trigger already arrived durably on the input channel
        // -- its SignalReceived consumed the re-arm, moving the container off
        // `awaiting-signal` -- but the body spawn had not yet committed. Advance
        // to that event with the delivered input WITHOUT re-parking, so the
        // trigger is not dropped.
        currentInput = plan.input;
        eventIndex = plan.eventIndex + 1;
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

    // Terminal-is-final unless the section tolerates a body failure. A cancelled
    // body always ends the section (a drain/operator decision, never tolerated);
    // a failed body ends it only under the default `end` policy. Under
    // `tolerate`, a failed body falls through to the re-arm below -- the
    // ChildCompleted{failed} committed above still records the occurrence.
    if (
      terminalStatus === "cancelled" ||
      (terminalStatus === "failed" &&
        bodyFailurePolicyOf(primitive) !== "tolerate")
    ) {
      // Throwing lands the parent terminal via `runPrimitiveSafe`; the run does
      // not relaunch.
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
 * BUFFER-FIFO (operator ruling): a signal is RELAYED, never dropped. Emitting
 * the container's `SignalAwaited` runs the reducer's FIFO pairing:
 *   (a) the emit pre-consumes a signal queued before it (container reduces to
 *       in-flight) -- bind it from the log's pairing and relay, no await; or
 *   (b) the container is left awaiting -- race the signal's arrival against the
 *       body producing its next event (a gate the body timed out itself) and
 *       against abort. Signal-first relays it directly; body-first retires the
 *       now-stale relay await (`SignalAwaitAbandoned`) unless a signal landed
 *       and was consumed during the race, in which case it is relayed
 *       idempotently. An untimed body gate has a single exit -- the signal
 *       always wins -- so this is byte-identical in shape to the approval proxy.
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

  // Fail-loud guard on the one residual divergence from the reducer's pairing:
  // `boundSignalForContainerAwait` is container-scoped and faithful ONLY while
  // THIS container is the sole step awaiting `name`. If ANY other step is
  // already awaiting the SAME author name in this parent run -- another
  // section's signal-relay proxy OR a plain author `awaitSignal` gate (which
  // reduces with no parkKind) -- the reducer consumes a delivery by
  // `state.steps` Map-insertion order across ALL steps (see
  // `handleSignalReceived`), not this container's seq order, so the helper
  // could mis-bind and relay a payload the reducer delivered to the other
  // awaiter. Refuse the topology loudly rather than route a signal to the wrong
  // step. Mirrors `hasForeignSameNameAwaiter` on the plain-gate resume path; a
  // single section re-awaiting a name sequentially is always one active awaiter
  // and stays faithful. Parity with the resume guard is why the parkKind is NOT
  // filtered here -- a plain-gate sibling reduces to `"approval"` and would slip
  // a signal-relay-only check.
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

  // Emit the container's signal-relay await over the body's author name. The
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

  // (b) The container is awaiting; race the signal's arrival against the body
  // advancing on its own.
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
 * crash-recovery (`reestablish-signal-relay`, whose re-spawned body re-parks on
 * `name` silently, so the await is re-driven from its durable seq without a
 * re-emit). Returns the body's NEXT event for the caller's loop.
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
  if (abort.aborted) {
    raceAbort.abort();
  } else {
    abort.addEventListener("abort", onOuterAbort, { once: true });
  }
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
    // parkOnSignal's awaiter-commits contract, so the container log carries it
    // in every host; a production delivery that also committed it dedups by
    // signalId -- then relay it into the body and take the body's next event.
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

  // The body produced its next event before the signal arrived: its gate timed
  // out and it moved on, so the container's relay await is stale. Stop the
  // outstanding awaitNext, then disambiguate whether a signal landed and was
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
 * phase locates its lifecycle point; the log recovers a delivered-but-unrelayed
 * approval grant, whose `SignalReceived` reduces the container step to
 * `in-flight` and strips its `awaitingSignal` (so the correlation and decision
 * are no longer in the reduced state).
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

function planOnTriggerResume(
  primitive: OnTriggerPrimitive,
  state: RunState,
  log: readonly WorkflowEvent[],
): OnTriggerResumePlan {
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
  // ORDERING IS LOAD-BEARING: the body-TERMINAL checks (terminal-is-final for
  // failed/cancelled, reawait-input for completed) MUST precede the
  // container-in-flight throw below. This is what lets the signal-relay abandon
  // path own no distinct resume arm: after a body's timed gate abandons, the
  // container drops to the ordinary in-flight driving state and every sub-state
  // is already owned -- a body that then completed is caught HERE (reawait-
  // input), not by the in-flight throw. Inverting the order would wrongly fail a
  // post-abandon-completed body.
  // Terminal-is-final unless the section tolerates a body failure (mirrors the
  // steady-state drive loop). A cancelled body always ends; a failed body ends
  // only under the default `end` policy. A tolerated failure falls through to
  // the completed block below, which re-adopts the SAME input park a completed
  // body does -- never a bare new arm (which would wedge the section).
  if (
    child.terminalStatus === "cancelled" ||
    (child.terminalStatus === "failed" &&
      bodyFailurePolicyOf(primitive) !== "tolerate")
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
  throw new Error(
    `onTrigger ${primitive.id} resume: body child ${childRunId} is in flight and the container is not parked, but no delivered approval grant or relay signal was found`,
  );
}

/**
 * The resume token for a crash-recovered loop iteration whose body was parked
 * or mid-relay at the crash, derived purely from the container's reduced
 * `state` plus its durable `log` for the given `iteration`. It yields ONLY the
 * active re-link the body needs on its next drive: re-establish the container's
 * signal-relay race, relay a grant/signal delivered but not relayed before the
 * crash, or re-adopt an approval park.
 *
 * `undefined` means "nothing to re-link", and `runLoop`'s forward drive
 * re-adopts the iteration from the child's own durable log -- a terminal body
 * short-circuits, a body that crashed mid-invocation settles failed, a body
 * that had not yet worked re-runs. This is where the planner diverges from
 * `planOnTriggerResume`, which throws in the same spot: `runLoop` owns the
 * iteration cursor (it passes `iteration` in) rather than an event cursor it
 * would have to reset, so there is no `fresh` arm to reset and no
 * `terminal-is-final` arm -- the `isIterationDone` replay and the forward
 * drive already own the terminal and re-adopt cases.
 */
function planLoopResume(
  primitive: LoopPrimitive,
  state: RunState,
  log: readonly WorkflowEvent[],
  iteration: number,
): SuspendableOccurrenceResume | undefined {
  const childRunId = `${primitive.id}__${String(iteration)}`;
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
      // The container is proxy-parked on the body's author `awaitSignal` gate.
      // Recover the durable await's seq (the FIFO binding key) so the resume
      // re-drives the race over the same await without re-emitting it.
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
  // re-adopted body, else the body's silently re-parked gate would wait forever
  // for a signal already consumed. Any OTHER in-flight body carries no delivered
  // grant/signal, so it needs no token and the forward drive re-adopts it.
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
  return undefined;
}

/**
 * Recover the correlation and delivered decision of the container's most recent
 * approval park from the durable log. Used only for the narrow crash window
 * where the grant's `SignalReceived` landed before the driver relayed it into
 * the body -- the container step reduces to `in-flight` with its
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
 * Recover an input re-arm whose next trigger was delivered but whose body spawn
 * had not yet committed at the crash -- the input sibling of
 * `recoverDeliveredApprovalGrant`. After a body completes, the container
 * re-arms an `input` park for event N+1; when that park's `SignalReceived`
 * landed (moving the container to `in-flight` and stripping its
 * `awaitingSignal`) but `ChildSpawned` N+1 was not written, the delivered
 * trigger survives only in the log. Return its payload so the resume advances
 * to N+1 instead of re-parking and dropping it.
 *
 * The seq DISCRIMINATOR is load-bearing: the input re-arm await must be NEWER
 * than the highest `ChildSpawned` for this container. An input await OLDER than
 * that belongs to an already-spawned event, and advancing on its
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
 * Recover a signal delivered to the container's last signal-relay await but not
 * yet relayed into the body -- the signal-relay sibling of
 * `recoverDeliveredApprovalGrant`. The delivery's `SignalReceived` consumed the
 * container's await (moving it to `in-flight`), so the payload lives only in the
 * log; bind it by replaying the reducer's FIFO pairing. Returns undefined when
 * the container has no signal-relay await, or its last one carries no paired
 * signal (an await that was abandoned rather than consumed).
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
 * re-heuristicking it. A newest-observed heuristic will not do here: it returns the
 * NEWEST observed `SignalReceived` for the name, whereas the reducer consumes
 * the OLDEST queued one, so under multiple queued signals for a name it binds
 * the wrong payload.
 *
 * Two reducer behaviors the pairing depends on, both replayed here:
 *   - dedup by `signalId`: a redelivered `SignalReceived` is a no-op in the
 *     reducer (`observedSignalIds`) yet still lands in the log, so the replay
 *     must skip an already-seen id rather than double-advance the FIFO.
 *   - retire an abandoned awaiter: `SignalAwaitAbandoned` drops the awaiter, so
 *     a later signal queues rather than pairing with the retired await; the
 *     replay pops the container's active waiter on abandon to match. The
 *     container awaits a given name single-file, so at most one unpaired waiter
 *     exists at any point.
 *
 * PRECONDITION: `log` is the COMPLETE parent-run log from seq 1. The replay
 * reconstructs the reducer's queue/waiters from `emptyState`, so a windowed
 * suffix would start mid-stream and mis-pair; a non-full log fails loud below.
 *
 * ASSUMPTION: a UNIQUE container awaiter per name in the parent run. The
 * pairing is container-scoped (it filters `SignalAwaited`/`SignalAwaitAbandoned`
 * to `containerStepId`), which matches the reducer ONLY while this container is
 * the sole step awaiting the name. Two sections concurrently awaiting the same
 * name would let the reducer consume by `state.steps` Map-insertion order
 * across all steps; `driveContainerSignalRelay` refuses that topology loudly at
 * the proxy-park, so this helper never sees it.
 */
// Exported for direct unit tests: the reducer-FIFO-replay binding is the
// correctness crux of the signal-relay pass, and its correction cases (dedup,
// FIFO-oldest, abandon-retire) are only reachable on the pre-consume/race-landed
// log-read paths, so they are proven against constructed logs rather than only
// through the runtime driver.
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
  loopId: string,
  iteration: number,
): boolean {
  const child = state.children.get(`${loopId}__${String(iteration)}`);
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
 * value, keyed by the body step id. The suspendable-child drive returns only a
 * terminal status, so the loop reads the iteration's own (durable) child run to
 * rebuild the step-output record its scoped `StepCompleted` records and its
 * `while`/`carry` functions consume -- the same shape the former in-process
 * iteration returned directly.
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
 * Prune the not-taken branch of a completed loop with skip sentinels,
 * BEFORE the loop's own StepCompleted lands, so the scheduler only ever
 * hands back the live side. Converged -> the normal `after`-dependents
 * run and `onExhausted` is pruned; exhausted -> `onExhausted` runs and
 * the normal dependents are pruned. `onExhausted` names the loop in its
 * own `after` (enforced at definition time), so it is excluded from the
 * normal-dependent set here.
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
  // On a resume where routing already happened before the crash, the
  // sentinels are durable; re-emitting a StepStarted for one would throw
  // step-already-started. Skip anything already in state.steps.
  const state = await reloadState(env, runId);
  for (const skipId of toSkip) {
    if (abort.aborted) break;
    if (state.steps.has(skipId)) continue;
    const sentinel = { skipped: true, loopId: primitive.id, outcome };
    await emitStepStartedWithValue(env, runId, skipId, sentinel);
    await emitStepCompletedWithValue(env, runId, skipId, sentinel);
  }
}

/**
 * Event-sourced timer wait.
 *
 * Tells the scheduler to commit `TimerFired{timerId}` at `fireAt`,
 * then subscribes to the run's log tail and resolves on the matching
 * `TimerFired`. The scheduler is the single writer of `TimerFired`
 * to the log; the runtime body never commits `TimerFired` itself.
 *
 * Disposing the scheduler entry on abort cancels the pending
 * `TimerFired` commit so a stale TimerFired does not land in the log
 * after the awaiter has already settled on a sibling event (e.g. an
 * `awaitSignal` step whose signal arrived before the timeout).
 *
 * The replay base is `state.lastSeq + 1`: the scheduler may commit
 * `TimerFired` before the subscriber's `for await` reaches the first
 * iteration, so the subscription must start from the seq immediately
 * after the caller's last-observed event rather than from `"head"`,
 * which would miss a TimerFired that landed during the
 * `await env.repoStore.subscribe(...)` setup.
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
  // Segment boundary: the run parks here, tailing the durable log for
  // the scheduler-committed `TimerFired`. Flush the buffered segment
  // (the `TimerSet` -- and, on the retry path, the preceding
  // `StepFailed`/`AttemptScheduled`) to durable storage BEFORE
  // computing `subscribeFromSeq` and subscribing, so the out-of-process
  // scheduler can tail the durable `TimerSet` and a crash-while-waiting
  // leaves a resumable pre-suspension log.
  await flush(env, runId);
  const subscribeFromSeq = (await reloadState(env, runId)).lastSeq + 1;
  const ac = new AbortController();
  const onOuterAbort = (): void => {
    ac.abort();
  };
  if (abort.aborted) {
    throw new Error("aborted");
  }
  // Drain observation point #3: waitForTimer entry. If drain is
  // already aborted and the step's behavior is `"cancel"`, bail
  // immediately without arming the subscription.
  if (shouldAbortForDrain(drain, stepId)) {
    throw new Error("aborted: drain requested");
  }
  abort.addEventListener("abort", onOuterAbort, { once: true });
  // Listen for drain transitions that land mid-wait. A drain that
  // fires after the subscription has armed must abort the local
  // controller so the `for await` ends cleanly.
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
    // The subscription ended without a matching TimerFired and the
    // outer abort did not fire. The only ways to get here are an
    // explicit consumer-side `return()` (we are the consumer; this
    // does not happen) or the substrate closing the stream
    // unexpectedly. Either is a substrate-level invariant violation.
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
  // v1 runs the inner steps sequentially. A parallel fan-out would
  // need per-item commit serialization against the same run log
  // beyond what the existing commit chain offers, plus a parallelism
  // bound on the env. The spec does not commit to either semantic;
  // sequential keeps the event log readable and the runtime simple.
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
      // The outer map's retry policy applies as the fan-out-level
      // default when the inner step does not declare its own. The
      // inner step's policy already rides in via `...inner`; the
      // spread below only fills in from the map when the inner is
      // missing one.
      ...(inner.retry === undefined && primitive.retry !== undefined
        ? { retry: primitive.retry }
        : {}),
    };
    const output = await runStep(env, runId, scopedStep, itemCtx, abort);
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
  // Mark every step in the not-selected branch's transitive downstream
  // closure as skipped before the gate's own StepCompleted lands, so
  // the DAG scheduler treats them as resolved without ever invoking
  // their bodies. The selected branch's closure is left untouched and
  // proceeds through the normal schedule path. Honoring `abort` in
  // the loop keeps cancellation from leaving the skip closure half-
  // written -- the runtime body's cancel sweep then picks up the
  // remaining steps via CancelPropagated.
  const toSkip = collectBranchClosure(definition, [notSelected], [selected]);
  for (const skipId of toSkip) {
    if (abort.aborted) break;
    const sentinel = {
      skipped: true,
      gateId: primitive.id,
      branch: notSelected,
    };
    await emitStepStartedWithValue(env, runId, skipId, sentinel);
    // The skipped step's output is committed through the substrate
    // as a structured sentinel so a diamond-join step that reads
    // both branches' outputs sees a well-defined value for the
    // not-selected side. The sentinel names the gate and the
    // not-selected branch head so the join author can branch on
    // `skipped` without ambiguity against a legitimate `null` output.
    await emitStepCompletedWithValue(env, runId, skipId, sentinel);
  }
  const output = { branch: selected, value };
  await emitStepCompletedWithValue(env, runId, primitive.id, output);
  return output;
}

/**
 * Compute the set of steps to skip when the `notSelected` branch roots
 * are suppressed in favor of the `selected` roots.
 *
 * The skip set is the transitive downstream closure of the not-selected
 * roots, MINUS any step also reachable from the selected roots. A
 * diamond-join step that lists both a selected and a not-selected root
 * in its `after` is reachable from the selected side and must stay live.
 * Both sides are sets: a `gate` calls this with singleton roots
 * (`then`/`else`), while a `loop` calls it with `onExhausted` against the
 * set of the loop's normal dependents. Computing `reachableFromSelected`
 * as one union closure over all selected roots keeps the diamond guard a
 * plain set-membership test regardless of how many roots each side has.
 */
function collectBranchClosure(
  definition: WorkflowDefinition,
  notSelected: readonly string[],
  selected: readonly string[],
): readonly string[] {
  const selectedSet = new Set(selected);
  const reachableFromSelected = downstreamClosure(definition, selected);
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

function downstreamClosure(
  definition: WorkflowDefinition,
  starts: readonly string[],
): Set<string> {
  const visited = new Set<string>();
  const queue: string[] = starts.filter((id) => id in definition.steps);
  while (queue.length > 0) {
    const id = queue.shift();
    if (id === undefined) break;
    if (visited.has(id)) continue;
    visited.add(id);
    for (const [otherId, primitive] of Object.entries(definition.steps)) {
      const after = primitive.after;
      if (after === undefined) continue;
      if (after.includes(id) && !visited.has(otherId)) {
        queue.push(otherId);
      }
    }
  }
  return visited;
}

/**
 * Symmetric to `emitStepCompletedWithValue`: route a primitive's
 * semantic input through the substrate so the committed
 * `StepStarted.input.ref` is round-trippable through
 * `env.blobs.resolveRef`. Audit consumers can read the actual input
 * the primitive saw rather than a literal marker.
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
 * Commit a `StepCompleted` event whose output is a real value the
 * runtime materialized (`runMap`, `runGate`, `runChildWorkflow`,
 * `runAwaitSignal`, `runEscalation`). Routing through
 * `env.blobs.recordOutput` lets resume rehydrate the output via the
 * standard substrate path -- without this, downstream selectors that
 * target a non-`step` primitive's output crash on resume because the
 * hydration loop only resolves substrate-readable refs.
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
 * Reconstruct how a single admitted `awaitSignal` gate left `awaiting-signal`,
 * by replaying the reducer's signal FIFO over the full run log and folding the
 * gate's own `TimerFired` as a competing mover. Returns whether a delivered
 * signal moved the gate (with the bound payload and its `signalId`) or the
 * gate's timer fired first, or `undefined` when the log shows nothing moved it
 * (a corrupt in-flight residual the caller surfaces loudly).
 *
 * The caller guarantees, via {@link hasForeignSameNameAwaiter}, that
 * `selfStepId` is the SOLE awaiter of `signalName`, so the reducer's global
 * "first awaiting step for this name" scan can only ever resolve to this gate;
 * a per-gate replay therefore reproduces the reduction faithfully. It mirrors
 * the two reducer rules that decide the binding: a delivery arriving with no
 * awaiter present queues, and the gate's `SignalAwaited` drains the queue HEAD
 * (oldest-first), while a redelivered `signalId` is a dedup no-op. When
 * `selfTimerId` is given, a `TimerFired` for it moves the gate off
 * `awaiting-signal` exactly as `handleTimerFired` does, so whichever of the
 * delivered signal or the fired timer moves the gate first wins the race.
 *
 * Deliberately NOT merged with {@link boundSignalForContainerAwait}: that
 * binder is byte-frozen (the container-relay correctness crux), pairs a
 * container awaiting single-file by `SignalAwaited` seq, honors
 * `SignalAwaitAbandoned`, and returns a bound `signalId`; this one folds a
 * timer mover and returns a timeout-vs-payload discriminant for a plain gate.
 * Unifying them would require editing the frozen binder.
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
 * anywhere in the run. `reconstructGateOutcome` replays the reducer FIFO scoped
 * to a SINGLE awaiter of the name, so a second same-name awaiter -- even one
 * that already COMPLETED -- breaks that assumption: the log can no longer say
 * which gate consumed which delivery. The in-flight short-circuit refuses that
 * topology rather than risk binding a payload to the wrong gate; only a run
 * where `selfStepId` is the sole awaiter of the name is provably unambiguous. A
 * completed same-name sibling is the case a phase-scoped in-flight count would
 * miss, so the predicate keys on the durable `SignalAwaited` marker, which
 * outlives the sibling's completion.
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
 * Find an unfired pending timer bound to `stepId` in the reduced state.
 * On a re-park resume of an awaiting-signal step with a timeout, its
 * original `TimerSet` is still pending (a fired timeout would have moved
 * the step to `in-flight`); re-arming re-adopts it rather than minting a
 * duplicate.
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
 * Recover the signal name a step is parked on from the reduced state. A
 * step in `awaiting-signal` carries its channel in `awaitingSignal.name`,
 * reduced from the step's durable `SignalAwaited`. On a crash-resume the
 * agent `step`-suspend arm re-enters `runStep` with only the definition's
 * `StepPrimitive`, which does NOT carry the runtime-minted
 * `signalName(correlationId)` channel (that name lives only on the log);
 * this recovers it so the resume can re-park on the same channel. Mirrors
 * `findUnfiredTimerForStep`: a per-step lookup from reduced state.
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
 * Signal-park core, shared by `runAwaitSignal` and the step-suspend arm
 * in `runStep`. Given a step already marked started, it emits the
 * `SignalAwaited` marker (unless the step is already `awaiting-signal` on
 * a re-park resume), arms the optional timeout timer, flushes the
 * segment, parks on the signal channel via `awaitNext`, and on a received
 * signal commits `SignalReceived` and returns the payload WITHOUT
 * completing the step.
 *
 * The completion seam stays with the caller: `runAwaitSignal` completes
 * the gate with the raw payload, whereas the `runStep` step-suspend arm
 * re-invokes the agent against the payload and completes with the reply.
 *
 * The seam: this helper owns only the park/flush/awaitNext/resolve block.
 * The two pieces of resume idempotency that sit ABOVE it stay with the
 * caller, because the `step`-origin caller diverges there:
 *
 *   - The in-flight-with-logged-`SignalReceived` short-circuit (the
 *     crash-after-signal-before-`StepCompleted` window) recovers a payload
 *     bound to an `awaitSignal` gate by signal name, which is not how a
 *     step-suspend caller would recover.
 *   - The `StepStarted` emit is owned by the caller: `runAwaitSignal` owns
 *     its gate's `StepStarted`, whereas an agent step emits its own via
 *     the normal `runStep` entry.
 *
 * `state` is the reduced state as of the `SignalAwaited` decision point;
 * the caller has already emitted `StepStarted` (and reloaded) when
 * starting fresh. The re-park guard here reads `state` to skip re-emitting
 * `SignalAwaited` when the step is already `awaiting-signal`.
 *
 * Returns a discriminated result rather than throwing on timeout: a park with
 * a `timeout` may resolve either with the delivered signal (`timedOut: false`)
 * or because its timer fired (`timedOut: true`), and only the caller knows
 * whether a fired timer routes onward (`awaitSignal.onTimeout`) or fails the
 * step. A park with no `timeout` never yields `timedOut: true`.
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
  // Re-emit `SignalAwaited` only when the gate is not already awaiting it.
  // On a re-park resume the gate is already `awaiting-signal` (StepStarted
  // + SignalAwaited durable), so this is skipped and the tail re-parks on
  // the signal channel for a signal that has not yet arrived.
  //
  // A fresh control-plane suspension also notifies the host so the hub can
  // co-write the approval/correlation rows, but that notify is DEFERRED to
  // after the durable flush below (see the `env.onPark` call past the
  // flush). The correlationId must be recorded in the workflow log (this
  // `SignalAwaited` event) before it is transmitted to the hub: a crash
  // between the notify and the flush would otherwise strand a hub row for a
  // correlation the log cannot reconstruct on resume. Capturing the park
  // here but transmitting after the flush keeps identity durable-before-
  // transmit. The park is captured only on the fresh-emit branch, so a
  // re-park resume (which skips the SignalAwaited re-emit) does not
  // re-notify on every scheduler pass; the initial park notifies exactly
  // once, and the host's idempotent register absorbs the re-emit driven
  // from durable state on a later re-establishment.
  let parkToNotify: WorkflowPark | undefined;
  // Author `awaitSignal` gate (non-reserved name) captured on the fresh park
  // so the suspendable-child seam's `onSignalPark` sink fires after the flush,
  // mirroring the `parkToNotify` deferral below. Set only on the author branch.
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
      // Record the park kind so recovery can distinguish an input park from an
      // approval one after a crash/reconnect (see parked-correlations).
      ...(opts.parkKind !== undefined ? { parkKind: opts.parkKind } : {}),
    };
    state = await commit(env, runId, awaited);
    // Only a park on a reserved `signalName(correlationId)` channel (the
    // agent-step suspend arm) carries a correlation the resolver routes a
    // decision back on; a plain `awaitSignal` gate parked on an
    // author-chosen name is not a control-plane suspension, so
    // `correlationIdFromSignalName` returns `undefined` and no notify fires.
    const correlationId = correlationIdFromSignalName(opts.signalName);
    if (correlationId !== undefined) {
      if (opts.parkKind === "input") {
        // An input park carries no snapshot and does NOT notify the hub.
        // The supervisor (not the hub) owns the next input delivery, so
        // the child forwards the correlationId upstream so the supervisor
        // can cache it for signal delivery.
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
        // treats it as mandatory). This is the layer that owns the park, so it
        // is where the invariant is enforced: a correlated suspend that carries
        // no snapshot -- e.g. a director `caps.suspend` -- fails loud here rather
        // than mislabelling a snapshot-less park and crashing the co-write three
        // hops downstream. Failing before the flush is correct: nothing has been
        // transmitted yet, so a snapshot-less park leaves no durable or hub trace.
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
  // The per-step timeout commits TimerSet before asking the scheduler
  // to fire, so the pairing with the scheduler-committed `TimerFired`
  // is explicit in the log. Without TimerSet, a production scheduler
  // that reads logs at startup to re-arm unfired timers cannot see
  // signal-await timeouts -- the deadline would be silently lost
  // across a crash. The scheduler is the single writer of TimerFired
  // to the log; the runtime body only commits TimerSet here and then
  // tails the log for TimerFired via `repoStore.subscribe`.
  let timerId: string | undefined;
  let fireAtDate: Date | undefined;
  let subscribeFromSeq: number | undefined;
  if (opts.timeout !== undefined) {
    let beforeTimer = await reloadState(env, runId);
    // On a re-park resume the durable log already carries this step's
    // TimerSet with its original id in `pendingTimers` (unfired -- a fired
    // timeout would have moved the step to `in-flight` and been refused).
    // Re-adopt that timer rather than minting a second one: a duplicate
    // TimerSet would double-count the deadline and leave two scheduler
    // entries racing to fire.
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

  // Segment boundary: the run is about to park on the signal channel
  // (and, when a timeout is set, tail the durable log for the
  // scheduler-committed `TimerFired`). Flush the buffered
  // `SignalAwaited` (+ `TimerSet`) to durable storage BEFORE parking so
  // (a) the out-of-process scheduler can tail the durable `TimerSet`
  // and arm the timeout, (b) a crash-while-suspended leaves a
  // complete pre-suspension log that resume reconstructs the
  // awaiting-signal state from, and (c) the control-plane suspension is
  // durable in the log before the host is notified below, so the hub is
  // never told about a correlation the log cannot reconstruct on resume.
  // `subscribeFromSeq` above was computed from the in-memory tip; the
  // flush makes the durable tip match, so the timer-watch subscription
  // starts exactly past the flushed markers.
  await flush(env, runId);

  // Notify the host of the fresh control-plane suspension only now that it
  // is durable in the log. Transmitting the correlationId after the flush
  // is the invariant that closes the crash-across-park boundary: a crash
  // before this point leaves no hub row (the notify never fired) and the
  // log has no `SignalAwaited`, so resume re-parks cleanly; a crash after
  // it leaves both the durable `SignalAwaited` and (possibly) the hub row,
  // which the re-emit driven from durable state reconciles idempotently.
  if (parkToNotify !== undefined) {
    env.onPark?.(parkToNotify);
  }
  // The author-signal sibling notify: a suspendable-child body's `awaitSignal`
  // gate is surfaced up so the section can proxy it. Fired after the flush like
  // `onPark` so the `SignalAwaited` is durable before the section observes it.
  if (signalParkToNotify !== undefined) {
    env.onSignalPark?.({ runId, name: signalParkToNotify });
  }

  // Drain observation point #4: signal-park entry. If drain has
  // fired and the step's behavior is `"cancel"` (an awaitSignal whose
  // author explicitly opted in to cancel-on-drain), abort
  // immediately. `awaitSignal` defaults to `"wait"` so the typical
  // human-in-the-loop pause sits through drain untouched -- the
  // supervisor's drainTimeout accumulator pauses while this step is
  // the in-flight work.
  if (shouldAbortForDrain(env.drain, opts.stepId)) {
    throw new Error("aborted: drain requested");
  }
  const combinedAbort = new AbortController();
  const onOuterAbort = (): void => {
    combinedAbort.abort();
  };
  abort.addEventListener("abort", onOuterAbort, { once: true });
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
    // Distinguish timeout from outer cancellation: the safe-runner's
    // catch treats `cancelling` phase specially, but a timeout that
    // fires while the run is still `running` is not a failure here -- it
    // is a routing decision the caller owns (route onward via onTimeout,
    // or fail the step). Return the discriminated timeout rather than
    // throwing. The scheduler has already committed TimerFired by the time
    // the watch loop set `timerFired = true`; the runtime body MUST NOT
    // commit a second TimerFired here -- single-writer is the invariant.
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
 * directly. Every control-plane park (approval, input, signal-relay) carries no
 * `timeout`, so it never yields the discriminated timeout; a timeout here is a
 * wiring bug that {@link requireSignalPayload} surfaces loudly. Only the
 * `awaitSignal` gate, which owns the fail-vs-route decision, calls
 * `parkOnSignalResult` directly.
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
  // Read the log once for resume idempotency. On a run re-driving the
  // durable log, the gate's `StepStarted`/`SignalAwaited`/`TimerSet` are
  // already committed; re-emitting any of them throws in the state
  // machine, so each marker below is emitted only when absent (mirroring
  // runLoop). A fresh gate has no state entry, so all three emit normally.
  let state = await reloadState(env, runId);
  const resumed = state.steps.has(primitive.id);

  // Short-circuit resume: an `awaitSignal` step found `in-flight` means a
  // mover already took it off `awaiting-signal` -- a `SignalReceived` (or a
  // pre-await queued signal consumed by `SignalAwaited`), or, for a timed gate,
  // a `TimerFired`. The step only lacks its `StepCompleted` (or, on timeout,
  // its routing/failure) -- the crash-after-move-before-StepCompleted window
  // (`isResumableReceivedAwaitSignalStep`). The reduced `StepState` records
  // neither which mover won nor the payload, so reconstruct both from the
  // durable log and complete without parking on `awaitNext`.
  if (resumed && state.steps.get(primitive.id)?.phase === "in-flight") {
    const log = await env.repoStore.read(runId);
    // The in-flight gate's payload survives only on the log; the replay below
    // binds it by signal name, which is faithful only while this gate is the
    // sole awaiter of the name. Another step awaiting the same name -- even a
    // sibling that already completed -- makes the binding ambiguous, so refuse
    // rather than risk recovering a wrong payload.
    if (hasForeignSameNameAwaiter(log, primitive.name, primitive.id)) {
      throw new RuntimeResumeUnsupportedError(
        primitive.id,
        "in-flight",
        `another awaitSignal gate for ${primitive.name} awaited the signal on a different step, so the consumed signal cannot be unambiguously bound to step ${primitive.id}`,
      );
    }
    // Reconstruct which mover took the gate off `awaiting-signal`: a delivered
    // signal (recovering its payload) or, for a timed gate, the gate's own
    // `TimerFired`. That timer is the gate's durable `TimerSet`; a no-timeout
    // gate has none to fold. Fail loud if the log shows nothing moved the gate.
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
  // block live in the shared `parkOnSignal` core. The two resume
  // idempotency pieces ABOVE this call -- the in-flight-received
  // short-circuit and the `StepStarted` emit -- stay here because
  // runAwaitSignal owns its gate's `StepStarted` (the step-suspend arm
  // emits its own via runStep) and recovers the crash-window payload by
  // binding it to an awaitSignal gate by name. The completion seam is
  // also owned here: an awaitSignal gate completes with the raw delivered
  // payload, whereas the step-suspend arm re-invokes and completes with a
  // reply.
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
  // branch schedule off its `after`), exactly as `routeLoopOutcome` does for a
  // loop's onExhausted. A fired timer routes to the onTimeout target and prunes
  // the normal successors; a delivered signal takes the normal successors and
  // prunes the onTimeout branch. The gate completes either way -- a long-lived
  // section keeps working through a timed-out body gate instead of failing.
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
  const delay = primitive.duration ?? computeDelayToUntil(primitive.until, env);
  await emitStepStartedWithValue(env, runId, primitive.id, {
    ...(primitive.duration !== undefined
      ? { duration: primitive.duration }
      : {}),
    ...(primitive.until !== undefined ? { until: primitive.until } : {}),
    ...(primitive.drainBehavior !== undefined
      ? { drainBehavior: primitive.drainBehavior }
      : {}),
  });
  let state = await reloadState(env, runId);
  const timerId = env.newId("timer");
  const fireAtDate = new Date(env.clock().getTime() + delay);
  const timerSet: WorkflowEvent = {
    kind: "TimerSet",
    seq: state.lastSeq + 1,
    at: env.clock().toISOString(),
    timerId,
    fireAt: fireAtDate.toISOString(),
    stepId: primitive.id,
  };
  state = await commit(env, runId, timerSet);
  await waitForTimer(
    env,
    runId,
    timerId,
    fireAtDate,
    abort,
    env.drain,
    primitive.id,
  );
  void state;
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
  void parent;
  // Bound the spawn chain BEFORE committing StepStarted/ChildSpawned. A
  // reject here lands a clean StepFailed on this spawn step (runPrimitiveSafe
  // synthesizes it) and never writes a phantom child-run log. The child runs
  // one rung deeper; the ceiling is tree-wide (threaded from this run).
  const childDepth = depth + 1;
  assertSpawnDepthWithinLimit(childDepth, primitive.id, maxChildSpawnDepth);
  // Post-extraction the child definition is the internal `{ ref }` handle: the
  // deploy step lifts the authored inline child to a standalone definition and
  // the host resolves it from an in-memory closure map keyed by this ref. An
  // inline child reaching the runtime is a deploy-step bug -- the same
  // contract `runOnTrigger` enforces on its body.
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
  // Allocate the child run-id locally and commit StepStarted +
  // ChildSpawned *before* invoking the spawn callback so the parent
  // audit log records the spawn ahead of any child-side work. A
  // crash between the spawn-launch and the post-await commit would
  // otherwise leave the parent log with no record that the child
  // was spawned at all, and a concurrent cancel sweep iterating
  // state.children would not find the child to issue
  // ChildCancelRequested against.
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
  // Segment boundary: the parent is about to hand off to and AWAIT a
  // sub-run (which commits its own events -- including its terminal --
  // to the same workflow-run repo while this await blocks). Flush the
  // parent's buffered pre-spawn events (RunStarted .. ChildSpawned) to
  // durable storage BEFORE the child runs so the parent's audit log
  // records the spawn ahead of any child-side work and a concurrent
  // cancel sweep iterating state.children finds the child to cascade
  // against -- the exact invariant the ChildSpawned-before-spawn
  // ordering above exists to uphold. Without this flush the parent's
  // RunStarted would not become durable until the parent's own
  // terminal, so its runs/<parentRunId>/ subtree would materialize
  // AFTER its children's.
  await flush(env, parentRunId);
  // Wrap the spawn callback so a throw still lands a closing
  // ChildCompleted event for the orphan. Without this, ChildSpawned
  // would persist in state.children with `terminalStatus: undefined`
  // and a future resume would treat it as a live child to cascade
  // cancellation to. The catch commits ChildCompleted with status
  // "failed" so state.children stays coherent, then rethrows so
  // runPrimitiveSafe lands StepFailed on the parent's spawn step.
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
    // A child run that ended `failed` or `cancelled` propagates to
    // the parent step as a failure. The runtime is the layer with
    // enough information to know the child did not succeed; pushing
    // the decision to a downstream gate makes the gating mandatory
    // and silent-if-forgotten. runPrimitiveSafe's catch lands the
    // StepFailed when the throw bubbles out of this runner.
    throw new ChildWorkflowFailedError(
      `child run ${childRunId} (${definitionRef}) ended ${child.terminalStatus}`,
      child.terminalStatus,
    );
  }
  const output = { childRunId, terminalStatus: child.terminalStatus };
  await emitStepCompletedWithValue(env, parentRunId, primitive.id, output);
  return output;
}

/**
 * Sentinel error type the `childWorkflow` primitive throws when the
 * spawned child run ends in a non-success terminal phase. The runtime
 * body's safe-runner catches it and commits `StepFailed` on the
 * parent's spawn step; downstream parent steps then see the parent
 * step as failed rather than `completed` with a hidden
 * `terminalStatus` payload.
 */
class ChildWorkflowFailedError extends Error {
  readonly childTerminalStatus: "failed" | "cancelled";
  constructor(message: string, childTerminalStatus: "failed" | "cancelled") {
    super(message);
    this.name = "ChildWorkflowFailedError";
    this.childTerminalStatus = childTerminalStatus;
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
