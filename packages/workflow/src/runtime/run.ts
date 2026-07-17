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

import type {
  ActionPrimitive,
  AwaitSignalPrimitive,
  ChildWorkflowPrimitive,
  EscalationPrimitive,
  GatePrimitive,
  LoopPrimitive,
  MapPrimitive,
  Primitive,
  SleepPrimitive,
  StepPrimitive,
  WorkflowDefinition,
} from "../definition/index";
import { hashDefinition } from "../definition/index";
import { evaluate, type SelectorContext } from "./selectors";
import {
  hasFailedStep,
  isCrashedInvocationStep,
  isResumableAwaitingSignalStep,
  isResumableInFlightLoopStep,
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
import type { RunResult, WorkflowRun, WorkflowRuntimeEnv } from "./env";
import { shouldAbortForDrain } from "./drain";
import { RuntimeResumeUnsupportedError } from "./errors";
import { scopedStepId } from "./step-scope";
import {
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
   * later), and an `awaitSignal` step (no timeout) left `in-flight` by an
   * already-logged `SignalReceived` (runAwaitSignal completes it from that
   * logged event -- the crash-after-signal-before-StepCompleted window).
   * A seed log whose tail leaves an invocation-boundary step -- an agent
   * `step` or a deterministic `action` -- `in-flight` is a crash
   * mid-invocation: the runtime settles it as a terminal `StepFailed`
   * (at-most-once refusal) instead of re-invoking it. A step left
   * `awaiting-timer`, mid-`map`, or otherwise `in-flight` (a
   * `childWorkflow`, or a timeout-bearing `awaitSignal` left `in-flight`
   * and indistinguishable in reduced state from a fired timeout) stays
   * unsupported and surfaces as `RuntimeResumeUnsupportedError`.
   *
   * When omitted, the runtime reduces canonical state from the durable
   * log for `runId`: an empty log starts fresh and emits `RunStarted`;
   * a non-terminal log is adopted and driven to terminal -- the same
   * recovery the seed path performs, for a supervisor re-fire that
   * carries no seed; an already-terminal log is returned as-is without
   * re-driving.
   */
  resumeFromEvents?: readonly WorkflowEvent[];
}

/**
 * Run a workflow against a `WorkflowRuntimeEnv`. The function is the
 * single runtime body invoked by both `runLocal` and the (future)
 * child-process entry point.
 *
 * Resume contract: recovery runs against canonical state, whether it
 * arrived as an `options.resumeFromEvents` seed or was reduced from a
 * durable log this call adopts (a supervisor re-fire that carries no
 * seed). A supplied seed log must satisfy two constraints:
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
 *      `awaiting-signal`, or -- with no timeout -- `in-flight` from a
 *      received signal). An invocation-boundary step -- an agent `step`
 *      or a deterministic `action` -- left `in-flight` is a crash
 *      mid-invocation and settles as a terminal `StepFailed` rather
 *      than re-invoking. A step left `awaiting-timer`, mid-`map`, or
 *      otherwise `in-flight` (including a timeout-bearing `awaitSignal`
 *      left `in-flight`) is unsupported: the runtime body has no surface
 *      for re-arming the timer scheduler entry or the inner-map
 *      iteration state from the log alone, so it surfaces as
 *      `RuntimeResumeUnsupportedError` and the host (supervisor) owns
 *      the recovery decision.
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
  // no-op for a fresh re-fire that supplied no seed.
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
  // prior (crashed) process left on disk that this process is re-firing
  // fresh (the supervisor re-fires a parked inbound message with
  // `runId = messageId` and NO `resumeFromEvents`). Reducing the durable
  // log answers the only question that matters for recovery -- "does the
  // canonical log carry residual work?" -- identically for both, so
  // every decision below (terminal short-circuit, crashed-in-flight
  // settling, the RunStarted emit) keys on canonical state rather than
  // on how the events reached this process.
  let state = await reloadState(env, runId);

  // Terminal short-circuit. A re-fire whose canonical log is already
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
  // an `in-flight` coordination container (mid-`map`, timeout-bearing
  // `awaitSignal` reduced to `in-flight`, `childWorkflow`), or an
  // `awaiting-signal`/`awaiting-timer` step -- still surfaces
  // `RuntimeResumeUnsupportedError`: those have a live re-arming surface
  // the host owns (rebuild the map state, distinguish a fired timeout
  // from a received signal, re-park on a later signal), so declining
  // honestly is correct there.
  //
  // The pass runs whenever canonical state is `running`, whether the
  // residual arrived via a `resumeFromEvents` seed OR was adopted from a
  // durable log this process is re-firing fresh. Keying on
  // `state.phase === "running"` (not on whether a seed was supplied) is
  // what settles a crashed step under the fresh re-fire recovery; the
  // RunStarted emit below is skipped in that case because the canonical
  // phase is already `running`.
  const crashedInFlight: { stepId: string; attempt: number }[] = [];
  if (state.phase === "running") {
    for (const [stepId, stepState] of state.steps) {
      // Resumable carve-outs, each re-offered by `nextSchedulable` on the
      // SAME predicate:
      //   - a mid-loop container (or its in-flight synthetic iteration
      //     step): runLoop re-derives its cursor from the log;
      //   - an `awaitSignal` step still `awaiting-signal`: runAwaitSignal
      //     skips its already-emitted markers and re-parks on the signal
      //     channel, holding a live awaiter for a later signal;
      //   - an `awaitSignal` step (no timeout) left `in-flight` by an
      //     already-logged `SignalReceived`: runAwaitSignal short-circuits
      //     to completion from that logged event (the
      //     crash-after-signal-before-StepCompleted window).
      if (
        isResumableInFlightLoopStep(definition, stepId, stepState.phase) ||
        isResumableAwaitingSignalStep(definition, stepId, stepState.phase) ||
        isResumableReceivedAwaitSignalStep(definition, stepId, stepState.phase)
      ) {
        continue;
      }
      // A crashed-mid-invocation step (agent step or action). Collect it
      // for terminal settling AFTER this loop -- committing StepFailed
      // inline would leave `state` stale and merely relocate the stall to
      // the main loop.
      if (isCrashedInvocationStep(definition, stepId, stepState.phase)) {
        crashedInFlight.push({
          stepId,
          attempt: stepState.currentAttempt,
        });
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
      // (an empty or RunStarted-less log), so the fresh re-fire recovery
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
  // adopted from a durable log this process is re-firing fresh (the
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
 * already terminal, without re-driving it. Used by the fresh-re-fire
 * terminal short-circuit: a re-fire against an already-terminal durable
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
): Promise<unknown> {
  switch (primitive.kind) {
    case "step":
      return runStep(env, runId, primitive, selectorCtx, abort);
    case "action":
      return runAction(env, runId, primitive, selectorCtx, abort);
    case "loop":
      return runLoop(definition, env, runId, primitive, selectorCtx, abort);
    case "map":
      return runMap(env, runId, primitive, selectorCtx, abort);
    case "gate":
      return runGate(definition, env, runId, primitive, selectorCtx, abort);
    case "awaitSignal":
      return runAwaitSignal(env, runId, primitive, abort);
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
): Promise<unknown> {
  try {
    return await runPrimitive(
      definition,
      env,
      runId,
      primitive,
      selectorCtx,
      abort,
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
  let resumeFromPark: { signalName: string; correlationId: string } | undefined;
  if (entryState.steps.get(step.id)?.phase === "awaiting-signal") {
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
    resumeFromPark = { signalName: parkedSignalName, correlationId };
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
      let resume: { correlationId: string; decision: unknown } | undefined;
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
        resume = { correlationId: resumeFromPark.correlationId, decision };
        resumeFromPark = undefined;
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
          break;
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
          },
          parkState,
          stepAbort.signal,
        );
        resume = { correlationId: result.suspend.correlationId, decision };
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
): Promise<unknown> {
  const runLoopIteration = env.runLoopIteration;
  const loopFns = env.loopFns;
  if (runLoopIteration === undefined || loopFns === undefined) {
    throw new Error(
      `loop ${primitive.id} requires runLoopIteration and loopFns on the env; this host does not support loops`,
    );
  }
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

  let iterations = iteration;
  for (let i = iteration; !terminated && i < primitive.maxIterations; i += 1) {
    iterations = i + 1;
    const stepId = scopedStepId(primitive.id, i);
    const childRunId = `${primitive.id}__${String(i)}`;

    state = await reloadState(env, runId);
    if (!state.steps.has(stepId)) {
      await emitStepStartedWithValue(env, runId, stepId, currentInput);
    }
    state = await reloadState(env, runId);
    if (!state.children.has(childRunId)) {
      const spawned: WorkflowEvent = {
        kind: "ChildSpawned",
        seq: state.lastSeq + 1,
        at: env.clock().toISOString(),
        stepId,
        childRunId,
        childDefinitionRef: primitive.body.id,
      };
      state = await commit(env, runId, spawned);
      void state;
    }
    // Flush the spawn record durable before the child runs so a resumed
    // parent log records the spawn ahead of any child-side work.
    await flush(env, runId);

    const res = await runLoopIteration({
      bodyDefinition: primitive.body,
      childRunId,
      input: currentInput,
      parentRunId: runId,
      parentStepId: stepId,
      signal: abort,
    });

    let after = await reloadState(env, runId);
    if (after.children.get(childRunId)?.terminalStatus === undefined) {
      const childCompleted: WorkflowEvent = {
        kind: "ChildCompleted",
        seq: after.lastSeq + 1,
        at: env.clock().toISOString(),
        childRunId,
        terminalStatus: res.terminalStatus,
      };
      after = await commit(env, runId, childCompleted);
      void after;
    }
    after = await reloadState(env, runId);
    if (after.steps.get(stepId)?.phase !== "completed") {
      await emitStepCompletedWithValue(env, runId, stepId, res.output);
    }
    await flush(env, runId);

    if (res.terminalStatus !== "completed") {
      // A failed or cancelled iteration is a real failure, not an
      // exhaustion. Throw so runPrimitiveSafe lands StepFailed (or
      // CancelPropagated when the run is cancelling) on the loop node.
      // Note: throwing skips routeLoopOutcome, so neither branch is
      // pruned; both the normal dependents and onExhausted then run
      // before the run settles failed, per the engine's "a failed
      // dependency is resolved" scheduling (the same as a failed gate).
      // The mutually-exclusive routing holds only on the success path.
      throw new Error(
        `loop ${primitive.id} iteration ${String(i)} ended ${res.terminalStatus}`,
      );
    }

    if (!whileFn(res.output, currentInput)) {
      outcome = "converged";
      break;
    }
    if (i + 1 >= primitive.maxIterations) {
      outcome = "exhausted";
      break;
    }
    currentInput = carryFn(res.output, currentInput);
  }

  await routeLoopOutcome(definition, env, runId, primitive, outcome, abort);
  const output = { outcome, iterations, carry: currentInput };
  await emitStepCompletedWithValue(env, runId, primitive.id, output);
  return output;
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

/**
 * Recover the `SignalReceived` that consumed an awaitSignal step on a
 * resume that finds the step `in-flight`. The reduced `StepState` carries
 * no payload, so the payload survives only on the durable log. Match the
 * signal by name and by membership in `observedSignalIds` (the reduction
 * that moved the step off `awaiting-signal` also recorded the signal's id
 * there), preferring the last such event so a name that legitimately
 * carried multiple deliveries resolves to the one actually consumed.
 */
function findConsumedSignal(
  log: readonly WorkflowEvent[],
  signalName: string,
  state: RunState,
): { payload: unknown; signalId: string } | undefined {
  let found: { payload: unknown; signalId: string } | undefined;
  for (const event of log) {
    if (
      event.kind === "SignalReceived" &&
      event.signalName === signalName &&
      state.observedSignalIds.has(event.signalId)
    ) {
      found = { payload: event.payload, signalId: event.signalId };
    }
  }
  return found;
}

/**
 * Count how many `awaitSignal` gates for `signalName` are sitting in the
 * same crash window as the step being recovered: gates that emitted a
 * `SignalAwaited` for this name and are still `in-flight` (consumed a
 * signal, no `StepCompleted` yet). `findConsumedSignal` binds a payload to
 * a gate by signal name alone, so when more than one same-name gate is
 * concurrently in this window it cannot tell which gate consumed which
 * delivery -- returning the last matching payload would silently bind it
 * to the wrong gate. This count lets the short-circuit path refuse that
 * ambiguous topology while still recovering the provably-unambiguous
 * single-gate case.
 */
function countConcurrentInFlightAwaiters(
  log: readonly WorkflowEvent[],
  signalName: string,
  state: RunState,
): number {
  const awaiterStepIds = new Set<string>();
  for (const event of log) {
    if (event.kind === "SignalAwaited" && event.signalName === signalName) {
      awaiterStepIds.add(event.stepId);
    }
  }
  let count = 0;
  for (const stepId of awaiterStepIds) {
    if (state.steps.get(stepId)?.phase === "in-flight") {
      count += 1;
    }
  }
  return count;
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
 */
async function parkOnSignal(
  env: WorkflowRuntimeEnv,
  runId: string,
  opts: {
    stepId: string;
    signalName: string;
    timeout?: number;
  },
  state: ReturnType<typeof resumeFromLog>,
  abort: AbortSignal,
): Promise<unknown> {
  // Re-emit `SignalAwaited` only when the gate is not already awaiting it.
  // On a re-park resume the gate is already `awaiting-signal` (StepStarted
  // + SignalAwaited durable), so this is skipped and the tail re-parks on
  // the signal channel for a signal that has not yet arrived.
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
    };
    state = await commit(env, runId, awaited);
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
  // and arm the timeout, and (b) a crash-while-suspended leaves a
  // complete pre-suspension log that resume reconstructs the
  // awaiting-signal state from. `subscribeFromSeq` above was computed
  // from the in-memory tip; the flush makes the durable tip match, so
  // the timer-watch subscription starts exactly past the flushed
  // markers.
  await flush(env, runId);

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
    return received.payload;
  } catch (cause) {
    // Distinguish timeout from outer cancellation: the safe-runner's
    // catch treats `cancelling` phase specially, but a timeout that
    // fires while the run is still `running` must surface as
    // StepFailed. The scheduler has already committed TimerFired by
    // the time the watch loop set `timerFired = true`; the runtime
    // body MUST NOT commit a second TimerFired here -- single-writer
    // is the invariant.
    if (timerFired) {
      throw new Error(
        `signal-await on ${opts.signalName} timed out after ${String(opts.timeout)}ms`,
      );
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

async function runAwaitSignal(
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

  // Short-circuit resume: an `awaitSignal` step found `in-flight` means an
  // already-logged `SignalReceived` (or a pre-await queued signal consumed
  // by `SignalAwaited`) moved it off `awaiting-signal`. The signal is
  // logically received; the step only lacks its `StepCompleted` -- the
  // crash-after-signal-before-StepCompleted window
  // (`isResumableReceivedAwaitSignalStep`). The payload survives only on
  // the durable log (the reduced `StepState` carries no payload slot), so
  // recover it from the logged `SignalReceived` and complete without
  // parking on `awaitNext`. The predicate admits this shape only when the
  // step has no timeout, so a `TimerFired`-induced `in-flight`
  // (indistinguishable in reduced state) never reaches here.
  if (resumed && state.steps.get(primitive.id)?.phase === "in-flight") {
    const log = await env.repoStore.read(runId);
    // `findConsumedSignal` matches by signal name only, so it cannot bind a
    // payload to the right gate when more than one same-name gate consumed a
    // delivery and none has completed. Refuse that ambiguous topology rather
    // than silently recovering a wrong payload; the single-gate case (count
    // === 1) stays provably correct.
    if (countConcurrentInFlightAwaiters(log, primitive.name, state) > 1) {
      throw new RuntimeResumeUnsupportedError(
        primitive.id,
        "in-flight",
        `more than one concurrent awaitSignal gate for ${primitive.name} consumed a signal with no StepCompleted, so the consumed payload cannot be bound to a gate`,
      );
    }
    const received = findConsumedSignal(log, primitive.name, state);
    if (received === undefined) {
      throw new Error(
        `runAwaitSignal resume: step ${primitive.id} is in-flight but no consumed SignalReceived for ${primitive.name} is in the log`,
      );
    }
    await emitStepCompletedWithValue(
      env,
      runId,
      primitive.id,
      received.payload,
    );
    return received.payload;
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
  const payload = await parkOnSignal(
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
  await emitStepCompletedWithValue(env, runId, primitive.id, payload);
  return payload;
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
): Promise<unknown> {
  void parent;
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
    definitionRef: primitive.definitionRef,
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
    childDefinitionRef: primitive.definitionRef,
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
      definitionRef: primitive.definitionRef,
      childRunId,
      input: childInput,
      parentRunId,
      parentStepId: primitive.id,
      signal: abort,
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
      `child run ${childRunId} (${primitive.definitionRef}) ended ${child.terminalStatus}`,
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
