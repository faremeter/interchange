// The single runtime body.
//
// `runtimeRun` is the only entry point both `runLocal` and the
// future child-process entry point invoke. The body switches on env
// keys -- never on which host process it is running in. An explicit
// source-level test in `run.test.ts` asserts that this file contains
// no environment-shaped discriminator references; the discipline lets
// us swap the env implementations underneath without re-validating the
// body.

import type {
  AwaitSignalPrimitive,
  ChildWorkflowPrimitive,
  EscalationPrimitive,
  GatePrimitive,
  MapPrimitive,
  Primitive,
  SleepPrimitive,
  StepPrimitive,
  WorkflowDefinition,
} from "../definition/index";
import { hashDefinition } from "../definition/index";
import { evaluate, type SelectorContext } from "./selectors";
import { hasFailedStep, isRunDone, nextSchedulable } from "./dag";
import {
  commit as commitToChain,
  dropChain,
  reloadState as reloadStateInChain,
} from "./commit-chain";
import type { RunResult, WorkflowRun, WorkflowRuntimeEnv } from "./env";
import { shouldAbortForDrain } from "./drain";
import { RuntimeResumeUnsupportedError } from "./errors";
import {
  isTerminalRunPhase,
  resumeFromLog,
  TransitionError,
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
   * DAG's `nextSchedulable` set (i.e. the seed log stopped between
   * step boundaries, not mid-primitive). Seed logs whose tail leaves
   * a step `awaiting-signal`, `awaiting-timer`, mid-`map`, or
   * otherwise `in-flight` are unsupported: the in-process runtime
   * has no surface for re-arming the signal channel, the timer
   * scheduler entry, or the inner-map iteration state from the log
   * alone. `runtimeRun` rejects such seed logs with
   * `RuntimeResumeUnsupportedError` rather than stalling.
   *
   * When omitted, the runtime starts fresh from `emptyState(runId)`
   * and emits `RunStarted` itself.
   */
  resumeFromEvents?: readonly WorkflowEvent[];
}

/**
 * Run a workflow against a `WorkflowRuntimeEnv`. The function is the
 * single runtime body invoked by both `runLocal` and the (future)
 * child-process entry point.
 *
 * Resume contract: when `options.resumeFromEvents` is supplied, the
 * seed log must satisfy two constraints:
 *
 *   1. The env's `BlobSubstrate` either holds the blob: refs the seed
 *      log references, or is durable enough that the refs are
 *      resolvable by the same substrate that minted them. The
 *      `runLocal` in-memory substrate is `ephemeral` and starts empty
 *      per instance; resume against a fresh one with a seed log that
 *      contains blob: refs fails fast with a targeted error.
 *   2. The seed log is either complete-or-cancelled or aligned on
 *      step boundaries -- mid-step resume (a step left
 *      `awaiting-signal`, `awaiting-timer`, mid-`map`, or otherwise
 *      `in-flight`) is unsupported. The runtime body has no surface
 *      for re-arming the signal channel, the timer scheduler entry,
 *      or the inner-map iteration state from the log alone. Such
 *      seed logs surface as `RuntimeResumeUnsupportedError`; the
 *      host (supervisor) owns the recovery decision (crash and let
 *      a fresh deploy pick up the live log, re-inject the awaited
 *      signal, etc).
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
        await commit(env, runId, event);
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

function commit(
  env: WorkflowRuntimeEnv,
  runId: string,
  event: WorkflowEvent,
): Promise<ReturnType<typeof resumeFromLog>> {
  return commitToChain(env, runId, event);
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
  let state = resumeFromLog(runId, initialEvents);

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

  // Reject seed logs that leave any step in a non-terminal phase the
  // runtime body cannot re-arm. The DAG's `nextSchedulable` skips any
  // step that already appears in `state.steps` (the safe-runner needs
  // a fresh `StepStarted` to advance one), so a step left
  // `in-flight`, `awaiting-signal`, or `awaiting-timer` would stall
  // the main loop with no schedulable primitive. Surface the
  // limitation honestly rather than dressing it up as a generic
  // stall: the host decides whether to crash, alert, or recover via
  // redeploy. Cancellation paths are exempt -- the cleanup branch
  // owns settling steps whose phase is `cancelling`.
  if (initialEvents.length > 0 && state.phase === "running") {
    for (const [stepId, stepState] of state.steps) {
      if (
        stepState.phase === "in-flight" ||
        stepState.phase === "awaiting-signal" ||
        stepState.phase === "awaiting-timer"
      ) {
        throw new RuntimeResumeUnsupportedError(
          stepId,
          stepState.phase,
          `seed log tail leaves step ${stepId} in phase ${stepState.phase} with no schedulable primitive on the DAG`,
        );
      }
    }
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
      // A `cancel("self", ...)` racing the very first `RunStarted`
      // commit can land `CancelRequested` first (legal from `pending`
      // since the state-machine admits early-lifecycle cancellation).
      // The chain then reloads, sees phase=cancelling, and rejects
      // `RunStarted` with `code: "phase"`. Reload and proceed to the
      // main loop, which routes through the cancellation cleanup
      // branch and emits `RunCancelled`.
      if (cause instanceof TransitionError && cause.code === "phase") {
        state = await reloadState(env, runId);
      } else {
        throw cause;
      }
    }
  }

  const inFlight = new Set<string>();
  const stepOutputs: Record<string, unknown> = {};
  // On resume, hydrate stepOutputs from the log's StepCompleted events
  // so downstream steps can resolve `{ from: "steps.<id>.output" }`
  // selectors. Without hydration, the runtime starts with an empty
  // stepOutputs and any selector referencing a previously-completed
  // step's output throws, landing as a spurious StepFailed.
  //
  // Every primitive that produces a value commits its StepCompleted
  // with a substrate-resolvable ref (inline:<json> or blob:<key>);
  // there are no marker refs to filter. An ephemeral substrate that
  // cannot serve the seed log's blob refs surfaces here as a
  // targeted error rather than a deep resolve failure.
  const seedBlobRefs = initialEvents.filter(
    (e): e is typeof e & { kind: "StepCompleted" } =>
      e.kind === "StepCompleted" && e.output.ref.startsWith("blob:"),
  );
  if (seedBlobRefs.length > 0 && env.blobs.ephemeral) {
    throw new Error(
      `resume requires the BlobSubstrate that recorded the seed log's blob refs (${String(seedBlobRefs.length)} blob output(s) present); the runLocal in-memory substrate is ephemeral and starts empty. Pass the originating env, or use a durable substrate.`,
    );
  }
  for (const event of initialEvents) {
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
      state = await commit(env, runId, cancelled);
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
      state = await commit(env, runId, terminal);
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
  return commit(env, runId, cancelled);
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
      state = await commit(env, runId, started);
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
      const result = await env.invokeStep({
        agent: step.agent,
        input,
        authzContext: {
          stepId: step.id,
          attempt,
          runId,
        },
        signal: stepAbort.signal,
      });
      const outputRef = (
        await env.blobs.recordOutput(step.id, attempt, result.output)
      ).ref;
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
      return result.output;
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
      id: `${primitive.id}[${String(i)}]`,
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
  const toSkip = collectBranchClosure(definition, notSelected, selected);
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
 * Compute the set of steps that should be skipped when the gate's
 * not-selected branch is suppressed.
 *
 * The skip set is the transitive downstream closure of the not-selected
 * branch head, MINUS any step that is also reachable from the selected
 * branch head. A diamond-join step that lists both branches in its
 * `after` is reachable from the selected branch and must stay live --
 * its body must still run with the selected branch's output as input.
 */
function collectBranchClosure(
  definition: WorkflowDefinition,
  notSelected: string,
  selected: string,
): readonly string[] {
  // defineWorkflow rejects gate.then === gate.else, so notSelected and
  // selected are guaranteed to differ here. No defensive equality
  // check is needed.
  if (!(notSelected in definition.steps)) return [];
  const reachableFromSelected = downstreamClosure(definition, selected);
  const skip = new Set<string>();
  const queue: string[] = [notSelected];
  while (queue.length > 0) {
    const id = queue.shift();
    if (id === undefined) break;
    if (skip.has(id)) continue;
    if (id === selected) continue;
    if (reachableFromSelected.has(id)) continue;
    skip.add(id);
    for (const [otherId, primitive] of Object.entries(definition.steps)) {
      const after = primitive.after;
      if (after === undefined) continue;
      if (
        after.includes(id) &&
        !skip.has(otherId) &&
        otherId !== selected &&
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
  start: string,
): Set<string> {
  const visited = new Set<string>();
  if (!(start in definition.steps)) return visited;
  const queue: string[] = [start];
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

async function runAwaitSignal(
  env: WorkflowRuntimeEnv,
  runId: string,
  primitive: AwaitSignalPrimitive,
  abort: AbortSignal,
): Promise<unknown> {
  await emitStepStartedWithValue(env, runId, primitive.id, {
    name: primitive.name,
    ...(primitive.timeout !== undefined ? { timeout: primitive.timeout } : {}),
    ...(primitive.onTimeout !== undefined
      ? { onTimeout: primitive.onTimeout }
      : {}),
    ...(primitive.drainBehavior !== undefined
      ? { drainBehavior: primitive.drainBehavior }
      : {}),
  });
  let state = await reloadState(env, runId);
  const awaited: WorkflowEvent = {
    kind: "SignalAwaited",
    seq: state.lastSeq + 1,
    at: env.clock().toISOString(),
    stepId: primitive.id,
    signalName: primitive.name,
    ...(primitive.timeout !== undefined
      ? {
          timeoutAt: new Date(
            env.clock().getTime() + primitive.timeout,
          ).toISOString(),
        }
      : {}),
  };
  state = await commit(env, runId, awaited);
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
  if (primitive.timeout !== undefined) {
    timerId = env.newId("timer");
    fireAtDate = new Date(env.clock().getTime() + primitive.timeout);
    let beforeTimer = await reloadState(env, runId);
    const timerSet: WorkflowEvent = {
      kind: "TimerSet",
      seq: beforeTimer.lastSeq + 1,
      at: env.clock().toISOString(),
      timerId,
      fireAt: fireAtDate.toISOString(),
      stepId: primitive.id,
    };
    beforeTimer = await commit(env, runId, timerSet);
    subscribeFromSeq = beforeTimer.lastSeq + 1;
  }
  void state;

  // Drain observation point #4: runAwaitSignal entry. If drain has
  // fired and the step's behavior is `"cancel"` (an awaitSignal whose
  // author explicitly opted in to cancel-on-drain), abort
  // immediately. `awaitSignal` defaults to `"wait"` so the typical
  // human-in-the-loop pause sits through drain untouched -- the
  // supervisor's drainTimeout accumulator pauses while this step is
  // the in-flight work.
  if (shouldAbortForDrain(env.drain, primitive.id)) {
    throw new Error("aborted: drain requested");
  }
  const combinedAbort = new AbortController();
  const onOuterAbort = (): void => {
    combinedAbort.abort();
  };
  abort.addEventListener("abort", onOuterAbort, { once: true });
  // Listen for drain transitions that land mid-await.
  const onDrain = (): void => {
    if (shouldAbortForDrain(env.drain, primitive.id)) {
      combinedAbort.abort();
    }
  };
  env.drain.signal.addEventListener("abort", onDrain, { once: true });
  let timerDispose: (() => void) | undefined;
  let timerFired = false;
  let timerWaitAbort: AbortController | undefined;
  let timerWatch: Promise<void> | undefined;
  if (
    primitive.timeout !== undefined &&
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
      primitive.name,
      combinedAbort.signal,
    );
    let next = await reloadState(env, runId);
    const signalReceived: WorkflowEvent = {
      kind: "SignalReceived",
      seq: next.lastSeq + 1,
      at: env.clock().toISOString(),
      signalName: primitive.name,
      signalId: received.signalId,
      payload: received.payload,
    };
    next = await commit(env, runId, signalReceived);
    void next;
    await emitStepCompletedWithValue(
      env,
      runId,
      primitive.id,
      received.payload,
    );
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
        `signal-await on ${primitive.name} timed out after ${String(primitive.timeout)}ms`,
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
