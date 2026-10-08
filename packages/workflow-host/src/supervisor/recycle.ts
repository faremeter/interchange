// Supervisor "same deploy tree, fresh process" path. Recycle tears the
// existing workflow-process child down and stands a new one up against
// the SAME deploy tree; it never refetches the tree, consults an updated
// definition, or re-resolves agents -- that is redeploy, a different
// path with different authorization and rollback. A recycle must never
// grow a "maybe also refetch" mode; that would erase the orthogonality.
//
// Six steps, all under the per-repo lock:
//   1. `drain` -- caller-supplied (a no-op for the `crash` origin,
//      where the child is already dead); drainTimeout escalation applies
//      normally.
//   2. `kill` -- SIGTERM, then SIGKILL after the kill-timeout; a no-op
//      on an already-dead handle.
//   3. `respawn` -- fresh channelId, HMAC key, IPC keypair; re-read
//      per-step credentials; spawn via the injected subprocess spawner.
//   4-5. `self-discover` + `resume` -- run inside the child before it
//      emits `ready`; the supervisor only waits, deadline-bounded.
//   6. Buffered mail -- every inbound message enqueues into the
//      substrate-backed inbox regardless of phase; the new dispatch
//      loop dequeues in arrival order (the `receivedAt` prefix
//      preserves FIFO across the gap). No in-memory drain step.
//
// The supervisor holds the mail-bus registration across the gap (no
// re-register). Before the kill, `replayProcessingToInbox()` moves any
// in-flight `processing/` entries back to `inbox/` under their original
// keys; without it the dying dispatch loop leaves an orphaned entry no
// live loop owns.
//
// Three origins funnel through `triggerRecycle`: operator command,
// supervisor policy, and child self-initiated `recycle.request`. All
// land in the same code path; the reason string is the only
// origin-specific data carried forward.

import { getLogger } from "@intx/log";

import { generateKeyPair } from "@intx/crypto";

import {
  createControlChannelSender,
  generateChannelId,
  generateHmacKey,
  receiveControlChannel,
  receiveEventChannel,
  type ControlChannelSender,
  type ControlPayload,
  type EventPayload,
} from "../ipc/index";

import {
  assembleCredentialsSnapshot,
  type CredentialsSnapshot,
} from "./credentials";
import type { SubprocessHandle, WorkflowSupervisorBindings } from "./types";
import { buildChildSpawnEnv } from "./spawn-env";
import {
  DEFAULT_KILL_TIMEOUT_MS,
  DEFAULT_READY_TIMEOUT_MS,
  defaultClearTimer,
  defaultSetTimer,
  killChildHandle,
  waitDeadline,
} from "./child-termination";

const logger = getLogger(["workflow-host", "supervisor", "recycle"]);

/**
 * Bound on the supervisor's mail buffer across the kill/respawn gap.
 * A real workflow's inbound rate is well below this; saturation means
 * an upstream or a recycle is stuck, which the operator must see.
 */
export const MAX_BUFFERED_MAIL = 256;

/**
 * Default supervisor-policy check interval; operator-overridable via
 * the supervisor's policy bindings.
 */
export const DEFAULT_POLICY_INTERVAL_MS = 60_000;

/**
 * Origin tag the recycle path stamps onto its log messages. `crash`
 * is an unexpected child exit driving the same respawn; steps 1-2
 * degrade to no-ops for it (see header).
 */
export type RecycleOrigin = "operator" | "policy" | "self" | "crash";

export interface RecycleAttempt {
  /** Origin the recycle was initiated from. */
  readonly origin: RecycleOrigin;
  /** Human-readable reason carried with the recycle through to the audit log. */
  readonly reason: string;
  /** ChannelId the recycled child was minted with. */
  readonly newChannelId: string;
  /** ChannelId the previous child was running under. */
  readonly previousChannelId: string;
}

/**
 * Per-handle subprocess wiring. The supervisor passes the live
 * child's wiring in and gets the new child's back via
 * `installNewChild`.
 */
export interface ChildWiring {
  handle: SubprocessHandle;
  controlSender: ControlChannelSender;
  channelId: string;
  eventPump: Promise<void>;
}

/**
 * Bindings the supervisor passes into `triggerRecycle` -- the subset
 * of supervisor state the sequence touches. Excludes the
 * mail-subscription disposer and the mail-bus binding: the supervisor
 * holds the registration across the recycle.
 */
export interface RecycleContext {
  /** The supervisor's full bindings, reused on respawn for credentials and spawn. */
  readonly bindings: WorkflowSupervisorBindings;
  /**
   * Every step id in the deployment's flat step-id namespace (the
   * definition's `stepOrder` plus every `loop` body's step ids), for
   * credentials re-assembly.
   */
  readonly stepOrder: readonly string[];
  /** Definition hash carried on respawn env (unchanged across recycle). */
  readonly definitionHash: string;
  /**
   * Warm-keep flag carried on the respawn env (design §3b). Unchanged
   * across recycle: the respawned child rebuilds its empty cache
   * lazily, so the decision must survive.
   */
  readonly warmKeep: boolean;
  /** Forward target for InferenceEvents the new child publishes. */
  readonly onInferenceEvent: (event: EventPayload) => void;
  /** Live child wiring on entry; replaced before return. */
  readonly current: ChildWiring;
  /** Supervisor-side drain primitive; sends the existing drain mail. */
  readonly drain: (deadlineMs: number) => Promise<void>;
  /**
   * Replay any `processing/` entries for the deployment's mail
   * address back to `inbox/` so FIFO survives. Invoked after drain
   * settles, before the kill -- the window where a processing entry
   * has no owner.
   */
  readonly replayProcessingToInbox: () => Promise<void>;
  /**
   * Abort the prior cohort's terminal source and wake its dispatch
   * loop so it exits before the kill. Invoked after drain and replay
   * settle: earlier starves drain accumulators of terminal events;
   * later races the kill against the loop's next iteration.
   */
  readonly abortPriorCohort: () => void;
  /**
   * Sink the supervisor uses to install the new child wiring once
   * the child has emitted `ready` and the credentials snapshot is
   * re-assembled.
   */
  readonly installNewChild: (next: {
    wiring: ChildWiring;
    credentialsSnapshot: CredentialsSnapshot;
    /**
     * Live upstream control iterator the new child's receiver yields.
     * The supervisor's upstream-control pump keeps consuming it after
     * `installNewChild` returns.
     */
    controlIncoming: AsyncGenerator<ControlPayload, void, void>;
  }) => void;
  /**
   * Crash hook the new child's IPC channels wire to, same shape as
   * spawn-time onCrash so a frame violation tears the deployment down
   * through the same path.
   */
  readonly onCrash: (reason: string) => void;
  /**
   * Optional kill-timeout override (ms). Defaults to
   * `DEFAULT_KILL_TIMEOUT_MS`.
   */
  readonly killTimeoutMs?: number;
  /**
   * Deadline (ms) for the respawned child's `ready` handshake,
   * matching the spawn path's bound. The supervisor resolves the
   * effective value; the `??` fallback here only fires for a direct
   * test caller.
   */
  readonly readyTimeoutMs?: number;
  /**
   * Optional drain deadline (ms) used in step 1; the recycle path's
   * own wait before step 2, distinct from the supervisor's
   * drainTimeout accumulator. Defaults to the accumulator's default.
   */
  readonly drainDeadlineMs?: number;
  /**
   * Optional setTimer/clearTimer pair for the SIGKILL escalation
   * wait; tests inject a deterministic timer.
   */
  readonly setTimer?: (cb: () => void, ms: number) => unknown;
  readonly clearTimer?: (handle: unknown) => void;
}

export interface TriggerRecycleOpts {
  origin: RecycleOrigin;
  reason: string;
}

/**
 * Run the six-step recycle sequence. Returns once the new child has
 * emitted `ready`; the supervisor installs the new wiring via
 * `ctx.installNewChild` before that point.
 */
export async function triggerRecycle(
  ctx: RecycleContext,
  opts: TriggerRecycleOpts,
): Promise<RecycleAttempt> {
  const drainDeadlineMs = ctx.drainDeadlineMs ?? 60_000;
  const killTimeoutMs = ctx.killTimeoutMs ?? DEFAULT_KILL_TIMEOUT_MS;
  const previousChannelId = ctx.current.channelId;

  logger.info`recycle ${opts.origin} requested: ${opts.reason} (previousChannelId=${previousChannelId})`;

  // Step 1: drain via the supervisor's shared drain primitive; each
  // in-flight step's `drainBehavior` decides abort vs continue.
  await ctx.drain(drainDeadlineMs);

  // Replay any in-flight `processing/` entries back to `inbox/`
  // before the kill: the dying dispatch loop aborted on cohort
  // teardown and will not reach `markConsumed`, leaving an orphaned
  // entry.
  try {
    await ctx.replayProcessingToInbox();
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    logger.warn`recycle: replayProcessingToInbox before kill failed: ${message}`;
  }

  // Abort the prior cohort now -- after drain and replay ran against
  // a live cohort, before the kill drops the child. Earlier starves
  // drain accumulators of terminal events; later races the loop's
  // next iteration against the controlSender about to disappear.
  ctx.abortPriorCohort();

  // Step 2: kill. SIGTERM first, then SIGKILL after `killTimeoutMs`;
  // the injected spawner's handle owns the Node primitives.
  await killChildHandle(ctx.current.handle, killTimeoutMs, {
    logger,
    ...(ctx.setTimer !== undefined ? { setTimer: ctx.setTimer } : {}),
    ...(ctx.clearTimer !== undefined ? { clearTimer: ctx.clearTimer } : {}),
  });

  // Step 3: respawn. Fresh channelId, HMAC key, Ed25519 IPC keypair;
  // per-step credentials re-read so a grants update since the
  // original spawn is reflected. The deploy tree is UNCHANGED.
  const channelId = generateChannelId();
  const hmacKey = generateHmacKey();
  const ipcKeypair = await (
    ctx.bindings.ipcKeyPairFactory ?? generateKeyPair
  )();
  const env = buildChildSpawnEnv({
    substrateEnv: ctx.bindings.substrateEnv,
    dynamicSpawnEnv: ctx.bindings.dynamicSpawnEnv,
    channelId,
    hmacKey,
    hostPublicKey: ipcKeypair.publicKey,
    anchorRunId: ctx.bindings.anchorRunId,
    deploymentMailAddress: ctx.bindings.deploymentMailAddress,
    stepCount: ctx.bindings.stepCount,
    definitionHash: ctx.definitionHash,
    warmKeep: ctx.warmKeep,
  });

  const handle = ctx.bindings.subprocessSpawner({
    binaryPath: ctx.bindings.binaryPath,
    env,
  });

  const controlSender = createControlChannelSender({
    privateKeySeed: ipcKeypair.privateKey,
    channelId,
    writer: handle.controlWriter,
  });

  const controlIncoming = receiveControlChannel({
    publicKey: { bootstrapFromReady: true },
    channelId,
    reader: handle.controlReader,
    onCrash: ctx.onCrash,
  });

  const readyPromise = waitForReady(controlIncoming);
  // Attach a benign catch at creation so a child exiting during the
  // credentials read does not reject `readyPromise` with no handler
  // attached. Mirrors the spawn path's guard.
  void readyPromise.catch(() => undefined);

  const eventIter = receiveEventChannel({
    hmacKey,
    channelId,
    reader: handle.eventReader,
    onCrash: ctx.onCrash,
  });
  const eventPump = pumpEvents(eventIter, ctx.onInferenceEvent);

  // The handshake below and the pre-handshake credentials-read reap
  // both need these timer bindings.
  const setTimer = ctx.setTimer ?? defaultSetTimer;
  const clearTimer = ctx.clearTimer ?? defaultClearTimer;

  // Re-read per-step credentials; a grants update since the previous
  // child's lifetime is picked up here, so recycle doubles as the
  // grant-refresh path. The deploy tree is not consulted.
  //
  // A rejected read reaps the new child here: it is spawned and wired
  // but not yet installed on `state`, so the supervisor's
  // recycle-failure teardown (which reaps the PRIOR cohort) cannot
  // see it.
  let credentialsSnapshot: CredentialsSnapshot;
  try {
    credentialsSnapshot = await assembleCredentialsSnapshot({
      repoStore: ctx.bindings.repoStore,
      principal: ctx.bindings.readPrincipal,
      stepOrder: ctx.stepOrder,
      anchorRunId: ctx.bindings.anchorRunId,
      deriveStepAddress: ctx.bindings.deriveStepAddress,
      ...(ctx.bindings.deriveStepRepoId !== undefined
        ? { deriveStepRepoId: ctx.bindings.deriveStepRepoId }
        : {}),
    });
  } catch (cause) {
    await reapUnreadyChild(handle, eventPump, controlIncoming, {
      killTimeoutMs,
      setTimer,
      clearTimer,
      phase: "credentials read failure",
    });
    throw cause;
  }

  // Steps 4 + 5: self-discover + resume run inside the child before
  // `ready`; the supervisor waits, deadline-bounded. Fold ready/failed
  // into values, race a resolve-only deadline, clear the timer on
  // every path.
  //
  // NOTE: the credentials read above sits OUTSIDE this deadline; a
  // wedged substrate is bounded at its own layer.
  const readyTimeoutMs = ctx.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS;
  const readyOutcome = readyPromise.then(
    (info) => ({ kind: "ready" as const, info }),
    (err: unknown) => ({ kind: "failed" as const, err }),
  );
  const readyDeadline = waitDeadline(setTimer, readyTimeoutMs);
  const readyRace = await Promise.race([
    readyOutcome,
    readyDeadline.promise.then(() => ({ kind: "timeout" as const })),
  ]);
  clearTimer(readyDeadline.handle);
  if (readyRace.kind !== "ready") {
    // The new child was never installed on `state`, so the
    // supervisor's recycle-failure teardown (which reaps the PRIOR
    // cohort) would leak it; reap it here. `killChildHandle` on an
    // already-dead handle is a no-op, so the `failed` path is reaped
    // too (a control-channel end does not guarantee the process
    // died).
    await reapUnreadyChild(handle, eventPump, controlIncoming, {
      killTimeoutMs,
      setTimer,
      clearTimer,
      phase: "handshake failure",
    });
    if (readyRace.kind === "timeout") {
      throw new Error(
        `workflow-host supervisor recycle: child did not emit ready within ${String(readyTimeoutMs)}ms; killed`,
      );
    }
    throw readyRace.err;
  }
  const readyInfo = readyRace.info;
  logger.info`recycle ${opts.origin}: child ready (pid=${String(readyInfo.childPid)}, newChannelId=${channelId})`;

  const newWiring: ChildWiring = {
    handle,
    controlSender,
    channelId,
    eventPump,
  };
  ctx.installNewChild({
    wiring: newWiring,
    credentialsSnapshot,
    controlIncoming,
  });

  // Step 6: buffered mail. The new dispatch loop dequeues the
  // kill/respawn-gap entries from the substrate-backed inbox in
  // arrival order; no in-memory drain step is needed.
  return {
    origin: opts.origin,
    reason: opts.reason,
    newChannelId: channelId,
    previousChannelId,
  };
}

/**
 * Reap a respawned child that was spawned and wired but never
 * installed on `state`. Kill first, then finalize the pumps: process
 * death drives EOF on both channels, ending `waitForReady`'s
 * in-flight `next()` and `pumpEvents`; awaiting either before the
 * kill would hang behind still-open channels. Killing an
 * already-dead handle is a no-op, so a child that died on its own is
 * reaped safely too.
 */
async function reapUnreadyChild(
  handle: SubprocessHandle,
  eventPump: Promise<void>,
  controlIncoming: AsyncGenerator<ControlPayload, void, void>,
  deps: {
    killTimeoutMs: number;
    setTimer: (cb: () => void, ms: number) => unknown;
    clearTimer: (handle: unknown) => void;
    phase: string;
  },
): Promise<void> {
  await killChildHandle(handle, deps.killTimeoutMs, {
    logger,
    setTimer: deps.setTimer,
    clearTimer: deps.clearTimer,
  });
  void eventPump.catch((cause: unknown) => {
    const message = cause instanceof Error ? cause.message : String(cause);
    logger.warn`recycle: reaped child eventPump failed after ${deps.phase}: ${message}`;
  });
  void controlIncoming.return(undefined).catch((cause: unknown) => {
    const message = cause instanceof Error ? cause.message : String(cause);
    logger.warn`recycle: reaped child controlIncoming.return failed after ${deps.phase}: ${message}`;
  });
}

/**
 * Iterate the child's control-receive iterator until the `ready`
 * frame lands; same shape as the supervisor's spawn-time helper.
 */
async function waitForReady(
  iter: AsyncGenerator<ControlPayload, void, void>,
): Promise<{ childPid: number }> {
  // Explicit `next()` instead of `for await ... return` so the
  // generator is not finalized when `ready` lands: the supervisor's
  // upstream-control pump keeps iterating it after recycle returns.
  while (true) {
    const next = await iter.next();
    if (next.done === true) {
      throw new Error(
        "workflow-host supervisor recycle: control channel ended before child emitted ready",
      );
    }
    const payload = next.value;
    if (payload.type === "ready") {
      return { childPid: payload.data.childPid };
    }
    // Other upstream payloads before `ready` are dropped silently;
    // later traffic flows through the supervisor's pump once recycle
    // returns.
  }
}

async function pumpEvents(
  iter: AsyncGenerator<EventPayload, void, void>,
  onInferenceEvent: (event: EventPayload) => void,
): Promise<void> {
  for await (const event of iter) {
    onInferenceEvent(event);
  }
}

// =============================================================
// Supervisor-policy periodic check
// =============================================================

export interface RecyclePolicyBounds {
  /**
   * Maximum uptime (ms) for the workflow-process child before a
   * recycle is triggered. `undefined` disables the bound.
   */
  maxUptimeMs?: number;
  /**
   * Maximum resident-set size (bytes) before a recycle is triggered.
   * `undefined` disables the bound; an absent `readRssBytes` callback
   * disables it regardless of the threshold.
   */
  maxRssBytes?: number;
  /**
   * Maximum age (ms) since grants were last refreshed before a
   * recycle is triggered. An absent `readGrantsAgeMs` callback
   * disables the bound regardless of the threshold.
   */
  maxGrantsAgeMs?: number;
}

export interface RecyclePolicyOpts {
  /** Bounds the policy evaluates each tick. */
  bounds: RecyclePolicyBounds;
  /** Tick interval in ms. Defaults to `DEFAULT_POLICY_INTERVAL_MS`. */
  intervalMs?: number;
  /** Wall-clock reader; production wires `() => Date.now()`. */
  now: () => number;
  /** Spawn-time wall-clock the policy compares against `now()`. */
  spawnedAt: number;
  /** Per-tick RSS reader; absent disables the `maxRssBytes` bound. */
  readRssBytes?: () => number | undefined;
  /** Per-tick grants-age reader; absent disables the staleness bound. */
  readGrantsAgeMs?: () => number | undefined;
  /** Timer setter; production wires `setInterval`-style via `setTimer`. */
  setTimer: (cb: () => void, ms: number) => unknown;
  /** Timer disposer; production wires the matching `clearTimer`. */
  clearTimer: (handle: unknown) => void;
  /**
   * Recycle entry point the policy invokes on a threshold trip; the
   * supervisor's `recycle()` method in production.
   */
  trigger: (reason: string) => Promise<void>;
}

export interface RecyclePolicy {
  /** Stop the timer; idempotent. */
  stop(): void;
  /** Evaluate the bounds once and trigger if any are tripped. */
  tick(): Promise<void>;
}

/**
 * Start the supervisor-policy periodic recycle check; returns a
 * handle the supervisor calls `stop()` on at shutdown. Single-trigger
 * per tick: one `trigger` invocation with the first tripped bound's
 * reason.
 */
export function createRecyclePolicy(opts: RecyclePolicyOpts): RecyclePolicy {
  const intervalMs = opts.intervalMs ?? DEFAULT_POLICY_INTERVAL_MS;
  let stopped = false;
  let timerHandle: unknown = null;

  async function tick(): Promise<void> {
    if (stopped) return;
    const reason = evaluateBounds(opts);
    if (reason !== null) {
      try {
        await opts.trigger(reason);
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`recycle policy trigger failed: ${message}`;
      }
    }
  }

  function arm(): void {
    if (stopped) return;
    timerHandle = opts.setTimer(() => {
      void tick().finally(() => arm());
    }, intervalMs);
  }
  arm();

  return {
    stop(): void {
      if (stopped) return;
      stopped = true;
      if (timerHandle !== null) opts.clearTimer(timerHandle);
      timerHandle = null;
    },
    tick,
  };
}

function evaluateBounds(opts: RecyclePolicyOpts): string | null {
  if (opts.bounds.maxUptimeMs !== undefined) {
    const uptimeMs = opts.now() - opts.spawnedAt;
    if (uptimeMs >= opts.bounds.maxUptimeMs) {
      return `max-uptime: uptime ${String(uptimeMs)}ms >= threshold ${String(opts.bounds.maxUptimeMs)}ms`;
    }
  }
  if (
    opts.bounds.maxRssBytes !== undefined &&
    opts.readRssBytes !== undefined
  ) {
    const rss = opts.readRssBytes();
    if (rss !== undefined && rss >= opts.bounds.maxRssBytes) {
      return `max-rss: rss ${String(rss)} bytes >= threshold ${String(opts.bounds.maxRssBytes)} bytes`;
    }
  }
  if (
    opts.bounds.maxGrantsAgeMs !== undefined &&
    opts.readGrantsAgeMs !== undefined
  ) {
    const ageMs = opts.readGrantsAgeMs();
    if (ageMs !== undefined && ageMs >= opts.bounds.maxGrantsAgeMs) {
      return `grants-staleness: age ${String(ageMs)}ms >= threshold ${String(opts.bounds.maxGrantsAgeMs)}ms`;
    }
  }
  return null;
}
