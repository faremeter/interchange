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
// supervisor policy (periodic bounds check), and child self-initiated
// `recycle.request` over control IPC. All land in the same code path;
// the reason string is the only origin-specific data carried forward.

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
 * Default supervisor-policy check interval. The policy thread wakes
 * roughly every minute, evaluates the configured bounds against the
 * live child, and triggers a recycle if any threshold has been
 * crossed. Operator-overridable via the supervisor's policy bindings.
 */
export const DEFAULT_POLICY_INTERVAL_MS = 60_000;

/**
 * Origin tag the recycle path stamps onto its log messages. The
 * `crash` origin drives the same respawn sequence after an unexpected
 * child exit; see the header note about steps 1-2 degrading to no-ops
 * for it.
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
 * Per-handle subprocess wiring the recycle path owns. The supervisor
 * passes the live child's wiring on entry and gets the freshly-spawned
 * child's wiring back via `installNewChild`.
 */
export interface ChildWiring {
  handle: SubprocessHandle;
  controlSender: ControlChannelSender;
  channelId: string;
  eventPump: Promise<void>;
}

/**
 * Bindings the supervisor passes into `triggerRecycle`. Mirrors the
 * subset of supervisor state the recycle sequence touches; it
 * intentionally excludes the mail-subscription disposer (the
 * supervisor holds the registration across the recycle) and the
 * mail-bus binding itself (the recycle path does not re-register).
 */
export interface RecycleContext {
  /** The supervisor's full bindings, reused on respawn for credentials and spawn. */
  readonly bindings: WorkflowSupervisorBindings;
  /**
   * Every step id in this deployment's flat step-id namespace -- the
   * definition's own `stepOrder` plus the step ids of every `loop`
   * body it carries -- for credentials re-assembly.
   */
  readonly stepOrder: readonly string[];
  /** Definition hash carried on respawn env (unchanged across recycle). */
  readonly definitionHash: string;
  /**
   * Warm-keep flag carried on the respawn env (design §3b). Unchanged
   * across recycle: the respawned child rebuilds its empty warm-agent
   * cache lazily, so the decision must survive the respawn.
   */
  readonly warmKeep: boolean;
  /** Forward target for InferenceEvents the new child publishes. */
  readonly onInferenceEvent: (event: EventPayload) => void;
  /** Live child wiring on entry; replaced before return. */
  readonly current: ChildWiring;
  /** Supervisor-side drain primitive; sends the existing drain mail. */
  readonly drain: (deadlineMs: number) => Promise<void>;
  /**
   * Replay any `processing/` claim-check entries for the deployment's
   * mail address back to `inbox/` so FIFO ordering survives the
   * recycle. Invoked after drain settles, before the kill -- the
   * window where a processing entry would exist with no owner.
   */
  readonly replayProcessingToInbox: () => Promise<void>;
  /**
   * Abort the prior cohort's terminal source and wake the dispatch
   * loop so it exits before the kill. Invoked after drain and replay
   * settle: earlier would starve drain accumulators of live terminal
   * events; later would race the kill against the dispatch loop's
   * next iteration.
   */
  readonly abortPriorCohort: () => void;
  /**
   * Onward sink the supervisor uses to install the new child wiring
   * once the freshly-spawned child has emitted `ready` and the
   * credentialsSnapshot has been re-assembled.
   */
  readonly installNewChild: (next: {
    wiring: ChildWiring;
    credentialsSnapshot: CredentialsSnapshot;
    /**
     * Live upstream control iterator the new child's receiver yields.
     * The supervisor's upstream-control pump continues consuming it
     * after `installNewChild` returns so child-initiated
     * `recycle.request` frames keep funneling through
     * `triggerRecycle`.
     */
    controlIncoming: AsyncGenerator<ControlPayload, void, void>;
  }) => void;
  /**
   * Crash hook the new child's IPC channels wire to. Identical shape
   * to the supervisor's spawn-time onCrash so a frame violation on the
   * recycled wiring tears the deployment down through the same path.
   */
  readonly onCrash: (reason: string) => void;
  /**
   * Optional kill-timeout override (ms). Defaults to
   * `DEFAULT_KILL_TIMEOUT_MS`.
   */
  readonly killTimeoutMs?: number;
  /**
   * Deadline (ms) for the respawned child's `ready` handshake, matching
   * the bound the spawn path applies. The supervisor resolves the
   * effective value at its edge (`bindings.readyTimeoutMs ??
   * DEFAULT_READY_TIMEOUT_MS`) and passes it through; the `??` fallback
   * here only fires for a direct test caller.
   */
  readonly readyTimeoutMs?: number;
  /**
   * Optional drain deadline (ms) used in step 1. The supervisor's own
   * drainTimeout accumulator escalates separately; this deadline is
   * the wait the recycle path itself observes before proceeding to
   * step 2. Defaults to the drain accumulator's default.
   */
  readonly drainDeadlineMs?: number;
  /**
   * Optional setTimer/clearTimer pair used by the SIGKILL escalation
   * wait. Production wires `setTimeout`/`clearTimeout`; tests inject
   * a deterministic timer so the SIGKILL window is observable.
   */
  readonly setTimer?: (cb: () => void, ms: number) => unknown;
  readonly clearTimer?: (handle: unknown) => void;
}

export interface TriggerRecycleOpts {
  origin: RecycleOrigin;
  reason: string;
}

/**
 * Run the six-step recycle sequence. The function returns once the
 * new child has emitted `ready` and the supervisor has drained its
 * buffered mail into it; the supervisor installs the new wiring via
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

  // Step 1: drain via the supervisor's shared drain primitive. Each
  // in-flight step's `drainBehavior` decides abort vs continue;
  // `drainTimeout` escalation lands through the existing
  // supervisor-drain origin.
  await ctx.drain(drainDeadlineMs);

  // Replay any in-flight `processing/` entries back to `inbox/`
  // before the kill. The dying dispatch loop already aborted on
  // cohort teardown and will not reach its `markConsumed`, so
  // without this the entry would be orphaned with no live owner.
  try {
    await ctx.replayProcessingToInbox();
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    logger.warn`recycle: replayProcessingToInbox before kill failed: ${message}`;
  }

  // Abort the prior cohort now -- after drain and replay have run
  // against a live cohort, before the kill drops the child. Aborting
  // up front would starve drain accumulators of live terminal events;
  // aborting after the kill would race the dispatch loop's next
  // iteration against the controlSender about to disappear.
  ctx.abortPriorCohort();

  // Step 2: kill. SIGTERM first, then SIGKILL after `killTimeoutMs`;
  // the injected spawner's handle owns the Node primitives.
  await killChildHandle(ctx.current.handle, killTimeoutMs, {
    logger,
    ...(ctx.setTimer !== undefined ? { setTimer: ctx.setTimer } : {}),
    ...(ctx.clearTimer !== undefined ? { clearTimer: ctx.clearTimer } : {}),
  });

  // Step 3: respawn. Fresh channelId, HMAC key, Ed25519 IPC keypair.
  // Per-step credentials are re-read so a grants update that landed
  // since the original spawn is reflected in the new child's
  // snapshot. The deploy tree is UNCHANGED.
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
  // Attach a benign catch at creation, before the fold below consumes
  // the rejection: a child that exits during the credentials read
  // rejects `readyPromise` with no handler yet attached. Mirrors the
  // spawn path's identical guard.
  void readyPromise.catch(() => undefined);

  const eventIter = receiveEventChannel({
    hmacKey,
    channelId,
    reader: handle.eventReader,
    onCrash: ctx.onCrash,
  });
  const eventPump = pumpEvents(eventIter, ctx.onInferenceEvent);

  // The child handshake below is deadline-bounded and needs these timer
  // bindings; the pre-handshake credentials-read reap needs them too, so
  // derive them before that read.
  const setTimer = ctx.setTimer ?? defaultSetTimer;
  const clearTimer = ctx.clearTimer ?? defaultClearTimer;

  // Re-read per-step credentials; a grants update that landed during
  // the previous child's lifetime is picked up here, so recycle
  // doubles as the supervisor's grant-refresh path. The deploy tree
  // is not consulted.
  //
  // The read can reject (a malformed grants file is precisely this
  // path's refresh case). The new child is spawned and wired but not
  // yet installed on `state`, so the supervisor's recycle-failure
  // teardown (which reaps the PRIOR cohort) cannot see it; reap it
  // here on failure or it leaks. The reap always has live handles
  // because the handle and pumps are constructed above.
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

  // Steps 4 + 5: self-discover + resume. These run inside the child
  // before it emits `ready`; the supervisor waits, bounded -- a child
  // that neither readies nor exits must not park the supervisor in
  // `recycling` forever. Fold ready/failed into values, race a
  // resolve-only deadline, clear the timer on every path.
  //
  // NOTE: `assembleCredentialsSnapshot` above sits OUTSIDE this
  // deadline; a wedged substrate is a supervisor-side fault bounded at
  // its own layer, not by overloading this handshake timer.
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
    // already-dead handle is a cheap no-op, so the `failed` path is
    // reaped too (a control-channel end does not guarantee the
    // process died, since the event channel is separate).
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

  // Step 6: buffered mail. Every inbound message enqueues into the
  // substrate-backed inbox regardless of phase, so the new dispatch
  // loop `installNewChild` started dequeues the kill/respawn-gap
  // entries in arrival order; no in-memory drain step is needed.
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
 * death drives EOF on both channels, unparks `waitForReady`'s
 * in-flight `next()` (letting `controlIncoming.return` complete) and
 * ends `pumpEvents`; awaiting either finalizer before the kill would
 * hang behind still-open channels. `killChildHandle` on an
 * already-dead handle is a cheap no-op, so a child that died on its
 * own is reaped safely too.
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
 * Iterate the new child's control-receive iterator until the `ready`
 * frame lands. Identical shape to the supervisor's spawn-time helper;
 * factored here so the recycle path does not depend on the
 * supervisor's private function.
 */
async function waitForReady(
  iter: AsyncGenerator<ControlPayload, void, void>,
): Promise<{ childPid: number }> {
  // Explicit `next()` instead of `for await ... return` so the
  // generator is not finalized when ready lands: the supervisor's
  // upstream-control pump keeps iterating the same generator after
  // recycle returns, and finalizing here would drop any subsequent
  // child-initiated upstream frames.
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
    // the receiver iterator already verified them, and later upstream
    // traffic flows through the supervisor's pump once recycle
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
   * Maximum resident-set size (bytes) for the workflow-process child
   * before a recycle is triggered. `undefined` disables the bound.
   * The supervisor's `readRssBytes` callback is consulted on every
   * policy tick; an absent callback disables the bound regardless of
   * the threshold.
   */
  maxRssBytes?: number;
  /**
   * Maximum age (ms) since grants were last refreshed before a
   * recycle is triggered. The supervisor's `readGrantsAgeMs` callback
   * is consulted on every policy tick; an absent callback disables
   * the bound regardless of the threshold.
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
   * Recycle entry point the policy invokes on a threshold trip. The
   * supervisor's `recycle()` method wraps `triggerRecycle` and is the
   * production callback.
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
 * Start the supervisor-policy periodic recycle check. Returns a
 * handle the supervisor calls `stop()` on at shutdown. The policy is
 * single-trigger per tick: even if multiple bounds are tripped on the
 * same tick, exactly one `trigger` invocation lands with a reason
 * naming the first tripped bound.
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
