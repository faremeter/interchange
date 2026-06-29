// =============================================================
// RECYCLE -- workflow-process supervisor library code
// =============================================================
//
// Recycle is the supervisor's "same deploy tree, fresh process" path.
// It tears the existing workflow-process child down and stands a new
// one up against the SAME deploy tree (same `workflow.json`, same
// per-step credential repos). It is STRICTLY ORTHOGONAL TO REDEPLOY:
//
//   - Recycle  = same deploy tree, fresh process.
//   - Redeploy = new deploy tree.
//
// Recycle does not refetch the deploy tree, does not consult an
// updated workflow definition, does not re-resolve agents. If a
// deploy-tree change is needed the host runs redeploy, which is a
// different code path with different authorization and a different
// rollback shape. The recycle module must never grow a "maybe also
// refetch the deploy tree" mode -- that would erase the orthogonality
// and let a recycle silently turn into a redeploy.
//
// Six-step sequence (locked):
//
//   1. `drain` -- send the existing drain control mail. Wait for
//      in-flight runs to drain per each step's `drainBehavior`.
//      `drainTimeout` escalation applies normally; the drain-timeout
//      accumulator and its `CancelRequested{origin: "supervisor-drain"}`
//      commit path are unchanged from the standalone-drain case.
//   2. `kill` -- terminate the workflow-process child cleanly. SIGTERM
//      first; if the child does not exit within the kill-timeout, the
//      handle's `kill(SIGKILL)` lands the hard stop. The supervisor's
//      injected subprocess spawner returns the child-handle API used
//      here; the recycle path does not reach into Node primitives.
//   3. `respawn` -- mint a new 16-byte hex channelId (the same shape
//      the initial spawn uses, per `generateChannelId`), generate a
//      fresh 32-byte HMAC key, re-read per-step credentials via the
//      injected `RepoStore`, and spawn a new Bun child via the same
//      subprocess-spawner binding. The new IPC anchors flow through
//      spawn-time env exactly as the initial spawn's anchors did.
//   4. `self-discover` -- the new child runs its existing self-
//      discovery on spawn. The recycle path does not coordinate this
//      step; it is the child's responsibility.
//   5. `resume` -- self-discovery resumes any in-flight runs from the
//      workflow-run log. The runtime body's seed-events path re-arms
//      timers, pending awaits, and uncancelled children.
//   6. Buffered mail is the supervisor's FIFO inbox claim-check queue
//      (the new child's dispatch loop picks up entries that arrived
//      during the kill/respawn gap once it starts). The recycle path
//      does NOT drain an in-memory mail buffer; every inbound message
//      enqueues into the substrate-backed inbox regardless of phase.
//
// Mail-address ownership across the gap: the supervisor holds the
// mail-bus registration across the recycle via the injected mail-bus
// binding. No re-register, no unregister. Inbound mail during the
// gap commits to the substrate-backed inbox; the new child's dispatch
// loop dequeues in arrival order (the envelope's `receivedAt` prefix
// preserves FIFO discipline across the gap).
//
// Before the kill lands the recycle path calls
// `ctx.replayProcessingToInbox()` so any in-flight `processing/`
// entries that the dying cohort's dispatch loop did not reach
// `markConsumed` for get moved back to `inbox/` under their original
// `<receivedAt>-<messageId>` keys. Without this step the dying child
// would leave an orphaned processing entry that no live dispatch
// loop owns.
//
// Three trigger origins funnel through `triggerRecycle(reason, ctx)`:
//
//   - Operator command -- the host receives a `recycle` request via
//     its caller-facing API and routes it to the supervisor's
//     `recycle()` method, which delegates here.
//   - Supervisor policy -- a periodic check (every ~minute) consults
//     configurable bounds (max-uptime, max-rss, grants-staleness;
//     defaults unlimited). On a threshold trip the policy calls
//     `triggerRecycle` with a reason tagged with the tripped bound.
//   - Workflow-process self-initiated -- the child sends a
//     `recycle.request` payload over control IPC. The supervisor's
//     upstream control-channel reader recognises the variant and
//     funnels it here.
//
// All three origins land in the same code path. The reason string is
// the only origin-specific data the path carries forward.

import { getLogger } from "@intx/log";

import { generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";

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

const logger = getLogger(["workflow-host", "supervisor", "recycle"]);

/**
 * Bound on the supervisor's mail buffer across the kill/respawn gap.
 * A real workflow's inbound rate is well below this; saturation
 * indicates either an upstream stuck on the deployment or a recycle
 * stuck partway through. Either case is one the operator must see.
 */
export const MAX_BUFFERED_MAIL = 256;

/**
 * Default kill-timeout between SIGTERM and SIGKILL during step 2.
 * Operator-overridable per deployment via the supervisor's recycle
 * bindings; this is the value used when no override is supplied.
 */
export const DEFAULT_KILL_TIMEOUT_MS = 5_000;

/**
 * Default supervisor-policy check interval. The policy thread wakes
 * roughly every minute, evaluates the configured bounds against the
 * live child, and triggers a recycle if any threshold has been
 * crossed. Operator-overridable via the supervisor's policy bindings.
 */
export const DEFAULT_POLICY_INTERVAL_MS = 60_000;

/**
 * Origin tag the recycle path stamps onto its log messages so an
 * operator scanning logs can distinguish operator-initiated,
 * policy-initiated, and self-initiated recycles at a glance.
 */
export type RecycleOrigin = "operator" | "policy" | "self";

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
 * passes the live child's wiring on entry; the recycle path replaces
 * it with the freshly-spawned child's wiring before returning.
 */
export interface ChildWiring {
  handle: SubprocessHandle;
  controlSender: ControlChannelSender;
  channelId: string;
  eventPump: Promise<void>;
}

/**
 * Bindings the supervisor passes into `triggerRecycle`. The shape
 * mirrors the subset of supervisor state the recycle sequence
 * touches; it intentionally does NOT include the supervisor's mail-
 * subscription disposer (the supervisor holds the registration across
 * the recycle) nor the mail-bus binding itself (the recycle path does
 * not re-register).
 */
export interface RecycleContext {
  /** The supervisor's full bindings, reused on respawn for credentials and spawn. */
  readonly bindings: WorkflowSupervisorBindings;
  /** Step ids in this deployment's `stepOrder` for credentials re-assembly. */
  readonly stepOrder: readonly string[];
  /** Definition hash carried on respawn env (unchanged across recycle). */
  readonly definitionHash: string;
  /**
   * Warm-keep flag carried on the respawn env (design §3b). Unchanged
   * across recycle: the respawned child rebuilds its empty warm-agent
   * cache lazily on the next message, so the deterministic warm-keep
   * decision must survive the respawn rather than be re-derived.
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
   * mail address back to `inbox/` so the FIFO ordering survives the
   * recycle. Invoked AFTER the drain step settles and BEFORE the kill
   * step lands, which eliminates the race window where a processing
   * entry would exist with no owner. The supervisor closes this
   * callback over its `inboxPrimitives.replayProcessingToInbox` plus
   * the deployment's substrate principal and repo identity.
   */
  readonly replayProcessingToInbox: () => Promise<void>;
  /**
   * Abort the prior cohort's terminal source and wake the dispatch
   * loop so it exits before the kill step lands. Invoked AFTER drain
   * and replay settle and BEFORE the kill -- earlier would starve the
   * drain step's accumulators of live terminal events; later would
   * race the kill against the dispatch loop's next iteration.
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
     * Live upstream control iterator the new child's receiver
     * yields. The supervisor's upstream-control pump consumes the
     * iterator after `installNewChild` returns so child-initiated
     * `recycle.request` frames on the new wiring continue to funnel
     * through `triggerRecycle`.
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

  // Step 1: drain. The supervisor's drain primitive is the same one
  // the standalone-drain path uses; the recycle path does not
  // reimplement the drain control mail or the drainTimeout
  // accumulator. The `drainBehavior` of each in-flight step decides
  // whether it aborts or continues; `drainTimeout` escalation lands
  // through the existing supervisor-drain origin.
  await ctx.drain(drainDeadlineMs);

  // After drain settles and BEFORE the kill lands, replay any
  // in-flight `processing/` entries back to `inbox/`. Without this
  // replay, a child whose run was mid-dispatch when drain expired
  // would leave its `processing/` entry orphaned -- the dispatch
  // loop for the dying cohort already aborted on cohort teardown
  // and will not reach its `markConsumed` step. The new child's
  // dispatch loop dequeues the recovered entries in arrival order
  // (the envelope's `receivedAt` prefix preserves FIFO discipline).
  try {
    await ctx.replayProcessingToInbox();
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    logger.warn`recycle: replayProcessingToInbox before kill failed: ${message}`;
  }

  // Abort the prior cohort here -- after drain and replay have run
  // against a live cohort, before the kill drops the child. Aborting
  // up front (in the supervisor wrapper) would starve drain
  // accumulators of live terminal events and force every recycle to
  // pay the full drainTimeout budget. Aborting after the kill would
  // race the dispatch loop's next iteration against the
  // controlSender that is about to disappear.
  ctx.abortPriorCohort();

  // Step 2: kill. SIGTERM first; if the child does not exit within
  // `killTimeoutMs`, the handle's hard kill lands. The injected
  // spawner's `SubprocessHandle.kill()` is the surface the recycle
  // path touches; the spawner owns the Node primitives.
  await killChildHandle(ctx.current.handle, killTimeoutMs, ctx);

  // Step 3: respawn. Fresh channelId, fresh HMAC key, fresh Ed25519
  // IPC keypair. Per-step credentials are re-read so a grants update
  // that landed since the original spawn is reflected in the new
  // child's snapshot. The deploy tree (`workflow.json`, agents,
  // workflow-asset repo) is UNCHANGED.
  const channelId = generateChannelId();
  const hmacKey = generateHmacKey();
  const ipcKeypair = await (
    ctx.bindings.ipcKeyPairFactory ?? generateKeyPair
  )();
  const env: Record<string, string> = {
    ...ctx.bindings.substrateEnv,
    IPC_CHANNEL_ID: channelId,
    IPC_HMAC_KEY: hexEncode(hmacKey),
    HOST_PUBKEY: hexEncode(ipcKeypair.publicKey),
    DEPLOYMENT_ID: ctx.bindings.deploymentId,
    DEFINITION_HASH: ctx.definitionHash,
    MAILBOX_ADDRESS: ctx.bindings.deploymentMailAddress,
    WARM_KEEP: ctx.warmKeep ? "true" : "false",
  };

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

  const eventIter = receiveEventChannel({
    hmacKey,
    channelId,
    reader: handle.eventReader,
    onCrash: ctx.onCrash,
  });
  const eventPump = pumpEvents(eventIter, ctx.onInferenceEvent);

  // Re-read per-step credentials. A grants update that landed during
  // the previous child's lifetime is picked up here -- the recycle
  // doubles as the supervisor's grant-refresh path. The deploy tree
  // is not consulted; this read is against the `agent-state` repos
  // alone, whose contents are independent of `workflow.json`.
  const credentialsSnapshot = await assembleCredentialsSnapshot({
    repoStore: ctx.bindings.repoStore,
    principal: ctx.bindings.readPrincipal,
    stepOrder: ctx.stepOrder,
    deploymentId: ctx.bindings.deploymentId,
    deriveStepAddress: ctx.bindings.deriveStepAddress,
    ...(ctx.bindings.deriveStepRepoId !== undefined
      ? { deriveStepRepoId: ctx.bindings.deriveStepRepoId }
      : {}),
  });

  // Steps 4 + 5: self-discover + resume. These run inside the child
  // before it emits `ready`; the supervisor waits.
  const readyInfo = await readyPromise;
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

  // Step 6: the FIFO inbox claim-check queue holds any mail that
  // arrived during the kill/respawn gap (every inbound message
  // enqueues into the substrate-backed inbox regardless of phase).
  // The new dispatch loop that `installNewChild` started dequeues
  // those entries in arrival order; the recycle path does not need
  // an in-memory drain step.

  return {
    origin: opts.origin,
    reason: opts.reason,
    newChannelId: channelId,
    previousChannelId,
  };
}

/**
 * Issue SIGTERM and wait for the child to exit. If the exit does not
 * land within `killTimeoutMs`, escalate to SIGKILL and wait again.
 * The supervisor's spawner returns the `exited` promise; the recycle
 * path does not consult OS primitives directly.
 */
async function killChildHandle(
  handle: SubprocessHandle,
  killTimeoutMs: number,
  ctx: RecycleContext,
): Promise<void> {
  const setTimer = ctx.setTimer ?? defaultSetTimer;
  const clearTimer = ctx.clearTimer ?? defaultClearTimer;

  handle.kill("SIGTERM");
  const sigTermDeadline = waitDeadline(setTimer, killTimeoutMs);
  const exitedFirst = await Promise.race([
    handle.exited.then(() => "exited" as const),
    sigTermDeadline.promise.then(() => "deadline" as const),
  ]);
  if (exitedFirst === "exited") {
    clearTimer(sigTermDeadline.handle);
    return;
  }
  clearTimer(sigTermDeadline.handle);
  logger.warn`recycle: SIGTERM did not land within ${String(killTimeoutMs)}ms; escalating to SIGKILL`;
  handle.kill("SIGKILL");
  await handle.exited.catch(() => {
    /* swallowed: a non-zero exit on SIGKILL is the expected outcome;
       the recycle path treats handle exit as success regardless of
       code. */
  });
}

function waitDeadline(
  setTimer: (cb: () => void, ms: number) => unknown,
  ms: number,
): { promise: Promise<void>; handle: unknown } {
  let h: unknown;
  const promise = new Promise<void>((resolve) => {
    h = setTimer(() => resolve(), ms);
  });
  return { promise, handle: h };
}

function defaultSetTimer(cb: () => void, ms: number): unknown {
  return setTimeout(cb, ms);
}

function defaultClearTimer(handle: unknown): void {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- production wiring; the handle is the value `setTimeout` returned, narrowed back at the boundary
  clearTimeout(handle as ReturnType<typeof setTimeout>);
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
  // generator is not finalized when ready lands. The supervisor's
  // upstream-control pump continues iterating the same generator
  // after the recycle path returns; finalizing it here would silently
  // drop any subsequent child-initiated upstream frames.
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
    // Other upstream control payloads encountered before `ready` are
    // dropped silently; the receiver iterator already verified them
    // and any later upstream traffic flows through the supervisor's
    // pump once recycle returns.
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
