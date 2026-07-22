// Per-deployment supervisor.
//
// The supervisor is the host-side object that owns one workflow-process
// child for the lifetime of an active deployment: an in-host object,
// not a separate OS process. The host process holds one supervisor
// instance per deployment.
//
// Spawn lifecycle:
//   1. Mint a fresh `channelId` (16 bytes hex).
//   2. Mint a fresh 32-byte HMAC key for the event channel.
//   3. Mint a fresh Ed25519 keypair for the control channel (the
//      "IPC signing key" -- orthogonal to the supervisor's principal-
//      signing key the host's `signAsPrincipal` callback wraps).
//   4. Build a spawn-time env carrying only:
//        - `IPC_CHANNEL_ID`
//        - `IPC_HMAC_KEY` (hex)
//        - `HOST_PUBKEY` (the IPC keypair's 32-byte public key, hex;
//          NEVER the private key, NEVER the principal-signing key)
//      plus the substrate-config keys the host injected.
//   5. Invoke `bindings.subprocessSpawner` with the binary path and
//      env. The spawner returns a handle exposing the control
//      channel writer/reader and the event channel reader.
//   6. Wire the control-channel sender (Ed25519-signed by the IPC
//      private key) and the event-channel receiver (HMAC-verified).
//   7. Wait for the child's `ready` frame on the control channel;
//      hold any inbound mail in the supervisor's buffer until then.
//   8. Register the deployment's mail address via the mail bus.
//   9. Forward inbound mail to the child via `trigger.fire` frames.
//
// The supervisor's `Bun.spawn` is invoked via the injected
// `bindings.subprocessSpawner` callback so tests stub it. The
// supervisor spawns the binary and does not depend on the
// `runWorkflowChild` body.
//
// CancelRequested signing:
//   Every CancelRequested origin -- `self`, `supervisor-drain`,
//   `supervisor-operator`, `hub-admin` -- flows through the same
//   supervisor-signed path via `commitCancelRequested`. The `self`-
//   origin case is the workflow-process forwarding its stated reason
//   over the control IPC; the supervisor wraps it into a signed
//   event without consulting the child for the signature.

import { type } from "arktype";

import { getLogger } from "@intx/log";

import {
  sampleStructuralCounters,
  forceRepack,
  type RepackToggle,
} from "./dispatch-attribution";

import { generateKeyPair } from "@intx/crypto";
import {
  enqueueInbox as defaultEnqueueInbox,
  dequeueToProcessing as defaultDequeueToProcessing,
  markConsumed as defaultMarkConsumed,
  readOwnedMessageIds,
  replayProcessingToInbox as defaultReplayProcessingToInbox,
  DEFAULT_CONSUMED_RETENTION_MS,
  type NewlyTerminalRun,
  type Principal,
  type WorkflowRunSupervisorPrincipal,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions/substrate";
import { base64Decode, base64Encode, deriveMessageId } from "@intx/types";
import type { SignalKind } from "@intx/types";
import { RepoId } from "@intx/types/sidecar";
import type {
  ApprovalSnapshot,
  InferenceSource,
  OutboundMessage,
} from "@intx/types/runtime";
import type { CancelOrigin } from "@intx/workflow";

import {
  createControlChannelSender,
  generateChannelId,
  generateHmacKey,
  receiveControlChannel,
  receiveEventChannel,
  type ControlChannelSender,
  type ControlPayload,
  type EventPayload,
  type OutboundMessagePayload,
} from "../ipc/index";

import {
  assembleCredentialsSnapshot,
  type CredentialsSnapshot,
} from "./credentials";
import { commitCancelRequested } from "./cancel-signing";
import { buildChildSpawnEnv } from "./spawn-env";
import { compactRunEvents } from "./run-event-compaction";
import {
  createDrainTimeoutAccumulator,
  DEFAULT_DRAIN_TIMEOUT_MS,
  type DrainTimeoutAccumulator,
} from "./drain-timeout";
import {
  createRecyclePolicy,
  triggerRecycle,
  type ChildWiring,
  type RecycleAttempt,
  type RecycleOrigin,
  type RecyclePolicy,
} from "./recycle";
import type {
  DeriveMailAuditRef,
  DispatchStructuralCounters,
  DispatchSubstrateLeg,
  InboxPrimitives,
  MailAuditRef,
  SubprocessHandle,
  TerminalEventSource,
  TerminalRunEvent,
  WorkflowSupervisorBindings,
} from "./types";
import {
  createTerminalBroadcaster,
  type TerminalBroadcaster,
} from "./terminal-broadcaster";
import {
  DEFAULT_KILL_TIMEOUT_MS,
  DEFAULT_READY_TIMEOUT_MS,
  defaultClearTimer,
  defaultSetTimer,
  killChildHandle,
  waitDeadline,
} from "./child-termination";

const logger = getLogger(["workflow-host", "supervisor"]);

/**
 * Default watchdog timeout for the supervisor's
 * `synchronouslyDispatchTerminalWrite`. The handler holds the
 * `substrate.write.response` back to the child until the dispatch
 * loop's `markConsumed` settles for the matching terminal event; an
 * unbounded wait would chain into a child / runtime / dispatch loop
 * deadlock if `markConsumed` never armed (bug in the dispatch loop, a
 * torn-down cohort, a stalled inbox primitive). 30s sits between the
 * recycle path's `DEFAULT_KILL_TIMEOUT_MS` (5s, a hard process-level
 * kill cap) and `DEFAULT_DRAIN_TIMEOUT_MS` (60s, the per-deployment
 * drain budget) -- generous enough to absorb a slow legitimate
 * markConsumed, tight enough to surface a real deadlock long before
 * the drainTimeout would otherwise mask it.
 */
export const DEFAULT_TERMINAL_WRITE_WATCHDOG_MS = 30_000;

/**
 * Default watchdog for `reEmitParkedCorrelations`' wait on the child's
 * `parked-correlations.response`. Shares the terminal-write watchdog's 30s
 * budget: generous enough for a healthy child to enumerate its in-flight runs
 * and load each parked snapshot, tight enough that a wedged-but-alive child
 * does not hang the reconnect caller until some coarser timeout intervenes.
 */
export const DEFAULT_PARKED_QUERY_WATCHDOG_MS = 30_000;

/**
 * Public surface returned by `createWorkflowSupervisor`. Each method
 * advances the supervisor through one lifecycle transition; the
 * supervisor's internal state is encapsulated.
 */
export interface WorkflowSupervisor {
  /**
   * Spawn the workflow-process child, complete the IPC handshake,
   * assemble the credentialsSnapshot, register the deployment's mail
   * address, and begin forwarding inbound mail. Resolves once the
   * child's `ready` frame has been received and credentials have
   * been pushed.
   */
  spawn(opts: SpawnOpts): Promise<SpawnResult>;
  /**
   * Sign and commit a CancelRequested event under the named origin.
   * Used by the host directly for `supervisor-operator` and `hub-
   * admin` origins; the `self` origin is invoked indirectly by the
   * supervisor when the child requests cancellation over the
   * control IPC.
   */
  requestCancel(opts: CancelRequestOpts): Promise<CancelCommitInfo>;
  /**
   * Tear the deployment down: unregister the mail address, kill the
   * child, dispose subscriptions, await child exit. Idempotent.
   */
  shutdown(): Promise<void>;
  /**
   * Send the supervisor's `drain` control mail to the child and arm
   * a drainTimeout accumulator against every in-flight run. The
   * child's `DrainController` flips its signal on receipt and the
   * runtime body picks the change up at the four observation
   * points; cancel-mode steps abort locally, wait-mode steps continue
   * running. On accumulator expiry, the supervisor commits a signed
   * `CancelRequested{origin: "supervisor-drain"}` per run via the
   * accumulator's existing path. The promise resolves once the
   * `drain` mail has been forwarded; the accumulators tick in the
   * background and stop on shutdown or terminal-phase reach. The
   * recycle path reuses this primitive verbatim for its drain step.
   */
  drain(opts: DrainOpts): Promise<void>;
  /**
   * Recycle the child: drain -> kill -> respawn with a fresh
   * channelId. Funnels every recycle origin (operator command,
   * supervisor policy, child self-initiated) through the same
   * `triggerRecycle` code path.
   */
  recycle(opts: RecycleOpts): Promise<RecycleAttempt>;
  /**
   * Deliver a workflow-run signal to the child by sending a
   * `signal.deliver` control IPC frame. The child commits the
   * resulting `SignalReceived` event through its own substrate, which
   * keeps the workflow-run repo's single-writer invariant intact -- the
   * child is the only writer of `runs/<runId>/events/` on the sidecar
   * side, and the pack-push pipeline propagates the commit to the hub
   * without racing against a concurrent host-side write.
   *
   * Throws when the supervisor is not in a phase where it can address
   * the child (idle / stopping / stopped); the caller is responsible
   * for serializing delivery against `spawn` completion.
   */
  deliverSignal(opts: DeliverSignalOpts): Promise<void>;
  /**
   * Push a rotated inference-source list to the child's warm single-step
   * agent. Mirrors `deliverSignal`: the supervisor is the single producer
   * of `sources-updated` control frames, and delivery is phase-guarded to
   * starting/running so a frame is never written into a recycling child's
   * closing pipe. Throws otherwise.
   */
  deliverSources(opts: DeliverSourcesOpts): Promise<void>;
  /**
   * Re-register every correlation the child is currently parked on by
   * querying it for its parked correlations and re-emitting each through
   * `onSuspensionRegister`. Recovers a `park.notify` register the hub may have
   * missed while it was down at suspend time.
   *
   * Best-effort and safe to call at any time, including concurrently. Unlike
   * `deliverSignal`/`deliverSources`, which throw when the child is not
   * addressable, this NO-OPS on a non-addressable phase (idle / stopping /
   * stopped / recycling): a re-establishment landing mid-recycle must not
   * crash, and the next spawn re-drives it. A query that fails or times out is
   * logged and dropped; the hub co-write is idempotent, so the next
   * re-establishment re-drives it. The caller need not guard the call site.
   */
  reEmitParkedCorrelations(): Promise<void>;
  /**
   * Current snapshot of the credentials pushed to the child. Surfaced
   * so the host can audit the per-step contentHash without
   * round-tripping the substrate. Returns `null` before spawn.
   */
  getCredentialsSnapshot(): CredentialsSnapshot | null;
}

export type SpawnOpts = {
  /** Step ids in this deployment's `stepOrder` for credentials assembly. */
  stepOrder: readonly string[];
  /** Content hash of the deployment's workflow definition. */
  definitionHash: string;
  /**
   * Whether the spawned child warm-keeps its agent across messages
   * (design §3b). The host sets this true only for the single-step
   * long-lived deployment the deploy projection marked a warm candidate;
   * the supervisor threads it into the child's spawn env as `WARM_KEEP`
   * so the child's run-loop builds a warm-agent cache. Carried
   * explicitly so the warm-keep decision is deterministic and survives
   * recycle (the recycle path re-spawns with the same env).
   */
  warmKeep: boolean;
  /**
   * Callback the supervisor invokes for each verified InferenceEvent
   * the child publishes. Mirrors the existing `agent.event` event
   * sink the host exposes; the supervisor is the in-host translator.
   */
  onInferenceEvent: (event: EventPayload) => void;
};

export type SpawnResult = {
  /** Child process pid. */
  pid: number;
  /** IPC channelId minted for this spawn. */
  channelId: string;
  /** Initial credentials snapshot pushed to the child. */
  credentialsSnapshot: CredentialsSnapshot;
};

export type CancelRequestOpts = {
  runId: string;
  origin: CancelOrigin;
  reason: string;
  /** ISO-8601 commit timestamp. */
  at: string;
};

export type CancelCommitInfo = {
  commitSha: string;
  seq: number;
};

export type DrainOpts = {
  /**
   * Wire `deadlineMs` carried on the `drain` control frame so the
   * child can echo the policy in its logs. The supervisor-side
   * `drainTimeout` accumulator is driven by
   * `WorkflowSupervisorBindings.drainTimeoutMs`, not by this value:
   * the timeout policy is a per-deployment operator setting baked
   * into the supervisor's bindings, not a per-call argument.
   */
  deadlineMs: number;
};

export type DeliverSignalOpts = {
  /** Run the signal targets. The child rejects a delivery whose runId is unknown. */
  runId: string;
  /** Signal name the run's `awaitSignal` step matches against. */
  signalName: string;
  /**
   * Producer-supplied dedup id. The workflow-run state machine
   * rejects duplicate deliveries via `observedSignalIds`; callers
   * mint a fresh value per call.
   */
  signalId: string;
  /** Opaque signal payload the awaiter resolves with. */
  payload: unknown;
};

export type DeliverSourcesOpts = {
  /**
   * The rotated ordered inference-source failover chain; element 0 is the
   * active source. The wire boundary enforces a non-empty list with unique
   * ids whose head is the default.
   */
  sources: InferenceSource[];
  /** The default source id; the wire boundary requires it to equal `sources[0].id`. */
  defaultSource: string;
};

export type RecycleOpts = {
  reason: string;
  /**
   * Origin of the recycle request. Defaults to `"operator"` when the
   * supervisor's caller-facing API is invoked directly; the policy
   * timer wires `"policy"` and the child-side `recycle.request`
   * upstream frame wires `"self"`.
   */
  origin?: RecycleOrigin;
};

/**
 * Raised when a `pendingMerges` entry or a
 * `markConsumedCompletionWaiters` waiter is rejected because the
 * cohort it was registered against has been aborted (cohort transition
 * during a recycle, or a supervisor shutdown). Callers awaiting the
 * resolved value receive an instance of this error so the failure mode
 * is recognisable from a generic substrate-merge or markConsumed
 * failure.
 */
export class MergeAbortedError extends Error {
  constructor(reason: string) {
    super(`supervisor cohort aborted before completion: ${reason}`);
    this.name = "MergeAbortedError";
  }
}

/**
 * Construct a per-deployment supervisor. All host-specific
 * dependencies are pulled in via `bindings`; nothing in the
 * supervisor reaches into `process.env` or a singleton.
 */
export function createWorkflowSupervisor(
  bindings: WorkflowSupervisorBindings,
): WorkflowSupervisor {
  let state: SupervisorState = { phase: "idle" };
  /**
   * In-flight runIds the supervisor knows about. A runId enters this
   * set when the supervisor forwards a `trigger.fire` for it on the
   * control channel; the runId leaves the set when the dispatch
   * loop's terminal-event watcher fires `markConsumed`. The drain
   * path arms one accumulator per entry here.
   */
  const inFlightRuns = new Set<string>();
  // D2 attribution (measurement-only): the runId the dispatch loop is
  // currently servicing. Set at `dispatch-start`, cleared after
  // `reply-produced`. The dispatch loop is strictly serial (one message
  // in flight at a time -- the sustained interactive case the bench
  // drives), so a child-proxied WAL `substrate.write.request` (whose
  // `agent-state/<key>/...` preservePrefix carries no runId) is
  // unambiguously attributable to this runId. Run-event writes carry the
  // runId in their `runs/<runId>/events/` prefix and do not need it.
  let currentDispatchRunId: string | null = null;
  /**
   * Per-run drainTimeout accumulators armed by `drain()`. Held so
   * `shutdown()` can stop every accumulator cleanly before tearing
   * the deployment down (an accumulator left running would otherwise
   * fire `setTimeout` after the supervisor has been disposed).
   */
  const drainAccumulators = new Map<string, DrainTimeoutAccumulator>();
  const accumulatorFactory =
    bindings.drainTimeoutAccumulatorFactory ?? createDrainTimeoutAccumulator;
  const drainNow = bindings.now ?? Date.now;
  const drainSetTimer =
    bindings.setTimer ?? ((cb: () => void, ms: number) => setTimeout(cb, ms));
  const drainClearTimer =
    bindings.clearTimer ??
    ((h: unknown) => {
      // The production `drainSetTimer` returns the value of
      // `setTimeout`, so the only handles flowing through
      // `drainClearTimer` are `Timeout` objects. `clearTimeout`
      // accepts `Timeout | undefined` -- the `undefined` branch is
      // a no-op which is the right behaviour for the defensive
      // path here.
      if (h !== null && typeof h === "object") {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- handle round-trip: the matching `drainSetTimer` returns `ReturnType<typeof setTimeout>`; the accumulator preserves opaqueness, which forces a re-assertion here
        clearTimeout(h as ReturnType<typeof setTimeout>);
      }
    });
  const drainTimeoutMs = bindings.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
  const terminalWriteWatchdogMs =
    bindings.terminalWriteWatchdogMs ?? DEFAULT_TERMINAL_WRITE_WATCHDOG_MS;
  const parkedQueryWatchdogMs =
    bindings.parkedQueryWatchdogMs ?? DEFAULT_PARKED_QUERY_WATCHDOG_MS;

  // Pure observability: invoke the dispatch-timing hook (when wired) at
  // the two per-message boundaries the 4.7 latency gate brackets. A
  // throwing observer is swallowed and logged so a benchmark hook bug
  // cannot wedge the dispatch loop.
  function emitDispatchTiming(
    runId: string,
    marker: "dispatch-start" | "reply-produced",
    atMs: number,
  ): void {
    const observer = bindings.onDispatchTiming;
    if (observer === undefined) return;
    try {
      observer({ kind: "roundtrip", runId, marker, atMs });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`onDispatchTiming observer threw for ${runId} (${marker}): ${message}`;
    }
  }

  // D2 per-leg attribution (measurement-only). Emits a paired
  // start/end mark around one of the five substrate legs so each leg's
  // per-message slope/floor can be fit independently. The `end` mark
  // carries the structural counters sampled at commit time (runs/ and
  // consumed/ fan-out, loose-object count, .git byte size) so the slope
  // can be correlated with the grower that explains it. Pure
  // observability: a throwing observer is swallowed + logged so a
  // benchmark hook bug cannot wedge dispatch, and no clock or directory
  // is sampled when the observer is unwired.
  function legMarkStart(runId: string, leg: DispatchSubstrateLeg): number {
    if (bindings.onDispatchTiming === undefined) return 0;
    const atMs = performance.now();
    try {
      bindings.onDispatchTiming({
        kind: "leg",
        runId,
        leg,
        phase: "start",
        atMs,
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`onDispatchTiming leg observer threw for ${runId} (${leg} start): ${message}`;
    }
    return atMs;
  }

  function legMarkEnd(runId: string, leg: DispatchSubstrateLeg): void {
    const observer = bindings.onDispatchTiming;
    if (observer === undefined) return;
    const atMs = performance.now();
    let counters: DispatchStructuralCounters | undefined;
    try {
      counters = sampleStructuralCounters(
        bindings.repoStore.getRepoDir(bindings.workflowRunRepoId),
      );
    } catch (cause) {
      // A counter read that throws must not perturb the measured leg;
      // surface it on the log and emit the end mark without counters so
      // the timing slope is still recoverable.
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`structural-counter sample failed for ${runId} (${leg}): ${message}`;
    }
    try {
      observer({
        kind: "leg",
        runId,
        leg,
        phase: "end",
        atMs,
        ...(counters !== undefined ? { counters } : {}),
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`onDispatchTiming leg observer threw for ${runId} (${leg} end): ${message}`;
    }
  }

  // §10c forced-repack A/B (measurement-only). Absent toggle => never
  // repacks; the dispatch path forks no `git gc`. When wired, the
  // dispatch loop calls `maybeRepack` once per dispatched message after
  // `markConsumed`, and every `everyMessages`-th message forces a repack
  // of the workflow-run repo under the single-writer discipline (the
  // dispatch loop is the sole writer and blocks on the synchronous gc, so
  // no commit can interleave).
  const repackToggle: RepackToggle | undefined = bindings.repackEveryMessages;
  let dispatchedSinceRepack = 0;
  function maybeRepack(runId: string): void {
    if (repackToggle === undefined) return;
    dispatchedSinceRepack += 1;
    if (dispatchedSinceRepack < repackToggle.everyMessages) return;
    dispatchedSinceRepack = 0;
    const repoDir = bindings.repoStore.getRepoDir(bindings.workflowRunRepoId);
    const result = forceRepack(repoDir);
    if (!result.ok) {
      logger.warn`forced repack failed after ${runId}: ${result.detail}`;
      return;
    }
    // The repack itself is not a per-message leg: the A/B compares the
    // per-leg slopes of a whole WITH-repack run against a whole
    // WITHOUT-repack run. Logged (not emitted on the leg channel) so the
    // repack cadence + duration are visible in the supervisor log without
    // contaminating any leg's per-message series. The structural counters
    // sampled right after confirm loose-object count collapsed -- the
    // direct evidence the gc ran.
    const after = sampleStructuralCounters(repoDir);
    logger.info`forced repack after ${runId}: ${result.durationMs.toFixed(1)}ms; looseObjects now ${String(after.looseObjects)}, gitBytes now ${String(after.gitBytes)}`;
  }

  /**
   * Classify a child-proxied `substrate.write.request` into the D2 leg it
   * represents, plus the runId the per-message OLS fit groups on.
   * `runs/<runId>/events/` is the run-event bracket commit (runId from the
   * prefix); `agent-state/...` is the D1 conversation WAL append (no runId
   * in the prefix -- attributed to the dispatch loop's current serial
   * runId). Any other prefix is an unmarked proxied write. Returns `null`
   * when no observer is wired (so the supervisor samples nothing) or the
   * prefix is not an attributed leg.
   */
  function classifyProxiedWriteLeg(
    preservePrefix: string,
  ): { leg: DispatchSubstrateLeg; runId: string } | null {
    if (bindings.onDispatchTiming === undefined) return null;
    const runEventMatch = /^runs\/([^/]+)\/events\/$/.exec(preservePrefix);
    if (runEventMatch !== null) {
      const runId = runEventMatch[1];
      if (runId !== undefined) return { leg: "runevent", runId };
    }
    if (preservePrefix.startsWith("agent-state/")) {
      if (currentDispatchRunId !== null) {
        return { leg: "wal", runId: currentDispatchRunId };
      }
    }
    return null;
  }

  // Avoid sampling the clock when no observer is wired (the hot path in
  // production). When an observer is present, the dispatch loop samples
  // `performance.now()` BEFORE `dequeueToProcessing` so the claim-check
  // READ falls inside the measured per-message interval, and stamps the
  // `dispatch-start` mark with that pre-dequeue sample once the runId is
  // known.
  function dispatchTimingEnabled(): boolean {
    return bindings.onDispatchTiming !== undefined;
  }
  const inboxPrimitives: InboxPrimitives = bindings.inboxPrimitives ?? {
    enqueueInbox: defaultEnqueueInbox,
    dequeueToProcessing: defaultDequeueToProcessing,
    markConsumed: defaultMarkConsumed,
    replayProcessingToInbox: defaultReplayProcessingToInbox,
  };
  const deriveMailAuditRef: DeriveMailAuditRef =
    bindings.deriveMailAuditRef ?? defaultInProcessMailAuditRef;
  const defaultInboxWritePrincipal: WorkflowRunSupervisorPrincipal = {
    kind: "supervisor",
    deploymentId: bindings.deploymentId,
  };
  const inboxWritePrincipal: Principal =
    bindings.inboxWritePrincipal ?? defaultInboxWritePrincipal;
  // Resolve the consumed-dedup retention horizon once at the bindings
  // edge (the layer that owns the operator config); every markConsumed
  // is threaded the concrete value. See `WorkflowSupervisorBindings.
  // consumedRetentionMs` for the operator-owned invariant.
  const consumedRetentionMs =
    bindings.consumedRetentionMs ?? DEFAULT_CONSUMED_RETENTION_MS;
  // Resolve the spawn ready-handshake timeout and its timers once at the
  // bindings edge. The timers reuse the same injectable pair the drain
  // path resolves (`bindings.setTimer`/`clearTimer`); the ready-timeout
  // race and its kill-escalation drive them, and tests substitute a
  // deterministic timer through the same bindings.
  const readyTimeoutMs = bindings.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS;
  const readySetTimer = bindings.setTimer ?? defaultSetTimer;
  const readyClearTimer = bindings.clearTimer ?? defaultClearTimer;
  /**
   * Resolved on every successful `enqueueInbox`; the dispatch loop
   * awaits this promise after a null dequeue so it returns to
   * dequeueing the moment a fresh entry lands. Replaced with a fresh
   * promise on every wake so the loop's next iteration starts from a
   * clean signal.
   */
  let dispatchWake: { promise: Promise<void>; resolve: () => void } =
    makeDispatchWake();
  function makeDispatchWake(): {
    promise: Promise<void>;
    resolve: () => void;
  } {
    let resolver: () => void = () => undefined;
    const promise = new Promise<void>((resolve) => {
      resolver = resolve;
    });
    return { promise, resolve: resolver };
  }
  function wakeDispatch(): void {
    const prev = dispatchWake;
    dispatchWake = makeDispatchWake();
    prev.resolve();
  }
  /**
   * Cached per-spawn context the recycle path needs to respawn the
   * child against the same deploy tree. Populated on `spawn(opts)`;
   * cleared on `shutdown`. The recycle path never mutates the
   * `stepOrder` or `definitionHash` -- the orthogonality with redeploy
   * lives at this field: a deploy-tree change would land via a
   * different code path that minted a new supervisor.
   */
  let spawnContext: SpawnContext | null = null;
  let recyclePolicy: RecyclePolicy | null = null;
  let recycleInProgress = false;

  function onChildCrash(reason: string): void {
    logger.error`workflow-process control channel crash: {reason}`;
    void shutdownInternal({ reason });
  }

  function onMailMessage(rawMessage: Uint8Array): void {
    // Every inbound mail flows through the FIFO inbox claim-check
    // queue, regardless of the supervisor's current phase. The
    // dispatch loop (started by `spawn()` and restarted by the
    // recycle path's `installNewChild`) drains the inbox in arrival
    // order and forwards each entry to the child as a `trigger.fire`.
    //
    // The substrate's per-repo lock serializes concurrent enqueues
    // against drains and replays; arrival ordering is preserved by
    // the envelope's `receivedAt` prefix on the inbox filename.
    if (
      state.phase === "idle" ||
      state.phase === "stopping" ||
      state.phase === "stopped"
    ) {
      // The host's higher-level lifecycle is already tearing the
      // deployment down; the message drops on the floor rather than
      // landing in an inbox no live dispatch loop will service.
      return;
    }
    void enqueueInboundMail(rawMessage).catch((cause) => {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`enqueueInbox failed: ${message}`;
    });
  }

  async function enqueueInboundMail(rawMessage: Uint8Array): Promise<void> {
    const messageId = await deriveMessageId(rawMessage);
    const mailAuditRef: MailAuditRef = deriveMailAuditRef(
      messageId,
      rawMessage,
    );
    const receivedAt = Date.now();
    // Inline the raw mail bytes on the claim-check envelope so the
    // workflow-process child can recover its step input by messageId at
    // `trigger.fired` time. The supervisor is the sole mail owner (§3a)
    // and has no separate durable byte store the child reads; the bytes
    // survive the inbox->processing transition verbatim and are dropped
    // when `markConsumed` writes the dedup index.
    const rawMessageBase64 = base64Encode(rawMessage);
    // D2 leg: `enqueueInbox` runs in `onMailMessage` BEFORE dispatch, so
    // it is paid OUTSIDE the dispatch-start..reply-produced window -- its
    // growth is invisible to the 4.7 bracket. The leg mark, keyed by the
    // same messageId the dispatch loop later uses as the runId, makes the
    // out-of-window cost visible and joinable to the in-window legs.
    legMarkStart(messageId, "enqueue");
    await inboxPrimitives.enqueueInbox(
      bindings.repoStore,
      inboxWritePrincipal,
      bindings.workflowRunRepoId,
      {
        address: bindings.deploymentMailAddress,
        messageId,
        receivedAt,
        mailAuditRef,
        rawMessage: rawMessageBase64,
      },
    );
    legMarkEnd(messageId, "enqueue");
    wakeDispatch();
  }

  /**
   * Pump child-initiated upstream control frames after `ready` has
   * landed. The supervisor's primary `waitForReady` consumed the
   * `ready` frame and returned; this generator continues the iterator
   * and recognises the upstream variants the protocol allows from the
   * child (today: `recycle.request`). Any frame the supervisor does
   * not recognise on the upstream side is dropped after a logged
   * warning -- the receiver iterator already validated the envelope
   * and signature.
   */
  async function pumpUpstreamControl(
    iter: AsyncGenerator<ControlPayload, void, void>,
    cohortBroadcaster: TerminalBroadcaster,
  ): Promise<void> {
    for await (const payload of iter) {
      if (payload.type === "recycle.request") {
        logger.info`workflow-process self-initiated recycle.request: ${payload.data.reason}`;
        // Run the recycle off the iterator's loop so the iterator can
        // continue draining frames the supervisor's drain step will
        // produce. The recycle path tears the iterator down via the
        // existing kill of the child handle.
        void recycle({
          reason: `self-initiated: ${payload.data.reason}`,
          origin: "self",
        }).catch((cause) => {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.error`self-initiated recycle failed: ${message}`;
        });
        return;
      }
      if (payload.type === "substrate.write.request") {
        // Run the write off the iterator's loop so the iterator can
        // continue draining other upstream frames (notably the
        // substrate.merge.response that resolves the merge round-trip
        // for this very write -- if the loop were blocked here, the
        // merge response could not be consumed and the write would
        // deadlock).
        void handleSubstrateWriteRequest(payload.data).catch((cause) => {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.error`substrate.write.request handler crashed: ${message}`;
        });
        continue;
      }
      if (payload.type === "substrate.merge.response") {
        // Resume the pending merge round-trip with the child's
        // response. The handler resolves a per-write awaiter inside
        // the substrate write handler's merge callback.
        resolveMergeResponse(payload.data);
        continue;
      }
      if (payload.type === "outbound.message") {
        // OUTBOUND half of mailbox ownership (§3a). The child produced a
        // reply or invoked a mail-send tool; the supervisor is the sole
        // mail owner and performs the actual signed send through the
        // host's real transport. Run it off the iterator's loop so the
        // iterator keeps draining other upstream frames while the host
        // transport assembles and signs the mail; the handler owns the
        // `outbound.result` reply that resolves the child's awaiter.
        void handleOutboundMessage(payload.data).catch((cause) => {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.error`outbound.message handler crashed: ${message}`;
        });
        continue;
      }
      if (payload.type === "terminal.event") {
        // The workflow-process child mirrors every terminal-run commit
        // over the control IPC. Fan it out to the COHORT'S broadcaster
        // -- captured at pump-start time, not resolved dynamically
        // against the supervisor's current `state`. The pump is one-
        // to-one with its cohort's `controlIncoming` iterator: a
        // buffered `terminal.event` the OLD child emitted before kill
        // landed must NEVER route to the NEW cohort's broadcaster.
        // Without this binding, a stale OLD-cohort frame for a runId
        // the NEW cohort happens to be dispatching under the same id
        // (the normal recycle/replay case) would falsely settle the
        // NEW cohort's `waitForRunTerminal` and commit `markConsumed`
        // on a run still in flight. The broadcaster's own `dispose()`
        // on cohort teardown turns post-dispose notify into a no-op,
        // so a stale frame dequeued after the cohort was torn down
        // drops cleanly without leaking into any successor cohort.
        const event = terminalEventFromPayload(payload.data);
        cohortBroadcaster.notify(payload.data.runId, event);
        continue;
      }
      if (payload.type === "park.notify") {
        // The workflow-process child reported a control-plane suspension: an
        // agent step parked on a reserved `signalName(correlationId)` channel.
        // Register it the same way the reconnect re-emit driver does, through
        // the shared `registerSuspension` transform.
        registerSuspension({
          runId: payload.data.runId,
          correlationId: payload.data.correlationId,
          kind: payload.data.kind,
          ...(payload.data.snapshot !== undefined
            ? { snapshot: payload.data.snapshot }
            : {}),
        });
        continue;
      }
      if (payload.type === "parked-correlations.response") {
        // The child answered a `reEmitParkedCorrelations` query. Resolve the
        // awaiting driver; a response with no pending entry (the query already
        // timed out and dropped it) is logged and dropped, never thrown, so it
        // cannot tear the pump down.
        resolveParkedResponse(payload.data);
        continue;
      }
      logger.warn`workflow-process upstream control payload ignored: type=${payload.type}`;
    }
  }

  // Pending merge round-trips keyed by the child's `requestId`. The
  // substrate-write handler installs an entry under each `requestId`
  // before emitting `substrate.merge.request` upstream; the matching
  // `substrate.merge.response` resolves the awaiter so the supervisor's
  // merge callback continues. The entry stays alive only across one
  // merge round-trip; the substrate may invoke the callback multiple
  // times per write (per-repo lock retry semantics), so a fresh
  // requestId-scoped allocator-per-merge-call is used.
  type PendingMerge = {
    resolve: (
      result:
        | { ok: true; files: Record<string, string | Uint8Array> }
        | { ok: false; reason: string },
    ) => void;
  };
  const pendingMerges = new Map<string, PendingMerge>();

  /**
   * Reject every pending merge round-trip and every
   * `markConsumed` completion waiter. Invoked on cohort transitions
   * (shutdown, recycle's `installNewChild`) so closures awaiting these
   * promises do not outlive the cohort that armed them. Without this,
   * a `handleSubstrateWriteRequest` mid-merge or a dispatch-loop
   * caller awaiting `markConsumed` would sit on a resolver that the
   * dying control channel will never invoke.
   */
  function rejectCohortAwaiters(reason: string): void {
    for (const [requestId, entry] of pendingMerges) {
      pendingMerges.delete(requestId);
      entry.resolve({ ok: false, reason: `cohort aborted: ${reason}` });
    }
    for (const [requestId, entry] of pendingParkedQueries) {
      pendingParkedQueries.delete(requestId);
      entry.settle(null);
    }
    for (const [runId, waiter] of markConsumedCompletionWaiters.entries()) {
      markConsumedCompletionWaiters.delete(runId);
      waiter.reject(
        new MergeAbortedError(`markConsumed waiter (${runId}): ${reason}`),
      );
    }
  }

  function resolveMergeResponse(
    data: Extract<ControlPayload, { type: "substrate.merge.response" }>["data"],
  ): void {
    const entry = pendingMerges.get(data.requestId);
    if (entry === undefined) {
      logger.warn`substrate.merge.response landed with no pending entry; requestId=${data.requestId} dropped`;
      return;
    }
    pendingMerges.delete(data.requestId);
    if (data.result.ok) {
      const files: Record<string, string | Uint8Array> = {};
      try {
        for (const file of data.result.files) {
          files[file.path] = base64ToBytes(file.contentBase64);
        }
      } catch (cause) {
        // `base64ToBytes` throws loudly on malformed child-supplied
        // content. This runs synchronously from `pumpUpstreamControl`'s
        // `for await`, so an escaping throw would tear the pump down and
        // stop draining every other upstream control frame for the
        // cohort. Mirror the child-side `decodeMergeRequest` hardening:
        // resolve the pending merge as a failure so the write handler
        // surfaces it as a structured substrate.write.response.
        const reason = cause instanceof Error ? cause.message : String(cause);
        entry.resolve({
          ok: false,
          reason: `supervisor substrate.merge.response: decode failed: ${reason}`,
        });
        return;
      }
      entry.resolve({ ok: true, files });
      return;
    }
    entry.resolve({ ok: false, reason: data.result.reason });
  }

  // Stamp the deployment identity the supervisor owns onto a child-supplied
  // park and hand it to the host's suspension-register sink. Shared by the
  // `park.notify` arm (the happy-path emit) and `reEmitParkedCorrelations`
  // (the re-establishment re-emit). Best-effort: a throwing sink is logged,
  // not rethrown, so it cannot tear the upstream pump down or abort a re-emit
  // partway through the parked set. The sink (production: the sidecar) turns
  // the stamped registration into a `signal.correlation.register` frame the
  // hub co-writes the run's routing + approval rows from.
  function registerSuspension(park: {
    runId: string;
    correlationId: string;
    kind: SignalKind;
    snapshot?: ApprovalSnapshot;
  }): void {
    if (bindings.onSuspensionRegister === undefined) {
      logger.warn`suspension register for runId=${park.runId} but no onSuspensionRegister sink is wired; correlation ${park.correlationId} not registered`;
      return;
    }
    try {
      bindings.onSuspensionRegister({
        runId: park.runId,
        correlationId: park.correlationId,
        kind: park.kind,
        deploymentId: bindings.deploymentId,
        agentAddress: bindings.deploymentMailAddress,
        ...(park.snapshot !== undefined
          ? { approvalSnapshot: park.snapshot }
          : {}),
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`onSuspensionRegister sink threw for runId=${park.runId} correlationId=${park.correlationId}: ${message}`;
    }
  }

  // Pending `reEmitParkedCorrelations` queries keyed by the supervisor-minted
  // `requestId`. The driver installs an entry, sends `parked-correlations.
  // request`, and awaits; the matching `parked-correlations.response` resolves
  // it. Supervisor-minted (not child-echoed like `pendingMerges`) because the
  // supervisor originates this request; the counter lives in the factory
  // closure so a recycle does not reset it and let a late old-child response
  // resolve a new query.
  type ParkedCorrelations = Extract<
    ControlPayload,
    { type: "parked-correlations.response" }
  >["data"]["parked"];
  // Settle with the child's `parked` list, or `null` when the cohort is torn
  // down before it answers. Mirrors `pendingMerges`: a settle-with-sentinel,
  // never a reject, because a cohort abort is an ordinary outcome the driver
  // handles -- and Trigger A fires the driver concurrently with arbitrary
  // shutdown/recycle, so a rejection here would surface as an unhandled
  // rejection the moment a teardown aborts an in-flight auto-emit.
  type PendingParkedQuery = {
    settle: (parked: ParkedCorrelations | null) => void;
  };
  const pendingParkedQueries = new Map<string, PendingParkedQuery>();
  let parkedQuerySeq = 0;

  function resolveParkedResponse(
    data: Extract<
      ControlPayload,
      { type: "parked-correlations.response" }
    >["data"],
  ): void {
    const entry = pendingParkedQueries.get(data.requestId);
    if (entry === undefined) {
      logger.warn`parked-correlations.response landed with no pending entry; requestId=${data.requestId} dropped`;
      return;
    }
    pendingParkedQueries.delete(data.requestId);
    entry.settle(data.parked);
  }

  async function reEmitParkedCorrelations(): Promise<void> {
    // No-op (NOT throw) when the child is not addressable. `deliverSignal`
    // throws on non-running/starting (including recycling) because it points a
    // write at the dying child's closing pipe and wants the caller to retry;
    // this driver's contract is the opposite -- the next re-establishment
    // re-drives it -- so skipping recycling and letting the next spawn's
    // re-emit cover it is correct here, not a missed guard.
    if (state.phase !== "running" && state.phase !== "starting") {
      logger.info`reEmitParkedCorrelations: child not addressable (phase=${state.phase}); skipping`;
      return;
    }
    const controlSender = state.controlSender;
    const requestId = `pc-${String((parkedQuerySeq += 1))}`;
    const responded = new Promise<ParkedCorrelations | null>((resolve) => {
      pendingParkedQueries.set(requestId, { settle: resolve });
    });
    // Watchdog: a wedged-but-alive child never tears its cohort down, so the
    // cohort-abort settle would never fire and this await would hang the
    // re-establishment caller. On expiry, drop the pending entry and return;
    // the next re-establishment re-drives (the hub co-write is idempotent).
    let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
    const watchdog = new Promise<"timeout">((resolve) => {
      timeoutHandle = setTimeout(() => {
        timeoutHandle = null;
        if (pendingParkedQueries.delete(requestId)) {
          logger.warn`reEmitParkedCorrelations: requestId=${requestId} did not respond within ${String(parkedQueryWatchdogMs)}ms; re-registration retries on the next re-establishment`;
        }
        resolve("timeout");
      }, parkedQueryWatchdogMs);
    });
    let outcome: ParkedCorrelations | null | "timeout";
    try {
      await controlSender.send({
        type: "parked-correlations.request",
        data: { requestId },
      });
      outcome = await Promise.race([responded, watchdog]);
    } catch (cause) {
      // The downstream send failed (a closing pipe). Best-effort: drop the
      // pending entry, log, and return; the next re-establishment re-drives.
      pendingParkedQueries.delete(requestId);
      if (timeoutHandle !== null) clearTimeout(timeoutHandle);
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`reEmitParkedCorrelations query failed: ${message}; re-registration retries on the next re-establishment`;
      return;
    }
    if (timeoutHandle !== null) clearTimeout(timeoutHandle);
    // `timeout` (watchdog fired) or `null` (cohort torn down before the child
    // answered): nothing to re-emit; the next re-establishment re-drives.
    if (outcome === "timeout" || outcome === null) return;
    for (const parked of outcome) {
      registerSuspension(parked);
    }
  }

  /**
   * OUTBOUND half of mailbox ownership (§3a). The workflow-process child
   * never holds the agent's signing key; it forwards the structured
   * outbound message plus the sender (agent) address up over the control
   * channel and the supervisor performs the actual signed send through
   * the host's real transport (`bindings.mailBus.sendOutbound`). The
   * host transport signs with the sender's `CryptoProvider` -- the same
   * `executeSend` path the in-process agent uses -- so the outbound mail
   * carries the AGENT's signature with full parity to the pre-supervisor
   * path. A send failure (unregistered sender, signing failure,
   * transport rejection) surfaces back to the child as a structured
   * `{ ok: false, reason }` so the agent's mail-tool call fails loudly
   * rather than silently dropping the send.
   */
  async function handleOutboundMessage(
    data: Extract<ControlPayload, { type: "outbound.message" }>["data"],
  ): Promise<void> {
    const controlSender = activeControlSender();
    if (controlSender === null) {
      // The request arrived after the control sender was cleared (the
      // supervisor is mid-recycle or tearing down). The child's read end
      // is being closed alongside this transition, so its pending
      // mail-tool awaiter surfaces a pipe-close error on its own read.
      // There is no sender to write the `outbound.result` on; dropping
      // the frame is the only available action, logged loudly.
      logger.warn`outbound.message received outside running phase; requestId=${data.requestId} dropped (child awaiter will fail on pipe close)`;
      return;
    }
    try {
      const message = outboundMessageFromPayload(data.message);
      const receipt = await bindings.mailBus.sendOutbound(
        data.senderAddress,
        message,
      );
      await controlSender.send({
        type: "outbound.result",
        data: {
          requestId: data.requestId,
          result: {
            ok: true,
            messageId: receipt.messageId,
            status: receipt.status,
          },
        },
      });
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      await controlSender.send({
        type: "outbound.result",
        data: {
          requestId: data.requestId,
          result: { ok: false, reason },
        },
      });
    }
  }

  async function handleSubstrateWriteRequest(
    data: Extract<ControlPayload, { type: "substrate.write.request" }>["data"],
  ): Promise<void> {
    const controlSender = activeControlSender();
    if (controlSender === null) {
      // The request arrived after `activeControlSender()` was
      // cleared (the supervisor is in `recycling` mid-swap, or
      // `draining`/`stopping`/`stopped`). The child's read end of
      // the IPC pipe is being torn down alongside this transition,
      // so the child's pending waiter will surface a pipe-close
      // error on its own read rather than wedge. Dropping the
      // frame here is the only available action -- there is no
      // sender to write the response on, and routing the response
      // to whatever next-cohort sender exists would deliver it to
      // the wrong child. Logged loudly so persistent occurrences
      // surface in operator logs.
      logger.warn`substrate.write.request received outside running phase; requestId=${data.requestId} dropped (child waiter will fail on pipe close)`;
      return;
    }
    const validatedRepoId = RepoId(data.repoId);
    if (validatedRepoId instanceof type.errors) {
      onChildCrash(
        `substrate.write.request repoId failed validation: ${validatedRepoId.summary}`,
      );
      return;
    }
    // The child proxies workflow-run writes; an inbound request for a
    // different repo kind is a protocol violation. The supervisor owns
    // the write contract for the workflow-run repo specifically.
    if (validatedRepoId.kind !== "workflow-run") {
      await controlSender.send({
        type: "substrate.write.response",
        data: {
          requestId: data.requestId,
          result: {
            ok: false,
            reason: `supervisor substrate.write.request: repoId.kind must be "workflow-run", got ${JSON.stringify(validatedRepoId.kind)}`,
          },
        },
      });
      return;
    }
    // The substrate principal authoring the proxied write is the
    // `workflow-process` principal scoped to this supervisor's
    // deployment. The child has no write authority of its own (it
    // holds no private key on the host process), but the workflow-run
    // kind handler is the authority that accepts the
    // `workflow-process` principal for `runs/<runId>/` writes
    // (including the origin-specific CancelRequested checks that pin
    // `self` to `workflow-process`). Authoring proxied writes under
    // this kind preserves the on-disk audit semantics the original
    // child-direct-write path produced; the only architectural change
    // is which process owns the substrate write contract.
    const writePrincipal: WorkflowRunWorkflowProcessPrincipal = {
      kind: "workflow-process",
      deploymentId: bindings.deploymentId,
    };
    // The commit's terminal detection comes from the kind handler's
    // typed `newlyTerminalRuns` signal (returned below), not a sniff of
    // the merged files: the handler authoritatively determines, during
    // validation, which runs reached a terminal event in this commit.
    // Holding the substrate.write.response on that signal gates the
    // child's runtime-body progress on the inbox transition landing,
    // closing the window where a downstream consumer observes
    // RunCompleted ahead of the matching consumed/ entry on this
    // supervisor (the cross-process hub-pack ordering is still racy, but
    // the local supervisor's state is self-consistent at the response
    // boundary).
    // D2 leg classification (measurement-only). The child proxies two
    // distinct substrate commits through this one handler, discriminated
    // by the write's `preservePrefix`:
    //   - `runs/<runId>/events/`   -> the run-event bracket commit
    //     (RunStarted/StepStarted/StepCompleted/RunCompleted; one message
    //     may produce several, each a separate write -- the D2
    //     post-processing sums and counts them per message).
    //   - `agent-state/<key>/...`  -> the D1 conversation WAL append /
    //     checkpoint (the control leg). No runId in the prefix; attributed
    //     to the dispatch loop's current serial runId.
    // Any other prefix is a non-attributed proxied write (cancel/drain
    // audit) and is left unmarked. The runId join key matches the leg the
    // benchmark's per-message OLS fit groups on.
    const legClassification = classifyProxiedWriteLeg(data.preservePrefix);
    if (legClassification !== null) {
      legMarkStart(legClassification.runId, legClassification.leg);
    }
    try {
      const { commitSha, newlyTerminalRuns } =
        await bindings.repoStore.writeTreePreservingPrefix(
          writePrincipal,
          validatedRepoId,
          data.ref,
          {
            preservePrefix: data.preservePrefix,
            message: data.message,
            merge: async (existing) => {
              const sender = activeControlSender();
              if (sender === null) {
                throw new Error(
                  "supervisor substrate.write.request: control channel unavailable for merge round-trip",
                );
              }
              const result = await new Promise<
                | { ok: true; files: Record<string, string | Uint8Array> }
                | { ok: false; reason: string }
              >((resolve) => {
                pendingMerges.set(data.requestId, { resolve });
                const wireExisting: {
                  path: string;
                  contentBase64: string;
                }[] = [];
                for (const [path, bytes] of existing) {
                  wireExisting.push({
                    path,
                    contentBase64: bytesToBase64(bytes),
                  });
                }
                void sender
                  .send({
                    type: "substrate.merge.request",
                    data: {
                      requestId: data.requestId,
                      existing: wireExisting,
                    },
                  })
                  .catch((cause) => {
                    pendingMerges.delete(data.requestId);
                    const reason =
                      cause instanceof Error ? cause.message : String(cause);
                    resolve({
                      ok: false,
                      reason: `supervisor substrate.merge.request send failed: ${reason}`,
                    });
                  });
              });
              if (!result.ok) {
                throw new Error(
                  `supervisor substrate.write.request: child merge failed: ${result.reason}`,
                );
              }
              return result.files;
            },
          },
        );
      // D2 leg end: the substrate commit (hash objects, write tree,
      // advance ref under the per-repo lock) just resolved. Stamped here,
      // before the terminal-write markConsumed-coupling wait below, so the
      // run-event/wal leg measures only its own commit and not the
      // dispatch loop's markConsumed (which the `markconsumed` leg owns).
      if (legClassification !== null) {
        legMarkEnd(legClassification.runId, legClassification.leg);
      }
      const watchdog =
        await synchronouslyDispatchTerminalWrite(newlyTerminalRuns);
      if (!watchdog.ok) {
        await controlSender.send({
          type: "substrate.write.response",
          data: {
            requestId: data.requestId,
            result: { ok: false, reason: watchdog.reason },
          },
        });
        return;
      }
      await controlSender.send({
        type: "substrate.write.response",
        data: {
          requestId: data.requestId,
          result: { ok: true, commitSha },
        },
      });
      // Seal each run that reached a terminal event in this commit: fold
      // its per-event files into one combined events.jsonl. Off the hot
      // path -- the child's write has already been acknowledged above -- so
      // a failure is logged and does not block dispatch; the run is left in
      // per-event form, which readers handle. There is no later trigger for
      // a run whose fold is interrupted here (e.g. by a crash before the
      // fold commits): the terminal signal fires once. A bounded recovery
      // sweep is not yet implemented; until then such a run stays
      // per-event. The fold commit carries no newly-added terminal event,
      // so it does not re-fire this terminal-write coupling.
      for (const { runId } of newlyTerminalRuns) {
        void compactRunEvents({
          substrate: bindings.repoStore,
          repoId: validatedRepoId,
          ref: data.ref,
          deploymentId: bindings.deploymentId,
          runId,
        }).catch((cause) => {
          logger.warn`compaction of run ${runId} failed: ${cause instanceof Error ? cause.message : String(cause)}`;
        });
      }
    } catch (cause) {
      // Clean up any merge awaiter that the substrate may not have
      // reached (e.g. the write threw before invoking the merge
      // callback at all, leaving the map empty -- safe), and the
      // common case where the write reached merge but then threw
      // downstream (the awaiter is already resolved by the merge
      // reply path, so the delete here is a no-op).
      pendingMerges.delete(data.requestId);
      const reason = cause instanceof Error ? cause.message : String(cause);
      await controlSender.send({
        type: "substrate.write.response",
        data: {
          requestId: data.requestId,
          result: { ok: false, reason },
        },
      });
    }
  }

  // Per-runId synchronization between the substrate-write handler
  // and the dispatch loop's `markConsumed`. The handler arms a
  // waiter when it commits a terminal-event blob and waits for the
  // dispatch loop to fire `resolveMarkConsumedWaiter(runId)` before
  // sending the substrate.write.response back to the child.
  const markConsumedCompletionWaiters = new Map<
    string,
    { resolve: () => void; reject: (err: Error) => void }
  >();

  function resolveMarkConsumedWaiter(runId: string): void {
    const waiter = markConsumedCompletionWaiters.get(runId);
    if (waiter === undefined) return;
    markConsumedCompletionWaiters.delete(runId);
    waiter.resolve();
  }

  /**
   * Hold the substrate.write.response until the dispatch loop's
   * markConsumed settles for each run the kind handler reports as newly
   * terminal in this commit. Terminal-ness comes from the handler's typed
   * `newlyTerminalRuns` signal -- determined authoritatively during
   * validation -- not re-derived from the committed path shape, so it
   * survives the run-event layout changing (e.g. compaction folding a
   * run's per-event files into one combined file). The wait is per-runId
   * so multiple runs can proceed concurrently if a future dispatch loop
   * ever processes more than one mail in parallel.
   *
   * A watchdog timeout (`terminalWriteWatchdogMs`) caps each wait so a
   * never-arming markConsumed (a bug in the dispatch loop, a torn-down
   * cohort, a stalled inbox primitive) does not deadlock the child's
   * write -- and therefore the runtime body, and therefore the dispatch
   * loop. On expiry the waiter is force-released and a structured failure
   * propagates back to the child as
   * `{ ok: false, reason: "terminal-write watchdog timeout: ..." }`.
   */
  async function synchronouslyDispatchTerminalWrite(
    newlyTerminalRuns: readonly NewlyTerminalRun[],
  ): Promise<{ ok: true } | { ok: false; reason: string }> {
    const holds: Promise<{ ok: true } | { ok: false; reason: string }>[] = [];
    for (const { runId, terminalEventJson } of newlyTerminalRuns) {
      if (!inFlightRuns.has(runId)) continue;
      holds.push(holdResponseForMarkConsumed(runId, terminalEventJson));
    }
    if (holds.length === 0) return { ok: true };
    const results = await Promise.all(holds);
    return results.find((r) => !r.ok) ?? { ok: true };
  }

  async function holdResponseForMarkConsumed(
    runId: string,
    terminalEventJson: string,
  ): Promise<{ ok: true } | { ok: false; reason: string }> {
    const completed = new Promise<void>((resolve, reject) => {
      markConsumedCompletionWaiters.set(runId, { resolve, reject });
    });
    const broadcaster = activeTerminalBroadcaster();
    if (broadcaster !== null) {
      const synthetic = synthesizeTerminalEvent(terminalEventJson);
      if (synthetic !== null) {
        broadcaster.notify(runId, synthetic);
      }
    }
    let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
    const watchdog = new Promise<{ ok: false; reason: string }>((resolve) => {
      timeoutHandle = setTimeout(() => {
        timeoutHandle = null;
        // Force-release the waiter so the dispatch loop's eventual
        // resolve does not strand a dangling map entry, then surface
        // the structured failure to the caller. The reason text is
        // logged through the package logger so the watchdog is not
        // silent on the host side.
        const stillPending =
          markConsumedCompletionWaiters.get(runId) !== undefined;
        if (stillPending) {
          markConsumedCompletionWaiters.delete(runId);
        }
        const reason = `terminal-write watchdog timeout: markConsumed for runId=${runId} did not settle within ${String(terminalWriteWatchdogMs)}ms`;
        logger.error`${reason}`;
        resolve({ ok: false, reason });
      }, terminalWriteWatchdogMs);
    });
    const result = await Promise.race([
      completed.then(() => ({ ok: true }) as const),
      watchdog,
    ]);
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
    }
    return result;
  }

  function synthesizeTerminalEvent(
    terminalEventJson: string,
  ): TerminalRunEvent | null {
    let parsed: unknown;
    try {
      parsed = JSON.parse(terminalEventJson);
    } catch {
      return null;
    }
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      !("type" in parsed) ||
      !("seq" in parsed)
    ) {
      return null;
    }
    const body = parsed as {
      type?: unknown;
      seq?: unknown;
      at?: unknown;
      error?: { message?: unknown };
    };
    if (typeof body.seq !== "number") return null;
    const at = typeof body.at === "string" ? body.at : new Date().toISOString();
    if (body.type === "RunCompleted") {
      return { kind: "RunCompleted", seq: body.seq, at };
    }
    if (body.type === "RunCancelled") {
      return { kind: "RunCancelled", seq: body.seq, at };
    }
    if (body.type === "RunFailed") {
      // The wire schema makes `error.message` required when the event
      // type is `RunFailed`. An event that doesn't carry one is a
      // contract violation upstream of the supervisor; coercing it to an
      // empty string would silently hide the producer bug.
      if (typeof body.error?.message !== "string") {
        throw new Error(
          `synthesizeTerminalEvent: RunFailed event missing required error.message`,
        );
      }
      return {
        kind: "RunFailed",
        seq: body.seq,
        at,
        error: { message: body.error.message },
      };
    }
    return null;
  }

  function activeControlSender(): ControlChannelSender | null {
    if (
      state.phase === "starting" ||
      state.phase === "running" ||
      state.phase === "recycling"
    ) {
      return state.controlSender;
    }
    return null;
  }

  function activeTerminalBroadcaster(): TerminalBroadcaster | null {
    if (
      state.phase === "starting" ||
      state.phase === "running" ||
      state.phase === "recycling"
    ) {
      return state.terminalBroadcaster;
    }
    return null;
  }

  async function wireChild(args: {
    channelId: string;
    hmacKey: Uint8Array;
    ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
    handle: SubprocessHandle;
    onInferenceEvent: (event: EventPayload) => void;
  }): Promise<{
    wiring: ChildWiring;
    readyPromise: Promise<{ childPid: number }>;
    controlIncoming: AsyncGenerator<ControlPayload, void, void>;
  }> {
    const controlSender = createControlChannelSender({
      privateKeySeed: args.ipcKeypair.privateKey,
      channelId: args.channelId,
      writer: args.handle.controlWriter,
    });

    const controlIncoming = receiveControlChannel({
      publicKey: { bootstrapFromReady: true },
      channelId: args.channelId,
      reader: args.handle.controlReader,
      onCrash: onChildCrash,
    });

    const readyPromise = waitForReady(controlIncoming);

    const eventIter = receiveEventChannel({
      hmacKey: args.hmacKey,
      channelId: args.channelId,
      reader: args.handle.eventReader,
      onCrash: (reason) => {
        logger.error`workflow-process event channel crash: {reason}`;
        void shutdownInternal({ reason });
      },
    });
    const eventPump = pumpEvents(eventIter, args.onInferenceEvent);

    return {
      wiring: {
        handle: args.handle,
        controlSender,
        channelId: args.channelId,
        eventPump,
      },
      readyPromise,
      controlIncoming,
    };
  }

  async function spawn(opts: SpawnOpts): Promise<SpawnResult> {
    if (state.phase !== "idle") {
      throw new Error(
        `supervisor: spawn called in phase ${state.phase}; expected idle`,
      );
    }
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    const ipcKeypair = await (bindings.ipcKeyPairFactory ?? generateKeyPair)();
    const env = buildChildSpawnEnv({
      substrateEnv: bindings.substrateEnv,
      dynamicSpawnEnv: bindings.dynamicSpawnEnv,
      channelId,
      hmacKey,
      hostPublicKey: ipcKeypair.publicKey,
      deploymentId: bindings.deploymentId,
      deploymentMailAddress: bindings.deploymentMailAddress,
      stepCount: bindings.stepCount,
      definitionHash: opts.definitionHash,
      warmKeep: opts.warmKeep,
    });

    const handle = bindings.subprocessSpawner({
      binaryPath: bindings.binaryPath,
      env,
    });

    let wired: Awaited<ReturnType<typeof wireChild>>;
    try {
      wired = await wireChild({
        channelId,
        hmacKey,
        ipcKeypair,
        handle,
        onInferenceEvent: opts.onInferenceEvent,
      });
    } catch (cause) {
      // wireChild threw before any state record owns the handle, so
      // shutdownInternal -- which reaches the handle through the
      // active-state record -- would early-return on the "idle" phase
      // without killing it. Kill the freshly-spawned child directly to
      // avoid orphaning the OS process.
      await killChildHandle(handle, DEFAULT_KILL_TIMEOUT_MS, {
        setTimer: readySetTimer,
        clearTimer: readyClearTimer,
        logger,
      });
      throw cause;
    }

    // The ready handshake below folds `wired.readyPromise` into an
    // outcome value, handling its rejection. But a startup teardown that
    // fires BEFORE the handshake -- a throw during credentials assembly
    // or mail registration -- kills the child, and that kill rejects
    // `readyPromise` (the control channel ends). Attach a benign handler
    // now so the rejection is never unhandled on that path; the
    // handshake's own fold still observes the outcome when it runs.
    void wired.readyPromise.catch(() => {
      /* handled by the ready-handshake fold when the handshake runs */
    });

    // Cohort abort controller covers terminal-event watcher
    // lifetime AND dispatch-loop lifetime; the abort fires on
    // shutdown and on every recycle's `installNewChild`. The
    // controller is minted unconditionally so the dispatch loop
    // always has a cancellation source. The cohort broadcaster
    // matches the same lifetime: the supervisor's pumpUpstreamControl
    // fans `terminal.event` upstream frames into it, and consumers
    // (dispatch loop, drain accumulators) subscribe through its
    // `source` accessor.
    state = {
      phase: "starting",
      handle,
      controlSender: wired.wiring.controlSender,
      channelId,
      eventPump: wired.wiring.eventPump,
      onInferenceEvent: opts.onInferenceEvent,
      mailUnsubscribe: null,
      credentialsSnapshot: null,
      terminalCohortAbort: new AbortController(),
      terminalBroadcaster: createTerminalBroadcaster(),
      dispatchLoop: null,
      replayDone: null,
    };

    // Everything from here to the successful `return` runs with the state
    // record in "starting" (then "running"). A throw at any of these
    // steps -- credentials assembly, mail registration, the ready
    // handshake, the credentials push, the dispatch-loop start -- routes
    // through shutdownInternal, the single owner of starting/running
    // teardown: it kills the handle and releases the mail subscription
    // and address registration installed below.
    try {
      const credentialsSnapshot = await assembleCredentialsSnapshot({
        repoStore: bindings.repoStore,
        principal: bindings.readPrincipal,
        stepOrder: opts.stepOrder,
        deploymentId: bindings.deploymentId,
        deriveStepAddress: bindings.deriveStepAddress,
        ...(bindings.deriveStepRepoId !== undefined
          ? { deriveStepRepoId: bindings.deriveStepRepoId }
          : {}),
      });
      state.credentialsSnapshot = credentialsSnapshot;

      // Replay any orphaned `processing/` entries back to `inbox/`
      // BEFORE the dispatch loop's first dequeue. A crash mid-dispatch
      // in a prior supervisor incarnation can leave an entry in
      // `processing/` with no owner; the FIFO contract requires the
      // entry move back to `inbox/` so the next dispatch picks it up
      // in its original arrival position. The replay runs off the
      // spawn critical path (the substrate write may roundtrip through
      // the pack-pushing wrap and a slow hub), but `runDispatchLoop`
      // takes the promise as an argument and awaits it before its
      // first `dequeueToProcessing` so a fresh inbound mail that lands
      // during the replay window cannot ship ahead of the orphan once
      // the replay completes.
      const replayDone = readOwnedMessageIds(
        bindings.repoStore,
        bindings.workflowRunRepoId,
      )
        .then((ownedMessageIds) =>
          inboxPrimitives.replayProcessingToInbox(
            bindings.repoStore,
            inboxWritePrincipal,
            bindings.workflowRunRepoId,
            bindings.deploymentMailAddress,
            { ownedMessageIds },
          ),
        )
        .then(() => {
          wakeDispatch();
        })
        .catch((cause) => {
          // Documented best-effort: a failed replay leaves orphaned
          // `processing/` entries parked and the dispatch loop will
          // then ship newly-enqueued mail ahead of them, violating
          // the FIFO contract described in the comment above.
          // Tightening this to a fatal `onChildCrash` was attempted
          // but caused spurious crashes in the integration suite
          // where the first spawn legitimately has no
          // `processing/` directory to replay; resolving that
          // requires either a no-op-on-missing variant of
          // `replayProcessingToInbox` or a dispatch-loop periodic
          // sweep that picks up parked orphans. Left as logged
          // best-effort until that lands.
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`replayProcessingToInbox on spawn failed: ${message}`;
        });
      // Hold the replay promise on the active-state record so
      // `shutdownInternal` awaits its settlement before tearing the
      // bindings down. A shutdown that lands while the replay is in
      // flight would otherwise leave the substrate write pending past
      // the supervisor's exit.
      state.replayDone = replayDone;

      bindings.mailBus.registerAddress(bindings.deploymentMailAddress);
      const mailUnsubscribe = bindings.mailBus.subscribeMailForAddress(
        bindings.deploymentMailAddress,
        onMailMessage,
      );
      state.mailUnsubscribe = mailUnsubscribe;

      // Bound the `ready` handshake. `wired.readyPromise` resolves on `ready`
      // and rejects when the control channel ends (the child exited); a child
      // that neither readies nor exits would block here forever. Fold all three
      // outcomes into values so the single `readyClearTimer` below runs on every
      // path -- ready, child-exit failure, and timeout -- before we act on the
      // result. A `Promise.race` that could reject would skip the clear on the
      // child-exit path and leak an armed deadline that keeps the event loop
      // alive for up to `readyTimeoutMs`. The deadline is resolve-only, so it
      // contributes no rejection of its own. Kill on timeout uses the
      // SIGTERM->SIGKILL escalation because a wedged child may ignore SIGTERM;
      // SIGKILL guarantees `exited` settles.
      const readyOutcome = wired.readyPromise.then(
        (info) => ({ kind: "ready" as const, info }),
        (err: unknown) => ({ kind: "failed" as const, err }),
      );
      const readyDeadline = waitDeadline(readySetTimer, readyTimeoutMs);
      const readyRace = await Promise.race([
        readyOutcome,
        readyDeadline.promise.then(() => ({ kind: "timeout" as const })),
      ]);
      readyClearTimer(readyDeadline.handle);
      if (readyRace.kind === "timeout") {
        await killChildHandle(wired.wiring.handle, DEFAULT_KILL_TIMEOUT_MS, {
          setTimer: readySetTimer,
          clearTimer: readyClearTimer,
          logger,
        });
        // The SIGTERM->SIGKILL escalation above is deliberate: a wedged
        // child may ignore the plain kill shutdownInternal issues. The
        // outer catch then runs shutdownInternal for the "starting"-phase
        // teardown (subscription + address release); its kill against the
        // already-killed handle is idempotent.
        throw new Error(
          `workflow-host supervisor: child did not emit ready within ${readyTimeoutMs}ms; killed`,
        );
      }
      if (readyRace.kind === "failed") {
        // The child exited during the handshake; the outer catch releases
        // the subscription and registration via shutdownInternal.
        throw readyRace.err;
      }
      const readyInfo = readyRace.info;

      // Push the assembled credentialsSnapshot to the child before the
      // mail buffer drains. Without this, the child's
      // `createCredentialsBackedAuthorize` closure observes a null
      // snapshot ref on the first authorize call and throws "no
      // credentialsSnapshot active"; the run's first step fails before
      // the runtime body can commit `StepCompleted`. The send rides the
      // same control channel `trigger.fire` uses, so the ordering
      // guarantee (`grants-updated` lands before `trigger.fire`) holds
      // for buffered and post-ready inbound mail alike.
      //
      // Suppressed when `onRunStart` is wired: that binding makes the
      // dispatch loop push a per-run snapshot before each `trigger.fire`,
      // so the spawn-time push would only mask a broken per-run barrier
      // (the child would already hold grants and never hit its throw-on-
      // null guard). The per-run push is then the sole grants source.
      if (bindings.onRunStart === undefined) {
        await wired.wiring.controlSender.send({
          type: "grants-updated",
          data: {
            snapshot: {
              steps: credentialsSnapshot.steps.map((s) => ({
                stepId: s.stepId,
                address: s.address,
                grants: [...s.grants],
                contentHash: s.contentHash,
              })),
            },
          },
        });
      }

      // Transition to running. The dispatch loop (started below)
      // picks up any pre-ready buffered mail through the FIFO inbox
      // queue rather than through an in-memory buffer; arrival order
      // is preserved by the envelope's `receivedAt` prefix on the
      // inbox filename.
      const startingPhaseCohortAbort = state.terminalCohortAbort;
      if (startingPhaseCohortAbort === null) {
        throw new Error(
          "supervisor: terminalCohortAbort missing after spawn handshake",
        );
      }
      const startingPhaseBroadcaster = state.terminalBroadcaster;
      const dispatchLoop = runDispatchLoop(
        wired.wiring.controlSender,
        startingPhaseCohortAbort,
        startingPhaseBroadcaster,
        replayDone,
      );
      // Surface dispatch-loop failures via the logger; the loop's own
      // catch already swallows per-iteration faults, but a structural
      // failure (e.g. the cohort abort handler itself throws) lands
      // here.
      void dispatchLoop.catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`dispatch loop terminated with error: ${message}`;
      });
      state = {
        phase: "running",
        handle,
        controlSender: wired.wiring.controlSender,
        channelId,
        eventPump: wired.wiring.eventPump,
        onInferenceEvent: opts.onInferenceEvent,
        mailUnsubscribe,
        credentialsSnapshot,
        terminalCohortAbort: startingPhaseCohortAbort,
        terminalBroadcaster: startingPhaseBroadcaster,
        dispatchLoop,
        replayDone,
      };
      // Kick the dispatch loop in case mail landed in the inbox
      // before the loop's first `await dispatchWake`. A wake against a
      // freshly-minted promise is a no-op; the dispatch loop's first
      // dequeue happens unconditionally.
      wakeDispatch();

      // Cache the spawn context for the recycle path. The recycle path
      // reuses the same stepOrder/definitionHash/onInferenceEvent on
      // every respawn -- those are the strict-orthogonality anchors
      // with redeploy, and the supervisor never mutates them.
      const now = bindings.recyclePolicyNow ?? defaultNow;
      spawnContext = {
        stepOrder: opts.stepOrder,
        definitionHash: opts.definitionHash,
        warmKeep: opts.warmKeep,
        onInferenceEvent: opts.onInferenceEvent,
        spawnedAt: now(),
      };

      // Start the upstream control pump so the supervisor sees the
      // child's `recycle.request` (and any future upstream variant) as
      // it arrives. The pump exits when the iterator ends, which
      // happens when the child closes its end of the control channel
      // -- either on shutdown or on recycle's `kill` step. The pump
      // closes over the cohort's broadcaster captured at pump-start
      // time so a `terminal.event` frame the iterator dequeues after a
      // recycle has minted a new cohort routes to THIS cohort's (now
      // disposed) broadcaster, not the successor's.
      void pumpUpstreamControl(
        wired.controlIncoming,
        startingPhaseBroadcaster,
      ).catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`upstream control pump failed: ${message}`;
      });

      // Trigger A: re-register every correlation the freshly-ready child is
      // parked on. A `park.notify` register can be lost while the hub is down
      // at the original suspend; a child that resumes such a parked run (a
      // sidecar restart re-spawning this deployment, or a recycle -- see the
      // matching call in `installNewChild`) re-parks without re-emitting, so
      // the supervisor re-drives it from every re-establishment. Fire-and-
      // forget after the pump is armed to route the response: best-effort,
      // watchdog-bounded, and an empty round-trip when nothing is parked.
      void reEmitParkedCorrelations().catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`re-emit of parked correlations on re-establishment failed: ${message}`;
      });

      // Arm the recycle policy. The policy is a no-op when all bounds
      // are `undefined`; bounds resolution lives inside `createRecyclePolicy`.
      if (bindings.recyclePolicy !== undefined) {
        const setTimer = bindings.recyclePolicySetTimer ?? defaultSetTimer;
        const clearTimer =
          bindings.recyclePolicyClearTimer ?? defaultClearTimer;
        recyclePolicy = createRecyclePolicy({
          bounds: bindings.recyclePolicy,
          now,
          spawnedAt: spawnContext.spawnedAt,
          ...(bindings.readRssBytes !== undefined
            ? { readRssBytes: bindings.readRssBytes }
            : {}),
          ...(bindings.readGrantsAgeMs !== undefined
            ? { readGrantsAgeMs: bindings.readGrantsAgeMs }
            : {}),
          setTimer,
          clearTimer,
          trigger: async (reason) => {
            await recycle({ reason, origin: "policy" });
          },
        });
      }

      return {
        pid: readyInfo.childPid,
        channelId,
        credentialsSnapshot,
      };
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      // Defense-in-depth for a distinct invariant: the original spawn
      // `cause` must survive the unwind. `shutdownInternal` is designed to
      // be total and should not throw, but if it ever regresses this guard
      // logs the secondary teardown error rather than letting it replace
      // `cause` and hide the real startup failure. Mirrors the
      // recycle-failure catch, which preserves its cause the same way.
      await shutdownInternal({
        reason: `spawn failed during startup: ${message}`,
      }).catch((shutdownCause) => {
        const inner =
          shutdownCause instanceof Error
            ? shutdownCause.message
            : String(shutdownCause);
        logger.error`shutdown after spawn failure also threw: ${inner}`;
      });
      throw cause;
    }
  }

  /**
   * Project the active cohort's terminal broadcaster as a
   * `TerminalEventSource` the drainTimeout accumulator factory accepts.
   * Wraps the broadcaster's `source` so the iterator settles with
   * `done: true` on cohort abort -- without the abort wrap an
   * accumulator armed mid-cohort would block on the broadcaster even
   * after the supervisor has aborted the cohort.
   */
  function perCohortTerminalSource(
    cohortAbort: AbortController | null,
    broadcaster: TerminalBroadcaster | null,
  ): TerminalEventSource | null {
    if (cohortAbort === null) return null;
    if (broadcaster === null) return null;
    const signal = cohortAbort.signal;
    return (runId: string) => ({
      [Symbol.asyncIterator](): AsyncIterator<TerminalRunEvent> {
        if (signal.aborted) {
          return {
            next: () => Promise.resolve({ value: undefined, done: true }),
            return: (value?: unknown) => Promise.resolve({ value, done: true }),
          };
        }
        const inner = broadcaster.source(runId)[Symbol.asyncIterator]();
        let onAbort: (() => void) | null = null;
        const abortPromise = new Promise<{
          value: TerminalRunEvent | undefined;
          done: true;
        }>((resolve) => {
          onAbort = () => resolve({ value: undefined, done: true });
          signal.addEventListener("abort", onAbort, { once: true });
        });
        function detach(): void {
          if (onAbort !== null) {
            signal.removeEventListener("abort", onAbort);
            onAbort = null;
          }
        }
        return {
          async next(): Promise<IteratorResult<TerminalRunEvent>> {
            if (signal.aborted) {
              detach();
              if (typeof inner.return === "function") {
                await inner.return(undefined).catch(() => {
                  /* swallowed: best-effort finalisation. */
                });
              }
              return { value: undefined, done: true };
            }
            const result = await Promise.race([inner.next(), abortPromise]);
            if (result.done === true) {
              detach();
              if (signal.aborted && typeof inner.return === "function") {
                await inner.return(undefined).catch(() => {
                  /* swallowed: best-effort finalisation. */
                });
              }
            }
            return result;
          },
          async return(): Promise<IteratorResult<TerminalRunEvent>> {
            detach();
            if (typeof inner.return === "function") {
              await inner.return(undefined).catch(() => {
                /* swallowed: best-effort finalisation. */
              });
            }
            return { value: undefined, done: true };
          },
        };
      },
    });
  }

  /**
   * Forward one dequeued inbox entry to the child as `trigger.fire`
   * and record its runId as in-flight. The runId is the messageId
   * the envelope carries (one run per trigger fire per discovery
   * Q3.1); the same value is what the dispatch loop waits on via
   * `terminalEventSource`.
   */
  async function forwardDispatchedEntry(
    sender: ControlChannelSender,
    messageId: string,
    receivedAt: number,
  ): Promise<string> {
    await sender.send({
      type: "trigger.fire",
      data: {
        runId: messageId,
        messageId,
        receivedAt,
      },
    });
    inFlightRuns.add(messageId);
    return messageId;
  }

  /**
   * Push the run's grants snapshot to the child ahead of its
   * `trigger.fire`. Returns `true` if the barrier FAILED (the caller must
   * skip the fire; the run has already been settled as `RunFailed`) and
   * `false` if the barrier passed or is not armed (`onRunStart` unwired,
   * where `spawn` supplied the snapshot instead).
   *
   * The sink is a request/response contract: the supervisor awaits the
   * returned snapshot and awaits the `grants-updated` send so both land on
   * the child's control channel before the trigger. A throw from either --
   * the sink itself or the control send -- is surfaced as a synthesized
   * `RunFailed` fanned out to this run's broadcaster watcher, never
   * swallowed, so the run fails deterministically instead of the child
   * authorizing against a stale or absent snapshot.
   */
  async function pushRunGrants(
    sender: ControlChannelSender,
    runId: string,
    broadcaster: TerminalBroadcaster,
  ): Promise<boolean> {
    if (bindings.onRunStart === undefined) return false;
    try {
      const snapshot = await bindings.onRunStart({
        runId,
        deploymentId: bindings.deploymentId,
      });
      await sender.send({
        type: "grants-updated",
        data: {
          snapshot: {
            steps: snapshot.steps.map((s) => ({
              stepId: s.stepId,
              address: s.address,
              grants: [...s.grants],
              contentHash: s.contentHash,
            })),
          },
        },
      });
      return false;
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`onRunStart grants barrier failed for run ${runId}; failing the run: ${message}`;
      broadcaster.notify(runId, {
        kind: "RunFailed",
        seq: 0,
        at: new Date().toISOString(),
        error: {
          message: `workflow-host supervisor: run ${runId} not authorized; grants barrier failed before trigger.fire: ${message}`,
        },
      });
      return true;
    }
  }

  /**
   * One iteration of the dispatch loop: dequeue the FIFO-first inbox
   * entry, forward it as a `trigger.fire`, wait for the corresponding
   * run's terminal event (or for the cohort to abort), then
   * `markConsumed`. Returns `true` if a dispatch landed (caller should
   * loop immediately) and `false` if the inbox was empty (caller
   * should await the next wake).
   */
  async function dispatchOne(
    sender: ControlChannelSender,
    cohortAbort: AbortController,
    broadcaster: TerminalBroadcaster,
  ): Promise<boolean> {
    if (cohortAbort.signal.aborted) return false;
    // Subscribe to the terminal broadcaster BEFORE forwarding the
    // trigger.fire so a terminal event the child notifies between
    // forward and subscribe cannot be missed. The broadcaster fires
    // its listeners synchronously inside `notify`; with the subscribe
    // ordered first the listener buffers the event until the
    // dispatch loop's `iter.next()` consumes it.
    const beforeDequeueMs = dispatchTimingEnabled() ? performance.now() : 0;
    const dequeued = await inboxPrimitives.dequeueToProcessing(
      bindings.repoStore,
      inboxWritePrincipal,
      bindings.workflowRunRepoId,
      bindings.deploymentMailAddress,
    );
    if (dequeued === null) return false;
    const envelope = dequeued.envelope;
    const runId = envelope.messageId;
    currentDispatchRunId = runId;
    emitDispatchTiming(runId, "dispatch-start", beforeDequeueMs);
    // D2 leg: the claim-check dequeue READ. `dispatch-start` is sampled
    // BEFORE the dequeue (so the roundtrip bracket includes the read);
    // the dequeue leg's own start mark is that same pre-dequeue sample
    // re-stamped under the leg channel, and its end is now (the read just
    // completed). Emitting the start retroactively here -- rather than
    // before the await -- keeps the leg keyed by the runId, which is only
    // known after the dequeue resolves.
    if (bindings.onDispatchTiming !== undefined) {
      try {
        bindings.onDispatchTiming({
          kind: "leg",
          runId,
          leg: "dequeue",
          phase: "start",
          atMs: beforeDequeueMs,
        });
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`onDispatchTiming leg observer threw for ${runId} (dequeue start): ${message}`;
      }
    }
    legMarkEnd(runId, "dequeue");
    const iterable = broadcaster.source(runId);
    const iter = iterable[Symbol.asyncIterator]();
    // Per-run grants barrier. When `onRunStart` is wired, push this run's
    // grants snapshot BEFORE the `trigger.fire` so the child's authorize
    // closure binds to it rather than throwing on a null snapshot. The push
    // and the fire share the child's control channel, so a `grants-updated`
    // awaited here is observed by the child ahead of the trigger. A barrier
    // failure (the sink throws, or the push fails) fails the run loudly --
    // a synthesized `RunFailed` fanned out to this run's watcher -- and the
    // trigger is NOT fired, so no step ever runs against absent grants.
    const barrierFailed = await pushRunGrants(sender, runId, broadcaster);
    if (!barrierFailed) {
      await forwardDispatchedEntry(
        sender,
        envelope.messageId,
        envelope.receivedAt,
      );
    }
    await waitForRunTerminal(iter, cohortAbort.signal);
    emitDispatchTiming(runId, "reply-produced", performance.now());
    inFlightRuns.delete(runId);
    if (cohortAbort.signal.aborted) {
      // The cohort tore down before the terminal event arrived (or
      // alongside it). Skip `markConsumed` so the recycle path's
      // drain-side replay can reclaim the processing entry.
      currentDispatchRunId = null;
      resolveMarkConsumedWaiter(runId);
      return false;
    }
    // D2 leg: `markConsumed` is paid AFTER `reply-produced` (stamped
    // above), so its growth is invisible to the 4.7 round-trip bracket --
    // the leg mark makes the out-of-window cost visible.
    legMarkStart(runId, "markconsumed");
    try {
      await inboxPrimitives.markConsumed(
        bindings.repoStore,
        inboxWritePrincipal,
        bindings.workflowRunRepoId,
        {
          address: bindings.deploymentMailAddress,
          messageId: envelope.messageId,
          runId,
          consumedAt: Date.now(),
          retentionHorizonMs: consumedRetentionMs,
        },
      );
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`markConsumed failed for run ${runId}: ${message}`;
    }
    legMarkEnd(runId, "markconsumed");
    resolveMarkConsumedWaiter(runId);
    // §10c forced-repack A/B (measurement-only; no-op when unwired).
    maybeRepack(runId);
    currentDispatchRunId = null;
    return true;
  }

  /**
   * Wait until the run's terminal event lands on the cohort
   * broadcaster's iterator or the cohort aborts. The caller is
   * responsible for minting the iterator before forwarding the
   * `trigger.fire` so the listener is already armed when the child's
   * upstream `terminal.event` frame arrives.
   */
  async function waitForRunTerminal(
    iter: AsyncIterator<TerminalRunEvent>,
    abortSignal: AbortSignal,
  ): Promise<void> {
    let onAbort: (() => void) | null = null;
    const abortPromise = new Promise<{ done: true }>((resolve) => {
      if (abortSignal.aborted) {
        resolve({ done: true });
        return;
      }
      onAbort = () => resolve({ done: true });
      abortSignal.addEventListener("abort", onAbort, { once: true });
    });
    try {
      while (true) {
        if (abortSignal.aborted) return;
        const result = await Promise.race([iter.next(), abortPromise]);
        if (result.done === true) return;
        // A terminal event for this runId arrived; stop waiting.
        return;
      }
    } finally {
      if (onAbort !== null) {
        abortSignal.removeEventListener("abort", onAbort);
      }
      if (typeof iter.return === "function") {
        await iter.return(undefined).catch(() => {
          /* swallowed: best-effort finalisation of the watcher iterator. */
        });
      }
    }
  }

  /**
   * The dispatch loop body. Runs until the cohort aborts; each
   * iteration drains one inbox entry through the FIFO claim-check
   * pipeline. The loop is restarted by `installNewChild` after a
   * recycle and torn down by `shutdownInternal` and on cohort abort.
   *
   * `replayGate` is the promise the spawn-time
   * `replayProcessingToInbox` settles on. The loop awaits it before
   * its first `dequeueToProcessing`: a fresh `mail.inbound` that
   * enqueues during the replay window must not ship ahead of an
   * orphaned `processing/` entry the replay is still moving back to
   * `inbox/`. The gate is `null` for the recycle path's restart,
   * where `triggerRecycle` already awaited its own replay before
   * calling `installNewChild`.
   */
  async function runDispatchLoop(
    sender: ControlChannelSender,
    cohortAbort: AbortController,
    broadcaster: TerminalBroadcaster,
    replayGate: Promise<void> | null,
  ): Promise<void> {
    if (replayGate !== null) {
      await replayGate;
      if (cohortAbort.signal.aborted) return;
    }
    while (!cohortAbort.signal.aborted) {
      let dispatched: boolean;
      try {
        dispatched = await dispatchOne(sender, cohortAbort, broadcaster);
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`dispatch loop iteration failed: ${message}`;
        // A failure deep inside the substrate is one the operator
        // must see. The loop continues -- a transient failure should
        // not wedge the deployment -- but the loop pauses on the
        // wake so we do not busy-spin against a persistent fault.
        dispatched = false;
      }
      if (dispatched) continue;
      if (cohortAbort.signal.aborted) return;
      const wake = dispatchWake.promise;
      const abortPromise = new Promise<void>((resolve) => {
        if (cohortAbort.signal.aborted) {
          resolve();
          return;
        }
        cohortAbort.signal.addEventListener("abort", () => resolve(), {
          once: true,
        });
      });
      await Promise.race([wake, abortPromise]);
    }
  }

  async function requestCancel(
    opts: CancelRequestOpts,
  ): Promise<CancelCommitInfo> {
    const result = await commitCancelRequested({
      substrate: bindings.repoStore,
      repoId: bindings.workflowRunRepoId,
      ref: bindings.workflowRunRef,
      deploymentId: bindings.deploymentId,
      runId: opts.runId,
      origin: opts.origin,
      reason: opts.reason,
      at: opts.at,
      signAsPrincipal: bindings.signAsPrincipal,
    });
    return { commitSha: result.commitSha, seq: result.seq };
  }

  async function shutdown(): Promise<void> {
    await shutdownInternal({ reason: "shutdown requested" });
  }

  async function shutdownInternal(opts: { reason: string }): Promise<void> {
    if (state.phase === "idle" || state.phase === "stopped") return;
    const prior = state;
    state = { phase: "stopping" };
    // shutdownInternal is designed to be TOTAL: when a child is up it must
    // always kill it and always reach `stopped`, no matter which teardown
    // step throws. Rather than depend on every step being individually
    // non-throwing (an approach that has already leaked an escape hatch),
    // the whole teardown body runs inside one `try`, and the two
    // load-bearing actions -- the child kill and the `phase = "stopped"`
    // transition -- live in the `finally`, so a throw anywhere above them
    // still runs both. This is the documented shutdown carve-out to the
    // fail-loud rule: leaking the child or wedging the supervisor in
    // `stopping` is strictly worse than logging and continuing, so the
    // steps that can throw surface at `logger.warn` and execution proceeds.
    // (`terminalCohortAbort.abort`, `rejectCohortAwaiters`, and
    // `wakeDispatch` cannot throw, and the broadcaster's `dispose` is total
    // by construction; they sit inside the `try` regardless so the
    // invariant survives if that ever changes.)
    const accumulatorsToDispose = [...drainAccumulators.values()];
    try {
      // Stop every armed drainTimeout accumulator before tearing the child
      // down. An accumulator left running would otherwise fire its
      // `setTimeout` callback (or its terminal-event watcher's settle hook)
      // against a shutdown-mid-flight supervisor. Guard each `stop` so one
      // throwing accumulator does not leave the rest armed.
      for (const accumulator of accumulatorsToDispose) {
        try {
          accumulator.stop();
        } catch (cause) {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`drain accumulator stop threw during shutdown: ${message}`;
        }
      }
      drainAccumulators.clear();
      if (
        prior.phase === "starting" ||
        prior.phase === "running" ||
        prior.phase === "recycling"
      ) {
        prior.terminalCohortAbort.abort();
        // Reject every pending merge round-trip and markConsumed waiter
        // so handler closures awaiting them (including fire-and-forget
        // `handleSubstrateWriteRequest` instances) cannot outlive the
        // dying cohort. Without this, the `await new Promise` inside
        // each handler would sit forever on a resolver the dying control
        // channel will never invoke.
        rejectCohortAwaiters("shutdown");
        // Dispose the cohort broadcaster so any minted iterator settles
        // with `done: true` -- the dispatch loop's `waitForRunTerminal`
        // and any drainTimeout watcher unblock through the same shutdown
        // path the cohort abort drives.
        prior.terminalBroadcaster.dispose();
        // Wake the dispatch loop so its `dispatchWake` await settles
        // and the loop notices the cohort abort. Without the wake, the
        // loop's `Promise.race` would sit on the wake promise until
        // some other actor woke it.
        wakeDispatch();
      }
      // Await every accumulator's `disposed()` so a pending escalation
      // commit or terminal-event watcher coroutine cannot outlive the
      // supervisor and fire against torn-down bindings.
      await Promise.all(
        accumulatorsToDispose.map((a) =>
          a.disposed().catch(() => {
            /* swallowed: each accumulator already logs its own failure. */
          }),
        ),
      );
      if (
        (prior.phase === "running" || prior.phase === "recycling") &&
        prior.dispatchLoop !== null
      ) {
        await prior.dispatchLoop.catch(() => {
          /* swallowed: dispatch-loop failures are surfaced by the
             loop's own logger; the shutdown path only waits for the
             loop's last iteration to settle. */
        });
      }
      if (
        (prior.phase === "starting" ||
          prior.phase === "running" ||
          prior.phase === "recycling") &&
        prior.replayDone !== null
      ) {
        // Await the spawn-time replayProcessingToInbox before tearing
        // the bindings down. The replay's substrate write
        // (`processing/` -> `inbox/` rename via a tree commit) must
        // settle before the supervisor's exit; without the await the
        // substrate I/O outlives the supervisor and a subsequent boot
        // can observe a partially-applied replay.
        await prior.replayDone.catch(() => {
          /* swallowed: the replay's own catch already surfaces the
             failure to the supervisor's warn channel; the shutdown
             path only waits for the substrate write to settle. */
        });
      }
      if (recyclePolicy !== null) {
        try {
          recyclePolicy.stop();
        } catch (cause) {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`recycle policy stop threw during shutdown: ${message}`;
        }
        recyclePolicy = null;
      }
      spawnContext = null;
      if (
        prior.phase === "starting" ||
        prior.phase === "running" ||
        prior.phase === "recycling"
      ) {
        if (prior.mailUnsubscribe !== null) {
          try {
            prior.mailUnsubscribe();
          } catch (cause) {
            const message =
              cause instanceof Error ? cause.message : String(cause);
            logger.warn`mail unsubscribe threw during shutdown: ${message}`;
          }
        }
        try {
          bindings.mailBus.unregisterAddress(bindings.deploymentMailAddress);
        } catch (cause) {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`mail bus unregisterAddress threw: ${message}`;
        }
      }
    } finally {
      // Load-bearing: the child kill and the `stopped` transition run
      // whatever happened above, so a throwing teardown step can neither
      // leak the child nor wedge the supervisor in `stopping`. The kill is
      // itself guarded so a throw here cannot re-escape the `finally`.
      if (
        prior.phase === "starting" ||
        prior.phase === "running" ||
        prior.phase === "recycling"
      ) {
        try {
          prior.handle.kill();
        } catch (cause) {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`child kill threw during shutdown: ${message}`;
        }
        await prior.handle.exited.catch(() => {
          /* swallowed: the host has already been told the deployment is
             coming down; an error surfaced from the spawner is the
             process exiting with a non-zero code, which is what the
             shutdown path expects. */
        });
        await prior.eventPump.catch(() => {
          /* swallowed for the same reason as above. */
        });
      }
      state = { phase: "stopped" };
    }
    logger.info`supervisor shutdown complete (${opts.reason})`;
  }

  async function drain(opts: DrainOpts): Promise<void> {
    await drainImpl(opts, { fromRecycle: false });
  }

  /**
   * Internal drain implementation. The `fromRecycle` flag admits the
   * `recycling` phase for the recycle path's drain step (which runs
   * BEFORE `abortPriorCohort` and `kill`, against the still-live
   * controlSender). External callers leave it `false`, so a stray
   * drain that lands during the kill/respawn gap is dropped silently
   * at the public surface rather than writing into a controlSender
   * that `triggerRecycle` is about to tear down.
   *
   * The asymmetry with `deliverSignal`, which throws on `recycling`,
   * is intentional: `drain()` is documented as best-effort no-op for
   * `idle`/`stopping`/`stopped` so the host shutdown sequence can
   * call it unconditionally without sniffing the phase; tightening
   * `recycling` to a throw would break that contract for callers
   * that interleave drain and shutdown. The dropped frame surfaces
   * in operator logs only; callers that need a guaranteed-delivered
   * drain should consult the supervisor's phase first.
   */
  async function drainImpl(
    opts: DrainOpts,
    ctx: { fromRecycle: boolean },
  ): Promise<void> {
    // Drain is meaningful only when a workflow-process child is up;
    // calling it from `idle`/`stopping`/`stopped` is a no-op so the
    // higher-level host shutdown sequence can call drain
    // unconditionally without sniffing the phase. The recycle path
    // calls drain via `drainImpl({}, { fromRecycle: true })` and
    // admits `recycling` because the drain step runs against a
    // still-live controlSender before the kill lands.
    if (
      state.phase !== "running" &&
      state.phase !== "starting" &&
      !(ctx.fromRecycle && state.phase === "recycling")
    ) {
      return;
    }
    // Forward the `drain` control mail to the child. The child's
    // `DrainController` flips its signal on receipt; the runtime
    // body's four observation points read the change on the next
    // tick. The supervisor never blocks on the child's acknowledgement
    // -- the accumulator below is the deadline-keeper, not the round
    // trip.
    await state.controlSender.send({
      type: "drain",
      data: { deadlineMs: opts.deadlineMs },
    });
    // Arm one accumulator per in-flight run. Each accumulator's
    // `escalate` path commits a signed `CancelRequested{origin:
    // "supervisor-drain"}` against the workflow-run repo via the
    // existing `commitCancelRequested` substrate path, so the runtime
    // body's cancellation cascade tears the run down without the
    // supervisor having to thread any per-run wiring beyond what the
    // accumulator already encapsulates.
    const cohortSource = perCohortTerminalSource(
      state.terminalCohortAbort,
      state.terminalBroadcaster,
    );
    for (const runId of inFlightRuns) {
      if (drainAccumulators.has(runId)) continue;
      const accumulator = accumulatorFactory({
        substrate: bindings.repoStore,
        repoId: bindings.workflowRunRepoId,
        ref: bindings.workflowRunRef,
        deploymentId: bindings.deploymentId,
        runId,
        signAsPrincipal: bindings.signAsPrincipal,
        drainTimeoutMs,
        now: drainNow,
        setTimer: drainSetTimer,
        clearTimer: drainClearTimer,
        ...(cohortSource !== null ? { terminalEventSource: cohortSource } : {}),
      });
      drainAccumulators.set(runId, accumulator);
      accumulator.start();
    }
  }

  async function recycle(opts: RecycleOpts): Promise<RecycleAttempt> {
    if (recycleInProgress) {
      throw new Error("supervisor: recycle already in progress");
    }
    if (state.phase !== "running") {
      throw new Error(
        `supervisor: recycle called in phase ${state.phase}; expected running`,
      );
    }
    if (spawnContext === null) {
      throw new Error(
        "supervisor: recycle called without a spawn context; spawn() must complete first",
      );
    }
    recycleInProgress = true;
    const origin: RecycleOrigin = opts.origin ?? "operator";
    const prior = state;
    const priorContext = spawnContext;
    // The cohort abort no longer fires up-front. triggerRecycle drives
    // the drain and replay steps against a LIVE cohort first, then
    // invokes `abortPriorCohort` (the callback below) between replay
    // and the kill step. Aborting up-front would starve drain
    // accumulators of live terminal events; aborting after the kill
    // would race the dispatch loop's next iteration against the
    // controlSender that's about to disappear.
    const priorDispatchLoop = prior.dispatchLoop;
    // Transition to `recycling`. Inbound mail continues to flow through
    // `enqueueInbox` unchanged; the prior dispatch loop is still alive
    // for the drain window and keeps forwarding to the dying child.
    // After triggerRecycle's `abortPriorCohort` callback fires, the
    // loop notices the abort and exits before the kill lands. The new
    // dispatch loop picks up the inbox once `installNewChild` swaps
    // the wiring.
    state = {
      phase: "recycling",
      handle: prior.handle,
      controlSender: prior.controlSender,
      channelId: prior.channelId,
      eventPump: prior.eventPump,
      onInferenceEvent: prior.onInferenceEvent,
      mailUnsubscribe: prior.mailUnsubscribe,
      credentialsSnapshot: prior.credentialsSnapshot,
      terminalCohortAbort: prior.terminalCohortAbort,
      terminalBroadcaster: prior.terminalBroadcaster,
      dispatchLoop: null,
      replayDone: null,
    };
    let attempt: RecycleAttempt;
    try {
      attempt = await triggerRecycle(
        {
          bindings,
          stepOrder: priorContext.stepOrder,
          definitionHash: priorContext.definitionHash,
          warmKeep: priorContext.warmKeep,
          onInferenceEvent: priorContext.onInferenceEvent,
          current: {
            handle: prior.handle,
            controlSender: prior.controlSender,
            channelId: prior.channelId,
            eventPump: prior.eventPump,
          },
          drain: async (deadlineMs) => {
            // The recycle path's drain step shares the drain
            // primitive but bypasses the public surface's `recycling`
            // silent-no-op so the still-live controlSender (this
            // step runs BEFORE abortPriorCohort + kill) receives the
            // frame. The public `drain()` silently no-ops on
            // `recycling` for external callers because the
            // kill/respawn gap can leave the controlSender dying.
            await drainImpl({ deadlineMs }, { fromRecycle: true });
          },
          replayProcessingToInbox: async () => {
            await inboxPrimitives.replayProcessingToInbox(
              bindings.repoStore,
              inboxWritePrincipal,
              bindings.workflowRunRepoId,
              bindings.deploymentMailAddress,
            );
          },
          abortPriorCohort: () => {
            // Fired by triggerRecycle between drain/replay and kill.
            // The prior dispatch loop notices the abort on its next
            // wake and exits before the kill drops the child.
            prior.terminalCohortAbort.abort();
            wakeDispatch();
          },
          installNewChild: ({
            wiring,
            credentialsSnapshot,
            controlIncoming,
          }) => {
            // Phase guard: a `shutdown()` that landed during the
            // kill/respawn gap (between `subprocessSpawner` and this
            // callback) has flipped `state.phase` to `stopping` or
            // `stopped`. The new child is now an orphan -- the
            // supervisor was supposed to be tearing down, not
            // installing a fresh cohort. Kill the new wiring's
            // handle and bail out without registering it on
            // `state`. `shutdownInternal`'s own teardown path has
            // already disposed the prior cohort; there is nothing
            // for this callback to do.
            if (state.phase !== "recycling") {
              // Kill the orphan child and release its event-channel /
              // upstream-control resources so they cannot survive as
              // unowned promises. Without this, the eventPump and
              // controlIncoming iterator would have no `state`
              // bookkeeping to drive their cleanup -- a rejection
              // inside `pumpEvents` would surface as an unhandled
              // rejection, and the upstream control iterator's
              // exit would never be observed.
              wiring.handle.kill("SIGTERM");
              void wiring.eventPump.catch((cause: unknown) => {
                const message =
                  cause instanceof Error ? cause.message : String(cause);
                logger.warn`orphan-cohort eventPump failed during phase-guard teardown: ${message}`;
              });
              void controlIncoming.return(undefined).catch((cause: unknown) => {
                const message =
                  cause instanceof Error ? cause.message : String(cause);
                logger.warn`orphan-cohort controlIncoming.return failed during phase-guard teardown: ${message}`;
              });
              return;
            }
            // The previous cohort was aborted inside triggerRecycle
            // by the `abortPriorCohort` callback (after drain and
            // replay, before kill) so the prior dispatch loop did not
            // race the kill/respawn gap. Stop every armed accumulator
            // (they were tracking runs that lived inside the killed
            // child); the resumed child re-discovers any survivors
            // and the next `drain()` mints fresh accumulators
            // against the new cohort.
            for (const accumulator of drainAccumulators.values()) {
              accumulator.stop();
            }
            drainAccumulators.clear();
            // Reject every pending merge round-trip and markConsumed
            // waiter registered against the dying cohort so handler
            // closures cannot survive the kill/respawn gap. The new
            // child will re-issue substrate writes through fresh
            // handlers under the new cohort's channel.
            rejectCohortAwaiters("recycle");
            // Dispose the prior cohort's broadcaster so any minted
            // iterator still held by the aborted dispatch loop or a
            // stopped accumulator settles with `done: true`. The next
            // cohort gets a fresh broadcaster wired below.
            prior.terminalBroadcaster.dispose();
            // Mint a fresh cohort abort and start a new dispatch
            // loop against the new child's controlSender.
            const newCohortAbort = new AbortController();
            const newBroadcaster = createTerminalBroadcaster();
            const newDispatchLoop = runDispatchLoop(
              wiring.controlSender,
              newCohortAbort,
              newBroadcaster,
              null,
            );
            void newDispatchLoop.catch((cause) => {
              const message =
                cause instanceof Error ? cause.message : String(cause);
              logger.error`dispatch loop (post-recycle) terminated with error: ${message}`;
            });
            // Transition back to running with the new wiring; the
            // mail subscription and registration are unchanged.
            state = {
              phase: "running",
              handle: wiring.handle,
              controlSender: wiring.controlSender,
              channelId: wiring.channelId,
              eventPump: wiring.eventPump,
              onInferenceEvent: priorContext.onInferenceEvent,
              mailUnsubscribe: prior.mailUnsubscribe,
              credentialsSnapshot,
              terminalCohortAbort: newCohortAbort,
              terminalBroadcaster: newBroadcaster,
              dispatchLoop: newDispatchLoop,
              replayDone: null,
            };
            // Cache fresh spawn context with the updated spawnedAt
            // so the policy timer's uptime check resets on recycle.
            const now = bindings.recyclePolicyNow ?? defaultNow;
            spawnContext = {
              stepOrder: priorContext.stepOrder,
              definitionHash: priorContext.definitionHash,
              warmKeep: priorContext.warmKeep,
              onInferenceEvent: priorContext.onInferenceEvent,
              spawnedAt: now(),
            };
            // Re-arm the upstream control pump on the new wiring's
            // iterator. The old wiring's iterator ended when the
            // recycle path killed the predecessor handle. The new
            // pump closes over the NEW cohort's broadcaster so a
            // `terminal.event` arriving on the new iterator routes
            // to the new cohort's listeners; the prior cohort's pump
            // (still draining its own iterator) closed over the
            // prior cohort's broadcaster and is unaffected by this
            // wiring swap.
            void pumpUpstreamControl(controlIncoming, newBroadcaster).catch(
              (cause) => {
                const message =
                  cause instanceof Error ? cause.message : String(cause);
                logger.error`upstream control pump (post-recycle) failed: ${message}`;
              },
            );
            // Kick the new dispatch loop so it picks up any inbox
            // entries the previous cohort's replayProcessingToInbox
            // just moved back.
            wakeDispatch();

            // Trigger A on the recycle seam: the respawned child re-parks any
            // surviving parked run without re-emitting, and a recycle leaves
            // the hub link untouched so the reconnect trigger never fires --
            // so re-drive the re-registration here too. Same fire-and-forget
            // contract as the spawn seam; the fresh cohort's controlSender is
            // in `state` now, and its pump (armed above) routes the response.
            void reEmitParkedCorrelations().catch((cause) => {
              const message =
                cause instanceof Error ? cause.message : String(cause);
              logger.warn`re-emit of parked correlations on re-establishment failed: ${message}`;
            });
          },
          onCrash: onChildCrash,
          // Edge-resolved once at the supervisor factory; recycle bounds
          // the respawn handshake with the same value the spawn path uses.
          readyTimeoutMs,
          ...(bindings.recyclePolicySetTimer !== undefined
            ? { setTimer: bindings.recyclePolicySetTimer }
            : {}),
          ...(bindings.recyclePolicyClearTimer !== undefined
            ? { clearTimer: bindings.recyclePolicyClearTimer }
            : {}),
        },
        { origin, reason: opts.reason },
      );
      // After the recycle, await the previous cohort's dispatch
      // loop so a teardown coroutine cannot survive past the
      // recycle's return point.
      if (priorDispatchLoop !== null) {
        await priorDispatchLoop.catch(() => {
          /* swallowed: dispatch-loop failures are surfaced by the
             loop's own logger. */
        });
      }
    } catch (cause) {
      // `triggerRecycle` failed after we transitioned to `recycling`.
      // Leaving the supervisor in `recycling` indefinitely would wedge
      // every subsequent operation; the only recovery would be a host-
      // level shutdown. Tear the prior cohort down through the same
      // path a real shutdown uses so the supervisor reaches a clean
      // `stopped` state, then re-throw so the operator sees the
      // recycle failure and can redeploy.
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`recycle failed; tearing supervisor down: ${message}`;
      await shutdownInternal({
        reason: `recycle failed: ${message}`,
      }).catch((shutdownCause) => {
        const inner =
          shutdownCause instanceof Error
            ? shutdownCause.message
            : String(shutdownCause);
        logger.error`shutdown after recycle failure also threw: ${inner}`;
      });
      throw cause;
    } finally {
      recycleInProgress = false;
    }
    return attempt;
  }

  async function deliverSignal(opts: DeliverSignalOpts): Promise<void> {
    // The supervisor is the single producer of `signal.deliver` control
    // IPC frames. Routing every signal delivery through the same child
    // makes the workflow-process the single writer of `runs/<runId>/events/`
    // on the sidecar side; the pack-push pipeline that propagates the
    // commit to the hub never observes a concurrent writer at the
    // same ref.
    //
    // `recycling` is rejected: during recycle, `state.controlSender`
    // still points at the dying child's sender, so a `signal.deliver`
    // either buffers behind the SIGTERM (best case) or writes into a
    // closed pipe and is silently lost (worst case). Rejecting here
    // surfaces the race to the caller so they can retry after the
    // recycle completes.
    if (state.phase !== "running" && state.phase !== "starting") {
      throw new Error(
        `supervisor: deliverSignal called in phase ${state.phase}; expected starting/running`,
      );
    }
    await state.controlSender.send({
      type: "signal.deliver",
      data: {
        runId: opts.runId,
        signalName: opts.signalName,
        signalId: opts.signalId,
        payload: opts.payload,
      },
    });
  }

  async function deliverSources(opts: DeliverSourcesOpts): Promise<void> {
    // The supervisor is the single producer of `sources-updated` control
    // frames. `recycling` is rejected for the same reason as
    // `deliverSignal`: `state.controlSender` still points at the dying
    // child, so a frame would either buffer behind the SIGTERM or write
    // into a closed pipe and be lost. Rejecting surfaces the race so the
    // caller can retry once the recycle completes.
    if (state.phase !== "running" && state.phase !== "starting") {
      throw new Error(
        `supervisor: deliverSources called in phase ${state.phase}; expected starting/running`,
      );
    }
    await state.controlSender.send({
      type: "sources-updated",
      data: {
        sources: opts.sources,
        defaultSource: opts.defaultSource,
      },
    });
  }

  function getCredentialsSnapshot(): CredentialsSnapshot | null {
    if (state.phase === "starting" || state.phase === "running") {
      return state.credentialsSnapshot;
    }
    return null;
  }

  return {
    spawn,
    requestCancel,
    shutdown,
    drain,
    recycle,
    deliverSignal,
    deliverSources,
    reEmitParkedCorrelations,
    getCredentialsSnapshot,
  };
}

type SupervisorState =
  | { phase: "idle" }
  | { phase: "stopping" }
  | { phase: "stopped" }
  | ({ phase: "starting" } & ActiveState)
  | ({ phase: "running" } & ActiveState)
  | ({ phase: "recycling" } & ActiveState);

type ActiveState = {
  handle: SubprocessHandle;
  controlSender: ControlChannelSender;
  channelId: string;
  eventPump: Promise<void>;
  onInferenceEvent: (event: EventPayload) => void;
  mailUnsubscribe: (() => void) | null;
  credentialsSnapshot: CredentialsSnapshot | null;
  /**
   * Per-spawn cohort abort controller for terminal-event watchers
   * and the dispatch loop. Each watcher the supervisor mints inside
   * `drain()` (via the accumulator) borrows this controller's
   * signal; the dispatch loop borrows the same signal so its
   * `dispatchOne` iteration tears down cleanly on shutdown / recycle.
   * `shutdownInternal` aborts the controller alongside
   * `mailUnsubscribe`; `installNewChild` mints a fresh controller
   * for the recycled cohort so a watcher / loop iteration minted in
   * the previous cohort cannot survive into the next.
   */
  terminalCohortAbort: AbortController;
  /**
   * Per-cohort terminal-run event broadcaster. The supervisor's
   * upstream control pump fans `terminal.event` frames into this
   * broadcaster; the dispatch loop and any armed drainTimeout
   * accumulator subscribe through its `source` accessor. Lifetime
   * matches `terminalCohortAbort`: shutdown / recycle dispose the
   * broadcaster so every minted iterator settles with `done: true`.
   */
  terminalBroadcaster: TerminalBroadcaster;
  /**
   * Promise the dispatch loop's body resolves with on exit. The
   * `starting`-phase ActiveState carries `null` because the loop is
   * not started until the child emits `ready`; once `spawn()`
   * transitions to `running` the field carries the live loop
   * promise. `shutdownInternal` awaits this promise after aborting
   * the cohort so a dispatch-loop iteration that is mid-await
   * settles before the supervisor tears the bindings down.
   */
  dispatchLoop: Promise<void> | null;
  /**
   * Settles when the spawn-time `replayProcessingToInbox` resolves
   * (or rejects, swallowed via the supervisor's warn log). Tracked
   * on the active-state record so `shutdownInternal` awaits the
   * replay's substrate write before tearing the bindings down. The
   * dispatch loop borrows the same promise as its first-iteration
   * gate, so any inbound mail that enqueues during the replay
   * window cannot dispatch ahead of an orphaned `processing/`
   * entry. The recycle-path ActiveState carries `null` because
   * `triggerRecycle` awaits its own replay inline before
   * `installNewChild` transitions back to `running`.
   */
  replayDone: Promise<void> | null;
};

type SpawnContext = {
  stepOrder: readonly string[];
  definitionHash: string;
  /** Warm-keep flag carried on respawn env (unchanged across recycle). */
  warmKeep: boolean;
  onInferenceEvent: (event: EventPayload) => void;
  spawnedAt: number;
};

/**
 * Iterate the control-channel receive iterator until the child's
 * `ready` frame arrives. Upstream payloads other than `ready` (e.g.
 * `recycle.request`) appear after `ready`; the supervisor's
 * `pumpUpstreamControl` consumes them off the same iterator after
 * spawn returns.
 */
async function waitForReady(
  iter: AsyncGenerator<ControlPayload, void, void>,
): Promise<{ childPid: number }> {
  // Use explicit `next()` rather than `for await ... return` so the
  // generator is NOT finalized via `iter.return()` when ready lands.
  // The supervisor's upstream-control pump continues iterating the
  // same generator after `ready`, and a finalized generator would
  // immediately yield `{done: true}` to the pump and silently drop
  // the child's subsequent upstream frames (e.g. `recycle.request`).
  while (true) {
    const next = await iter.next();
    if (next.done === true) {
      throw new Error(
        "workflow-host supervisor: control channel ended before child emitted ready",
      );
    }
    const payload = next.value;
    if (payload.type === "ready") {
      return { childPid: payload.data.childPid };
    }
    // Drop other variants encountered before `ready`; the child is
    // not supposed to send anything else first, but the receiver
    // validated the envelope and signature, so a stray frame here is
    // a programming bug worth surfacing in the warning channel
    // rather than crashing the iterator.
  }
}

function defaultNow(): number {
  return Date.now();
}

/**
 * Drain the event-channel receive iterator into the host-supplied
 * sink. The function resolves when the iterator ends (child exit or
 * crash callback fired). Any thrown error is logged and surfaced
 * to the supervisor's shutdown path.
 */
async function pumpEvents(
  iter: AsyncGenerator<EventPayload, void, void>,
  onInferenceEvent: (event: EventPayload) => void,
): Promise<void> {
  for await (const event of iter) {
    onInferenceEvent(event);
  }
}

/**
 * Default `deriveMailAuditRef` derivation used when no host binding
 * is configured. The reference points at an "in-process" store with
 * the messageId as the path, which keeps the supervisor's library
 * tests independent of any audit-store wiring. Production hosts
 * supply a derivation coherent with their own mail-audit surface.
 */
function defaultInProcessMailAuditRef(
  messageId: string,
  _rawMessage: Uint8Array,
): MailAuditRef {
  return { store: "in-process", path: messageId };
}

/**
 * Project the wire shape of a `terminal.event` upstream control frame
 * into the workflow-vocabulary `TerminalRunEvent` discriminated union
 * the supervisor's downstream consumers (dispatch loop, drainTimeout
 * accumulators) reason about. The control-channel IPC validator
 * narrows `kind` and `error` upstream; the supervisor preserves that
 * narrowing here without re-validating.
 */
function terminalEventFromPayload(
  data: Extract<ControlPayload, { type: "terminal.event" }>["data"],
): TerminalRunEvent {
  if (data.kind === "RunCompleted") {
    return { kind: "RunCompleted", seq: data.seq, at: data.at };
  }
  if (data.kind === "RunCancelled") {
    return { kind: "RunCancelled", seq: data.seq, at: data.at };
  }
  // The wire schema makes `error.message` required when `kind` is
  // `RunFailed` (see control-channel `terminal.event` validator). A
  // missing message here would mean the upstream validator was bypassed
  // or the producer is non-conforming; surface that loudly rather than
  // silently coercing to an empty string.
  if (data.error === undefined || typeof data.error.message !== "string") {
    throw new Error(
      `terminalEventFromPayload: RunFailed payload missing required error.message (runId=${data.runId}, seq=${String(data.seq)})`,
    );
  }
  return {
    kind: "RunFailed",
    seq: data.seq,
    at: data.at,
    error: { message: data.error.message },
  };
}

/**
 * Reconstruct a runtime `OutboundMessage` from the IPC wire projection.
 * The wire shape (`OutboundMessagePayload`) carries attachment bytes
 * base64-encoded and spells every optional field with a `"?"` suffix; an
 * absent field is omitted on the wire and stays omitted on the
 * reconstructed message so `exactOptionalPropertyTypes` is honored (an
 * `undefined`-valued optional would violate it). The wire validator
 * narrows `type` to the `InterchangeType` union (see
 * `OutboundMessagePayload` in the control-channel module), so it carries
 * straight onto the message without a cast.
 */
function outboundMessageFromPayload(
  payload: OutboundMessagePayload,
): OutboundMessage {
  const message: OutboundMessage = {
    to: payload.to,
    type: payload.type,
  };
  if (payload.cc !== undefined) message.cc = payload.cc;
  if (payload.subject !== undefined) message.subject = payload.subject;
  if (payload.content !== undefined) message.content = payload.content;
  if (payload.payload !== undefined) message.payload = payload.payload;
  if (payload.summary !== undefined) message.summary = payload.summary;
  if (payload.inReplyTo !== undefined) message.inReplyTo = payload.inReplyTo;
  if (payload.correlationId !== undefined) {
    message.correlationId = payload.correlationId;
  }
  if (payload.sessionId !== undefined) message.sessionId = payload.sessionId;
  if (payload.tenantId !== undefined) message.tenantId = payload.tenantId;
  if (payload.attachments !== undefined) {
    message.attachments = payload.attachments.map((a) => ({
      name: a.name,
      contentType: a.contentType,
      data: base64ToBytes(a.dataBase64),
    }));
  }
  return message;
}

function bytesToBase64(bytes: Uint8Array): string {
  return base64Encode(bytes);
}

function base64ToBytes(value: string): Uint8Array {
  return base64Decode(value);
}
