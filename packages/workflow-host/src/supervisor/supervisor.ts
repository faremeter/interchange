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

import { generateKeyPair } from "@intx/crypto-node";
import {
  enqueueInbox as defaultEnqueueInbox,
  dequeueToProcessing as defaultDequeueToProcessing,
  markConsumed as defaultMarkConsumed,
  replayProcessingToInbox as defaultReplayProcessingToInbox,
  type Principal,
  type WorkflowRunSupervisorPrincipal,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import { RepoId } from "@intx/types/sidecar";
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
} from "../ipc/index";

import {
  assembleCredentialsSnapshot,
  type CredentialsSnapshot,
} from "./credentials";
import { commitCancelRequested } from "./cancel-signing";
import { commitRunEvent } from "./run-event-signing";
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
  InboxPrimitives,
  MailAuditRef,
  RecordRunEvent,
  SubprocessHandle,
  SupervisorDeployFrame,
  TerminalEventSource,
  TerminalRunEvent,
  WorkflowSupervisorBindings,
} from "./types";
import {
  createTerminalBroadcaster,
  type TerminalBroadcaster,
} from "./terminal-broadcaster";

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
 * Public surface returned by `createWorkflowSupervisor`. Each method
 * advances the supervisor through one lifecycle transition; the
 * supervisor's internal state is encapsulated.
 */
export interface WorkflowSupervisor {
  /**
   * Single ingress for `agent.deploy` frames. The supervisor owns
   * the routing decision between trivial (1-step) and multi-step
   * workflows -- the host hands the frame off and never re-decides.
   *
   * Trivial branch (Option Z, locked):
   *   - Calls `bindings.trivialLaunch(frame)` directly.
   *   - Does NOT open an IPC channel.
   *   - Does NOT spawn a workflow-process child.
   *   - Does NOT emit any workflow-run event. `signAsPrincipal`
   *     is not invoked; for a trivial deploy there is no
   *     workflow-process child to cancel, so cancellation stays
   *     session-destroy at the host's layer.
   *   - Does NOT assemble a `credentialsSnapshot`; the trivial
   *     branch leaves `getCredentialsSnapshot()` returning `null`.
   *
   * Multi-step branch (`steps.length >= 2`):
   *   - Provisions per-step `agent-state` repos, mints keys,
   *     spawns the workflow-process child via `subprocessSpawner`,
   *     registers the deployment's mail address, waits for the
   *     child's `ready` frame, and assembles the
   *     `credentialsSnapshot`. This is the body of `spawn(opts)`,
   *     which is the multi-step branch's worker today.
   *
   * The `agent.deploy` wire frame today carries only a
   * `HarnessConfig` (no workflow definition); every frame is
   * therefore trivial. The supervisor codifies the seam so a frame-
   * format extension that carries a `WorkflowDefinition` is a pure
   * data-shape change.
   */
  deploy(frame: SupervisorDeployFrame): Promise<void>;
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

  async function deploy(frame: SupervisorDeployFrame): Promise<void> {
    // The `agent.deploy` wire frame currently carries only a
    // `HarnessConfig`, which is the trivial-workflow shape (a single
    // step derived from the agent's harness). The branching seam
    // exists for the multi-step extension; for every frame today
    // the supervisor calls trivialLaunch unchanged.
    //
    // Trivial-branch invariants:
    //   - No IPC opens, no child spawn, no mail-bus registration.
    //   - credentialsSnapshot is not assembled; the multi-step
    //     branch owns it. `getCredentialsSnapshot()` continues to
    //     return null on the trivial path.
    //   - Run-lifecycle events are committed inline from the
    //     supervisor process via `signAsPrincipal`. The supervisor
    //     hands `recordRunEvent` to the host's `trivialLaunch`; the
    //     host calls it from its per-message reactor / harness
    //     lifecycle moments (`message.run.started` /
    //     `message.run.ended`) so the on-disk event chain matches
    //     the multi-step branch byte-for-byte.
    const recordRunEvent: RecordRunEvent = (event) =>
      commitRunEvent({
        substrate: bindings.repoStore,
        repoId: bindings.workflowRunRepoId,
        ref: bindings.workflowRunRef,
        deploymentId: bindings.deploymentId,
        event,
        signAsPrincipal: bindings.signAsPrincipal,
      });
    await bindings.trivialLaunch({
      agentAddress: frame.agentAddress,
      agentId: frame.agentId,
      config: frame.config,
      hubPublicKey: frame.hubPublicKey,
      recordRunEvent,
    });
  }

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
    await inboxPrimitives.enqueueInbox(
      bindings.repoStore,
      inboxWritePrincipal,
      bindings.workflowRunRepoId,
      {
        address: bindings.deploymentMailAddress,
        messageId,
        receivedAt,
        mailAuditRef,
      },
    );
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
      if (payload.type === "terminal.event") {
        // The workflow-process child mirrors every terminal-run commit
        // over the control IPC. Fan it out to the cohort's broadcaster
        // so the dispatch loop's `waitForRunTerminal` and any armed
        // drainTimeout accumulator settle in step with the child's
        // own substrate commit. The broadcaster is per-cohort: the
        // active cohort's broadcaster lives on `state.terminalBroadcaster`
        // for the spawn / recycling phases; outside those phases the
        // notification has nowhere to land and is dropped.
        const broadcaster = activeTerminalBroadcaster();
        if (broadcaster === null) {
          logger.warn`terminal.event received outside an active cohort; runId=${payload.data.runId} kind=${payload.data.kind} dropped`;
          continue;
        }
        const event = terminalEventFromPayload(payload.data);
        broadcaster.notify(payload.data.runId, event);
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
      for (const file of data.result.files) {
        files[file.path] = base64ToBytes(file.contentBase64);
      }
      entry.resolve({ ok: true, files });
      return;
    }
    entry.resolve({ ok: false, reason: data.result.reason });
  }

  async function handleSubstrateWriteRequest(
    data: Extract<ControlPayload, { type: "substrate.write.request" }>["data"],
  ): Promise<void> {
    const controlSender = activeControlSender();
    if (controlSender === null) {
      logger.warn`substrate.write.request received outside running phase; requestId=${data.requestId} dropped`;
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
    // Capture the prospective files from each merge round-trip so
    // the post-commit inspection below can detect terminal-event
    // writes and synchronously trigger the dispatch loop's
    // markConsumed before the substrate.write.response unblocks the
    // child. Holding the response gates the child's
    // runtime-body progress on the inbox transition landing,
    // closing the window where a downstream consumer observes
    // RunCompleted ahead of the matching consumed/ entry on this
    // supervisor (the cross-process hub-pack ordering is still
    // racy, but the local supervisor's state is self-consistent
    // at the response boundary).
    let lastMergedFiles: Record<string, string | Uint8Array> | null = null;
    try {
      const { commitSha } = await bindings.repoStore.writeTreePreservingPrefix(
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
            lastMergedFiles = result.files;
            return result.files;
          },
        },
      );
      if (lastMergedFiles !== null) {
        const watchdog = await synchronouslyDispatchTerminalWrite(
          data.preservePrefix,
          lastMergedFiles,
        );
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
      }
      await controlSender.send({
        type: "substrate.write.response",
        data: {
          requestId: data.requestId,
          result: { ok: true, commitSha },
        },
      });
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

  // Path-shape sniff. The terminal-write detection couples to the
  // workflow-run substrate's on-disk run-event path shape
  // (`runs/<runId>/events/<seq>.json`); the cleaner design is a
  // typed terminal signal the workflow-run kind handler emits at
  // commit time, but the handler does not surface that signal
  // today and the regex matches exactly one well-formed shape the
  // substrate is already responsible for emitting. If the substrate
  // ever rearranges that layout, the sniff silently stops firing
  // and the dispatch-loop sync degrades to "send response
  // immediately" -- the cross-package contract holds today but is
  // load-bearing.
  const RUN_EVENT_PATH_RE = /^runs\/([^/]+)\/events\/[0-9]+\.json$/;
  const TERMINAL_EVENT_TYPES = new Set([
    "RunCompleted",
    "RunFailed",
    "RunCancelled",
  ]);

  /**
   * If the supplied merged-files set contains a terminal event blob
   * for any runId in the active processing/ set, wait for the
   * dispatch loop's markConsumed to settle before sending the
   * substrate.write.response. The notification path is the
   * `notifyMarkConsumed` hook the dispatch loop fires at the end of
   * dispatchOne; the wait here is per-runId so multiple runs can
   * proceed concurrently if a future dispatch loop ever processes
   * more than one mail in parallel.
   *
   * A watchdog timeout (`terminalWriteWatchdogMs`) caps the wait so a
   * never-arming markConsumed (a bug in the dispatch loop, a torn-down
   * cohort, a stalled inbox primitive) does not deadlock the child's
   * write -- and therefore the runtime body, and therefore the dispatch
   * loop. On expiry the waiter is force-released and a structured
   * failure propagates back to the child as
   * `{ ok: false, reason: "terminal-write watchdog timeout: ..." }`.
   */
  async function synchronouslyDispatchTerminalWrite(
    preservePrefix: string,
    mergedFiles: Record<string, string | Uint8Array>,
  ): Promise<{ ok: true } | { ok: false; reason: string }> {
    // The preservePrefix shape for run-event writes is
    // `runs/<runId>/events/`; the substrate accepts any prefix that
    // ends with `/`, so a write whose prefix does not match the
    // runs/-events shape is not a terminal-event write and we
    // short-circuit.
    if (!preservePrefix.startsWith("runs/")) return { ok: true };
    let runId: string | null = null;
    let isTerminal = false;
    for (const [path, content] of Object.entries(mergedFiles)) {
      const match = RUN_EVENT_PATH_RE.exec(path);
      if (match === null) continue;
      const fromPath = match[1];
      if (fromPath === undefined) continue;
      const bytes =
        typeof content === "string"
          ? new TextEncoder().encode(content)
          : content;
      let parsed: unknown;
      try {
        parsed = JSON.parse(new TextDecoder().decode(bytes));
      } catch {
        continue;
      }
      if (
        typeof parsed !== "object" ||
        parsed === null ||
        !("type" in parsed)
      ) {
        continue;
      }
      const t = (parsed as { type?: unknown }).type;
      if (typeof t !== "string") continue;
      if (TERMINAL_EVENT_TYPES.has(t)) {
        runId = fromPath;
        isTerminal = true;
      }
    }
    if (!isTerminal || runId === null) return { ok: true };
    if (!inFlightRuns.has(runId)) {
      return { ok: true };
    }
    const waiterRunId = runId;
    const completed = new Promise<void>((resolve, reject) => {
      markConsumedCompletionWaiters.set(waiterRunId, { resolve, reject });
    });
    const broadcaster = activeTerminalBroadcaster();
    if (broadcaster !== null) {
      const synthetic = synthesizeTerminalEvent(mergedFiles, waiterRunId);
      if (synthetic !== null) {
        broadcaster.notify(waiterRunId, synthetic);
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
          markConsumedCompletionWaiters.get(waiterRunId) !== undefined;
        if (stillPending) {
          markConsumedCompletionWaiters.delete(waiterRunId);
        }
        const reason = `terminal-write watchdog timeout: markConsumed for runId=${waiterRunId} did not settle within ${String(terminalWriteWatchdogMs)}ms`;
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
    files: Record<string, string | Uint8Array>,
    runId: string,
  ): TerminalRunEvent | null {
    for (const [path, content] of Object.entries(files)) {
      const match = RUN_EVENT_PATH_RE.exec(path);
      if (match === null) continue;
      if (match[1] !== runId) continue;
      const bytes =
        typeof content === "string"
          ? new TextEncoder().encode(content)
          : content;
      let parsed: unknown;
      try {
        parsed = JSON.parse(new TextDecoder().decode(bytes));
      } catch {
        continue;
      }
      if (
        typeof parsed !== "object" ||
        parsed === null ||
        !("type" in parsed) ||
        !("seq" in parsed)
      ) {
        continue;
      }
      const body = parsed as {
        type?: unknown;
        seq?: unknown;
        at?: unknown;
        error?: { message?: unknown };
      };
      if (typeof body.seq !== "number") continue;
      const at =
        typeof body.at === "string" ? body.at : new Date().toISOString();
      if (body.type === "RunCompleted") {
        return { kind: "RunCompleted", seq: body.seq, at };
      }
      if (body.type === "RunCancelled") {
        return { kind: "RunCancelled", seq: body.seq, at };
      }
      if (body.type === "RunFailed") {
        // The wire schema makes `error.message` required when the event
        // type is `RunFailed`. A blob that doesn't carry one is a
        // contract violation upstream of the supervisor; coercing it
        // to an empty string would silently hide the producer bug.
        if (typeof body.error?.message !== "string") {
          throw new Error(
            `synthesizeTerminalEvent: RunFailed blob at ${path} missing required error.message`,
          );
        }
        return {
          kind: "RunFailed",
          seq: body.seq,
          at,
          error: { message: body.error.message },
        };
      }
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
    const env: Record<string, string> = {
      ...bindings.substrateEnv,
      IPC_CHANNEL_ID: channelId,
      IPC_HMAC_KEY: bytesToHex(hmacKey),
      HOST_PUBKEY: bytesToHex(ipcKeypair.publicKey),
      DEPLOYMENT_ID: bindings.deploymentId,
      DEFINITION_HASH: opts.definitionHash,
      MAILBOX_ADDRESS: bindings.deploymentMailAddress,
    };

    const handle = bindings.subprocessSpawner({
      binaryPath: bindings.binaryPath,
      env,
    });

    const wired = await wireChild({
      channelId,
      hmacKey,
      ipcKeypair,
      handle,
      onInferenceEvent: opts.onInferenceEvent,
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
    };

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
    // BEFORE the mail subscription fires. A crash mid-dispatch in a
    // prior supervisor incarnation can leave an entry in
    // `processing/` with no owner; the FIFO contract requires the
    // entry move back to `inbox/` so the next dispatch picks it up
    // in its original arrival position. The replay runs off the
    // spawn critical path (the substrate write may roundtrip through
    // the pack-pushing wrap and a slow hub) and the dispatch loop's
    // first iteration awaits the result via `wakeDispatch` so the
    // replay's completion is observable before any dispatch lands.
    const replayDone = inboxPrimitives
      .replayProcessingToInbox(
        bindings.repoStore,
        inboxWritePrincipal,
        bindings.workflowRunRepoId,
        bindings.deploymentMailAddress,
      )
      .then(() => {
        wakeDispatch();
      })
      .catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`replayProcessingToInbox on spawn failed: ${message}`;
      });
    // Hold the replay promise so `shutdownInternal` can await its
    // settlement before tearing the bindings down.
    void replayDone;

    bindings.mailBus.registerAddress(bindings.deploymentMailAddress);
    const mailUnsubscribe = bindings.mailBus.subscribeMailForAddress(
      bindings.deploymentMailAddress,
      onMailMessage,
    );
    state.mailUnsubscribe = mailUnsubscribe;

    const readyInfo = await wired.readyPromise;

    // Push the assembled credentialsSnapshot to the child before the
    // mail buffer drains. Without this, the child's
    // `createCredentialsBackedAuthorize` closure observes a null
    // snapshot ref on the first authorize call and throws "no
    // credentialsSnapshot active"; the run's first step fails before
    // the runtime body can commit `StepCompleted`. The send rides the
    // same control channel `trigger.fire` uses, so the ordering
    // guarantee (`grants-updated` lands before `trigger.fire`) holds
    // for buffered and post-ready inbound mail alike.
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
      onInferenceEvent: opts.onInferenceEvent,
      spawnedAt: now(),
    };

    // Start the upstream control pump so the supervisor sees the
    // child's `recycle.request` (and any future upstream variant) as
    // it arrives. The pump exits when the iterator ends, which
    // happens when the child closes its end of the control channel
    // -- either on shutdown or on recycle's `kill` step.
    void pumpUpstreamControl(wired.controlIncoming).catch((cause) => {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`upstream control pump failed: ${message}`;
    });

    // Arm the recycle policy. The policy is a no-op when all bounds
    // are `undefined`; bounds resolution lives inside `createRecyclePolicy`.
    if (bindings.recyclePolicy !== undefined) {
      const setTimer = bindings.recyclePolicySetTimer ?? defaultSetTimer;
      const clearTimer = bindings.recyclePolicyClearTimer ?? defaultClearTimer;
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
    const dequeued = await inboxPrimitives.dequeueToProcessing(
      bindings.repoStore,
      inboxWritePrincipal,
      bindings.workflowRunRepoId,
      bindings.deploymentMailAddress,
    );
    if (dequeued === null) return false;
    const envelope = dequeued.envelope;
    const runId = envelope.messageId;
    const iterable = broadcaster.source(runId);
    const iter = iterable[Symbol.asyncIterator]();
    await forwardDispatchedEntry(
      sender,
      envelope.messageId,
      envelope.receivedAt,
    );
    await waitForRunTerminal(iter, cohortAbort.signal);
    inFlightRuns.delete(runId);
    if (cohortAbort.signal.aborted) {
      // The cohort tore down before the terminal event arrived (or
      // alongside it). Skip `markConsumed` so the recycle path's
      // drain-side replay can reclaim the processing entry.
      resolveMarkConsumedWaiter(runId);
      return false;
    }
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
        },
      );
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`markConsumed failed for run ${runId}: ${message}`;
    }
    resolveMarkConsumedWaiter(runId);
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
   */
  async function runDispatchLoop(
    sender: ControlChannelSender,
    cohortAbort: AbortController,
    broadcaster: TerminalBroadcaster,
  ): Promise<void> {
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
    // Stop every armed drainTimeout accumulator before tearing the
    // child down. An accumulator left running would otherwise fire
    // its `setTimeout` callback (or its terminal-event watcher's
    // settle hook) against a shutdown-mid-flight supervisor; the
    // explicit `stop()` makes the lifecycle deterministic.
    const accumulatorsToDispose = [...drainAccumulators.values()];
    for (const accumulator of accumulatorsToDispose) {
      accumulator.stop();
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
    if (recyclePolicy !== null) {
      recyclePolicy.stop();
      recyclePolicy = null;
    }
    spawnContext = null;
    if (
      prior.phase === "starting" ||
      prior.phase === "running" ||
      prior.phase === "recycling"
    ) {
      if (prior.mailUnsubscribe !== null) prior.mailUnsubscribe();
      try {
        bindings.mailBus.unregisterAddress(bindings.deploymentMailAddress);
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`mail bus unregisterAddress threw: ${message}`;
      }
      prior.handle.kill();
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
    logger.info`supervisor shutdown complete (${opts.reason})`;
  }

  async function drain(opts: DrainOpts): Promise<void> {
    // Drain is meaningful only when a workflow-process child is up;
    // calling it from `idle`/`stopping`/`stopped` is a no-op so the
    // higher-level host shutdown sequence can call drain
    // unconditionally without sniffing the phase. The recycle path
    // calls drain from `recycling` -- the kill/respawn gap is the
    // window where the existing controlSender is still live.
    if (
      state.phase !== "running" &&
      state.phase !== "starting" &&
      state.phase !== "recycling"
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
    // Abort the prior cohort immediately so the prior dispatch loop
    // exits before any inbound mail arrives during the kill/respawn
    // gap. Without the up-front abort, the prior loop would race the
    // `installNewChild` step and forward gap-window mail through the
    // dying child's controlSender, which is observable as a missing
    // `trigger.fire` on the new child once `ready` lands.
    prior.terminalCohortAbort.abort();
    wakeDispatch();
    const priorDispatchLoop = prior.dispatchLoop;
    // Transition to `recycling`. Inbound mail arriving during the
    // kill/respawn gap continues to flow through `enqueueInbox`
    // unchanged; the dispatch loop for the prior cohort has already
    // been aborted above. The new dispatch loop picks up the inbox
    // once `installNewChild` swaps the wiring and starts the loop
    // against the new cohort.
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
    };
    let attempt: RecycleAttempt;
    try {
      attempt = await triggerRecycle(
        {
          bindings,
          stepOrder: priorContext.stepOrder,
          definitionHash: priorContext.definitionHash,
          onInferenceEvent: priorContext.onInferenceEvent,
          current: {
            handle: prior.handle,
            controlSender: prior.controlSender,
            channelId: prior.channelId,
            eventPump: prior.eventPump,
          },
          drain: async (deadlineMs) => {
            // The recycle path's drain step uses the same drain
            // primitive an operator drain command would use.
            await drain({ deadlineMs });
          },
          replayProcessingToInbox: async () => {
            await inboxPrimitives.replayProcessingToInbox(
              bindings.repoStore,
              inboxWritePrincipal,
              bindings.workflowRunRepoId,
              bindings.deploymentMailAddress,
            );
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
              wiring.handle.kill("SIGTERM");
              return;
            }
            // The previous cohort was aborted at the entry to
            // `recycle()` so the prior dispatch loop did not race
            // the kill/respawn gap. Stop every armed accumulator
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
            };
            // Cache fresh spawn context with the updated spawnedAt
            // so the policy timer's uptime check resets on recycle.
            const now = bindings.recyclePolicyNow ?? defaultNow;
            spawnContext = {
              stepOrder: priorContext.stepOrder,
              definitionHash: priorContext.definitionHash,
              onInferenceEvent: priorContext.onInferenceEvent,
              spawnedAt: now(),
            };
            // Re-arm the upstream control pump on the new wiring's
            // iterator. The old wiring's iterator ended when the
            // recycle path killed the predecessor handle.
            void pumpUpstreamControl(controlIncoming).catch((cause) => {
              const message =
                cause instanceof Error ? cause.message : String(cause);
              logger.error`upstream control pump (post-recycle) failed: ${message}`;
            });
            // Kick the new dispatch loop so it picks up any inbox
            // entries the previous cohort's replayProcessingToInbox
            // just moved back.
            wakeDispatch();
          },
          onCrash: onChildCrash,
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
      recycleInProgress = false;
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

  function getCredentialsSnapshot(): CredentialsSnapshot | null {
    if (state.phase === "starting" || state.phase === "running") {
      return state.credentialsSnapshot;
    }
    return null;
  }

  return {
    deploy,
    spawn,
    requestCancel,
    shutdown,
    drain,
    recycle,
    deliverSignal,
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
};

type SpawnContext = {
  stepOrder: readonly string[];
  definitionHash: string;
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

function defaultSetTimer(cb: () => void, ms: number): unknown {
  return setTimeout(cb, ms);
}

function defaultClearTimer(handle: unknown): void {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the handle is the value `setTimeout` returned, narrowed back at the boundary
  clearTimeout(handle as ReturnType<typeof setTimeout>);
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
 * Derive a stable message identifier from the raw bytes the bus
 * delivered. The RFC 2822 `Message-ID` header (if present) is the
 * canonical identifier the audit log surfaces as
 * `RunStarted.consumedMessageId`; downstream consumers join inbound
 * mail to workflow-run events on this value, so the header parse must
 * win when the sender emitted one. A message that lacks a
 * `Message-ID` header falls back to a sha256 of the raw bytes so
 * runs originating from non-RFC 2822 transports still receive a
 * deterministic identifier.
 *
 * The parser walks the message until the headers/body separator
 * (`CRLF CRLF` per RFC 2822 §2.1, with the lone-`LF` variant tolerated
 * to match common in-memory senders). Header-field unfolding follows
 * RFC 2822 §2.2.3: a continuation line begins with whitespace and
 * appends to the prior line. Header-name comparison is
 * case-insensitive per RFC 2822 §1.2.2.
 */
async function deriveMessageId(rawMessage: Uint8Array): Promise<string> {
  const messageIdFromHeader = parseMessageIdHeader(rawMessage);
  if (messageIdFromHeader !== null) {
    return messageIdFromHeader;
  }
  const crypto = await import("node:crypto");
  return crypto.createHash("sha256").update(rawMessage).digest("hex");
}

function parseMessageIdHeader(rawMessage: Uint8Array): string | null {
  const text = new TextDecoder("utf-8", { fatal: false }).decode(rawMessage);
  // Headers end at the first blank line. RFC 2822 mandates `CRLF CRLF`
  // but tolerate `LF LF` for callers that normalize line endings.
  let headerSection = text;
  const crlfBoundary = text.indexOf("\r\n\r\n");
  const lfBoundary = text.indexOf("\n\n");
  if (crlfBoundary >= 0 && (lfBoundary < 0 || crlfBoundary < lfBoundary)) {
    headerSection = text.slice(0, crlfBoundary);
  } else if (lfBoundary >= 0) {
    headerSection = text.slice(0, lfBoundary);
  }
  // Unfold continuation lines (a line starting with WSP belongs to
  // the prior header field).
  const lines = headerSection.split(/\r?\n/);
  const unfolded: string[] = [];
  for (const line of lines) {
    if (line.length > 0 && (line[0] === " " || line[0] === "\t")) {
      if (unfolded.length === 0) continue;
      unfolded[unfolded.length - 1] += " " + line.trim();
      continue;
    }
    unfolded.push(line);
  }
  for (const line of unfolded) {
    const colon = line.indexOf(":");
    if (colon < 0) continue;
    const name = line.slice(0, colon).trim().toLowerCase();
    if (name !== "message-id") continue;
    return line.slice(colon + 1).trim();
  }
  return null;
}

function bytesToHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
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

function bytesToBase64(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64");
}

function base64ToBytes(value: string): Uint8Array {
  return new Uint8Array(Buffer.from(value, "base64"));
}
