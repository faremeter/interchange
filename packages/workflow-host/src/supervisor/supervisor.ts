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
  RecordRunEvent,
  SubprocessHandle,
  SupervisorDeployFrame,
  TerminalEventSource,
  TerminalRunEvent,
  WorkflowSupervisorBindings,
} from "./types";

const logger = getLogger(["workflow-host", "supervisor"]);

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
 * Construct a per-deployment supervisor. All host-specific
 * dependencies are pulled in via `bindings`; nothing in the
 * supervisor reaches into `process.env` or a singleton.
 */
export function createWorkflowSupervisor(
  bindings: WorkflowSupervisorBindings,
): WorkflowSupervisor {
  let state: SupervisorState = { phase: "idle" };
  /**
   * Buffer for inbound mail that arrives after `subscribeMailForAddress`
   * fires but before the child signals `ready`. Once `ready` lands,
   * the supervisor drains this buffer into the control channel as
   * `trigger.fire` frames; subsequent inbound mail bypasses the
   * buffer. The same buffer is reused across a recycle: during the
   * kill/respawn gap the subscription pushes here, and the recycle
   * path drains the buffer into the new child once `ready` lands.
   */
  const mailBuffer: Uint8Array[] = [];
  /**
   * In-flight runIds the supervisor knows about. A runId enters this
   * set when the supervisor forwards a `trigger.fire` for it on the
   * control channel; the runId leaves the set via the recycle/drain
   * cleanup path that lands in 12a (terminal-event observation lives
   * on the event channel, which the supervisor will project into
   * this set in that commit). For 11a's drain wire-up the supervisor
   * arms one accumulator per entry here -- monotonic growth is fine
   * because every accumulator stops cleanly on `shutdown`.
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
    // During `starting` and `recycling` the supervisor buffers; the
    // buffer drains in arrival order once `ready` lands (initial
    // spawn) or the recycle path's step 6 runs (recycle).
    if (state.phase === "starting" || state.phase === "recycling") {
      mailBuffer.push(rawMessage);
      return;
    }
    if (state.phase === "running") {
      const sender = state.controlSender;
      void forwardMailAndTrack(sender, rawMessage).catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`forwardMail failed: ${message}`;
      });
      return;
    }
    // In any other phase (idle, stopping, stopped) the host's
    // higher-level lifecycle is already tearing the deployment down;
    // the message drops on the floor.
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
      if (payload.type === "pack.push.request") {
        // Run the push off the iterator's loop so the iterator can
        // continue draining other upstream frames (and so the response
        // ordering follows binding completion rather than blocking
        // every other upstream frame behind the hub round-trip).
        void handlePackPushRequest(payload.data).catch((cause) => {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.error`pack.push.request handler crashed: ${message}`;
        });
        continue;
      }
      logger.warn`workflow-process upstream control payload ignored: type=${payload.type}`;
    }
  }

  async function handlePackPushRequest(
    data: Extract<ControlPayload, { type: "pack.push.request" }>["data"],
  ): Promise<void> {
    const controlSender = activeControlSender();
    if (controlSender === null) {
      // The supervisor is no longer in a phase where it can reply.
      // Dropping the push silently would corrupt the child's pending
      // map; the child's IPC pump is by then in a teardown path
      // alongside the supervisor's, so the dropped response is
      // observable through the child's process exit.
      logger.warn`pack.push.request received outside running phase; pushId=${data.pushId} dropped`;
      return;
    }
    const binding = bindings.pushWorkflowRunPack;
    if (binding === undefined) {
      await controlSender.send({
        type: "pack.push.response",
        data: {
          pushId: data.pushId,
          result: {
            ok: false,
            reason:
              "supervisor: pushWorkflowRunPack binding not configured on WorkflowSupervisorBindings",
          },
        },
      });
      return;
    }
    const validatedRepoId = RepoId(data.repoId);
    if (validatedRepoId instanceof type.errors) {
      // The IPC envelope already validated the structural shape, but
      // the IPC schema keeps `kind` as a bare string so it does not
      // depend on the `RepoKind` enum's value. Narrowing here turns a
      // bogus kind into a protocol-violation crash rather than letting
      // a malformed `repoId` reach the host's `HubLink`.
      onChildCrash(
        `pack.push.request repoId failed validation: ${validatedRepoId.summary}`,
      );
      return;
    }
    let pack: Uint8Array;
    try {
      pack = decodeBase64Pack(data.packBase64);
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      // A malformed base64 payload is a child protocol violation; the
      // supervisor's contract with the child is that the wire shape
      // arrives well-formed (the IPC envelope already validated the
      // typed shape). Crash the receiver so the child's process exit
      // surfaces a structured failure rather than hanging on a
      // never-resolved pending push.
      onChildCrash(`pack.push.request packBase64 decode failed: ${message}`);
      return;
    }
    try {
      await binding({
        agentAddress: data.agentAddress,
        repoId: validatedRepoId,
        pack,
        ref: data.ref,
        commitSha: data.commitSha,
      });
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      await controlSender.send({
        type: "pack.push.response",
        data: {
          pushId: data.pushId,
          result: { ok: false, reason },
        },
      });
      return;
    }
    await controlSender.send({
      type: "pack.push.response",
      data: {
        pushId: data.pushId,
        result: { ok: true },
      },
    });
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

    state = {
      phase: "starting",
      handle,
      controlSender: wired.wiring.controlSender,
      channelId,
      eventPump: wired.wiring.eventPump,
      onInferenceEvent: opts.onInferenceEvent,
      mailUnsubscribe: null,
      credentialsSnapshot: null,
      terminalCohortAbort:
        bindings.terminalEventSource !== undefined
          ? new AbortController()
          : null,
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

    // Drain the buffered mail before transitioning to running. The
    // ordering matters: any inbound mail that landed during
    // `starting` must hit the child in arrival order before the
    // first post-ready message.
    const startingPhaseCohortAbort = state.terminalCohortAbort;
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
    };
    while (mailBuffer.length > 0) {
      const message = mailBuffer.shift();
      if (message === undefined) break;
      await forwardMailAndTrack(wired.wiring.controlSender, message);
    }

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
   * Bind the supervisor's `terminalEventSource` binding to the active
   * spawn cohort's `AbortController`. Each call mints an iterator that
   * (a) pre-aborts immediately if the cohort has already been torn
   * down, (b) returns the iterator if the cohort aborts mid-iteration,
   * and (c) delegates element production to the binding's per-runId
   * source. Returns `null` when no binding is configured; the
   * accumulator factory's `terminalEventSource` slot is then left
   * undefined and the accumulator settles on timeout only.
   */
  function perCohortTerminalSource(
    cohortAbort: AbortController | null,
  ): TerminalEventSource | null {
    const source = bindings.terminalEventSource;
    if (source === undefined) return null;
    if (cohortAbort === null) return null;
    const signal = cohortAbort.signal;
    return (runId: string) => ({
      [Symbol.asyncIterator](): AsyncIterator<TerminalRunEvent> {
        if (signal.aborted) {
          return {
            next: () => Promise.resolve({ value: undefined, done: true }),
            return: (value?: unknown) => Promise.resolve({ value, done: true }),
          };
        }
        const inner = source(runId)[Symbol.asyncIterator]();
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
   * Forward an inbound message to the child and record its runId as
   * in-flight. The standalone `forwardMail` helper below owns the
   * wire-shape derivation; this wrapper closes over the
   * `inFlightRuns` set so the drain path knows which accumulators to
   * arm.
   */
  async function forwardMailAndTrack(
    sender: ControlChannelSender,
    rawMessage: Uint8Array,
  ): Promise<void> {
    const runId = await forwardMail(sender, rawMessage);
    inFlightRuns.add(runId);
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
      (prior.phase === "starting" ||
        prior.phase === "running" ||
        prior.phase === "recycling") &&
      prior.terminalCohortAbort !== null
    ) {
      prior.terminalCohortAbort.abort();
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
    const cohortSource = perCohortTerminalSource(state.terminalCohortAbort);
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
    // Transition to `recycling` so the mail subscription handler
    // buffers inbound messages across the gap rather than racing
    // against the dead controlSender.
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
          mailBuffer,
          drain: async (deadlineMs) => {
            // The recycle path's drain step uses the same drain
            // primitive an operator drain command would use.
            await drain({ deadlineMs });
          },
          forwardMail: forwardMailAndTrack,
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
            // Tear down the previous cohort's terminal-event
            // watchers: abort the cohort controller and stop every
            // armed accumulator. The accumulators were tracking runs
            // that lived inside the killed child; the resumed child
            // re-discovers any survivors and the next `drain()` mints
            // fresh accumulators against the new cohort. Watcher
            // lifetimes stay tied to one child's run cohort per the
            // recycle contract.
            const previousCohortAbort = prior.terminalCohortAbort;
            if (previousCohortAbort !== null) previousCohortAbort.abort();
            for (const accumulator of drainAccumulators.values()) {
              accumulator.stop();
            }
            drainAccumulators.clear();
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
              terminalCohortAbort:
                bindings.terminalEventSource !== undefined
                  ? new AbortController()
                  : null,
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
   * Per-spawn cohort abort controller for terminal-event watchers. Each
   * watcher the supervisor mints inside `drain()` (via the accumulator)
   * borrows this controller's signal; the watcher's iterator finalises
   * when the controller aborts. `shutdownInternal` aborts the
   * controller alongside `mailUnsubscribe`; `installNewChild` mints a
   * fresh controller for the recycled cohort so a watcher minted in
   * the previous cohort cannot survive into the next.
   *
   * `null` when the `terminalEventSource` binding is absent (test
   * fixture default); accumulators in that case settle on timeout
   * only.
   */
  terminalCohortAbort: AbortController | null;
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
 * Forward an inbound RFC 2822 message to the child as a
 * `trigger.fire` control frame and return the runId the frame
 * carried so the caller can track it as in-flight. The supervisor
 * mints a `receivedAt` timestamp at forward time so the child sees
 * monotonically-increasing timestamps across reordered IPC frames.
 */
async function forwardMail(
  sender: ControlChannelSender,
  rawMessage: Uint8Array,
): Promise<string> {
  // The IPC `trigger.fire` payload carries the runId, messageId, and
  // receivedAt. The supervisor parses the RFC 2822 envelope's
  // Message-ID and mints a runId at fire time (one run per trigger
  // fire). The runId/messageId derivation here is a thin pass: the
  // message bytes are surfaced as the `messageId` discriminator
  // until the real RFC 2822 envelope parse lands with the deploy-
  // mail bridging.
  const messageId = await deriveMessageId(rawMessage);
  await sender.send({
    type: "trigger.fire",
    data: {
      runId: messageId,
      messageId,
      receivedAt: Date.now(),
    },
  });
  return messageId;
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
 * Decode the base64 form the child sent in `pack.push.request.data.packBase64`
 * into the raw pack bytes the host's `pushWorkflowRunPack` binding
 * consumes. Empty / malformed inputs throw so the supervisor's handler
 * surfaces the failure as a protocol violation.
 */
function decodeBase64Pack(packBase64: string): Uint8Array {
  if (packBase64.length === 0) {
    throw new Error("packBase64 must be a non-empty string");
  }
  if (!/^[A-Za-z0-9+/=]+$/.test(packBase64)) {
    throw new Error("packBase64 contains non-base64 characters");
  }
  const buf = Buffer.from(packBase64, "base64");
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
}
