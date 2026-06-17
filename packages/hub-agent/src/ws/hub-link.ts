// HubLink: the sidecar-side WebSocket protocol.
//
// Connects to the hub, sends the register or reconnect frame, forwards
// outbound mail and inference events, and handles inbound agent
// lifecycle commands. Per-agent key material lives on AgentKeyStore;
// the link calls into the store for challenge signing, deploy-commit
// verification, and hub-key bookkeeping. The wire layer itself never
// touches raw key bytes.

import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import { type } from "arktype";
import {
  HubFrame,
  type SidecarFrame,
  type AgentDeployFrame,
  type AgentUndeployFrame,
  type ChallengeFrame,
  type ChallengeFailedFrame,
  type SessionAbortFrame,
  type SessionStartFrame,
  type GrantsUpdateFrame,
  type SourcesUpdateFrame,
  type PackPushFrame,
  type PackDoneFrame,
  type PackAckFrame,
  type PackRejectFrame,
  type RepoId,
  type SignalDeliverFrame,
  type DrainDeliverFrame,
  type SyncRequestFrame,
  type DeployApplyErrorFrame,
} from "@intx/types/sidecar";
import { createPackReceiver, createPackSender } from "@intx/pack-transport";
import { base64Decode, base64Encode, hexDecode, hexEncode } from "@intx/types";

import type { AgentKeyStore } from "../agent-key-store";
import type {
  ConnectorStateSink,
  SessionEventSink,
  SessionManager,
} from "../session-manager";

const logger = getLogger(["interchange", "hub-agent", "ws"]);

const DEFAULT_PING_INTERVAL_MS = 30_000;
const DEFAULT_RECONNECT_DELAY_MS = 3_000;

/**
 * Schedules a deferred callback and returns a cancel function. Injection
 * point for tests: a fake scheduler records the callback so the test
 * can observe whether cancellation actually happened, without relying
 * on wall-clock waits.
 */
export type ReconnectScheduler = (
  callback: () => void,
  delayMs: number,
) => () => void;

const defaultScheduleReconnect: ReconnectScheduler = (callback, delayMs) => {
  const handle = setTimeout(callback, delayMs);
  return () => {
    clearTimeout(handle);
  };
};

/**
 * Result the deploy router returns to the link after the trivial
 * branch completes. Carries the values the link folds into the
 * outbound `agent.deploy.ack` frame; the link itself stays out of
 * the provisioning details.
 */
export type DeployRouterResult = {
  /** Hex-encoded agent public key the hub records for verification. */
  publicKey: string;
};

/**
 * Single-ingress deploy contract the link routes every `agent.deploy`
 * frame through. The workflow-host supervisor is the production
 * implementation -- the supervisor decides between the trivial
 * (1-step) passthrough and the multi-step IPC-backed branch. The
 * shape lives on hub-agent so the package boundary stays one-way
 * (`@intx/hub-agent` does not import `@intx/workflow-host`).
 */
export interface DeployRouter {
  deploy(frame: AgentDeployFrame): Promise<DeployRouterResult>;
  /**
   * Symmetric teardown for `deploy`. The link invokes this when an
   * `agent.undeploy` frame lands so the router can release any
   * per-deployment registrations the deploy path installed
   * (`MultistepMailRouter`, `MultistepSignalRouter`,
   * `MultistepDrainRouter`, `DeploymentAddressRegistry`). Optional
   * so test routers and the inline trivial test fixture can omit
   * the implementation.
   */
  undeploy?: (frame: AgentUndeployFrame) => Promise<void>;
}

/**
 * Per-address mail handler registry the link consults on every
 * `mail.inbound` frame before falling back to `transport.deliver` /
 * `sessions.commitInboundMail`. Production wires this against the
 * sidecar's `createMultistepMailRouter` so a multi-step deployment's
 * supervisor receives the bytes through its mail-bus subscription.
 * Trivial deploys never register a handler; their mail flows through
 * the legacy session path unchanged. The shape lives on hub-agent so
 * the link does not import the sidecar host's wiring module, and so
 * tests can substitute a stub.
 */
export interface MailInboundRouter {
  /**
   * Attempt to dispatch `message` to a handler registered against
   * `agentAddress`. Returns `true` if a handler claimed the message;
   * `false` if no handler is registered. A `true` return causes the
   * link to skip the legacy `transport.deliver` /
   * `sessions.commitInboundMail` fallback entirely.
   */
  tryRoute(agentAddress: string, message: Uint8Array): boolean;
}

/**
 * Per-deployment-address signal handler registry the link consults on
 * every inbound `signal.deliver` frame. Production wires this against
 * the sidecar's multi-step deploy registry so the frame flows into the
 * deployment's supervisor (which forwards `signal.deliver` over the
 * control IPC to the workflow-process child). Trivial deployments do
 * not register a handler; the link logs and drops a frame whose
 * `agentAddress` is unknown so the wire surface fails loudly rather
 * than silently absorbing a misrouted delivery.
 *
 * The shape lives on hub-agent so the link does not import the sidecar
 * host's wiring module, and so tests can substitute a stub.
 */
export interface SignalInboundRouter {
  /**
   * Attempt to dispatch `frame` to the supervisor registered against
   * `frame.agentAddress`. Returns a promise that resolves to `true`
   * when a handler accepted the frame, `false` when no handler is
   * registered; the promise rejects when the handler is registered but
   * the supervisor's `deliverSignal` itself throws. The link surfaces
   * a rejection through a logged warning -- a structured failure-reply
   * frame for signals does not exist on the wire today.
   */
  tryRoute(frame: SignalDeliverFrame): Promise<boolean>;
}

/**
 * Per-deployment-address drain handler registry the link consults on
 * every inbound `drain.deliver` frame. Production wires this against
 * the sidecar's multi-step deploy registry so the frame flows into the
 * deployment's supervisor (which forwards a `drain` control IPC frame
 * to the workflow-process child and arms one drainTimeout accumulator
 * per in-flight run). Trivial deployments do not register a handler;
 * the link logs and drops a frame whose `agentAddress` is unknown so
 * the wire surface fails loudly rather than silently absorbing a
 * misrouted delivery.
 *
 * The shape lives on hub-agent so the link does not import the sidecar
 * host's wiring module, and so tests can substitute a stub.
 */
export interface DrainInboundRouter {
  /**
   * Attempt to dispatch `frame` to the supervisor registered against
   * `frame.agentAddress`. Returns a promise that resolves to `true`
   * when a handler accepted the frame, `false` when no handler is
   * registered; the promise rejects when the handler is registered but
   * the supervisor's `drain` itself throws. The link surfaces a
   * rejection through a logged warning -- a structured failure-reply
   * frame for drain does not exist on the wire today.
   */
  tryRoute(frame: DrainDeliverFrame): Promise<boolean>;
}

export type HubLinkConfig = {
  hubURL: string;
  sidecarId: string;
  token: string;
  transport: HubTransport;
  sessions: SessionManager;
  /**
   * Key custody and per-frame crypto. HubLink calls into the store for
   * challenge signing, deploy-commit verification, hub-key recording,
   * and per-agent forgetting; it does not maintain its own copy of
   * those tables.
   */
  keyStore: AgentKeyStore;
  /**
   * Routes every inbound `agent.deploy` frame. Production wiring
   * supplies a router that calls `supervisor.deploy(frame)` on a
   * freshly-constructed workflow-host supervisor whose
   * `trivialLaunch` closes over `SessionManager.provisionAgent`.
   * The supervisor owns the trivial vs multi-step decision; the
   * link does not re-decide.
   */
  deployRouter: DeployRouter;
  /**
   * Optional pre-fallback mail dispatcher. When present, the link
   * consults this router on every inbound `mail.inbound` frame; a
   * `true` return takes the bytes off the legacy
   * `transport.deliver` + `sessions.commitInboundMail` path entirely.
   * Production wires this against the sidecar's multi-step deploy
   * registry so a deployment-address inbound flows into the
   * supervisor's mail-bus subscription. Trivial deployments (and
   * tests that exercise the legacy path) omit it; an absent router
   * is treated as "no handler ever claims" so behaviour is unchanged.
   */
  mailInboundRouter?: MailInboundRouter;
  /**
   * Optional pre-fallback signal dispatcher. When present, the link
   * routes every inbound `signal.deliver` frame through this router.
   * Production wires this against the sidecar's multi-step deploy
   * registry so a deployment-address signal flows into the
   * supervisor's `deliverSignal`. Trivial deployments (and tests that
   * do not exercise the signal-delivery surface) omit it; an absent
   * router causes inbound signal frames to be logged-and-dropped so a
   * misrouted delivery is observable rather than silent.
   */
  signalInboundRouter?: SignalInboundRouter;
  /**
   * Optional pre-fallback drain dispatcher. When present, the link
   * routes every inbound `drain.deliver` frame through this router.
   * Production wires this against the sidecar's multi-step deploy
   * registry so a deployment-address drain flows into the supervisor's
   * `drain`. Trivial deployments (and tests that do not exercise the
   * drain surface) omit it; an absent router causes inbound drain
   * frames to be logged-and-dropped so a misrouted delivery is
   * observable rather than silent.
   */
  drainInboundRouter?: DrainInboundRouter;
  pingIntervalMs?: number;
  reconnectDelayMs?: number;
  scheduleReconnect?: ReconnectScheduler;
};

export type HubLink = {
  /**
   * Open the connection. Must not be called after `close()`; calling it
   * on a closed client throws.
   */
  connect(): void;
  close(): void;
  sendEvent: SessionEventSink;
  sendConnectorState: ConnectorStateSink;
  /**
   * Ship a deploy.apply.error frame to the hub. Caller supplies the
   * agentAddress separately so the frame's other fields stay close to
   * the loader's failure-site description.
   */
  sendDeployApplyError: (
    agentAddress: string,
    payload: Omit<DeployApplyErrorFrame, "type" | "agentAddress">,
  ) => void;
  /**
   * Ship a workflow-run pack to the hub. Streams the supplied pack as
   * `repo.pack.push` chunks followed by a `repo.pack.done`, then
   * resolves on the matching `repo.pack.ack` (rejects on
   * `repo.pack.reject` with the carried reason). The hub routes the
   * pack to its `workflow-run` receiver because `repoId.kind` is
   * `"workflow-run"`.
   */
  pushWorkflowRunPack: (opts: {
    agentAddress: string;
    repoId: RepoId;
    pack: Uint8Array;
    ref: string;
    commitSha: string;
  }) => Promise<void>;
};

export function createHubLink(config: HubLinkConfig): HubLink {
  const {
    hubURL,
    sidecarId,
    token,
    transport,
    sessions,
    keyStore,
    deployRouter,
    mailInboundRouter,
    signalInboundRouter,
    drainInboundRouter,
    pingIntervalMs = DEFAULT_PING_INTERVAL_MS,
    reconnectDelayMs = DEFAULT_RECONNECT_DELAY_MS,
    scheduleReconnect = defaultScheduleReconnect,
  } = config;

  let ws: WebSocket | null = null;
  let closed = false;
  let pingTimer: ReturnType<typeof setInterval> | null = null;
  let cancelReconnect: (() => void) | null = null;
  let lastPongAt = 0;

  const packReceiver = createPackReceiver();
  // One sender owns the agent-state push path (`handleSyncRequest`,
  // `handleAgentUndeploy`) and the workflow-run push path
  // (`pushWorkflowRunPack`). transferIds for the two flows live in
  // disjoint namespaces (`undeploy-*` / sync-supplied / `workflow-run-*`),
  // so a single pending-id map is unambiguous; the protocol logic
  // (chunking, ack-handshake) lives once in `@intx/pack-transport`.
  const packSender = createPackSender({ sendFrame: (frame) => send(frame) });

  // Serialize frame processing so async handlers (deploy, undeploy, abort)
  // cannot race against each other.
  let messageQueue: Promise<void> = Promise.resolve();

  // Outbound frames queued while disconnected.
  const MAX_QUEUE = 1024;
  const queue: SidecarFrame[] = [];

  function send(frame: SidecarFrame): void {
    if (ws !== null && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(frame));
      return;
    }
    if (queue.length >= MAX_QUEUE) {
      logger.warn`Outbound queue full, dropping oldest frame`;
      queue.shift();
    }
    queue.push(frame);
  }

  function flush(): void {
    while (
      queue.length > 0 &&
      ws !== null &&
      ws.readyState === WebSocket.OPEN
    ) {
      ws.send(JSON.stringify(queue.shift()));
    }
  }

  // Wire the transport's remote send handler to push mail.outbound frames
  // for routing. These carry only the raw message and recipients — the hub
  // routes them to the destination sidecar.
  transport.setRemoteSendHandler(async (rawMessage, recipients) => {
    const encoded = base64Encode(rawMessage);
    send({
      type: "mail.outbound",
      rawMessage: encoded,
      recipients,
    });
  });

  // Forward every send to the hub for audit and event emission. Local-only
  // sends are marked delivered: true so the hub does not re-route them.
  // Remote sends are marked delivered: true as well — routing was already
  // handled by the RemoteSendHandler above.
  transport.addMessageSentHandler(async (ctx) => {
    const encoded = base64Encode(ctx.rawMessage);
    const sessionId = sessions.getSessionId(ctx.senderAddress);
    send({
      type: "mail.outbound",
      rawMessage: encoded,
      recipients: ctx.recipients,
      senderAddress: ctx.senderAddress,
      ...(sessionId !== undefined ? { sessionId } : {}),
      messageId: ctx.messageId,
      to: ctx.to,
      ...(ctx.cc.length > 0 ? { cc: ctx.cc } : {}),
      delivered: true,
    });
  });

  async function handleAgentDeploy(frame: AgentDeployFrame): Promise<void> {
    try {
      // The deploy router (production: workflow-host supervisor)
      // owns the agent.deploy framing decision -- trivial vs
      // multi-step -- and returns the deploy public key the link
      // folds into the outbound ack. The link itself does not
      // re-decide; routing lives on the supervisor side of the seam.
      const result = await deployRouter.deploy(frame);
      send({
        type: "agent.deploy.ack",
        agentAddress: frame.agentAddress,
        publicKey: result.publicKey,
      });
      logger.info`Provisioned agent ${frame.agentAddress}`;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "agent.error",
        agentAddress: frame.agentAddress,
        error: message,
      });
    }
  }

  async function handleSessionStart(frame: SessionStartFrame): Promise<void> {
    try {
      await sessions.startSession(frame.agentAddress);
      send({
        type: "session.start.ack",
        agentAddress: frame.agentAddress,
      });
      logger.info`Started session for ${frame.agentAddress}`;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "agent.error",
        agentAddress: frame.agentAddress,
        error: message,
      });
    }
  }

  async function handleAgentUndeploy(frame: AgentUndeployFrame): Promise<void> {
    let statePushed = false;

    // Release per-deployment routing state the deploy router installed
    // for this address (multi-step mail/signal/drain handlers and the
    // deployment-address mapping) before the session tears down. With
    // the registrations released, any in-flight `signal.deliver` /
    // `drain.deliver` / `mail.inbound` frame that lands during teardown
    // is rejected by the router rather than dispatched into a
    // soon-to-be-orphaned supervisor handler. Trivial-deploy routers
    // (and test stubs) omit the hook; an absent hook means there was
    // nothing to release.
    if (deployRouter.undeploy !== undefined) {
      try {
        await deployRouter.undeploy(frame);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn`Deploy router undeploy hook failed for ${frame.agentAddress}: ${msg}`;
      }
    }

    // Prune `workflowRunPackBootstrapped` entries recorded under this
    // address so a future workflow-run-repo reset for the same
    // `(kind, id, ref)` triple re-runs the bootstrap-retry arm. Without
    // the prune the flag survives across the deployment's lifetime,
    // grows unbounded over the link's lifetime, and a hub-side rotation
    // / disaster-recovery reset surfaces as a `non_fast_forward` on the
    // first post-reset push (the link skips the retry on the stale
    // flag).
    const bootstrapped = workflowRunPackBootstrappedByAddress.get(
      frame.agentAddress,
    );
    if (bootstrapped !== undefined) {
      for (const key of bootstrapped) {
        workflowRunPackBootstrapped.delete(key);
      }
      workflowRunPackBootstrappedByAddress.delete(frame.agentAddress);
    }

    // Stop the harness first.
    try {
      await sessions.destroySession(frame.agentAddress);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`Failed to stop session for ${frame.agentAddress}: ${msg}`;
    }

    // Best-effort state push to the hub before deleting the directory.
    // statePushed reflects whether we sent the pack frames, not whether
    // the hub acknowledged them. We intentionally skip waiting for
    // repo.pack.ack here to avoid blocking the undeploy on a round-trip
    // that may never complete if the hub is shutting down -- so the
    // pending Promise's rejection on disconnect is intentionally
    // swallowed below.
    try {
      const { pack, commitSha, ref } = await sessions.createStatePack(
        frame.agentAddress,
      );
      const repoId: RepoId = {
        kind: "agent-state",
        id: frame.agentAddress,
      };

      void packSender
        .send({
          agentAddress: frame.agentAddress,
          repoId,
          transferId: `undeploy-${frame.agentAddress}`,
          pack,
          ref,
          commitSha,
        })
        .catch(() => {
          // Intentional: undeploy's pack push is best-effort. See above.
        });

      statePushed = true;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`State push failed for ${frame.agentAddress}: ${msg}`;
    }

    // Delete the agent directory.
    try {
      await sessions.deleteAgentDir(frame.agentAddress);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`Failed to delete agent directory for ${frame.agentAddress}: ${msg}`;
    }

    keyStore.forgetAgent(frame.agentAddress);

    send({
      type: "agent.undeploy.ack",
      agentAddress: frame.agentAddress,
      statePushed,
    });
    logger.info`Undeployed agent ${frame.agentAddress}: ${frame.reason}`;
  }

  function handleChallenge(frame: ChallengeFrame): void {
    const responses: { address: string; signature: string }[] = [];

    for (const { address, nonce } of frame.challenges) {
      const nonceBytes = hexDecode(nonce);
      const addressBytes = new TextEncoder().encode(address);
      const payload = new Uint8Array(nonceBytes.length + addressBytes.length);
      payload.set(nonceBytes);
      payload.set(addressBytes, nonceBytes.length);

      const sig = keyStore.signChallenge(address, payload);
      if (sig === null) {
        logger.warn`No key pair for challenged address ${address}`;
        continue;
      }

      responses.push({
        address,
        signature: hexEncode(sig),
      });
    }

    send({ type: "challenge.response", responses });
  }

  async function handleChallengeFailed(
    frame: ChallengeFailedFrame,
  ): Promise<void> {
    // The hub rejected this agent during reconnect — tear it down so
    // the address is freed for future deploys. The agent may not have
    // an active session (provisioned but never started, or already
    // destroyed by a concurrent path), so any error from destroySession
    // is logged but does not abort the wire layer. Destroy first so any
    // disposer or mail-commit code still has the agent's crypto handle;
    // forget the key material only once the session lifecycle methods
    // have returned.
    try {
      await sessions.destroySession(frame.address);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`destroySession during challenge.failed for ${frame.address}: ${msg}`;
    }
    keyStore.forgetAgent(frame.address);

    logger.warn`Challenge failed for ${frame.address}, agent torn down: ${frame.reason}`;
  }

  async function handleSessionAbort(frame: SessionAbortFrame): Promise<void> {
    try {
      await sessions.abortSession(frame.agentAddress, frame.reason);
      send({ type: "session.ack", requestId: frame.requestId });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: message,
      });
    }
  }

  async function handleGrantsUpdate(frame: GrantsUpdateFrame): Promise<void> {
    try {
      await sessions.updateGrants(frame.agentAddress, frame.grants);
      send({ type: "session.ack", requestId: frame.requestId });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: message,
      });
    }
  }

  async function handleSourcesUpdate(frame: SourcesUpdateFrame): Promise<void> {
    try {
      await sessions.updateSources(
        frame.agentAddress,
        frame.sources,
        frame.defaultSource,
      );
      send({ type: "session.ack", requestId: frame.requestId });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: message,
      });
    }
  }

  function handlePackPush(frame: PackPushFrame): void {
    const reason = packReceiver.handlePush(frame);
    if (reason !== null) {
      send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason,
      });
    }
  }

  async function handlePackDone(frame: PackDoneFrame): Promise<void> {
    const result = packReceiver.handleDone(frame);
    if (result === null) {
      send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    try {
      if (frame.mountPath !== undefined) {
        // Asset pack: route to the workspace materializer. Use
        // frame.agentAddress for destination routing — frame.repoId.id
        // names the source asset at the hub, which is a different
        // entity than the destination agent.
        await sessions.applyAssetPack(
          frame.agentAddress,
          frame.mountPath,
          result.pack,
          result.ref,
          result.commitSha,
        );
      } else {
        const verifyCommit = (payload: string, signature: string) =>
          keyStore.verifyDeployCommit(frame.agentAddress, payload, signature);

        await sessions.applyDeployPack(
          frame.agentAddress,
          result.pack,
          result.ref,
          result.commitSha,
          frame.transferId,
          verifyCommit,
        );
      }
      send({
        type: "repo.pack.ack",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      // asset_materialization_failed errors mirror deploy materialization
      // errors into the same `corrupt` bucket — finer-grained
      // classification is out of scope for v1 asset packs.
      const reason = msg.startsWith("sha_mismatch")
        ? "sha_mismatch"
        : msg.startsWith("signature_invalid") ||
            msg.startsWith("signature_unsigned")
          ? "signature_invalid"
          : "corrupt";
      logger.warn`Pack apply failed for ${frame.agentAddress}: ${msg}`;
      send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason,
      });
    }
  }

  // Counter the boot edge consumes via `pushWorkflowRunPack` to mint
  // collision-free transferIds. Lives on the link so undeploy /
  // sync-request / workflow-run all share one monotonically increasing
  // sequence space.
  let workflowRunPackCounter = 0;

  // Per-(repoId.id, ref) flag tracking whether at least one workflow-run
  // pack push has been accepted by the hub, and a per-(repoId.id, ref)
  // serialization queue. Both are needed because the hub's
  // `receiveWorkflowRunPack` resolves the ref OUTSIDE the substrate's
  // per-repo lock, then enters `receivePack` which acquires the lock
  // and calls `initRepo` BEFORE the CAS check.
  //
  // First-push race:
  //   The hub's `initRepo` creates a `.gitignore` genesis commit on
  //   `refs/heads/main` inside the lock. `receivePackObjects`'s CAS
  //   then compares that genesis (now the ref's tip) against the
  //   caller-supplied `expectedOldSha` (null, because the caller's
  //   pre-lock `resolveRef` observed an absent repo) and rejects with
  //   `non_fast_forward`. The hub surfaces the failure as
  //   `reason: "corrupt"` on the wire.
  //
  // Concurrent-push race:
  //   Two pushes arriving close together both run their pre-lock
  //   `resolveRef` against the same hub state; whichever loses the
  //   `withRepoLock` race observes a stale `expectedOldSha` and
  //   rejects with `non_fast_forward`.
  //
  // We close both windows on the sender side: serialize every push
  // per `(repoId, ref)` so the second sender only fires after the
  // first has been acked or rejected, and retry the FIRST push once
  // to absorb the bootstrap race against the hub's `initRepo` step.
  // Re-shipping the same pack against the now-initialized hub repo
  // works because the hub's next `resolveRef` returns the genesis
  // sha (instead of null) and the CAS passes. The retry is bounded
  // to the first push per `(repoId, ref)` so a genuine corruption
  // surfaces verbatim once the repo has been bootstrapped.
  const workflowRunPackBootstrapped = new Set<string>();
  const workflowRunPackQueues = new Map<string, Promise<void>>();
  // Reverse index: agentAddress -> bootstrap keys recorded under that
  // address. `handleAgentUndeploy` consults this to prune
  // `workflowRunPackBootstrapped` entries owned by the just-undeployed
  // deployment so a future workflow-run-repo reset for the same
  // `(kind, id, ref)` triple re-runs the bootstrap-retry arm instead of
  // skipping it on the stale flag and failing with `non_fast_forward`.
  // Indexed by `agentAddress` (not `deploymentId`) because the link
  // does not own the address->deploymentId derivation -- the sidecar's
  // deploy router does. Every workflow-run push the link sees carries
  // the originating address explicitly, so the index closes the gap
  // structurally without leaking the derivation across the package
  // boundary.
  const workflowRunPackBootstrappedByAddress = new Map<string, Set<string>>();
  function workflowRunPackKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}:${repoId.id}:${ref}`;
  }

  async function handleSyncRequest(frame: SyncRequestFrame): Promise<void> {
    const { agentAddress, transferId } = frame;
    try {
      const { pack, commitSha, ref } =
        await sessions.createStatePack(agentAddress);
      const repoId: RepoId = { kind: "agent-state", id: agentAddress };

      await packSender.send({
        agentAddress,
        repoId,
        transferId,
        pack,
        ref,
        commitSha,
      });

      logger.info`State push complete for ${agentAddress} (${commitSha.slice(0, 8)})`;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`State push failed for ${agentAddress}: ${msg}`;
    }
  }

  function handlePackAck(frame: PackAckFrame): void {
    if (!packSender.handleAck(frame)) {
      logger.warn`Received repo.pack.ack for unknown transferId ${frame.transferId}`;
    }
  }

  function handlePackReject(frame: PackRejectFrame): void {
    if (!packSender.handleReject(frame)) {
      logger.warn`Received repo.pack.reject for unknown transferId ${frame.transferId}`;
    }
  }

  async function handleSignalDeliver(frame: SignalDeliverFrame): Promise<void> {
    if (signalInboundRouter === undefined) {
      logger.warn`Received signal.deliver for ${frame.agentAddress} but no signalInboundRouter is wired; dropping`;
      return;
    }
    try {
      const routed = await signalInboundRouter.tryRoute(frame);
      if (!routed) {
        logger.warn`signal.deliver for ${frame.agentAddress} did not match any registered deployment; dropping`;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`signal.deliver delivery failed for ${frame.agentAddress}: ${msg}`;
    }
  }

  async function handleDrainDeliver(frame: DrainDeliverFrame): Promise<void> {
    if (drainInboundRouter === undefined) {
      logger.warn`Received drain.deliver for ${frame.agentAddress} but no drainInboundRouter is wired; dropping`;
      return;
    }
    try {
      const routed = await drainInboundRouter.tryRoute(frame);
      if (!routed) {
        logger.warn`drain.deliver for ${frame.agentAddress} did not match any registered deployment; dropping`;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`drain.deliver delivery failed for ${frame.agentAddress}: ${msg}`;
    }
  }

  async function pushWorkflowRunPack(opts: {
    agentAddress: string;
    repoId: RepoId;
    pack: Uint8Array;
    ref: string;
    commitSha: string;
  }): Promise<void> {
    const key = workflowRunPackKey(opts.repoId, opts.ref);

    async function sendOnce(): Promise<void> {
      const transferId = `workflow-run-${++workflowRunPackCounter}-${opts.repoId.id}`;
      await packSender.send({
        agentAddress: opts.agentAddress,
        repoId: opts.repoId,
        transferId,
        pack: opts.pack,
        ref: opts.ref,
        commitSha: opts.commitSha,
      });
    }

    async function runWithBootstrap(): Promise<void> {
      if (workflowRunPackBootstrapped.has(key)) {
        await sendOnce();
        return;
      }
      try {
        await sendOnce();
      } catch (first) {
        // First push to a never-bootstrapped (repoId, ref) lost the
        // race with the hub substrate's `receivePack` initRepo step
        // (see the comment on `workflowRunPackBootstrapped` above).
        // The hub has now initialized the repo as a side effect of
        // the failed push; the retry uses the same pack but observes
        // the bootstrap genesis as the CAS baseline and lands.
        const reason = first instanceof Error ? first.message : String(first);
        logger.warn`Workflow-run pack push bootstrap retry for ${opts.repoId.id}/${opts.ref}: ${reason}`;
        await sendOnce();
      }
      workflowRunPackBootstrapped.add(key);
      let perAddress = workflowRunPackBootstrappedByAddress.get(
        opts.agentAddress,
      );
      if (perAddress === undefined) {
        perAddress = new Set<string>();
        workflowRunPackBootstrappedByAddress.set(opts.agentAddress, perAddress);
      }
      perAddress.add(key);
    }

    // Serialize pushes per (repoId, ref). The hub's `receiveWorkflowRunPack`
    // does its `resolveRef` outside the substrate's per-repo lock, so
    // overlapping pushes from this sender would each observe a stale
    // baseline and the second to acquire the hub-side lock would
    // reject with `non_fast_forward`. Chaining through this queue
    // keeps the receive ordering consistent end-to-end.
    const prior = workflowRunPackQueues.get(key) ?? Promise.resolve();
    const next = prior.catch(() => undefined).then(() => runWithBootstrap());
    workflowRunPackQueues.set(key, next);
    try {
      await next;
    } finally {
      // Drop the queue entry when the chain has settled and no
      // follower has appended, so a long-idle (repoId, ref) does not
      // hold a dead promise reference. A racing append replaces this
      // entry before we get here; the conditional avoids clobbering
      // a still-active chain.
      if (workflowRunPackQueues.get(key) === next) {
        workflowRunPackQueues.delete(key);
      }
    }
  }

  async function handleMessage(data: string): Promise<void> {
    let raw: unknown;
    try {
      raw = JSON.parse(data) as unknown;
    } catch {
      logger.warn`Received unparseable frame from hub`;
      return;
    }
    const validated = HubFrame(raw);
    if (validated instanceof type.errors) {
      logger.warn`Invalid hub frame: ${validated.summary}`;
      return;
    }
    const frame = validated;

    switch (frame.type) {
      case "mail.inbound": {
        const rawBytes = base64Decode(frame.rawMessage);
        // Multi-step deployments register the deployment-level mail
        // address on `mailInboundRouter` once their supervisor spawns.
        // For those addresses the legacy session path is not the right
        // receiver -- the transport mailbox is never registered for the
        // deployment address (no `startSession` ever runs against it),
        // and there is no `sessions` entry to satisfy
        // `commitInboundMail`. Routing through the registered handler
        // delivers the bytes to the supervisor's mail-bus subscription,
        // which is what the workflow-host's `awaitSignal` listens on.
        // Trivial deployments do not register a handler; the fallback
        // path is the legacy single-agent provisioning surface.
        //
        // Guard the router call with try/catch so a throwing handler
        // does not reject this `handleMessage` promise and wedge the
        // per-connection `messageQueue` chain. A rejected chain would
        // silently drop every subsequent frame -- including the
        // heartbeat `pong` -- and stall the link. Logging-and-dropping
        // mirrors the `signal.deliver` / `drain.deliver` arms.
        let routed = false;
        if (mailInboundRouter !== undefined) {
          try {
            routed = mailInboundRouter.tryRoute(frame.agentAddress, rawBytes);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.warn`mail.inbound router threw for ${frame.agentAddress}: ${msg}`;
          }
        }
        if (routed) {
          break;
        }
        deliverLocalMail(frame.agentAddress, rawBytes);
        void sessions.commitInboundMail(frame.agentAddress, rawBytes);
        break;
      }
      case "agent.deploy":
        await handleAgentDeploy(frame);
        break;
      case "session.start":
        await handleSessionStart(frame);
        break;
      case "agent.undeploy":
        await handleAgentUndeploy(frame);
        break;
      case "challenge":
        handleChallenge(frame);
        break;
      case "pong":
        lastPongAt = Date.now();
        break;
      case "challenge.failed":
        await handleChallengeFailed(frame);
        break;
      case "session.abort":
        await handleSessionAbort(frame);
        break;
      case "grants.update":
        await handleGrantsUpdate(frame);
        break;
      case "sources.update":
        await handleSourcesUpdate(frame);
        break;
      case "repo.pack.push":
        handlePackPush(frame);
        break;
      case "repo.pack.done":
        await handlePackDone(frame);
        break;
      case "sync.request":
        void handleSyncRequest(frame);
        break;
      case "signal.deliver":
        await handleSignalDeliver(frame);
        break;
      case "drain.deliver":
        await handleDrainDeliver(frame);
        break;
      case "repo.pack.ack":
        handlePackAck(frame);
        break;
      case "repo.pack.reject":
        handlePackReject(frame);
        break;
      default:
        logger.warn`Unknown frame type from hub: ${(frame as { type: string }).type}`;
    }
  }

  function deliverLocalMail(agentAddress: string, message: Uint8Array): void {
    try {
      transport.deliver(agentAddress, message);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.warn`Failed to deliver inbound mail to ${agentAddress}: ${msg}`;
    }
  }

  function connect(): void {
    // Reconnect cancellation in close() is the load-bearing protection
    // against post-close reconnect attempts. A caller invoking connect()
    // after close() is a misuse, not a recoverable state — fail loudly.
    if (closed) {
      throw new Error("HubLink.connect called after close");
    }

    ws = new WebSocket(hubURL);

    ws.addEventListener("open", () => {
      logger.info`Connected to hub at ${hubURL}`;

      lastPongAt = Date.now();
      pingTimer = setInterval(() => {
        if (Date.now() - lastPongAt >= pingIntervalMs * 2) {
          logger.warn`Hub pong timeout, closing connection`;
          if (pingTimer !== null) {
            clearInterval(pingTimer);
            pingTimer = null;
          }
          ws?.close();
          return;
        }
        send({ type: "ping" });
      }, pingIntervalMs);

      packReceiver.reset();
      packSender.cancelAll("Connection lost");

      void (async () => {
        try {
          const { restored, failed } = await sessions.restoreSessions();

          if (restored.length > 0) {
            const deployRefs: Record<string, string> = {};
            for (const entry of restored) {
              // SessionManager.restoreSessions populated AgentKeyStore's
              // in-memory keypair cache via scanKeys. Replay the
              // hub-side pairing record here so verifyDeployCommit can
              // accept incoming packs without re-running an agent.deploy.
              if (entry.hubPublicKey !== undefined) {
                keyStore.recordHubKey(entry.address, entry.hubPublicKey);
              }
              const ref = await sessions.getDeployRef(entry.address);
              if (ref !== null) {
                deployRefs[entry.address] = ref;
              }
            }
            send({
              type: "reconnect",
              sidecarId,
              token,
              agentAddresses: restored.map((e) => e.address),
              deployRefs,
            });
            if (failed.length > 0) {
              logger.warn`Reconnected ${String(restored.length)} agent(s), ${String(failed.length)} failed to restore`;
            } else {
              logger.info`Sent reconnect with ${String(restored.length)} agent(s)`;
            }
          } else {
            send({
              type: "register",
              sidecarId,
              token,
              agentAddresses: sessions.getAddresses(),
            });
          }

          flush();
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.error`Session restore failed, closing connection: ${msg}`;
          ws?.close();
        }
      })();
    });

    ws.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        // Attach a tail `.catch` to the chained handler so any
        // unhandled throw inside `handleMessage` is observed and
        // surfaces as a logged warning rather than rejecting the
        // shared `messageQueue` chain. A rejected chain wedges every
        // subsequent `messageQueue.then(...)` -- including the
        // heartbeat `pong` path -- and silently stalls the link.
        // Per-arm guards (mail/signal/drain) are the primary defence;
        // this catch is the belt-and-braces guarantee that no future
        // unguarded arm can wedge the link.
        const data = event.data;
        messageQueue = messageQueue.then(() =>
          handleMessage(data).catch((err: unknown) => {
            const msg = err instanceof Error ? err.message : String(err);
            logger.warn`Unhandled error in handleMessage: ${msg}`;
          }),
        );
      }
    });

    ws.addEventListener("close", () => {
      logger.info`Disconnected from hub`;
      ws = null;
      if (pingTimer !== null) {
        clearInterval(pingTimer);
        pingTimer = null;
      }
      if (!closed) {
        cancelReconnect = scheduleReconnect(() => {
          cancelReconnect = null;
          // Defense in depth for fake or misbehaving schedulers whose
          // cancel function is a no-op: re-check `closed` before
          // re-entering connect() so a fired-but-not-yet-executed
          // callback after close() does not propagate the
          // "called after close" throw out of the scheduler.
          if (closed) return;
          connect();
        }, reconnectDelayMs);
      }
    });

    ws.addEventListener("error", (event) => {
      logger.warn`WebSocket error: ${String(event)}`;
    });
  }

  function close(): void {
    closed = true;
    if (cancelReconnect !== null) {
      cancelReconnect();
      cancelReconnect = null;
    }
    if (pingTimer !== null) {
      clearInterval(pingTimer);
      pingTimer = null;
    }
    if (ws !== null) {
      ws.close();
      ws = null;
    }
  }

  const sendEvent: SessionEventSink = (agentAddress, sessionId, event) => {
    send({
      type: "agent.event",
      agentAddress,
      sessionId,
      event,
    });
  };

  const sendConnectorState: ConnectorStateSink = (
    agentAddress,
    connectorState,
  ) => {
    send({
      type: "connector.state.changed",
      agentAddress,
      connectorState,
    });
  };

  const sendDeployApplyError: HubLink["sendDeployApplyError"] = (
    agentAddress,
    payload,
  ) => {
    send({
      type: "deploy.apply.error",
      agentAddress,
      ...payload,
    });
  };

  return {
    connect,
    close,
    sendEvent,
    sendConnectorState,
    sendDeployApplyError,
    pushWorkflowRunPack,
  };
}
