// HubLink: the sidecar-side WebSocket protocol. Connects to the hub, sends
// the register frame, forwards outbound mail and inference events, and
// handles inbound agent lifecycle commands.

import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import { type } from "arktype";
import {
  HubFrame,
  MAX_MAIL_OUTBOUND_BODY_BYTES,
  type SidecarFrame,
  type RegisterFrame,
  type ReconnectFrame,
  type AgentDeployFrame,
  type AgentErrorFrame,
  type SessionErrorFrame,
  type AgentUndeployFrame,
  type WorkflowControlFrame,
  type WorkflowRunRefTips,
  type PackPushFrame,
  type PackDoneFrame,
  type PackAckFrame,
  type PackRejectFrame,
  type PackRejectReason,
  RepoId,
  type SignalDeliverFrame,
  type RunGrantsFrame,
  type SenderKeyRefreshFrame,
  type SenderKeyEvictFrame,
  type SignalCorrelationRegisterFrame,
  type SignalCorrelationRegisterAckFrame,
  type DrainDeliverFrame,
  type SourcesUpdateFrame,
  type CredentialsUpdateFrame,
  type SyncRequestFrame,
  type WorkflowProbeRequestFrame,
  type WorkflowProbeResultFrame,
} from "@intx/types/sidecar";
import type { SignalKind } from "@intx/types";
import { createPackReceiver, createPackSender } from "@intx/pack-transport";
import {
  createRegisterAcker,
  DEFAULT_REGISTER_ACK_MAX_ATTEMPTS,
  DEFAULT_REGISTER_ACK_TIMEOUT_MS,
} from "./register-acker";
import {
  verifyInboundSignature,
  outcomeForVerdict,
  decideInboundAdmission,
  type ResolvedInboundMailPolicy,
} from "./inbound-signature";
import { base64Decode, base64Encode } from "@intx/types";
import type {
  ApprovalSnapshot,
  CryptoProvider,
  InferenceEvent,
} from "@intx/types/runtime";

import type { AgentKeyStore } from "../agent-key-store";
import type { SessionManager } from "../session-manager";

/** Forwards a spawned child's verified InferenceEvents to the hub timeline. */
export type SessionEventSink = (
  agentAddress: string,
  sessionId: string,
  event: InferenceEvent,
) => void;

const logger = getLogger(["interchange", "hub-agent", "ws"]);

/**
 * Permissive envelope over a raw inbound frame that failed `HubFrame`
 * validation. The malformation is usually in a nested field, so the
 * top-level discriminator and correlation key survive to answer the
 * requester.
 */
const MalformedRequestEnvelope = type({
  "type?": "string",
  "requestId?": "string",
  "agentAddress?": "string",
  "transferId?": "string",
  // Validated only inside the pack branch below; validating here would sink
  // a non-pack frame that happens to carry a malformed `repoId`-shaped field.
  "repoId?": "unknown",
});

/**
 * Request/ack frames the hub correlates by `requestId` and answers with
 * `session.error`. Request-shaped frames in none of the three sets have no
 * requester to answer and are dropped.
 */
const SESSION_ERROR_REQUEST_TYPES: ReadonlySet<string> = new Set([
  "sources.update",
  "credentials.update",
]);

/** Request/ack frames the hub correlates by `agentAddress` and answers with `agent.error`. */
const AGENT_ERROR_REQUEST_TYPES: ReadonlySet<string> = new Set([
  "agent.deploy",
  "agent.undeploy",
]);

/**
 * Chunked-pack request frames the hub correlates by `transferId` and
 * answers with `repo.pack.reject`.
 */
const PACK_REJECT_REQUEST_TYPES: ReadonlySet<string> = new Set([
  "repo.pack.push",
  "repo.pack.done",
]);

/**
 * Classify an `applyAssetPack` failure message into a `repo.pack.reject`
 * reason. A structural rejection (symlink, submodule, escaping mountPath) is
 * `path_violation`; bad or incomplete bytes are `corrupt`. A wording drift
 * only reverts to `corrupt`, never misclassifies bytes as a path issue.
 */
export function classifyAssetPackRejectReason(msg: string): PackRejectReason {
  if (msg.startsWith("sha_mismatch")) return "sha_mismatch";
  if (
    msg.startsWith("signature_invalid") ||
    msg.startsWith("signature_unsigned")
  ) {
    return "signature_invalid";
  }
  if (/symlink at |submodule reference at |mountPath|escaping path/.test(msg)) {
    return "path_violation";
  }
  return "corrupt";
}

export function answerMalformedRequestFrame(
  raw: unknown,
  summary: string,
  send: (frame: SessionErrorFrame | AgentErrorFrame | PackRejectFrame) => void,
): boolean {
  const envelope = MalformedRequestEnvelope(raw);
  if (envelope instanceof type.errors) return false;
  const frameType = envelope.type;
  if (frameType === undefined) return false;
  if (
    SESSION_ERROR_REQUEST_TYPES.has(frameType) &&
    envelope.requestId !== undefined &&
    envelope.requestId.length > 0
  ) {
    send({
      type: "session.error",
      requestId: envelope.requestId,
      error: `malformed ${frameType} frame: ${summary}`,
    });
    return true;
  }
  if (
    AGENT_ERROR_REQUEST_TYPES.has(frameType) &&
    envelope.agentAddress !== undefined &&
    envelope.agentAddress.length > 0
  ) {
    send({
      type: "agent.error",
      agentAddress: envelope.agentAddress,
      error: `malformed ${frameType} frame: ${summary}`,
    });
    return true;
  }
  if (
    PACK_REJECT_REQUEST_TYPES.has(frameType) &&
    envelope.transferId !== undefined &&
    envelope.transferId.length > 0 &&
    envelope.agentAddress !== undefined &&
    envelope.agentAddress.length > 0
  ) {
    // A valid repo.pack.reject carries the structured repoId, recovered here
    // (kept out of the shared envelope). When it is malformed the frame is
    // dropped; "corrupt" is the reason for a frame that failed to parse.
    const repoId = RepoId(envelope.repoId);
    if (repoId instanceof type.errors) return false;
    send({
      type: "repo.pack.reject",
      agentAddress: envelope.agentAddress,
      repoId,
      transferId: envelope.transferId,
      reason: "corrupt",
    });
    return true;
  }
  return false;
}

const DEFAULT_PING_INTERVAL_MS = 30_000;
const DEFAULT_RECONNECT_DELAY_MS = 3_000;

/**
 * The reason `packSender.cancelAll` rejects an in-flight transfer on a
 * reconnect. A push failing with this is a dropped connection, not a
 * receiver-side rejection, so `runWithBootstrap` must not fast-retry it.
 */
const CONNECTION_LOST_REASON = "Connection lost";

function isConnectionLost(err: unknown): boolean {
  return err instanceof Error && err.message === CONNECTION_LOST_REASON;
}

/**
 * Schedules a deferred callback and returns a cancel function. Injection
 * point for tests to observe cancellation without wall-clock waits.
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
 * Result the deploy router returns once a deploy has staged; the link folds
 * it into the outbound `agent.deploy.ack` frame.
 */
export type DeployRouterResult = {
  /** Hex-encoded agent public key the hub records for verification. */
  publicKey: string;
};

/** Routes every inbound `agent.deploy` frame through the workflow-run substrate. */
export interface DeployRouter {
  deploy(frame: AgentDeployFrame): Promise<DeployRouterResult>;
  /** Release the per-deployment registrations the deploy path installed. Optional for test routers. */
  undeploy?: (frame: AgentUndeployFrame) => Promise<void>;
  /** Cancel a workflow or stop its process while retaining local inspection state. */
  control?: (frame: WorkflowControlFrame) => Promise<WorkflowControlOutcome>;
}

/** What a handled workflow control reports back to the Hub. */
export type WorkflowControlOutcome = {
  /** A stopped worker's ref tips, which the Hub must hold to confirm the stop. */
  refTips?: WorkflowRunRefTips;
};

/** Registry for inbound `mail.inbound` frames; an unhandled address drops the mail. */
export interface MailInboundRouter {
  /**
   * Route `message` to the handler for `agentAddress`, or `null` when none is
   * registered. Returns the durable settlement; the link acks only on resolution.
   */
  tryRoute(agentAddress: string, message: Uint8Array): Promise<void> | null;
}

/** Registry for inbound `signal.deliver` frames; unhandled frames are logged and dropped. */
export interface SignalInboundRouter {
  /** Route `frame`; true when a handler accepted it, false when none is registered. */
  tryRoute(frame: SignalDeliverFrame): Promise<boolean>;
}

/** Registry for inbound `drain.deliver` frames; unhandled frames are logged and dropped. */
export interface DrainInboundRouter {
  /** Route `frame`; true when a handler accepted it, false when none is registered. */
  tryRoute(frame: DrainDeliverFrame): Promise<boolean>;
}

/** Registry for inbound `run.grants` frames; unhandled frames are logged and dropped. */
export interface GrantsInboundRouter {
  /** Route `frame`; true when a handler accepted it, false when none is registered. */
  tryRoute(frame: RunGrantsFrame): Promise<boolean>;
}

/** Registry for inbound `sources.update` frames. Request/ack: the link answers `session.ack`/`session.error`, so the hub never hangs. */
export interface SourcesInboundRouter {
  /** Route `frame`; true when accepted, false when none registered. Rejection becomes `session.error`. */
  tryRoute(frame: SourcesUpdateFrame): Promise<boolean>;
}

/** Registry for inbound `credentials.update` frames. Request/ack: the link answers `session.ack`/`session.error`, so the hub never hangs. */
export interface CredentialsInboundRouter {
  /** Route `frame`; true when accepted, false when none registered. Rejection becomes `session.error`. */
  tryRoute(frame: CredentialsUpdateFrame): Promise<boolean>;
}

/**
 * Applies a Hub-authoritative workflow-run ref before a replacement
 * supervisor is allowed to spawn. The websocket layer delegates the update
 * through this boundary.
 */
export type WorkflowRunPackApplier = (args: {
  agentAddress: string;
  repoId: RepoId;
  pack: Uint8Array;
  ref: string;
  commitSha: string;
}) => Promise<void>;

/**
 * The inert answer a probe execution produces, lifted off the
 * `workflow.probe.result` frame: the projection, its derived grant set, the
 * grant-walk snapshot, and the projection's content hash.
 */
export type WorkflowProbeResult = Pick<
  WorkflowProbeResultFrame,
  "projection" | "grants" | "grantWalkSnapshot" | "wireHash"
>;

/**
 * Seam the link routes every inbound `workflow.probe.request` through.
 * Production evaluates the frozen closure in a one-shot child and returns the
 * inert needs surface plus the derived grant set and content hash. A throw
 * becomes a `workflow.probe.error` reply so the hub's probe never hangs.
 */
export interface WorkflowProbeExecutor {
  probe(frame: WorkflowProbeRequestFrame): Promise<WorkflowProbeResult>;
}

/** Placeholder probe executor wired when no real one is supplied; rejects so a probe is answered. */
const defaultWorkflowProbeExecutor: WorkflowProbeExecutor = {
  probe() {
    return Promise.reject(
      new Error("workflow probe execution is not implemented on this sidecar"),
    );
  },
};

export type HubLinkConfig = {
  hubURL: string;
  sidecarId: string;
  token: string;
  transport: HubTransport;
  sessions: SessionManager;
  /**
   * Key custody and per-frame crypto: deploy-commit verification, hub-key
   * recording, and per-agent forgetting.
   */
  keyStore: AgentKeyStore;
  /**
   * Resolves a sender address to the crypto whose public key verifies that
   * sender's inbound mail, or `undefined` when no key is known.
   */
  resolveSenderCrypto: (address: string) => CryptoProvider | undefined;
  /**
   * Resolves a recipient deployment address to the TOTAL inbound-mail
   * admission policy the `mail.inbound` seam enforces for it. An address the
   * registry does not hold resolves to a fully-closed policy that rejects
   * every outcome, so the seam adds no fallback.
   */
  lookupInboundMailPolicy: (address: string) => ResolvedInboundMailPolicy;
  /**
   * Persists the hub-vouched public key for a sender address, overwriting any
   * previously cached key.
   */
  cacheSenderKey: (address: string, publicKey: string) => Promise<void>;
  /**
   * Durably removes the cached key for a sender address, on an inbound
   * `sender.key.evict` frame (the hub re-resolved a reported cached sender to
   * no durable key).
   */
  evictSenderKey: (address: string) => Promise<void>;
  /** Routes every inbound `agent.deploy` frame. */
  deployRouter: DeployRouter;
  /**
   * Optional inbound mail dispatcher; absent or a false return means no
   * handler claims the mail, so the link logs and drops it.
   */
  mailInboundRouter?: MailInboundRouter;
  /** Optional inbound signal dispatcher; absent or false means log-and-drop. */
  signalInboundRouter?: SignalInboundRouter;
  /** Optional inbound drain dispatcher; absent or false means log-and-drop. */
  drainInboundRouter?: DrainInboundRouter;
  /** Optional inbound grants dispatcher; absent or false means log-and-drop. */
  grantsInboundRouter?: GrantsInboundRouter;
  /**
   * Optional inbound sources-rotation dispatcher; answers `session.ack` on
   * acceptance and `session.error` otherwise, including when absent.
   */
  sourcesInboundRouter?: SourcesInboundRouter;
  /**
   * Optional inbound credential-delivery dispatcher; answers `session.ack` on
   * acceptance and `session.error` otherwise, including when absent.
   */
  credentialsInboundRouter?: CredentialsInboundRouter;
  /**
   * Restore boundary for Hub→sidecar workflow-run packs. Optional for hosts
   * that never accept provisioned workflow allocations; receiving such a pack
   * without an applier fails closed with `repo.pack.reject`.
   */
  applyWorkflowRunPack?: WorkflowRunPackApplier;
  /**
   * Optional workflow-probe executor. When present, the link routes every
   * inbound `workflow.probe.request` frame through it and answers
   * `workflow.probe.result` on success, `workflow.probe.error` when it
   * throws. Absent, a placeholder executor always throws, so a probe still
   * gets answered.
   */
  workflowProbeExecutor?: WorkflowProbeExecutor;
  /**
   * Returns the workflow-substrate deployment addresses this sidecar
   * currently hosts a live supervisor for. Called on every (re)connect to
   * announce them to the Hub in the reconnect frame; without this the Hub
   * drops the deployment's route on a WS reconnect. Defaults to none when
   * omitted.
   */
  getWorkflowAddresses?: () => string[];
  /**
   * Returns the rotatable (non-run) sender addresses this sidecar holds cached
   * keys for. Reported on each (re)connect frame so the Hub re-resolves and
   * re-pushes each key, catching a rotation that landed while the sidecar was
   * disconnected.
   */
  getCachedSenderAddresses?: () => string[];
  /**
   * Invoked after the allocation-authenticated reconnect frame is written, so
   * the workflow-run pack pusher can safely re-drive a cancelled push.
   */
  onWorkflowAddressesRoutable?: (addresses: string[]) => void;
  /**
   * Invoked on WS disconnect with the workflow-substrate addresses this link
   * hosts. Their Hub route is gone until the next reconnect, so the
   * workflow-run pack pusher blocks their pushes in the interim. Paired with
   * `onWorkflowAddressesRoutable`.
   */
  onWorkflowAddressesUnroutable?: (addresses: string[]) => void;
  pingIntervalMs?: number;
  reconnectDelayMs?: number;
  /** Per-attempt watchdog before re-sending an unacked correlation register. */
  registerAckTimeoutMs?: number;
  /** Total register sends (initial + retries) before giving up. */
  registerAckMaxAttempts?: number;
  scheduleReconnect?: ReconnectScheduler;
};

export type HubLink = {
  /** Open the connection. Must not be called after `close()`. */
  connect(): void;
  close(): void;
  sendEvent: SessionEventSink;
  /**
   * Register a control-plane suspension with the hub: sends a
   * `signal.correlation.register` frame so the hub co-writes the parked run's
   * routing + approval rows. Fire-and-forget; queues while disconnected.
   */
  sendSignalCorrelationRegister: (registration: {
    correlationId: string;
    runId: string;
    anchorRunId: string;
    agentAddress: string;
    kind: SignalKind;
    approvalSnapshot?: ApprovalSnapshot;
  }) => void;
  /**
   * Ship a workflow-run pack to the hub. Streams the pack as `repo.pack.push`
   * chunks followed by a `repo.pack.done`, then resolves on the matching
   * `repo.pack.ack` (rejects on `repo.pack.reject` with the carried reason).
   */
  pushWorkflowRunPack: (opts: {
    agentAddress: string;
    repoId: RepoId;
    pack: Uint8Array;
    ref: string;
    commitSha: string;
  }) => Promise<void>;
};

const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

/**
 * Warning when `hubURL` is cleartext `ws:` against a non-loopback host: the
 * hub-sidecar socket carries `agent.deploy` frames whose payload holds
 * decrypted inference API keys and credential material, which cross the
 * network unencrypted over such a socket. Other `127.0.0.0/8` addresses fall
 * through to the warning: over-warning on a local address is safe, whereas
 * missing a remote one is not. Returns `null` when the transport is safe.
 */
export function cleartextTransportWarning(hubURL: string): string | null {
  const { protocol, hostname } = new URL(hubURL);
  if (protocol !== "ws:") return null;
  if (LOOPBACK_HOSTS.has(hostname)) return null;
  return `Hub URL ${hubURL} uses cleartext ws://; deploy credentials cross the network unencrypted. Use wss:// with TLS for any non-loopback hub.`;
}

export function createHubLink(config: HubLinkConfig): HubLink {
  const {
    hubURL,
    sidecarId,
    token,
    transport,
    sessions,
    keyStore,
    resolveSenderCrypto,
    lookupInboundMailPolicy,
    cacheSenderKey,
    evictSenderKey,
    deployRouter,
    mailInboundRouter,
    signalInboundRouter,
    drainInboundRouter,
    grantsInboundRouter,
    sourcesInboundRouter,
    credentialsInboundRouter,
    applyWorkflowRunPack,
    workflowProbeExecutor = defaultWorkflowProbeExecutor,
    getWorkflowAddresses = () => [],
    getCachedSenderAddresses = () => [],
    onWorkflowAddressesRoutable,
    onWorkflowAddressesUnroutable,
    pingIntervalMs = DEFAULT_PING_INTERVAL_MS,
    reconnectDelayMs = DEFAULT_RECONNECT_DELAY_MS,
    registerAckTimeoutMs = DEFAULT_REGISTER_ACK_TIMEOUT_MS,
    registerAckMaxAttempts = DEFAULT_REGISTER_ACK_MAX_ATTEMPTS,
    scheduleReconnect = defaultScheduleReconnect,
  } = config;

  const cleartextWarning = cleartextTransportWarning(hubURL);
  if (cleartextWarning !== null) {
    logger.warn`${cleartextWarning}`;
  }

  let ws: WebSocket | null = null;
  let closed = false;
  let pingTimer: ReturnType<typeof setInterval> | null = null;
  let cancelReconnect: (() => void) | null = null;
  let lastPongAt = 0;
  // An OPEN socket is not application-ready until its one authoritative
  // register/reconnect frame is on the wire, so ordinary outbound traffic
  // stays in the bounded queue until the handshake has been sent.
  let handshakePending = true;

  const packReceiver = createPackReceiver();
  // One sender owns both push paths (agent-state and workflow-run), with
  // transferIds in disjoint namespaces, so a single pending-id map is
  // unambiguous.
  const packSender = createPackSender({ sendFrame: (frame) => send(frame) });

  // Retry `signal.correlation.register` until the hub acks it. A register is
  // fire-and-forget on the wire and can be lost on an open socket or evicted
  // from the bounded queue; the acker re-sends on a tight watchdog while the
  // link is open and gives up on disconnect, leaving the reconnect re-emit as
  // the backstop.
  const registerAcker = createRegisterAcker({
    sendFrame: (frame) => send(frame),
    isOpen: () => ws !== null && ws.readyState === WebSocket.OPEN,
    timeoutMs: registerAckTimeoutMs,
    maxAttempts: registerAckMaxAttempts,
  });

  // Serialize frame processing so async handlers (deploy, undeploy, abort)
  // cannot race against each other.
  let messageQueue: Promise<void> = Promise.resolve();

  // Outbound frames queued while disconnected.
  const MAX_QUEUE = 1024;
  const queue: SidecarFrame[] = [];

  function send(frame: SidecarFrame): void {
    if (
      ws !== null &&
      ws.readyState === WebSocket.OPEN &&
      (!handshakePending || frame.type === "ping")
    ) {
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
      ws.readyState === WebSocket.OPEN &&
      !handshakePending
    ) {
      ws.send(JSON.stringify(queue.shift()));
    }
  }

  function sendOnConnection(
    connection: WebSocket,
    frame: SidecarFrame,
  ): boolean {
    if (ws !== connection || connection.readyState !== WebSocket.OPEN) {
      return false;
    }
    connection.send(JSON.stringify(frame));
    return true;
  }

  /**
   * Send the initial handshake only if `connection` is still the active
   * socket: deploy-ref collection is asynchronous, and this fence keeps a late
   * completion from sending onto a newer reconnect attempt.
   */
  function completeHandshake(
    connection: WebSocket,
    frame: RegisterFrame | ReconnectFrame,
  ): void {
    if (!sendOnConnection(connection, frame)) return;
    handshakePending = false;
    flush();
    if (
      frame.type === "reconnect" &&
      frame.agentAddresses.length > 0 &&
      onWorkflowAddressesRoutable !== undefined
    ) {
      // The reconnect frame is processed ahead of later frames on the Hub's
      // per-socket queue, so pushes triggered here cannot overtake the route
      // restoration.
      onWorkflowAddressesRoutable(frame.agentAddresses);
    }
  }

  // Wire the transport's remote send handler to push mail.outbound frames
  // for routing. The sender comes from the transport's registered-address
  // check so the hub can bind the claim to this authenticated connection.
  transport.setRemoteSendHandler(
    async (rawMessage, recipients, senderAddress) => {
      // Pre-check the base64 payload length arithmetically (4 output
      // characters per 3 input bytes, padded up) rather than materializing
      // the encoding: an over-cap send is rejected before the encode
      // allocates the ~59MB base64 string a 44MB body would produce.
      const bodyBytes = Math.ceil(rawMessage.byteLength / 3) * 4;
      // Fail loud at the source: a throw surfaces to the producing agent as a
      // real error rather than a silent hub-side drop. The hub re-enforces
      // the cap on receive -- the authoritative backstop.
      if (bodyBytes > MAX_MAIL_OUTBOUND_BODY_BYTES) {
        throw new Error(
          `refusing to send mail.outbound from ${senderAddress}: rawMessage of ${String(bodyBytes)} bytes exceeds the ${String(MAX_MAIL_OUTBOUND_BODY_BYTES)}-byte cap`,
        );
      }
      const encoded = base64Encode(rawMessage);
      send({
        type: "mail.outbound",
        rawMessage: encoded,
        recipients,
        senderAddress,
      });
    },
  );

  // Forward every send to the hub for audit and event emission. Local-only
  // sends are marked delivered: true so the hub does not re-route them.
  transport.addMessageSentHandler(async (ctx) => {
    // Same arithmetic pre-check as the remote-send handler: skip an over-cap
    // frame before the encode allocates the ~59MB base64 string.
    const bodyBytes = Math.ceil(ctx.rawMessage.byteLength / 3) * 4;
    // Post-delivery audit/projection forward: the mail already went out
    // locally, so there is nothing left to fail. A throw here is swallowed by
    // the transport's allSettled, so an over-cap frame is logged and skipped.
    if (bodyBytes > MAX_MAIL_OUTBOUND_BODY_BYTES) {
      logger.error`Skipping delivered mail.outbound audit frame from ${ctx.senderAddress}: rawMessage of ${String(bodyBytes)} bytes exceeds the ${String(MAX_MAIL_OUTBOUND_BODY_BYTES)}-byte cap`;
      return;
    }
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
      // The deploy router stages the deploy and returns the public key the
      // link folds into the outbound ack; the link does not re-decide.
      const result = await deployRouter.deploy(frame);
      send({
        type: "agent.deploy.ack",
        agentAddress: frame.agentAddress,
        publicKey: result.publicKey,
      });
      logger.info`Deployed agent ${frame.agentAddress}`;
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

    // Release per-deployment routing state the deploy router installed for
    // this address before the session tears down, so any in-flight
    // `signal.deliver` / `drain.deliver` / `mail.inbound` frame that lands
    // during teardown is rejected rather than dispatched into a
    // soon-to-be-orphaned supervisor handler. Test stubs omit the hook; an
    // absent hook means there was nothing to release.
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
    // `(kind, id, ref)` triple re-runs the bootstrap-retry arm. Without the
    // prune the flag survives across the deployment's lifetime, grows
    // unbounded, and a hub-side reset surfaces as a `non_fast_forward` on the
    // first post-reset push.
    const bootstrapped = workflowRunPackBootstrappedByAddress.get(
      frame.agentAddress,
    );
    if (bootstrapped !== undefined) {
      for (const key of bootstrapped) {
        workflowRunPackBootstrapped.delete(key);
      }
      workflowRunPackBootstrappedByAddress.delete(frame.agentAddress);
    }

    // Best-effort state push to the hub before deleting the directory.
    // `statePushed` reflects whether we sent the pack frames, not whether the
    // hub acknowledged them: we skip waiting for `repo.pack.ack` so an
    // undeploy never blocks on a round-trip that may never complete if the
    // hub is shutting down; the pending promise's rejection on disconnect is
    // intentionally swallowed below.
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
          // Best-effort push; see above.
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
      if (frame.repoId.kind === "workflow-run") {
        if (frame.mountPath !== undefined) {
          throw new Error(
            "workflow_run_restore_invalid: workflow-run packs cannot carry mountPath",
          );
        }
        if (applyWorkflowRunPack === undefined) {
          throw new Error(
            "workflow_run_restore_unconfigured: no workflow-run pack applier is configured",
          );
        }
        await applyWorkflowRunPack({
          agentAddress: frame.agentAddress,
          repoId: frame.repoId,
          pack: result.pack,
          ref: result.ref,
          commitSha: result.commitSha,
        });
      } else if (frame.mountPath !== undefined) {
        // Asset pack: route to the workspace materializer. Use
        // frame.agentAddress for destination routing -- frame.repoId.id
        // names the source asset at the hub, a different entity than the
        // destination agent.
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
      const reason = classifyAssetPackRejectReason(msg);
      logger.warn`Pack apply failed for ${frame.agentAddress}: ${msg}`;
      send({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason,
        detail: msg,
      });
    }
  }

  // Counter the boot edge consumes via `pushWorkflowRunPack` to mint
  // collision-free transferIds shared with undeploy and sync-request.
  let workflowRunPackCounter = 0;

  // Per-(repoId.id, ref) flag (at least one push accepted by the hub) plus a
  // per-(repoId.id, ref) serialization queue, closing the hub's
  // resolve-outside-lock race. First-push race: the hub's `initRepo` creates a
  // genesis commit on `refs/heads/main` inside the lock, so the first push's
  // CAS against a null baseline rejects with `non_fast_forward` (surfaced as
  // "corrupt"). Concurrent-push race: overlapping pushes both observe a stale
  // baseline and the second to acquire the lock rejects. The queue serializes
  // every push per `(repoId, ref)`; the flag bounds the bootstrap retry to the
  // FIRST push, so a genuine corruption surfaces verbatim afterwards.
  const workflowRunPackBootstrapped = new Set<string>();
  const workflowRunPackQueues = new Map<string, Promise<void>>();
  // Reverse index: agentAddress -> bootstrap keys recorded under that
  // address, so `handleAgentUndeploy` can prune them. Indexed by
  // `agentAddress` (not `anchorRunId`) because the link does not own the
  // address->anchorRunId derivation -- the sidecar's deploy router does, and
  // every workflow-run push carries the originating address explicitly.
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

  function handleSignalCorrelationRegisterAck(
    frame: SignalCorrelationRegisterAckFrame,
  ): void {
    // A no-match is normal: the retry may have already been acked, exhausted,
    // or abandoned on a disconnect. Log at debug, not warn.
    if (!registerAcker.handleAck(frame.correlationId)) {
      logger.debug`Received signal.correlation.register.ack for uncorrelated ${frame.correlationId}`;
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

  async function handleRunGrants(frame: RunGrantsFrame): Promise<void> {
    if (grantsInboundRouter === undefined) {
      logger.warn`Received run.grants for ${frame.agentAddress} but no grantsInboundRouter is wired; dropping`;
      return;
    }
    try {
      const routed = await grantsInboundRouter.tryRoute(frame);
      if (!routed) {
        logger.warn`run.grants for ${frame.agentAddress} did not match any registered deployment; dropping`;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error`run.grants write failed for ${frame.agentAddress}: ${msg}`;
    }
  }

  async function handleSenderKeyRefresh(
    frame: SenderKeyRefreshFrame,
  ): Promise<void> {
    // Address-keyed, so a fault has no run to poison: swallow it after logging
    // at ERROR -- there is no reply channel and the link must never wedge. A
    // transient cache-write fault leaves the STALE key cached until the next
    // reconnect re-pushes, so this push is best-effort. Awaited inline on the
    // message chain (not detached like the `mail.inbound` durable write): the
    // co-resident `run.grants` handler caches sender keys on this same chain,
    // so serializing keeps last-write-wins deterministic against it.
    try {
      await cacheSenderKey(frame.address, frame.publicKey);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error`sender.key.refresh cache write failed for ${frame.address}: ${msg}`;
    }
  }

  async function handleSenderKeyEvict(
    frame: SenderKeyEvictFrame,
  ): Promise<void> {
    // Same shape as `handleSenderKeyRefresh` for the evict direction:
    // address-keyed, no reply channel, awaited inline so it serializes against
    // a concurrent refresh/grants write. A fault is swallowed at ERROR, leaving
    // the STALE key cached until the next reconnect re-evicts -- best-effort.
    try {
      await evictSenderKey(frame.address);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error`sender.key.evict cache removal failed for ${frame.address}: ${msg}`;
    }
  }

  async function handleSourcesUpdate(frame: SourcesUpdateFrame): Promise<void> {
    // Request/ack: the hub awaits a reply within its request timeout, so every
    // path answers `session.ack` or `session.error` (unlike the fire-and-forget
    // signal/drain frames). A missing router still answers, or the hub hangs.
    if (sourcesInboundRouter === undefined) {
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: "no sourcesInboundRouter is wired",
      });
      return;
    }
    try {
      const routed = await sourcesInboundRouter.tryRoute(frame);
      if (routed) {
        send({ type: "session.ack", requestId: frame.requestId });
      } else {
        send({
          type: "session.error",
          requestId: frame.requestId,
          error: `no deployment registered for ${frame.agentAddress}`,
        });
      }
    } catch (err) {
      // A registered address whose rotation was rejected: an invalid list or
      // the supervisor's `deliverSources` throwing (e.g. a recycling phase).
      // The reason rides back verbatim so the hub sees why it failed.
      const msg = err instanceof Error ? err.message : String(err);
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: msg,
      });
    }
  }

  async function handleCredentialsUpdate(
    frame: CredentialsUpdateFrame,
  ): Promise<void> {
    // Request/ack, exactly like `sources.update` (see above).
    if (credentialsInboundRouter === undefined) {
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: "no credentialsInboundRouter is wired",
      });
      return;
    }
    try {
      const routed = await credentialsInboundRouter.tryRoute(frame);
      if (routed) {
        send({ type: "session.ack", requestId: frame.requestId });
      } else {
        send({
          type: "session.error",
          requestId: frame.requestId,
          error: `no deployment registered for ${frame.agentAddress}`,
        });
      }
    } catch (err) {
      // A registered address whose delivery was rejected: an invalid delivery
      // or the supervisor's `deliverCredentials` throwing (e.g. a recycling
      // phase). The reason rides back verbatim so the hub sees why it failed.
      const msg = err instanceof Error ? err.message : String(err);
      send({
        type: "session.error",
        requestId: frame.requestId,
        error: msg,
      });
    }
  }

  async function handleWorkflowProbeRequest(
    frame: WorkflowProbeRequestFrame,
  ): Promise<void> {
    // `workflow.probe.request` is request/response, so every path answers
    // `workflow.probe.result` or `workflow.probe.error` -- never a
    // log-and-drop. A throw (including the placeholder executor's
    // not-implemented throw) rides back as an error reply so the hub's probe
    // fails fast instead of hanging.
    try {
      const result = await workflowProbeExecutor.probe(frame);
      send({
        type: "workflow.probe.result",
        requestId: frame.requestId,
        projection: result.projection,
        grants: result.grants,
        grantWalkSnapshot: result.grantWalkSnapshot,
        wireHash: result.wireHash,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      send({
        type: "workflow.probe.error",
        requestId: frame.requestId,
        error: msg,
      });
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
        // A disconnect that cancelled the transfer is NOT the initRepo
        // bootstrap race: re-sending on a fresh, not-yet-registered
        // connection would ship to a hub that dropped this address's route.
        // Reconnect recovery is owned by the pushing store's post-reconnect
        // re-drive, so re-throw. Only the genuine bootstrap race -- a
        // receiver reject against an uninitialised hub repo -- retries here.
        if (isConnectionLost(first)) {
          throw first;
        }
        // First push to a never-bootstrapped (repoId, ref) lost the race
        // with the hub substrate's `receivePack` initRepo step (see the
        // comment on `workflowRunPackBootstrapped`). The hub has now
        // initialized the repo as a side effect of the failed push; the
        // retry uses the same pack but observes the bootstrap genesis as the
        // CAS baseline and lands.
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

    // Serialize pushes per (repoId, ref): the hub's `receiveWorkflowRunPack`
    // resolves the ref outside the substrate's per-repo lock, so overlapping
    // pushes would each observe a stale baseline and the second to acquire
    // the hub-side lock would reject with `non_fast_forward`. Chaining keeps
    // the receive ordering consistent end-to-end.
    const prior = workflowRunPackQueues.get(key) ?? Promise.resolve();
    const next = prior.catch(() => undefined).then(() => runWithBootstrap());
    workflowRunPackQueues.set(key, next);
    try {
      await next;
    } finally {
      // Drop the queue entry when the chain has settled and no follower has
      // appended, so a long-idle (repoId, ref) does not hold a dead promise
      // reference. A racing append replaces this entry before we get here.
      if (workflowRunPackQueues.get(key) === next) {
        workflowRunPackQueues.delete(key);
      }
    }
  }

  async function handleWorkflowControl(
    connection: WebSocket,
    frame: WorkflowControlFrame,
  ): Promise<void> {
    if (ws !== connection) return;
    let error: string | undefined;
    let refTips: WorkflowRunRefTips | undefined;
    try {
      if (deployRouter.control === undefined)
        throw new Error("Workflow control is not supported by this sidecar");
      ({ refTips } = await deployRouter.control(frame));
    } catch (cause) {
      error = cause instanceof Error ? cause.message : String(cause);
    }
    // A reply belongs to the connection that issued the request: a reconnect
    // retries durably and must not receive a late reply from its predecessor.
    sendOnConnection(connection, {
      type: "workflow.control.ack",
      requestId: frame.requestId,
      ...(error !== undefined ? { error } : {}),
      ...(refTips !== undefined ? { refTips } : {}),
    });
  }

  async function handleMessage(
    data: string,
    connection: WebSocket,
  ): Promise<void> {
    let raw: unknown;
    try {
      raw = JSON.parse(data) as unknown;
    } catch {
      logger.warn`Received unparseable frame from hub`;
      return;
    }
    const validated = HubFrame(raw);
    if (validated instanceof type.errors) {
      // A malformed request/ack frame must still be answered or the hub's
      // request hangs to its timeout: reply with the matching error frame
      // when a correlation key survives; otherwise log and drop.
      answerMalformedRequestFrame(raw, validated.summary, send);
      logger.warn`Invalid hub frame: ${validated.summary}`;
      return;
    }
    const frame = validated;

    switch (frame.type) {
      case "mail.inbound": {
        const rawBytes = base64Decode(frame.rawMessage);
        // The inbound-signature verify gates delivery here, awaited INLINE on
        // the messageQueue chain because the verdict decides admission. It is
        // CPU-bound (cache read, Ed25519 verify, MIME re-parse; no I/O, no
        // lock) so it cannot hang and never throws: a fault degrades to an
        // `error` verdict.
        const verdict = await verifyInboundSignature(
          {
            raw: rawBytes,
            authenticatedSender: frame.authenticatedSender,
            messageId: frame.messageId,
            agentAddress: frame.agentAddress,
          },
          resolveSenderCrypto,
        );
        // The recipient deployment's resolved admission policy; an address
        // with no registered deployment resolves to the fully-closed policy
        // and rejects every outcome.
        const policy = lookupInboundMailPolicy(frame.agentAddress);
        const admission = decideInboundAdmission(verdict, policy);
        if (admission.rejectedBy !== null) {
          logger.warn(
            "Rejecting inbound mail for {agentAddress}: {rejectedBy} is not admitted (findings {findings}, headline outcome {outcome}, authenticatedSender {authenticatedSender}, messageId {messageId})",
            {
              agentAddress: frame.agentAddress,
              rejectedBy: admission.rejectedBy,
              findings: admission.findings,
              outcome: outcomeForVerdict(verdict),
              authenticatedSender: frame.authenticatedSender,
              messageId: frame.messageId ?? null,
            },
          );
          // A rejected mail drops like the no-handler path: no ack, no reply
          // frame; the hub's redelivery machinery handles the rest.
          break;
        }
        // Admitted: the supervisor's handler delivers the bytes to its
        // mail-bus subscription, which the workflow-host's `awaitSignal`
        // listens on.
        //
        // Guard the router call with try/catch so a synchronous throw does not
        // reject this `handleMessage` promise and wedge the per-connection
        // `messageQueue` chain. The durable settlement is observed off the
        // chain (below).
        let durable: Promise<void> | null = null;
        if (mailInboundRouter !== undefined) {
          try {
            durable = mailInboundRouter.tryRoute(frame.agentAddress, rawBytes);
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.warn`mail.inbound router threw for ${frame.agentAddress}: ${msg}`;
          }
        }
        if (durable === null) {
          logger.warn`Dropping mail.inbound for ${frame.agentAddress}: no registered handler`;
          break;
        }
        // Ack durable receipt only AFTER the inbox write settles, and only
        // for hub-originated mail carrying a hub-minted messageId (the ack
        // handshake). Observe the settlement DETACHED from the `messageQueue`
        // chain so a slow or failing inbox write never wedges frame
        // processing; on rejection no ack is sent, so the hub redelivers.
        const ackMessageId = frame.messageId;
        if (ackMessageId !== undefined) {
          void durable
            .then(() => {
              send({
                type: "mail.inbound.ack",
                agentAddress: frame.agentAddress,
                messageId: ackMessageId,
              });
            })
            .catch((err: unknown) => {
              const msg = err instanceof Error ? err.message : String(err);
              logger.warn`Withholding mail.inbound.ack for ${frame.agentAddress} ${ackMessageId}; hub will redeliver: ${msg}`;
            });
        } else {
          // Relayed agent-to-agent mail carries no hub-minted messageId and
          // does not participate in the ack handshake. Still observe the
          // settlement so a rejection is logged, not left unhandled.
          void durable.catch((err: unknown) => {
            const msg = err instanceof Error ? err.message : String(err);
            logger.warn`Inbound mail delivery failed for ${frame.agentAddress}: ${msg}`;
          });
        }
        break;
      }
      case "agent.deploy":
        await handleAgentDeploy(frame);
        break;
      case "agent.undeploy":
        await handleAgentUndeploy(frame);
        break;
      case "workflow.control":
        // Start in FIFO order, but leave the queue free for forced stop and
        // heartbeats while cooperative cancellation waits for the child.
        void handleWorkflowControl(connection, frame).catch(
          (cause: unknown) => {
            logger.warn`Workflow control reply failed: ${cause instanceof Error ? cause.message : String(cause)}`;
          },
        );
        break;
      case "pong":
        lastPongAt = Date.now();
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
      case "run.grants":
        await handleRunGrants(frame);
        break;
      case "sender.key.refresh":
        await handleSenderKeyRefresh(frame);
        break;
      case "sender.key.evict":
        await handleSenderKeyEvict(frame);
        break;
      case "drain.deliver":
        await handleDrainDeliver(frame);
        break;
      case "sources.update":
        await handleSourcesUpdate(frame);
        break;
      case "credentials.update":
        await handleCredentialsUpdate(frame);
        break;
      case "workflow.probe.request":
        await handleWorkflowProbeRequest(frame);
        break;
      case "repo.pack.ack":
        handlePackAck(frame);
        break;
      case "repo.pack.reject":
        handlePackReject(frame);
        break;
      case "signal.correlation.register.ack":
        handleSignalCorrelationRegisterAck(frame);
        break;
      default:
        logger.warn`Unknown frame type from hub: ${(frame as { type: string }).type}`;
    }
  }

  function connect(): void {
    // Reconnect cancellation in close() is the load-bearing protection
    // against post-close reconnect attempts; calling connect() after close()
    // is misuse, not a recoverable state -- fail loudly.
    if (closed) {
      throw new Error("HubLink.connect called after close");
    }

    handshakePending = true;
    const connection = new WebSocket(hubURL);
    ws = connection;

    connection.addEventListener("open", () => {
      if (ws !== connection) {
        connection.close();
        return;
      }
      logger.info`Connected to hub at ${hubURL}`;

      lastPongAt = Date.now();
      pingTimer = setInterval(() => {
        if (Date.now() - lastPongAt >= pingIntervalMs * 2) {
          logger.warn`Hub pong timeout, closing connection`;
          if (pingTimer !== null) {
            clearInterval(pingTimer);
            pingTimer = null;
          }
          connection.close();
          return;
        }
        send({ type: "ping" });
      }, pingIntervalMs);

      packReceiver.reset();
      packSender.cancelAll(CONNECTION_LOST_REASON);
      // Abandon register retries armed against the prior connection: any
      // still-parked correlation is re-registered by the reconnect re-emit
      // once the handshake re-routes the addresses, and a stale retry onto
      // this fresh socket would land unrouted.
      registerAcker.cancelAll();

      // A fresh sidecar sends register; one that restored a deployment sends
      // reconnect instead. Sending an empty register before reconnect would
      // expose a false empty inventory and let allocation reconciliation
      // restore Hub state over the live workflow.
      const restoredAddresses = getWorkflowAddresses();
      // Report cached rotatable senders on both frames (register-vs-reconnect
      // turns on workflow-address presence). Omit the field when empty to
      // honor its additive-optional wire shape.
      const cachedSenderAddresses = getCachedSenderAddresses();
      const senderReport =
        cachedSenderAddresses.length > 0 ? { cachedSenderAddresses } : {};
      if (restoredAddresses.length === 0) {
        completeHandshake(connection, {
          type: "register",
          sidecarId,
          token,
          agentAddresses: [],
          ...senderReport,
        });
      } else {
        completeHandshake(connection, {
          type: "reconnect",
          sidecarId,
          token,
          agentAddresses: restoredAddresses,
          ...senderReport,
        });
      }
    });

    connection.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        // Attach a tail `.catch` to the chained handler so an unhandled throw
        // inside `handleMessage` surfaces as a logged warning rather than
        // rejecting the shared `messageQueue` chain, which would wedge every
        // subsequent frame -- including the heartbeat `pong` -- and silently
        // stall the link.
        //
        // Ordinary frame handlers run to completion before the next begins;
        // workflow.control starts here but owns its asynchronous completion.
        // A downstream invariant depends on that ordering: the workflow
        // source-rotation persist rolls back on failure assuming no second
        // rotation is in flight, which holds only because sources.update
        // frames are processed one at a time.
        const data = event.data;
        messageQueue = messageQueue.then(() =>
          handleMessage(data, connection).catch((err: unknown) => {
            const msg = err instanceof Error ? err.message : String(err);
            logger.warn`Unhandled error in handleMessage: ${msg}`;
          }),
        );
      }
    });

    connection.addEventListener("close", () => {
      // A late close from a superseded attempt must not null or reschedule
      // the active socket. Normal reconnects also pass this fence: the next
      // socket is not created until this handler schedules it.
      if (ws !== connection) return;
      logger.info`Disconnected from hub`;
      ws = null;
      handshakePending = true;
      if (pingTimer !== null) {
        clearInterval(pingTimer);
        pingTimer = null;
      }
      // Abandon in-flight register retries: the link is down, so recovery
      // belongs to the reconnect re-emit.
      registerAcker.cancelAll();
      // The hub dropped every route this link held. Block workflow-run
      // pushes for the hosted deployments until the authenticated reconnect
      // re-routes them, so the pusher does not re-ship onto a fresh,
      // not-yet-registered connection. `onWorkflowAddressesRoutable` lifts
      // the block and re-drives.
      if (onWorkflowAddressesUnroutable !== undefined) {
        const hosted = getWorkflowAddresses();
        if (hosted.length > 0) {
          onWorkflowAddressesUnroutable(hosted);
        }
      }
      if (!closed) {
        cancelReconnect = scheduleReconnect(() => {
          cancelReconnect = null;
          // Re-check `closed` before re-entering connect() so a fired-but-
          // not-yet-executed callback after close() does not propagate the
          // "called after close" throw out of the scheduler.
          if (closed) return;
          connect();
        }, reconnectDelayMs);
      }
    });

    connection.addEventListener("error", (event) => {
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
    registerAcker.cancelAll();
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

  const sendSignalCorrelationRegister: HubLink["sendSignalCorrelationRegister"] =
    (registration) => {
      // Every ask-rail suspension carries a snapshot; one without it is a
      // wiring defect, so fail loud rather than send a frame the receiver
      // would reject.
      if (registration.approvalSnapshot === undefined) {
        throw new Error(
          `signal.correlation.register built with no approval snapshot for ${registration.correlationId}; ask-rail suspensions always carry one`,
        );
      }
      const frame: SignalCorrelationRegisterFrame = {
        type: "signal.correlation.register",
        correlationId: registration.correlationId,
        runId: registration.runId,
        anchorRunId: registration.anchorRunId,
        agentAddress: registration.agentAddress,
        kind: registration.kind,
        snapshot: registration.approvalSnapshot,
      };
      // Send through the acker, which retries until the hub acks the co-write.
      registerAcker.send(frame);
    };

  return {
    connect,
    close,
    sendEvent,
    sendSignalCorrelationRegister,
    pushWorkflowRunPack,
  };
}
