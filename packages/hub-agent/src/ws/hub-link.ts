// HubLink: the sidecar-side WebSocket protocol.
//
// Connects to the hub, sends the hello frame, forwards outbound
// mail and inference events, and handles inbound agent lifecycle
// commands. Per-agent key material lives on AgentKeyStore; the link calls into
// the store for deploy-commit verification and hub-key bookkeeping. The wire layer itself never
// touches raw key bytes.

import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import { type } from "arktype";
import {
  fitDeploymentError,
  Generation,
  HubFrame,
  MAX_MAIL_OUTBOUND_BODY_BYTES,
  type SidecarFrame,
  type AgentDeployFrame,
  type AgentDeployErrorFrame,
  type AgentUndeployErrorFrame,
  type SessionErrorFrame,
  type AgentUndeployFrame,
  type DeploymentStoppedFrame,
  type DeploymentRetention,
  type WorkflowControlFrame,
  type WorkflowRunRefTips,
  type HostedIncarnation,
  type IncarnationStamp,
  type MailInboundFrame,
  type PackPushFrame,
  type PackDoneFrame,
  type PackAckFrame,
  type PackRejectFrame,
  type PackRejectReason,
  type SignalDeliverFrame,
  type RunGrantsFrame,
  type SenderKeyRefreshFrame,
  type SenderKeyEvictFrame,
  type SignalCorrelationRegisterFrame,
  type SignalCorrelationRegisterAckFrame,
  type DrainDeliverFrame,
  type SourcesUpdateFrame,
  type CredentialsUpdateFrame,
  type WelcomeFrame,
  type WorkflowProbeRequestFrame,
  type WorkflowProbeResultFrame,
} from "@intx/types/sidecar";
import { RepoId } from "@intx/types/repo";
import type { SignalKind } from "@intx/types";
import {
  createPackReceiver,
  createPackSender,
  PackRejectedError,
} from "@intx/pack-transport";
import { createFrameLanes } from "./frame-lanes";
import {
  createRegisterAcker,
  DEFAULT_REGISTER_ACK_MAX_ATTEMPTS,
  DEFAULT_REGISTER_ACK_TIMEOUT_MS,
} from "./register-acker";
import {
  verifyInboundSignature,
  outcomeForVerdict,
  decideInboundAdmission,
  type InboundSignatureVerdict,
  type ResolvedInboundMailPolicy,
} from "./inbound-signature";
import { base64Decode, base64Encode, isRunAddress } from "@intx/types";
import type {
  ApprovalSnapshot,
  CryptoProvider,
  InferenceEvent,
} from "@intx/types/runtime";

import type { AgentKeyStore } from "../agent-key-store";
import { isSenderPublicKeyHex } from "../sender-key-cache";
import type { SessionManager } from "../session-manager";

/**
 * Sink the link exposes for forwarding a spawned child's verified
 * InferenceEvents to the hub timeline, keyed by the deploy's session id and
 * stamped with the incarnation that produced them.
 */
export type SessionEventSink = (
  agentAddress: string,
  generation: number,
  sessionId: string,
  event: InferenceEvent,
) => void;

const logger = getLogger(["interchange", "hub-agent", "ws"]);

/**
 * Permissive envelope over a raw inbound frame that failed `HubFrame`
 * validation. A malformed request/ack frame usually still carries an
 * intact discriminator and correlation key -- the malformation is in a
 * nested field -- so these top-level fields can be recovered to answer the
 * requester.
 */
const MalformedRequestEnvelope = type({
  "type?": "string",
  "requestId?": "string",
  "agentAddress?": "string",
  "transferId?": "string",
  // `repoId` and `generation` are carried as `unknown` and validated only
  // inside the branches that answer with them. Validating them here would fail
  // the whole envelope for a frame whose malformation is in one of them,
  // sinking its recovery through its own correlation key.
  "repoId?": "unknown",
  "generation?": "unknown",
});

/**
 * Inbound request/ack frames the sidecar dispatches that the hub
 * correlates by `requestId`, whose failure reply is a `session.error`.
 * `sources.update` and `credentials.update` qualify -- both are answered with a
 * `session.error`. Frames answered through the other correlation keys live in
 * `LIFECYCLE_ERROR_TYPES` and `PACK_REJECT_REQUEST_TYPES`; a malformed frame
 * of any other type is dropped unanswered.
 */
const SESSION_ERROR_REQUEST_TYPES: ReadonlySet<string> = new Set([
  "sources.update",
  "credentials.update",
]);

/**
 * Lifecycle requests the hub correlates by `requestId`, each answered with
 * its own typed error, which also names the incarnation.
 */
const LIFECYCLE_ERROR_TYPES: ReadonlyMap<
  string,
  AgentDeployErrorFrame["type"] | AgentUndeployErrorFrame["type"]
> = new Map([
  ["agent.deploy", "agent.deploy.error"],
  ["agent.undeploy", "agent.undeploy.error"],
]);

/**
 * Inbound chunked-pack request frames the hub correlates by `transferId`
 * and whose failure reply is a `repo.pack.reject`. The hub tracks these in
 * its per-transfer pending map with the longest timeout of any request
 * frame.
 */
const PACK_REJECT_REQUEST_TYPES: ReadonlySet<string> = new Set([
  "repo.pack.push",
  "repo.pack.done",
]);

/**
 * Answer a malformed inbound request/ack control frame with an error reply
 * so the hub's request does not hang to its timeout. Two control-frame
 * families answer through their `requestId`: sources.update and
 * credentials.update reply `session.error`; agent.deploy and agent.undeploy
 * reply their typed error, which also needs the frame's address and
 * generation. The fire-and-forget frames
 * (mail/signal/drain/...) have no requester waiting on a reply, so a
 * malformed one is correctly left to be logged and dropped by the caller.
 *
 * The chunked `repo.pack` streaming transfers (repo.pack.push,
 * repo.pack.done) are the third family: correlated by `transferId`,
 * rejected by `repo.pack.reject`. A valid reject also carries the frame's
 * `agentAddress` and structured `repoId`, so it is answerable only when
 * all three survive the malformation; when `repoId` (or the transferId) is
 * itself unrecoverable the frame is left to be logged and dropped, because
 * a valid `repo.pack.reject` cannot be constructed without them.
 *
 * Returns `true` when it answered; `false` when no correlation key is
 * recoverable (an unknown/absent type, a fire-and-forget frame, or a
 * request/ack frame whose key is itself missing) -- the caller then logs
 * and drops, because there is nothing to answer.
 */
/**
 * Classify an `applyAssetPack` failure message into a `repo.pack.reject` reason.
 * A structural rejection -- a symlink or submodule the checkout cannot reproduce
 * faithfully, or a mountPath that escapes -- is a `path_violation`, distinct from
 * `corrupt` (bad or incomplete bytes), and a workflow-run seed that would move
 * history the sidecar already has is a `conflict`. The match is on the messages
 * `writeTreeToDisk`, the mountPath guard and the seed guard raise; a wording
 * drift only reverts the reason to `corrupt`, never misclassifies bytes as a
 * path issue. The raw message rides on the frame's `detail` regardless, so the
 * operator always sees the specific cause.
 */
export function classifyAssetPackRejectReason(msg: string): PackRejectReason {
  if (msg.startsWith("sha_mismatch")) return "sha_mismatch";
  if (msg.startsWith("workflow_run_restore_conflict")) return "conflict";
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
  send: (
    frame:
      | SessionErrorFrame
      | AgentDeployErrorFrame
      | AgentUndeployErrorFrame
      | PackRejectFrame,
  ) => void,
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
  const lifecycleError = LIFECYCLE_ERROR_TYPES.get(frameType);
  if (
    lifecycleError !== undefined &&
    envelope.requestId !== undefined &&
    envelope.requestId.length > 0 &&
    envelope.agentAddress !== undefined &&
    envelope.agentAddress.length > 0
  ) {
    const generation = Generation(envelope.generation);
    if (generation instanceof type.errors) return false;
    send({
      type: lifecycleError,
      requestId: envelope.requestId,
      agentAddress: envelope.agentAddress,
      generation,
      error: fitDeploymentError(`malformed ${frameType} frame: ${summary}`),
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
    // A valid repo.pack.reject carries the frame's structured repoId, so
    // recover it here (kept out of the shared envelope to protect the other
    // families). When the repoId is itself malformed there is no valid
    // reject to build, so the frame is left to be dropped. The hub
    // correlates the reject by transferId alone; "corrupt" is the reason
    // for a frame that failed to parse.
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
const DEFAULT_WELCOME_TIMEOUT_MS = 30_000;

const SENDER_KEYS = Symbol("sender keys");

/**
 * The reason a workflow-run push fails when no welcomed connection can carry
 * it: the one that carried it closed, or none is welcomed yet. It is not a
 * rejection by the Hub, so the push path does not fast-retry it (see
 * `runWithBootstrap`), and the pushing store keeps the push for the re-drive
 * on the next `welcome` instead of failing a write with it.
 */
const CONNECTION_LOST_REASON = "Connection lost";

/** The incarnation a queued report was sent for. */
function reportingIncarnation(
  frame: SidecarFrame,
): { address: string; generation: number } | undefined {
  switch (frame.type) {
    case "mail.outbound":
      return { address: frame.senderAddress, generation: frame.generation };
    case "signal.correlation.register":
      return { address: frame.agentAddress, generation: frame.generation };
    default:
      return undefined;
  }
}

/** Whether a push failed because no welcomed connection could carry it. */
export function isConnectionLost(err: unknown): boolean {
  return err instanceof Error && err.message === CONNECTION_LOST_REASON;
}

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
 * Result the deploy router returns to the link once a deploy has
 * staged. Carries the values the link folds into the outbound
 * `agent.deploy.ack` frame; the link itself stays out of the deploy
 * details.
 */
export type DeployRouterResult = {
  /** Hex-encoded agent public key the hub records for verification. */
  publicKey: string;
};

/**
 * Single-ingress deploy contract the link routes every `agent.deploy`
 * frame through. The sidecar's workflow-run deploy router is the
 * production implementation -- it stages every deploy through the
 * workflow-run substrate. The shape lives on hub-agent so the package
 * boundary stays one-way (`@intx/hub-agent` does not import
 * `@intx/workflow-host`).
 */
export interface DeployRouter {
  /**
   * Stage the deploy of one incarnation. The router owns what the sidecar
   * hosts, which the link reads through `getIncarnations`. An address is
   * deployed once, so the router refuses a deploy of an address it already
   * holds. A throw is answered with `agent.deploy.error`.
   */
  deploy(frame: AgentDeployFrame): Promise<DeployRouterResult>;
  /**
   * Symmetric teardown for `deploy`. The link invokes this when an
   * `agent.undeploy` frame lands so the router can release any
   * per-deployment registrations and persistent state the deploy path installed
   * (`MultistepMailRouter`, `MultistepSignalRouter`,
   * `MultistepDrainRouter`, `DeploymentAddressRegistry`). It leaves an
   * incarnation with a higher generation than the frame's in place. A throw
   * is answered with `agent.undeploy.error`. The router deletes the agent
   * directory before discarding its durable teardown record. Optional so test
   * routers can omit the implementation; the link then deletes the directory.
   */
  undeploy?: (frame: AgentUndeployFrame) => Promise<void>;
  /** Cancel, stop, or request durable retention of a workflow's local inspection state. */
  control?: (frame: WorkflowControlFrame) => Promise<WorkflowControlOutcome>;
}

/** What a handled workflow control reports back to the Hub. */
export type WorkflowControlOutcome = {
  retention?: DeploymentRetention;
  /** Final history for a live stop, which the Hub must confirm receiving. */
  refTips?: WorkflowRunRefTips;
};

/**
 * Per-address mail handler registry the link consults on every
 * `mail.inbound` frame. Production wires this against the sidecar's
 * `createMultistepMailRouter` so a supervised deployment's supervisor
 * receives the bytes through its mail-bus subscription. Mail for an
 * address with no registered handler has no receiver and is dropped.
 * The shape lives on hub-agent so the link does not import the sidecar
 * host's wiring module, and so tests can substitute a stub.
 */
export interface MailInboundRouter {
  /**
   * Attempt to dispatch `message` to a handler registered against
   * `agentAddress`. Returns `null` if no handler is registered, in which
   * case the link logs and drops the mail (and sends no ack). Otherwise
   * returns the handler's durable settlement: a promise that resolves once
   * the message is durably accepted (its inbox write landed, or it was
   * already durably present) and rejects when it was not (a transient
   * failure, a stale refusal, or a tearing-down phase). The link sends a
   * `mail.inbound.ack` only on resolution, so resolve is the ack signal and
   * reject is the withhold signal.
   */
  tryRoute(agentAddress: string, message: Uint8Array): Promise<void> | null;
}

/**
 * Per-deployment-address signal handler registry the link consults on
 * every inbound `signal.deliver` frame. Production wires this against
 * the sidecar's multi-step deploy registry so the frame flows into the
 * deployment's supervisor (which forwards `signal.deliver` over the
 * control IPC to the workflow-process child). The link logs and drops
 * a frame whose `agentAddress` matches no registered handler so the
 * wire surface fails loudly rather than silently absorbing a misrouted
 * delivery.
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
 * per in-flight run). The link logs and drops a frame whose
 * `agentAddress` matches no registered handler so the wire surface
 * fails loudly rather than silently absorbing a misrouted delivery.
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

/**
 * Per-deployment-address grants registry the link consults on every
 * inbound `run.grants` frame. Production wires this against the sidecar's
 * multi-step deploy registry so the frame flows into the deployment's
 * wiring, which writes the run's grants to its `workflow-run` repo. The
 * link logs and drops a frame whose `agentAddress` matches no registered
 * handler so the wire surface fails loudly rather than silently absorbing
 * a misrouted delivery.
 *
 * The shape lives on hub-agent so the link does not import the sidecar
 * host's wiring module, and so tests can substitute a stub.
 */
export interface GrantsInboundRouter {
  /**
   * Attempt to dispatch `frame` to the deployment registered against
   * `frame.agentAddress`. Returns a promise that resolves to `true` when
   * a handler accepted the frame, `false` when no handler is registered;
   * the promise rejects when the handler is registered but the durable
   * grants write itself throws. The link surfaces a rejection through a
   * logged warning -- a structured failure-reply frame for run grants
   * does not exist on the wire today.
   *
   * The link caches the sender keys the frame carries before it dispatches
   * the frame. `senderKeysCached` is false when one of them did not land, and
   * the handler must then not let the run start, since its recipient could
   * not verify that sender's mail.
   */
  tryRoute(frame: RunGrantsFrame, senderKeysCached: boolean): Promise<boolean>;
}

/**
 * Per-deployment-address sources-rotation registry the link consults on
 * every inbound `sources.update` frame. Unlike signal/drain, `sources.update`
 * is a REQUEST/ACK frame, so the link answers `session.ack` / `session.error`
 * rather than logging and dropping -- a missing answer hangs the hub's
 * request for its full timeout.
 *
 * The shape lives on hub-agent so the link does not import the sidecar
 * host's wiring module, and so tests can substitute a stub.
 */
export interface SourcesInboundRouter {
  /**
   * Attempt to dispatch `frame` to the supervisor registered against
   * `frame.agentAddress`. Resolves `true` when a handler accepted the
   * rotation, `false` when no handler is registered (an unrouted address).
   * Rejects when the handler is registered but the rotation is invalid or
   * the supervisor's `deliverSources` throws; the link turns a rejection
   * into a `session.error` carrying the reason.
   */
  tryRoute(frame: SourcesUpdateFrame): Promise<boolean>;
}

/**
 * Per-deployment-address credential-delivery registry the link consults on
 * every inbound `credentials.update` frame. Like `sources.update`, this is a
 * REQUEST/ACK frame, so the link answers `session.ack` / `session.error`
 * rather than logging and dropping -- a missing answer hangs the hub's request.
 *
 * The shape lives on hub-agent so the link does not import the sidecar host's
 * wiring module, and so tests can substitute a stub.
 */
export interface CredentialsInboundRouter {
  /**
   * Attempt to dispatch `frame` to the supervisor registered against
   * `frame.agentAddress`. Resolves `true` when a handler accepted the
   * delivery, `false` when no handler is registered. Rejects when the handler
   * is registered but the delivery is invalid or the supervisor's
   * `deliverCredentials` throws; the link turns a rejection into a
   * `session.error` carrying the reason.
   */
  tryRoute(frame: CredentialsUpdateFrame): Promise<boolean>;
}

/**
 * Applies one Hub-authoritative workflow-run ref before a deployment's first
 * supervisor is allowed to spawn. The host owns the workflow substrate, so
 * the websocket layer validates and assembles the transfer but delegates the
 * actual ref update through this boundary.
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
 * `workflow.probe.result` frame: the workflow's needs-surface projection, the
 * inert grant set derived from it, the un-flattened grant walk snapshot the set
 * is derived from, and the projection's content hash.
 */
export type WorkflowProbeResult = Pick<
  WorkflowProbeResultFrame,
  "projection" | "grants" | "grantWalkSnapshot" | "wireHash"
>;

/**
 * Seam the link routes every inbound `workflow.probe.request` through.
 * Production wiring supplies an executor that materializes the frame's frozen
 * dependency closure, evaluates the `interchange.workflow` entry module to a
 * live `WorkflowDefinition` in a one-shot child, projects it to its inert
 * needs surface, and returns that projection plus the derived grant set and
 * content hash. `probe` throws when any step fails; the link turns a throw
 * into a `workflow.probe.error` reply so the hub's probe never hangs.
 *
 * The shape lives on hub-agent so the link does not import the sidecar host's
 * probe wiring, and so tests can substitute a stub.
 */
export interface WorkflowProbeExecutor {
  probe(frame: WorkflowProbeRequestFrame): Promise<WorkflowProbeResult>;
}

/**
 * Placeholder probe executor wired when no real one is supplied. It rejects so
 * the link answers `workflow.probe.error` -- never a silent drop -- until the
 * sidecar host wires an executor that runs the child evaluation.
 */
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
   * Key custody and per-frame crypto. HubLink calls into the store for
   * deploy-commit verification, hub-key recording, and per-agent forgetting;
   * it does not maintain its own copy of those tables.
   */
  keyStore: AgentKeyStore;
  /**
   * Resolves a sender address to the crypto whose public key verifies that
   * sender's inbound mail, or `undefined` when no key is known. The inbound
   * signature verify uses it to check each frame's signature against the key a
   * local cache holds, beside the check against the key the hub stamped on the
   * frame. Source-opaque by design: the link never learns whether the key came
   * from a local cache or a relayed foreign key.
   */
  resolveSenderCrypto: (address: string) => CryptoProvider | undefined;
  /**
   * Resolves a recipient deployment address to the TOTAL inbound-mail
   * admission policy the `mail.inbound` seam enforces for it. The host builds
   * this over the sidecar's per-address policy registry: a hydrated deployment
   * registers its resolved policy, and an address the registry does not hold
   * resolves to a fully-closed policy that rejects every outcome. The seam
   * indexes the returned map directly by the message's admission outcome, so
   * this lookup owns the unknown-address default and the seam adds no fallback.
   */
  lookupInboundMailPolicy: (address: string) => ResolvedInboundMailPolicy;
  /**
   * Persists the hub-vouched public key for a sender address, overwriting any
   * previously cached key. The link calls it for an inbound `sender.key.refresh`
   * frame and for each key a `run.grants` frame carries, passing the
   * hex-encoded key through unchanged. Source-opaque, the write peer of
   * `resolveSenderCrypto`: the host builds it over the sidecar's sender-key
   * cache (decode the hex, `put` the bytes) so the link never touches key
   * material or decoding. Required, not optional, because its read peer is
   * required and every composition that runs the link already holds the same
   * cache.
   */
  cacheSenderKey: (address: string, publicKey: string) => Promise<void>;
  /**
   * Durably removes the cached key for a sender address. The link calls it on
   * an inbound `sender.key.evict` frame, when the hub has re-resolved a reported
   * cached sender to no durable key (a deleted principal). The evicting peer of
   * `cacheSenderKey`: the host builds it over the sidecar's sender-key cache
   * (`evict` the address) so the link never touches the cache directly. Required
   * for the same reason as `cacheSenderKey` -- every composition that can cache
   * a key must be able to evict one, and both share the one cache.
   */
  evictSenderKey: (address: string) => Promise<void>;
  /**
   * Routes every inbound `agent.deploy` frame. Production wiring
   * supplies a router that stages each deploy through the workflow-run
   * substrate: a provision-step frame primes a per-step repo, and a
   * workflow frame spawns the supervised workflow-process child. The
   * router owns the routing decision; the link does not re-decide.
   */
  deployRouter: DeployRouter;
  /**
   * Optional inbound mail dispatcher. When present, the link consults
   * this router on every inbound `mail.inbound` frame. Production wires
   * this against the sidecar's multi-step deploy registry so a
   * deployment-address inbound flows into the supervisor's mail-bus
   * subscription. Absent (or a `false` return) means no handler claims
   * the mail, so the link logs and drops it.
   */
  mailInboundRouter?: MailInboundRouter;
  /**
   * Optional inbound signal dispatcher. When present, the link routes
   * every inbound `signal.deliver` frame through this router. Production
   * wires this against the sidecar's multi-step deploy registry so a
   * deployment-address signal flows into the supervisor's
   * `deliverSignal`. Absent (or a `false` return) causes inbound signal
   * frames to be logged-and-dropped so a misrouted delivery is
   * observable rather than silent.
   */
  signalInboundRouter?: SignalInboundRouter;
  /**
   * Optional inbound drain dispatcher. When present, the link routes
   * every inbound `drain.deliver` frame through this router. Production
   * wires this against the sidecar's multi-step deploy registry so a
   * deployment-address drain flows into the supervisor's `drain`. Absent
   * (or a `false` return) causes inbound drain frames to be
   * logged-and-dropped so a misrouted delivery is observable rather than
   * silent.
   */
  drainInboundRouter?: DrainInboundRouter;
  /**
   * Optional inbound grants dispatcher. When present, the link routes
   * every inbound `run.grants` frame through this router. Production wires
   * this against the sidecar's multi-step deploy registry so a
   * deployment-address grants frame flows into the deployment's wiring,
   * which writes the run's grants to its `workflow-run` repo. Absent (or a
   * `false` return) causes inbound grants frames to be logged-and-dropped
   * so a misrouted delivery is observable rather than silent.
   */
  grantsInboundRouter?: GrantsInboundRouter;
  /**
   * Optional inbound sources-rotation dispatcher. When present, the link
   * routes every inbound `sources.update` frame through this router and
   * answers the request/ack frame: `session.ack` when the router accepted
   * the rotation, `session.error` when no deployment is registered, when
   * the rotation is invalid, or when delivery throws. Absent means the
   * link answers `session.error` for every rotation -- required because a
   * request/ack frame with no reply hangs the hub's request.
   */
  sourcesInboundRouter?: SourcesInboundRouter;
  /**
   * Optional inbound credential-delivery dispatcher. When present, the link
   * routes every inbound `credentials.update` frame through this router and
   * answers the request/ack frame: `session.ack` when the router accepted the
   * delivery, `session.error` when no deployment is registered, when the
   * delivery is invalid, or when delivery throws. Absent means the link answers
   * `session.error` for every delivery -- required because a request/ack frame
   * with no reply hangs the hub's request.
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
   * inbound `workflow.probe.request` frame through it and answers the
   * request/response frame: `workflow.probe.result` when the executor returns
   * an inert projection + grant set + hash, `workflow.probe.error` when it
   * throws. Absent, the link wires a placeholder executor that always throws,
   * so a probe still gets answered with an error (never dropped) until the
   * sidecar host supplies a real executor -- required because a
   * request/response frame with no reply hangs the hub's probe.
   */
  workflowProbeExecutor?: WorkflowProbeExecutor;
  /**
   * Every deployment incarnation this sidecar holds, with what it is doing
   * with each. The `hello` frame reports all of them so the Hub reconciles
   * against them, and the link delivers mail, signals, drains, grants and
   * updates only to an incarnation it holds live. Defaults to none when omitted
   * (tests / deployments with no workflow substrate).
   */
  getIncarnations?: () => HostedIncarnation[];
  /**
   * Returns the rotatable (non-run) sender addresses this sidecar holds cached
   * keys for. Read at each (re)connect and reported on the hello frame so the
   * Hub re-resolves and re-pushes each key, catching a rotation that landed
   * while the sidecar was disconnected.
   *
   * Optional with an empty default, UNLIKE the required `cacheSenderKey`: this
   * is a report-path reader whose empty result is a legitimate steady state (no
   * cached senders, or a host with no sender substrate), the same shape as
   * `getIncarnations`. `cacheSenderKey` sits on the receive path a sidecar
   * must always be able to serve, so it is required; this one is not.
   */
  getCachedSenderAddresses?: () => string[];
  /**
   * Invoked on every `welcome` with the addresses of the held incarnations
   * the Hub routes on the new connection, possibly none. Everything owed to
   * the Hub is re-driven from here: the workflow-run pack pusher re-ships
   * what the Hub has not acknowledged for those addresses, their parked
   * correlations are registered again, and deployments that stopped on their
   * own are reported again.
   */
  onWorkflowAddressesRoutable?: (addresses: string[]) => void;
  /**
   * Invoked when a connection opens and when it closes, with the address of
   * every incarnation this link holds. None of them is routed until the next
   * `welcome`, so the workflow-run pack pusher blocks their pushes in the
   * interim. Paired with `onWorkflowAddressesRoutable`, which lifts the block.
   */
  onWorkflowAddressesUnroutable?: (addresses: string[]) => void;
  pingIntervalMs?: number;
  reconnectDelayMs?: number;
  /**
   * How long a connection waits for the Hub's `welcome` after its `hello`
   * before the link gives up on it and reconnects.
   */
  welcomeTimeoutMs?: number;
  /** Per-attempt watchdog before re-sending an unacked correlation register. */
  registerAckTimeoutMs?: number;
  /** Total register sends (initial + retries) before giving up. */
  registerAckMaxAttempts?: number;
  scheduleReconnect?: ReconnectScheduler;
  /** Arms each correlation-register retry; defaults to a real timer. */
  scheduleRegisterRetry?: ReconnectScheduler;
};

export type HubLink = {
  /**
   * Open the connection. Must not be called after `close()`; calling it
   * on a closed client throws.
   */
  connect(): void;
  close(): void;
  /**
   * Best-effort: an event produced while the link is not welcomed is dropped
   * rather than queued, so an outage cannot fill the queue with events.
   */
  sendEvent: SessionEventSink;
  /**
   * Register a control-plane suspension with the hub. Sends a
   * `signal.correlation.register` frame so the hub co-writes the parked run's
   * routing + approval rows. Fired by the sidecar's supervisor when a workflow
   * agent step parks on a reserved correlation channel; the fields converge at
   * this seam (`correlationId`/`runId`/`kind` from the child, `anchorRunId`/
   * `agentAddress`/`generation` stamped by the deployment that parked). Queues
   * while the link is not welcomed.
   */
  sendSignalCorrelationRegister: (registration: {
    correlationId: string;
    runId: string;
    anchorRunId: string;
    agentAddress: string;
    generation: number;
    kind: SignalKind;
    approvalSnapshot?: ApprovalSnapshot;
  }) => void;
  /**
   * Ship a workflow-run pack to the hub. Streams the supplied pack as
   * `repo.pack.push` chunks followed by a `repo.pack.done`, then
   * resolves on the matching `repo.pack.ack` (rejects on
   * `repo.pack.reject` with the carried reason). The hub routes the
   * pack to its `workflow-run` receiver because `repoId.kind` is
   * `"workflow-run"`, and refuses it unless `generation` is the
   * incarnation the connection hosts for the address, routed or not. Rejects
   * at once while the link is not welcomed; the pusher re-drives from
   * `welcome`.
   */
  pushWorkflowRunPack: (opts: {
    agentAddress: string;
    generation: number;
    repoId: RepoId;
    pack: Uint8Array;
    ref: string;
    commitSha: string;
  }) => Promise<void>;
  /**
   * Report a deployment that stopped though the Hub did not stop it. Dropped
   * while the link is not welcomed: the sidecar reports every such deployment
   * again after each `welcome`.
   */
  sendDeploymentStopped: (report: Omit<DeploymentStoppedFrame, "type">) => void;
};

const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

/**
 * The hub-sidecar WebSocket carries `agent.deploy` frames whose payload holds
 * decrypted inference API keys and credential material. Over cleartext `ws://`
 * to a non-loopback host those secrets cross the network unencrypted, so the
 * operator must be told to deploy behind TLS (`wss://`).
 *
 * Returns the warning message when `hubURL` is cleartext `ws:` against a
 * non-loopback host, or `null` when no warning is warranted. `null` means
 * strictly "parsed successfully, transport is acceptable" -- a malformed
 * `hubURL` throws from `new URL` rather than being reported as no-warning.
 * `new URL` normalizes the host (lowercase, canonical IPv4, bracketed IPv6),
 * so the set matches the common loopback spellings without variant handling.
 * Other `127.0.0.0/8` addresses fall through to the warning; over-warning on a
 * local address is safe, whereas missing a remote one is not.
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
    getIncarnations = () => [],
    getCachedSenderAddresses = () => [],
    onWorkflowAddressesRoutable,
    onWorkflowAddressesUnroutable,
    pingIntervalMs = DEFAULT_PING_INTERVAL_MS,
    reconnectDelayMs = DEFAULT_RECONNECT_DELAY_MS,
    welcomeTimeoutMs = DEFAULT_WELCOME_TIMEOUT_MS,
    registerAckTimeoutMs = DEFAULT_REGISTER_ACK_TIMEOUT_MS,
    registerAckMaxAttempts = DEFAULT_REGISTER_ACK_MAX_ATTEMPTS,
    scheduleReconnect = defaultScheduleReconnect,
    scheduleRegisterRetry,
  } = config;

  const cleartextWarning = cleartextTransportWarning(hubURL);
  if (cleartextWarning !== null) {
    logger.warn`${cleartextWarning}`;
  }

  let ws: WebSocket | null = null;
  let closed = false;
  let pingTimer: ReturnType<typeof setInterval> | null = null;
  let welcomeTimer: ReturnType<typeof setTimeout> | null = null;
  let cancelReconnect: (() => void) | null = null;
  let lastPongAt = 0;
  // Set by the Hub's `welcome` once it has reconciled this connection's
  // `hello`. Until then only `hello`, pings, and replies to the Hub's own
  // requests go out, so nothing that must be delivered reaches a Hub that has
  // not yet decided which of this sidecar's incarnations it keeps.
  let welcomed = false;

  const packReceiver = createPackReceiver();
  // Sidecar-initiated workflow-run pushes (`pushWorkflowRunPack`). A transfer
  // belongs to the connection that carried it: it is refused while the link
  // is not welcomed and cancelled when its connection closes, and the pusher
  // re-drives it from the next `welcome`.
  const packSender = createPackSender({
    sendFrame: (frame) => {
      const socket = welcomedSocket();
      if (socket === null) throw new Error(CONNECTION_LOST_REASON);
      socket.send(JSON.stringify(frame));
    },
  });

  // Retry `signal.correlation.register` until the hub acks it. A register is
  // fire-and-forget on the wire and can be lost on an open socket or evicted
  // from the bounded queue below; the acker re-sends on a tight watchdog while
  // the link is welcomed and gives up otherwise, leaving the re-emit on
  // `welcome` as the backstop. A retry before the welcome would only queue a
  // copy behind the one `report` already holds there.
  const registerAcker = createRegisterAcker({
    sendFrame: (frame) => {
      report(frame);
    },
    isOpen: () => welcomedSocket() !== null,
    timeoutMs: registerAckTimeoutMs,
    maxAttempts: registerAckMaxAttempts,
    ...(scheduleRegisterRetry !== undefined
      ? { scheduleRetry: scheduleRegisterRetry }
      : {}),
  });

  // Frames for one address start their handling in the order they arrive,
  // while frames for different addresses do not wait on each other. A mail
  // delivery leaves the lane once routed and a workflow control once started,
  // so a later undeploy can run while either is still settling. Writes to the
  // shared sender-key cache stay in arrival order across addresses: the
  // refresh and evict frames are barriers, and `run.grants`, which caches the
  // keys of its run's senders, also takes the sender-key lane.
  function logFrameError(err: unknown): void {
    const msg = err instanceof Error ? err.message : String(err);
    logger.warn`Unhandled error handling a hub frame: ${msg}`;
  }
  const frameLanes = createFrameLanes(logFrameError);
  // Consecutive requests for the same generation share teardown, including
  // across reconnects. An intervening address command ends that group.
  const pendingUndeploys = new Map<
    string,
    {
      generation: number;
      replies: { connection: WebSocket; requestId: string }[];
    }
  >();

  function incarnationOf(address: string): HostedIncarnation | undefined {
    return getIncarnations().find((held) => held.address === address);
  }

  // Why a Hub frame for one incarnation cannot be delivered, or null when it
  // can. A frame for an incarnation this sidecar does not hold live belongs to
  // one that was undeployed or never deployed here, and delivering it to
  // whatever holds the address now would credit the wrong deployment.
  function refusal(frame: {
    agentAddress: string;
    generation: number;
  }): string | null {
    const held = incarnationOf(frame.agentAddress);
    if (held === undefined) return `${frame.agentAddress} is not hosted here`;
    if (held.generation !== frame.generation) {
      return `${frame.agentAddress} generation ${String(held.generation)} is hosted here, not generation ${String(frame.generation)}`;
    }
    if (held.state !== "live") {
      return `${frame.agentAddress} generation ${String(held.generation)} is ${held.state}`;
    }
    return null;
  }

  // Reports queued while the link is not welcomed: frames the sidecar owes the
  // Hub that nothing re-sends from durable state. Every deployment on the
  // sidecar shares the bound, so an outage that outlasts it drops, loudly and
  // oldest first, what delivers no mail: a correlation registration, which the
  // re-emit on `welcome` sends again, then the audit copy the Hub records sent
  // mail from. Mail still to be routed is never dropped: its send fails
  // instead, and the sender sees the error.
  const MAX_QUEUE = 1024;
  const queue: SidecarFrame[] = [];

  function isMailToRoute(frame: SidecarFrame): boolean {
    return frame.type === "mail.outbound" && frame.delivered !== true;
  }

  // The queued frame an outage drops to make room, or -1 when all of it is
  // mail still to be routed.
  function droppableIndex(): number {
    const register = queue.findIndex(
      (queued) => queued.type === "signal.correlation.register",
    );
    if (register !== -1) return register;
    return queue.findIndex(
      (queued) => queued.type === "mail.outbound" && !isMailToRoute(queued),
    );
  }

  function welcomedSocket(): WebSocket | null {
    return ws !== null && welcomed && ws.readyState === WebSocket.OPEN
      ? ws
      : null;
  }

  function report(frame: SidecarFrame): void {
    const socket = welcomedSocket();
    if (socket !== null) {
      socket.send(JSON.stringify(frame));
      return;
    }
    if (queue.length >= MAX_QUEUE) {
      const index = droppableIndex();
      // A registration goes first, so one that finds no other queued is the
      // one dropped; the re-emit on `welcome` sends it again.
      if (
        frame.type === "signal.correlation.register" &&
        queue[index]?.type !== "signal.correlation.register"
      ) {
        logger.warn`Outbound queue full, dropping ${frame.type}`;
        return;
      }
      if (index === -1) {
        if (isMailToRoute(frame)) {
          throw new Error(
            `The outbound queue is full of mail waiting for the Hub; ${frame.type} was not queued`,
          );
        }
        logger.warn`Outbound queue full of mail waiting for the Hub, dropping ${frame.type}`;
        return;
      }
      const [dropped] = queue.splice(index, 1);
      logger.warn`Outbound queue full, dropping the oldest queued ${dropped?.type ?? "frame"}`;
    }
    queue.push(frame);
  }

  function flush(): void {
    for (
      let socket = welcomedSocket();
      socket !== null;
      socket = welcomedSocket()
    ) {
      const frame = queue.shift();
      if (frame === undefined) return;
      socket.send(JSON.stringify(frame));
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

  // Replies answer a request on the connection that carried it. The Hub gave
  // up on the request when that connection closed, so a reply produced later
  // is dropped rather than sent on a newer connection, where it could match an
  // unrelated request.
  type Reply = (frame: SidecarFrame) => void;

  function replyOn(connection: WebSocket): Reply {
    return (frame) => {
      if (!sendOnConnection(connection, frame)) {
        logger.debug`Dropping ${frame.type}: the connection that carried its request closed`;
      }
    };
  }

  // Wire the transport's remote send handler to push mail.outbound frames
  // for routing. The sender comes from the transport's registered-address
  // check so the hub can bind the claim to this authenticated connection, and
  // the frame names the sender's incarnation. A deployment finishes sending
  // before its teardown completes, so the incarnation held for the sender
  // address while it sends is the one sending.
  transport.setRemoteSendHandler(
    async (rawMessage, recipients, senderAddress) => {
      // Derive the base64 payload length arithmetically (4 output characters
      // per 3 input bytes, padded up) rather than materializing the encoding:
      // an over-cap send is rejected before the encode allocates the ~59MB
      // base64 string a 44MB body would produce, and the throw below stays
      // byte-for-byte identical to the encoded-length check it replaces.
      const bodyBytes = Math.ceil(rawMessage.byteLength / 3) * 4;
      // Fail loud at the source: this send is awaited through the transport, so
      // a throw surfaces to the producing agent as a real error rather than the
      // silent hub-side drop it would otherwise get. The hub re-enforces the cap
      // on receive -- that is the authoritative DoS backstop; this is the
      // producer-facing error.
      if (bodyBytes > MAX_MAIL_OUTBOUND_BODY_BYTES) {
        throw new Error(
          `refusing to send mail.outbound from ${senderAddress}: rawMessage of ${String(bodyBytes)} bytes exceeds the ${String(MAX_MAIL_OUTBOUND_BODY_BYTES)}-byte cap`,
        );
      }
      const sender = incarnationOf(senderAddress);
      if (sender === undefined) {
        throw new Error(
          `refusing to send mail.outbound from ${senderAddress}: no incarnation of it is hosted here`,
        );
      }
      // The Hub refuses a mail naming more than one workflow deployment and
      // stays the authority on that; refusing here gives the sender the error
      // the Hub can only log.
      const deployments = recipients.filter(isRunAddress);
      if (deployments.length > 1) {
        throw new Error(
          `refusing to send mail.outbound from ${senderAddress}: it names ${String(deployments.length)} workflow deployments (${deployments.join(", ")}), and a mail may name only one`,
        );
      }
      const encoded = base64Encode(rawMessage);
      report({
        type: "mail.outbound",
        rawMessage: encoded,
        recipients,
        senderAddress,
        generation: sender.generation,
      });
    },
  );

  // Forward every send to the hub for audit and event emission. Local-only
  // sends are marked delivered: true so the hub does not re-route them.
  // Remote sends are marked delivered: true as well — routing was already
  // handled by the RemoteSendHandler above.
  transport.addMessageSentHandler(async (ctx) => {
    // Same arithmetic pre-check as the remote-send handler above: the audit
    // forward skips an over-cap frame before the encode allocates the ~59MB
    // base64 string, keeping the logged byte count identical.
    const bodyBytes = Math.ceil(ctx.rawMessage.byteLength / 3) * 4;
    // This is the post-delivery audit/projection forward; the mail already went
    // out locally, so there is nothing left to fail. A throw here is swallowed
    // (the transport runs message-sent handlers under Promise.allSettled), so
    // an over-cap frame is logged loudly and skipped rather than thrown -- the
    // hub would drop it on receive regardless.
    if (bodyBytes > MAX_MAIL_OUTBOUND_BODY_BYTES) {
      logger.error`Skipping delivered mail.outbound audit frame from ${ctx.senderAddress}: rawMessage of ${String(bodyBytes)} bytes exceeds the ${String(MAX_MAIL_OUTBOUND_BODY_BYTES)}-byte cap`;
      return;
    }
    const sender = incarnationOf(ctx.senderAddress);
    if (sender === undefined) {
      logger.error`Skipping delivered mail.outbound audit frame from ${ctx.senderAddress}: no incarnation of it is hosted here`;
      return;
    }
    const encoded = base64Encode(ctx.rawMessage);
    const sessionId = sessions.getSessionId(ctx.senderAddress);
    report({
      type: "mail.outbound",
      rawMessage: encoded,
      recipients: ctx.recipients,
      senderAddress: ctx.senderAddress,
      generation: sender.generation,
      ...(sessionId !== undefined ? { sessionId } : {}),
      messageId: ctx.messageId,
      to: ctx.to,
      ...(ctx.cc.length > 0 ? { cc: ctx.cc } : {}),
      delivered: true,
    });
  });

  // Tear one incarnation down: the router releases the deployment and the
  // link drops what it keeps for the address. Every step runs even when an
  // earlier one fails, and the failures are thrown together, so a partial
  // teardown is reported rather than acknowledged. A pack transfer still in
  // flight is left for the Hub to answer, as it answers every transfer.
  async function tearDown(frame: AgentUndeployFrame): Promise<void> {
    const failures: Error[] = [];
    if (deployRouter.undeploy !== undefined) {
      try {
        await deployRouter.undeploy(frame);
      } catch (err) {
        failures.push(err instanceof Error ? err : new Error(String(err)));
      }
    } else {
      try {
        await sessions.deleteAgentDir(frame.agentAddress);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        failures.push(
          new Error(`deleting its agent directory failed: ${msg}`, {
            cause: err,
          }),
        );
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

    keyStore.forgetAgent(frame.agentAddress);
    forgetIncarnation(frame.agentAddress, frame.generation);

    if (failures.length > 0) {
      throw new AggregateError(
        failures,
        `Undeploying ${frame.agentAddress} generation ${String(frame.generation)} failed: ${failures.map((failure) => failure.message).join("; ")}`,
      );
    }
  }

  // Nothing an undeployed incarnation still owed the Hub goes out after its
  // teardown: the Hub no longer routes it.
  function forgetIncarnation(agentAddress: string, generation: number): void {
    const owedBy = (incarnation: { address: string; generation: number }) =>
      incarnation.address === agentAddress &&
      incarnation.generation <= generation;
    let dropped = 0;
    for (let index = queue.length - 1; index >= 0; index -= 1) {
      const queued = queue[index];
      const owing =
        queued === undefined ? undefined : reportingIncarnation(queued);
      if (owing !== undefined && owedBy(owing)) {
        queue.splice(index, 1);
        dropped += 1;
      }
    }
    registerAcker.cancelWhere((frame) =>
      owedBy({ address: frame.agentAddress, generation: frame.generation }),
    );
    if (dropped > 0) {
      logger.warn`Dropping ${String(dropped)} queued report(s) of ${agentAddress} generation ${String(generation)}: it was undeployed`;
    }
  }

  async function handleAgentDeploy(
    frame: AgentDeployFrame,
    reply: Reply,
  ): Promise<void> {
    const answering = {
      requestId: frame.requestId,
      agentAddress: frame.agentAddress,
      generation: frame.generation,
    };
    try {
      // The deploy router (production: the sidecar's workflow-run deploy
      // router) stages the deploy through the substrate and returns the
      // deploy public key the link folds into the outbound ack. It refuses a
      // deploy of an address it already holds an incarnation of.
      const result = await deployRouter.deploy(frame);
      reply({
        type: "agent.deploy.ack",
        ...answering,
        publicKey: result.publicKey,
      });
      logger.info`Deployed ${frame.agentAddress} generation ${String(frame.generation)}`;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      reply({
        type: "agent.deploy.error",
        ...answering,
        error: fitDeploymentError(message),
      });
    }
  }

  async function handleAgentUndeploy(
    frame: AgentUndeployFrame,
  ): Promise<string | undefined> {
    // A sidecar holds one incarnation of an address, so with a newer one held
    // nothing of the generation named is left here, and the newer one is not
    // the Hub's to remove through this frame.
    try {
      const held = incarnationOf(frame.agentAddress);
      if (held !== undefined && held.generation > frame.generation) return;
      await tearDown(frame);
      logger.info`Undeployed ${frame.agentAddress} generation ${String(frame.generation)}: ${frame.reason}`;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      logger.error`${message}`;
      return fitDeploymentError(message);
    }
  }

  function handlePackPush(frame: PackPushFrame, reply: Reply): void {
    const reason = packReceiver.handlePush(frame);
    if (reason !== null) {
      reply({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason,
      });
    }
  }

  async function handlePackDone(
    frame: PackDoneFrame,
    reply: Reply,
  ): Promise<void> {
    const result = packReceiver.handleDone(frame);
    if (result === null) {
      reply({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "corrupt",
      });
      return;
    }

    // A Hub pack stages a step or seeds a deployment's history before the
    // deployment is deployed. Applying one over a held incarnation would
    // rewrite what that deployment is running on.
    const held = incarnationOf(frame.agentAddress);
    if (held !== undefined) {
      reply({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason: "conflict",
        detail: `${frame.agentAddress} generation ${String(held.generation)} is hosted here`,
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
      reply({
        type: "repo.pack.ack",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const reason = classifyAssetPackRejectReason(msg);
      logger.warn`Pack apply failed for ${frame.agentAddress}: ${msg}`;
      reply({
        type: "repo.pack.reject",
        agentAddress: frame.agentAddress,
        repoId: frame.repoId,
        transferId: frame.transferId,
        reason,
        detail: msg,
      });
    }
  }

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
  // Indexed by `agentAddress` (not `anchorRunId`) because the link
  // does not own the address->anchorRunId derivation -- the sidecar's
  // deploy router does. Every workflow-run push the link sees carries
  // the originating address explicitly, so the index closes the gap
  // structurally without leaking the derivation across the package
  // boundary.
  const workflowRunPackBootstrappedByAddress = new Map<string, Set<string>>();
  function workflowRunPackKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}:${repoId.id}:${ref}`;
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
    // or abandoned on a disconnect. The ack still truthfully asserts the row
    // exists, so there is nothing to recover -- log at debug, not warn.
    if (!registerAcker.handleAck(frame.correlationId)) {
      logger.debug`Received signal.correlation.register.ack for uncorrelated ${frame.correlationId}`;
    }
  }

  async function handleSignalDeliver(
    frame: SignalDeliverFrame & IncarnationStamp,
  ): Promise<void> {
    const refused = refusal(frame);
    if (refused !== null) {
      logger.warn`Dropping signal.deliver: ${refused}`;
      return;
    }
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
    const refused = refusal(frame);
    if (refused !== null) {
      logger.warn`Dropping drain.deliver: ${refused}`;
      return;
    }
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

  async function handleMailInbound(
    frame: MailInboundFrame,
    {
      rawBytes,
      verdict,
    }: { rawBytes: Uint8Array; verdict: InboundSignatureVerdict },
    reply: Reply,
  ): Promise<void> {
    const refused = refusal(frame);
    if (refused !== null) {
      // Not delivered and not acknowledged: the Hub's retry or the
      // durable dispatch row delivers it to the incarnation it routes.
      logger.warn`Dropping mail.inbound ${frame.messageId ?? "(no messageId)"}: ${refused}`;
      return;
    }
    // The recipient deployment's resolved admission policy. `frame.
    // agentAddress` is the mail-router registration key, so an address
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
      // A rejected mail is simply not delivered -- the same drop as the
      // no-handler path below: no ack, no reply frame, no dispatch-fail.
      // The hub's redelivery and expiry machinery handles the rest.
      return;
    }
    // Admitted: deliver exactly as an admitted frame always has, keeping
    // the detached durable settlement and detached ack below off the
    // address's frame lane.
    //
    // Supervised deployments register the deployment-level mail
    // address on `mailInboundRouter` once their supervisor spawns;
    // that handler delivers the bytes to the supervisor's mail-bus
    // subscription, which is what the workflow-host's `awaitSignal`
    // listens on. Mail for an address with no registered handler has
    // no receiver -- the in-process session runtime that once backed
    // it is retired -- so it is logged and dropped.
    //
    // Guard the router call with try/catch so a synchronous throw is
    // logged against this address and the mail is dropped like mail with
    // no handler. The durable settlement is observed off the lane (below).
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
      return;
    }
    // Acknowledge durable receipt only AFTER the inbox write settles, and
    // only for hub-originated mail carrying a hub-minted messageId (the
    // ack handshake). Observe the settlement DETACHED from the address's
    // frame lane so a slow or failing inbox write never holds up its later
    // frames; on rejection (transient failure, stale refusal, or a
    // tearing-down phase) no ack is sent, so the hub redelivers. The ack
    // names the incarnation the mail was delivered to, so the Hub credits
    // it to no other.
    const ackMessageId = frame.messageId;
    if (ackMessageId !== undefined) {
      void durable
        .then(() => {
          reply({
            type: "mail.inbound.ack",
            agentAddress: frame.agentAddress,
            generation: frame.generation,
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
  }

  async function handleRunGrants(
    frame: RunGrantsFrame & IncarnationStamp,
    senderKeysCached: boolean,
  ): Promise<void> {
    const refused = refusal(frame);
    if (refused !== null) {
      logger.warn`Dropping run.grants for run ${frame.runId}: ${refused}`;
      return;
    }
    if (grantsInboundRouter === undefined) {
      logger.warn`Received run.grants for ${frame.agentAddress} but no grantsInboundRouter is wired; dropping`;
      return;
    }
    try {
      const routed = await grantsInboundRouter.tryRoute(
        frame,
        senderKeysCached,
      );
      if (!routed) {
        logger.warn`run.grants for ${frame.agentAddress} did not match any registered deployment; dropping`;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error`run.grants write failed for ${frame.agentAddress}: ${msg}`;
    }
  }

  // Caches the sender keys a run's grants carry on the sender-key lane, so they
  // land in the order the Hub sent them along with refreshes and evictions.
  // Resolves to whether every well-formed key landed. Anything that goes wrong
  // counts as not landed, so the run's grants fail instead of waiting forever.
  function cacheSenderKeys(frame: RunGrantsFrame): Promise<boolean> {
    const landed = Promise.withResolvers<boolean>();
    frameLanes.run([SENDER_KEYS], async () => {
      let allLanded = false;
      try {
        allLanded = await cacheEachSenderKey(frame);
      } finally {
        landed.resolve(allLanded);
      }
    });
    return landed.promise;
  }

  // A malformed key is a Hub defect the sidecar cannot repair, so it is
  // skipped as if the Hub had left it out, rather than fail every replay of
  // the run's grants.
  async function cacheEachSenderKey(frame: RunGrantsFrame): Promise<boolean> {
    let allLanded = true;
    for (const identity of frame.senderIdentities ?? []) {
      if (!isSenderPublicKeyHex(identity.publicKey)) {
        logger.error`Skipping a malformed sender key for ${identity.address} in the grants of run ${frame.runId}`;
        continue;
      }
      try {
        await cacheSenderKey(identity.address, identity.publicKey);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error`Caching the sender key for ${identity.address} in the grants of run ${frame.runId} failed: ${msg}`;
        allLanded = false;
      }
    }
    return allLanded;
  }

  // Checks an inbound mail's signature on the sender-key lane, so it is
  // verified against exactly the keys the Hub sent before it, never one a
  // later refresh, eviction or grant wrote, and without waiting for its
  // deployment's earlier frames. This is the one place raw inbound bytes meet
  // the hub-verified sender identity, and every producer -- relay, trigger,
  // durable dispatch -- converges here. The verify is CPU-bound, a cache read,
  // an Ed25519 verify and a MIME re-parse with no I/O, so it holds the lane
  // only briefly. The signature check never throws: a fault degrades to an
  // `error` verdict.
  // The in-process mail-memory transport does not deliver through this seam
  // and carries no hub-verified sender, so it is outside this path.
  function verifyInArrivalOrder(
    frame: MailInboundFrame,
  ): Promise<{ rawBytes: Uint8Array; verdict: InboundSignatureVerdict }> {
    const verified = Promise.withResolvers<{
      rawBytes: Uint8Array;
      verdict: InboundSignatureVerdict;
    }>();
    frameLanes.run([SENDER_KEYS], async () => {
      try {
        const rawBytes = base64Decode(frame.rawMessage);
        const verdict = await verifyInboundSignature(
          {
            raw: rawBytes,
            authenticatedSender: frame.authenticatedSender,
            messageId: frame.messageId,
            agentAddress: frame.agentAddress,
          },
          resolveSenderCrypto,
        );
        verified.resolve({ rawBytes, verdict });
      } catch (err) {
        verified.reject(err);
      }
    });
    // A body that is not base64 rejects when the mail reaches its turn on this
    // lane, but its delivery awaits that only at its own turn, and never when
    // its connection closes first. A rejection nothing has observed ends the
    // process, so it is marked observed here; the delivery that awaits it
    // still sees it.
    void verified.promise.catch(() => undefined);
    return verified.promise;
  }

  async function handleSenderKeyRefresh(
    frame: SenderKeyRefreshFrame,
  ): Promise<void> {
    // Address-keyed and cross-run: it caches one sender's current key and
    // touches no run, so unlike `run.grants` a fault here has no run to poison.
    // Swallow it after logging at ERROR -- there is no reply channel and the
    // link must never wedge. A malformed key (bad hex, wrong length) is simply
    // dropped. A transient cache-write fault is louder than it looks: the
    // sidecar keeps the STALE key and verifies this sender's mail against it
    // until the next reconnect re-pushes, so this push is best-effort, not
    // delivery-guaranteed.
    //
    // Awaited on the sender-key lane, NOT detached the way the `mail.inbound`
    // durable write is: the keys `run.grants` frames carry are cached on the
    // same lane, so writes for the same address land in the order the Hub sent
    // them, and inbound mail is verified on it too, against exactly the keys
    // that arrived before it.
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
    // Mirror of `handleSenderKeyRefresh` for the evict direction: address-keyed,
    // cross-run, no reply channel, awaited on the sender-key lane so it orders
    // deterministically against refresh and grants writes for the same
    // address. A fault swallowed after logging at ERROR keeps the STALE
    // key cached until the next reconnect re-evicts -- the evict is best-effort,
    // not delivery-guaranteed, exactly like the refresh it complements.
    try {
      await evictSenderKey(frame.address);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error`sender.key.evict cache removal failed for ${frame.address}: ${msg}`;
    }
  }

  async function handleSourcesUpdate(
    frame: SourcesUpdateFrame,
    reply: Reply,
  ): Promise<void> {
    // `sources.update` is request/ack (the hub awaits a reply within its
    // request timeout), so every path answers `session.ack` or
    // `session.error` -- unlike the fire-and-forget signal/drain frames
    // that log and drop. A missing router still answers, or the hub hangs.
    const refused = refusal(frame);
    if (refused !== null) {
      reply({
        type: "session.error",
        requestId: frame.requestId,
        error: refused,
      });
      return;
    }
    if (sourcesInboundRouter === undefined) {
      reply({
        type: "session.error",
        requestId: frame.requestId,
        error: "no sourcesInboundRouter is wired",
      });
      return;
    }
    try {
      const routed = await sourcesInboundRouter.tryRoute(frame);
      if (routed) {
        reply({ type: "session.ack", requestId: frame.requestId });
      } else {
        reply({
          type: "session.error",
          requestId: frame.requestId,
          error: `no deployment registered for ${frame.agentAddress}`,
        });
      }
    } catch (err) {
      // A registered address whose rotation was rejected: an invalid list
      // (the router validates before dispatch) or the supervisor's
      // `deliverSources` throwing (e.g. a recycling phase). The reason
      // rides back verbatim so the hub sees why the rotation failed.
      const msg = err instanceof Error ? err.message : String(err);
      reply({
        type: "session.error",
        requestId: frame.requestId,
        error: msg,
      });
    }
  }

  async function handleCredentialsUpdate(
    frame: CredentialsUpdateFrame,
    reply: Reply,
  ): Promise<void> {
    // `credentials.update` is request/ack, exactly like `sources.update`: every
    // path answers `session.ack` or `session.error`. A missing router still
    // answers, or the hub hangs.
    const refused = refusal(frame);
    if (refused !== null) {
      reply({
        type: "session.error",
        requestId: frame.requestId,
        error: refused,
      });
      return;
    }
    if (credentialsInboundRouter === undefined) {
      reply({
        type: "session.error",
        requestId: frame.requestId,
        error: "no credentialsInboundRouter is wired",
      });
      return;
    }
    try {
      const routed = await credentialsInboundRouter.tryRoute(frame);
      if (routed) {
        reply({ type: "session.ack", requestId: frame.requestId });
      } else {
        reply({
          type: "session.error",
          requestId: frame.requestId,
          error: `no deployment registered for ${frame.agentAddress}`,
        });
      }
    } catch (err) {
      // A registered address whose delivery was rejected: an invalid delivery
      // (the router validates before dispatch) or the supervisor's
      // `deliverCredentials` throwing (e.g. a recycling phase). The reason
      // rides back verbatim so the hub sees why the delivery failed.
      const msg = err instanceof Error ? err.message : String(err);
      reply({
        type: "session.error",
        requestId: frame.requestId,
        error: msg,
      });
    }
  }

  async function handleWorkflowProbeRequest(
    frame: WorkflowProbeRequestFrame,
    reply: Reply,
  ): Promise<void> {
    // `workflow.probe.request` is request/response (the hub awaits a reply
    // within its probe timeout), so every path answers `workflow.probe.result`
    // or `workflow.probe.error` -- never a log-and-drop. The executor runs the
    // child evaluation; a throw (including the placeholder executor's
    // not-implemented throw) rides back as an error reply so the hub's probe
    // fails fast instead of hanging.
    try {
      const result = await workflowProbeExecutor.probe(frame);
      reply({
        type: "workflow.probe.result",
        requestId: frame.requestId,
        projection: result.projection,
        grants: result.grants,
        grantWalkSnapshot: result.grantWalkSnapshot,
        wireHash: result.wireHash,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      reply({
        type: "workflow.probe.error",
        requestId: frame.requestId,
        error: msg,
      });
    }
  }

  async function pushWorkflowRunPack(opts: {
    agentAddress: string;
    generation: number;
    repoId: RepoId;
    pack: Uint8Array;
    ref: string;
    commitSha: string;
  }): Promise<void> {
    const key = workflowRunPackKey(opts.repoId, opts.ref);

    async function sendOnce(): Promise<void> {
      await packSender.send({
        agentAddress: opts.agentAddress,
        generation: opts.generation,
        repoId: opts.repoId,
        transferId: `workflow-run-${crypto.randomUUID()}`,
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
        // Only the genuine bootstrap race retries here: the Hub rejects the
        // push that raced its repository's creation as `corrupt`. Anything
        // else re-throws for the caller to latch, including a transfer refused
        // or cancelled because the link is not welcomed (the pushing store
        // re-drives it on `welcome`) and a push the Hub does not route, which
        // a resend would only have rejected again. Real corruption is also
        // `corrupt`, so it gets this one retry too.
        if (
          !(first instanceof PackRejectedError && first.reason === "corrupt")
        ) {
          throw first;
        }
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

  async function handleWorkflowControl(
    frame: WorkflowControlFrame,
    reply: Reply,
  ): Promise<void> {
    let error: string | undefined;
    let refTips: WorkflowRunRefTips | undefined;
    let retention: DeploymentRetention | undefined;
    try {
      if (deployRouter.control === undefined)
        throw new Error("Workflow control is not supported by this sidecar");
      ({ refTips, retention } = await deployRouter.control(frame));
    } catch (cause) {
      error = cause instanceof Error ? cause.message : String(cause);
    }
    reply({
      type: "workflow.control.ack",
      requestId: frame.requestId,
      ...(error !== undefined ? { error } : {}),
      ...(refTips !== undefined ? { refTips } : {}),
      ...(retention !== undefined ? { retention } : {}),
    });
  }

  // Nothing held is routed on a connection until its `welcome` says so, so
  // the workflow-run pusher keeps the pushes of every held incarnation until
  // then instead of sending them where nothing receives them.
  function blockHeldAddresses(): void {
    if (onWorkflowAddressesUnroutable === undefined) return;
    const held = getIncarnations().map((incarnation) => incarnation.address);
    if (held.length > 0) onWorkflowAddressesUnroutable(held);
  }

  function stopWelcomeTimer(): void {
    if (welcomeTimer !== null) {
      clearTimeout(welcomeTimer);
      welcomeTimer = null;
    }
  }

  function handleWelcome(frame: WelcomeFrame, connection: WebSocket): void {
    if (ws !== connection || welcomed) return;
    welcomed = true;
    stopWelcomeTimer();
    flush();
    if (onWorkflowAddressesRoutable === undefined) return;
    const held = new Map(
      getIncarnations().map((incarnation) => [
        incarnation.address,
        incarnation.generation,
      ]),
    );
    onWorkflowAddressesRoutable(
      frame.routed.flatMap((incarnation) =>
        held.get(incarnation.address) === incarnation.generation
          ? [incarnation.address]
          : [],
      ),
    );
  }

  function receiveFrame(data: string, connection: WebSocket): void {
    let raw: unknown;
    try {
      raw = JSON.parse(data) as unknown;
    } catch {
      logger.warn`Received unparseable frame from hub`;
      return;
    }
    const reply = replyOn(connection);
    // A frame is handled only while the connection that carried it is still
    // the link's. Once it closed, the Hub re-drives whatever it still wants
    // after the next `hello`, and a frame handled later could undo what that
    // `hello` reconciled.
    function onLanes(
      frameType: string,
      lanes: Parameters<typeof frameLanes.run>[0],
      handle: () => Promise<void>,
    ): void {
      for (const lane of lanes) {
        if (typeof lane === "string") pendingUndeploys.delete(lane);
      }
      frameLanes.run(lanes, async () => {
        if (ws !== connection) {
          logger.debug`Dropping ${frameType}: the connection that carried it closed`;
          return;
        }
        await handle();
      });
    }

    const validated = HubFrame(raw);
    if (validated instanceof type.errors) {
      // A malformed request/ack frame must still be answered, or the hub's
      // request hangs to its timeout. `sources.update` and `agent.deploy`
      // usually keep an intact correlation key even when a nested field is
      // malformed, so reply with the matching error frame; a fire-and-forget
      // frame (or one with no recoverable key) is only logged and dropped.
      // The answer waits for the frames its address received before it.
      const summary = validated.summary;
      const answer = async (): Promise<void> => {
        answerMalformedRequestFrame(raw, summary, reply);
        logger.warn`Invalid hub frame: ${summary}`;
      };
      const envelope = MalformedRequestEnvelope(raw);
      const address =
        envelope instanceof type.errors ? undefined : envelope.agentAddress;
      if (address === undefined || address.length === 0) {
        void answer().catch(logFrameError);
      } else {
        onLanes("an invalid frame", [address], answer);
      }
      return;
    }
    const frame = validated;

    switch (frame.type) {
      case "pong":
        if (ws === connection) lastPongAt = Date.now();
        return;
      case "welcome":
        handleWelcome(frame, connection);
        return;
      case "workflow.probe.request":
        void handleWorkflowProbeRequest(frame, reply).catch(logFrameError);
        return;
      case "repo.pack.ack":
      case "repo.pack.reject":
      case "signal.correlation.register.ack":
        // Answers to the sidecar's own requests only settle the transfer or
        // registration they answer, so their order among the address's frames
        // does not matter and they are handled as they arrive.
        if (ws === connection) {
          void handleFrame(frame, reply).catch(logFrameError);
        }
        return;
      case "sender.key.refresh":
      case "sender.key.evict":
        onLanes(frame.type, [SENDER_KEYS], () => handleFrame(frame, reply));
        return;
      case "run.grants": {
        const senderKeysCached = cacheSenderKeys(frame);
        onLanes(frame.type, [frame.agentAddress], async () =>
          handleRunGrants(frame, await senderKeysCached),
        );
        return;
      }
      case "mail.inbound": {
        const verified = verifyInArrivalOrder(frame);
        onLanes(frame.type, [frame.agentAddress], async () =>
          handleMailInbound(frame, await verified, reply),
        );
        return;
      }
      case "workflow.control":
        // Start in the address's order, but leave its lane free for a forced
        // stop and the deployment's undeploy while cooperative cancellation
        // waits for the child.
        onLanes(frame.type, [frame.agentAddress], async () => {
          void handleWorkflowControl(frame, reply).catch((cause: unknown) => {
            logger.warn`Workflow control reply failed: ${cause instanceof Error ? cause.message : String(cause)}`;
          });
        });
        return;
      case "agent.undeploy": {
        // Runs even once its connection closed; only the answer goes with the
        // connection. The Hub sends it when it will not route that generation
        // again, and may close the socket right behind it, so dropping it
        // would leave the deployment running with nothing left to remove it.
        const pending = pendingUndeploys.get(frame.agentAddress);
        const answering = { connection, requestId: frame.requestId };
        if (pending?.generation === frame.generation) {
          // A closed connection cannot receive an answer. Keep only the
          // current socket's waiters while teardown spans reconnects.
          pending.replies = pending.replies.filter(
            (waiting) => waiting.connection === connection,
          );
          pending.replies.push(answering);
          return;
        }
        const cleanup = { generation: frame.generation, replies: [answering] };
        pendingUndeploys.set(frame.agentAddress, cleanup);
        frameLanes.run([frame.agentAddress], async () => {
          try {
            const error = await handleAgentUndeploy(frame);
            for (const waiting of cleanup.replies) {
              const response = {
                requestId: waiting.requestId,
                agentAddress: frame.agentAddress,
                generation: frame.generation,
              };
              replyOn(waiting.connection)(
                error === undefined
                  ? { type: "agent.undeploy.ack", ...response }
                  : { type: "agent.undeploy.error", ...response, error },
              );
            }
          } finally {
            if (pendingUndeploys.get(frame.agentAddress) === cleanup)
              pendingUndeploys.delete(frame.agentAddress);
          }
        });
        return;
      }
      default:
        onLanes(frame.type, [frame.agentAddress], () =>
          handleFrame(frame, reply),
        );
    }
  }

  async function handleFrame(
    frame: Exclude<
      HubFrame,
      {
        type:
          | "pong"
          | "welcome"
          | "agent.undeploy"
          | "workflow.probe.request"
          | "workflow.control"
          | "run.grants"
          | "mail.inbound";
      }
    >,
    reply: Reply,
  ): Promise<void> {
    switch (frame.type) {
      case "agent.deploy":
        await handleAgentDeploy(frame, reply);
        break;
      case "repo.pack.push":
        handlePackPush(frame, reply);
        break;
      case "repo.pack.done":
        await handlePackDone(frame, reply);
        break;
      case "signal.deliver":
        await handleSignalDeliver(frame);
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
        await handleSourcesUpdate(frame, reply);
        break;
      case "credentials.update":
        await handleCredentialsUpdate(frame, reply);
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
    // against post-close reconnect attempts. A caller invoking connect()
    // after close() is a misuse, not a recoverable state — fail loudly.
    if (closed) {
      throw new Error("HubLink.connect called after close");
    }

    welcomed = false;
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
        sendOnConnection(connection, { type: "ping" });
      }, pingIntervalMs);

      packReceiver.reset();
      blockHeldAddresses();

      // The hello reports every incarnation this sidecar holds, including one
      // still deploying or tearing down, so the Hub decides from what is
      // actually here which to keep. Nothing that must be delivered goes out
      // until its `welcome`.
      const cachedSenderAddresses = getCachedSenderAddresses();
      sendOnConnection(connection, {
        type: "hello",
        sidecarId,
        token,
        incarnations: getIncarnations(),
        ...(cachedSenderAddresses.length > 0 ? { cachedSenderAddresses } : {}),
      });
      // A Hub that never answers would otherwise hold back everything this
      // sidecar must deliver for as long as the socket stays up.
      welcomeTimer = setTimeout(() => {
        welcomeTimer = null;
        if (ws !== connection || welcomed) return;
        logger.error`The Hub did not answer hello within ${String(welcomeTimeoutMs)}ms; reconnecting`;
        connection.close();
      }, welcomeTimeoutMs);
    });

    connection.addEventListener("message", (event) => {
      if (typeof event.data === "string") receiveFrame(event.data, connection);
    });

    connection.addEventListener("close", () => {
      // A late close from a superseded attempt must not null or reschedule the
      // active socket. Normal reconnects also pass this fence: the next socket
      // is not created until this handler schedules it.
      if (ws !== connection) return;
      logger.info`Disconnected from hub`;
      ws = null;
      welcomed = false;
      stopWelcomeTimer();
      if (pingTimer !== null) {
        clearInterval(pingTimer);
        pingTimer = null;
      }
      // Transfers and register retries belong to the connection that carried
      // them. Recovery belongs to the re-drive on the next `welcome`, and a
      // lingering watchdog would only fire onto a closed socket.
      packSender.cancelAll(CONNECTION_LOST_REASON);
      registerAcker.cancelAll();
      // The Hub dropped every route this link held.
      blockHeldAddresses();
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

    connection.addEventListener("error", (event) => {
      logger.warn`WebSocket error: ${String(event)}`;
    });
  }

  function close(): void {
    closed = true;
    stopWelcomeTimer();
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

  const sendEvent: SessionEventSink = (
    agentAddress,
    generation,
    sessionId,
    event,
  ) => {
    welcomedSocket()?.send(
      JSON.stringify({
        type: "agent.event",
        agentAddress,
        generation,
        sessionId,
        event,
      } satisfies SidecarFrame),
    );
  };

  const sendSignalCorrelationRegister: HubLink["sendSignalCorrelationRegister"] =
    (registration) => {
      // The ask rail is the only producer of this frame, and every ask-rail
      // suspension carries a snapshot. A registration without one is an
      // in-process wiring defect, not a wire condition: fail loud here rather
      // than send a snapshot-less frame the receiver would reject.
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
        generation: registration.generation,
        kind: registration.kind,
        snapshot: registration.approvalSnapshot,
      };
      // Send through the acker, which retries until the hub acks the co-write.
      registerAcker.send(frame);
    };

  const sendDeploymentStopped: HubLink["sendDeploymentStopped"] = (report) => {
    welcomedSocket()?.send(
      JSON.stringify({
        type: "deployment.stopped",
        ...report,
      } satisfies SidecarFrame),
    );
  };

  return {
    connect,
    close,
    sendEvent,
    sendSignalCorrelationRegister,
    pushWorkflowRunPack,
    sendDeploymentStopped,
  };
}
