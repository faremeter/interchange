// Websocket wire protocol for hub↔sidecar communication.
//
// One websocket connection per sidecar↔hub pair. All traffic is multiplexed
// as JSON frames with a `type` discriminator. The sidecar initiates the
// connection; the hub is the server.
//
// Mail bytes are base64-encoded in JSON frames. Binary frames would be more
// efficient but JSON is simpler to debug and inspect.

import { type } from "arktype";
import { GrantWalkSnapshot } from "./grant-snapshot";
import { ApprovalItem } from "./grants";
import { WireGrantRule } from "./grant-wire";
import {
  BoundedApprovalSnapshot,
  ConnectorThreadState,
  HarnessConfig,
  InferenceEvent,
  InferenceSource,
} from "./runtime";
import { SignalKind } from "./signals";
import { ToolPackageManifest } from "./tool-packages";
import { WorkflowDefinitionSource } from "./workflow-sources";

// ---------------------------------------------------------------------------
// Frame array-length ceilings
// ---------------------------------------------------------------------------
//
// Hostile-absurdity upper bounds on the unbounded `string[]` frame fields.
// They cap element COUNT, not byte size: millions of one-character elements
// cost little in bytes but force the receiver to allocate and iterate over an
// absurd count. Object-typed arrays are exempt — their elements carry many
// bytes each, so a payload-size limit is the right backstop there. An
// over-count frame fails this parse and drops through the invalid-frame path.

// A sidecar's reported agent addresses; the handler gates each against the
// allocation's single minted workflow address, so ~1 is legitimate. Generous
// absurdity backstop.
export const MAX_AGENT_ADDRESSES_FRAME = 512;

// A sidecar's reported cached sender addresses. MUST stay well above the
// hub-sessions `MAX_RESYNC_SENDER_ADDRESSES` handler cap (2048): that cap
// drives a graceful "resync the first N" degrade, and a schema ceiling at or
// below it would turn the degrade into a hard reconnect outage. The coupling
// is a documented invariant guarded by a test (types must not import from
// hub-sessions).
export const MAX_CACHED_SENDER_ADDRESSES_FRAME = 65536;

// A mail frame's recipient / To / Cc address lists. `recipients` is the
// routing set; `to`/`cc` are audit-only header metadata.
export const MAX_MAIL_ADDRESSES_FRAME = 1024;

// A workflow probe result's flattened grant strings (the deduped union of
// every step's grants); a reasonable absurdity ceiling, not a computed bound.
export const MAX_PROBE_GRANTS_FRAME = 8192;

// A credentials-update frame's revoked credential ids; a modest ceiling far
// above any real set.
export const MAX_CREDENTIAL_REVOCATIONS_FRAME = 1024;

// ---------------------------------------------------------------------------
// Frame payload byte limits
// ---------------------------------------------------------------------------
//
// Byte-size ceilings on the control socket, complementary to the
// element-count ceilings: the websocket's maxPayloadLength owns the
// whole-frame byte size, and the mail body cap owns one mail's rawMessage.

// The largest rawMessage (base64-encoded MIME) a `mail.outbound` frame may
// carry. Deliberately equal to `@intx/hub-api`'s `MAX_MAIL_BODY_BYTES` (the
// inbound HTTP mail route's body cap) so both paths enforce the same ceiling;
// they measure different quantities and stay separate constants held equal by
// a guard test. Enforced symmetrically: the hub drops an over-cap frame and
// the sidecar refuses to send one.
export const MAX_MAIL_OUTBOUND_BODY_BYTES = 44 * 1024 * 1024;

// Headroom for base64/JSON framing plus the count-capped address arrays, so
// `maxPayloadLength` never closes the socket on a legit mail frame at the cap.
const FRAME_OVERHEAD_BYTES = 20 * 1024 * 1024;

// The hub sidecar websocket's `maxPayloadLength`. Bun closes the connection
// on a received message larger than this, so it must clear the largest legit
// received frame: `mail.outbound` at the body cap plus framing overhead. It
// gates incoming messages only, so the hub->sidecar inline-asset deploy does
// not factor in.
export const MAX_SIDECAR_FRAME_BYTES =
  MAX_MAIL_OUTBOUND_BODY_BYTES + FRAME_OVERHEAD_BYTES;

// ---------------------------------------------------------------------------
// Sidecar → Hub
// ---------------------------------------------------------------------------

/**
 * Sent on first connect when the sidecar has no existing agents in its data
 * directory; declares it ready to receive agent.deploy frames.
 */
export const RegisterFrame = type({
  type: "'register'",
  sidecarId: "string",
  token: "string",
  agentAddresses: type("string")
    .array()
    .atMostLength(MAX_AGENT_ADDRESSES_FRAME),
  // The rotatable (non-run) sender addresses this sidecar holds cached keys
  // for, so a rotation that landed while the sidecar was disconnected
  // reaches its cache on reconnect. Additive-optional, omitted when empty;
  // absence means "nothing to refresh".
  "cachedSenderAddresses?": type("string")
    .array()
    .atMostLength(MAX_CACHED_SENDER_ADDRESSES_FRAME),
});
export type RegisterFrame = typeof RegisterFrame.infer;

/**
 * Sent on connect after a provisioned sidecar restores its deployment.
 * The bearer token binds the connection to one allocation generation, so the
 * Hub accepts only that allocation's workflow address.
 */
export const ReconnectFrame = type({
  type: "'reconnect'",
  sidecarId: "string",
  token: "string",
  agentAddresses: type("string")
    .array()
    .atMostLength(MAX_AGENT_ADDRESSES_FRAME),
  // As `RegisterFrame`: the register vs reconnect choice turns on
  // workflow-address presence, not sender-cache presence. Additive-optional,
  // omitted when empty.
  "cachedSenderAddresses?": type("string")
    .array()
    .atMostLength(MAX_CACHED_SENDER_ADDRESSES_FRAME),
});
export type ReconnectFrame = typeof ReconnectFrame.infer;

/**
 * Acknowledges a successful agent deployment, carrying the agent's
 * hex-encoded Ed25519 public key for identity and content provenance.
 */
export const AgentDeployAckFrame = type({
  type: "'agent.deploy.ack'",
  agentAddress: "string",
  publicKey: "string",
});
export type AgentDeployAckFrame = typeof AgentDeployAckFrame.infer;

/**
 * Reports a failed agent deployment.
 */
export const AgentErrorFrame = type({
  type: "'agent.error'",
  agentAddress: "string",
  error: "string",
});
export type AgentErrorFrame = typeof AgentErrorFrame.infer;

/**
 * A message from a local agent. When `delivered` is absent or false the hub
 * routes it to its recipients; when true it was already delivered locally and
 * is forwarded for audit/projection only. Structured metadata rides along so
 * the hub need not parse the raw MIME bytes.
 */
export const MailOutboundFrame = type({
  type: "'mail.outbound'",
  rawMessage: "string",
  recipients: type("string").array().atMostLength(MAX_MAIL_ADDRESSES_FRAME),
  senderAddress: "string",
  "sessionId?": "string",
  "messageId?": "string",
  "to?": type("string").array().atMostLength(MAX_MAIL_ADDRESSES_FRAME),
  "cc?": type("string").array().atMostLength(MAX_MAIL_ADDRESSES_FRAME),
  "delivered?": "boolean",
});
export type MailOutboundFrame = typeof MailOutboundFrame.infer;

/**
 * An InferenceEvent from the reactor, forwarded for UI consumption. Tagged
 * with the run address so the hub can route to the correct UI client.
 */
export const AgentEventFrame = type({
  type: "'agent.event'",
  agentAddress: "string",
  sessionId: "string",
  event: InferenceEvent,
});
export type AgentEventFrame = typeof AgentEventFrame.infer;

/**
 * Notifies the hub that the agent's connector-thread state changed, so the
 * hub can set threading headers on user-originated mail (routing it as
 * `continue` rather than `passthrough`). `connectorState` is `null` when no
 * active thread exists.
 */
export const ConnectorStateChangedFrame = type({
  type: "'connector.state.changed'",
  agentAddress: "string",
  connectorState: ConnectorThreadState.or("null"),
});
export type ConnectorStateChangedFrame =
  typeof ConnectorStateChangedFrame.infer;

/**
 * Keepalive ping sent by the sidecar. The hub responds with a pong frame.
 * If the hub stops receiving pings, it considers the sidecar dead.
 */
export const PingFrame = type({ type: "'ping'" });
export type PingFrame = typeof PingFrame.infer;

/**
 * Acknowledges a request from the hub (sources.update).
 */
export const SessionAckFrame = type({
  type: "'session.ack'",
  requestId: "string",
});
export type SessionAckFrame = typeof SessionAckFrame.infer;

/**
 * Reports an error processing a hub request.
 */
export const SessionErrorFrame = type({
  type: "'session.error'",
  requestId: "string",
  error: "string",
});
export type SessionErrorFrame = typeof SessionErrorFrame.infer;

/**
 * Acknowledges that an agent has been fully undeployed: workflow child
 * stopped, state pushed (best-effort), directory deleted.
 */
export const AgentUndeployAckFrame = type({
  type: "'agent.undeploy.ack'",
  agentAddress: "string",
  statePushed: "boolean",
});
export type AgentUndeployAckFrame = typeof AgentUndeployAckFrame.infer;

/**
 * Registers a control-signal correlation as a workflow agent step suspends;
 * the hub co-writes the `signal_correlation` routing row and the `approval`
 * row in one transaction so a delivered decision resolves back to the parked
 * run.
 *
 * `signalName` is deliberately NOT on the wire: it is a pure function of
 * `correlationId` (`signalName(correlationId)` in `./signals`), so the hub
 * computes it rather than trusting the sidecar.
 */
export const SignalCorrelationRegisterFrame = type({
  type: "'signal.correlation.register'",
  correlationId: "string",
  runId: "string",
  anchorRunId: "string",
  agentAddress: "string",
  kind: SignalKind,
  // Approver-facing snapshot of the suspended tool call, size-capped at this
  // trust boundary. Required: the ask rail is the only producer and always
  // carries one, so an absent snapshot fails the parse (never a null row).
  snapshot: BoundedApprovalSnapshot,
});
export type SignalCorrelationRegisterFrame =
  typeof SignalCorrelationRegisterFrame.infer;

// ---------------------------------------------------------------------------
// Hub → Sidecar
// ---------------------------------------------------------------------------

/**
 * Hub acknowledges a `signal.correlation.register`: the co-write for this
 * correlationId is durable (inserted now or found already present), so the
 * sidecar stops retrying. Keyed on correlationId alone — every producer
 * drives the same idempotent co-write, so the ack asserts one fact: a row
 * exists for this correlation.
 */
export const SignalCorrelationRegisterAckFrame = type({
  type: "'signal.correlation.register.ack'",
  agentAddress: "string",
  correlationId: "string",
});
export type SignalCorrelationRegisterAckFrame =
  typeof SignalCorrelationRegisterAckFrame.infer;

/**
 * A message to deliver to a local agent's INBOX, routed by the hub from UI
 * users or other sidecars.
 *
 * `messageId` is the hub-minted id of this delivery, carried so the sidecar's
 * `mail.inbound.ack` keys on the SAME id the hub tracks. It is the id minted
 * at ingress (also the message's `Message-ID` header), so a redelivery
 * replays identical bytes and the downstream `RunStarted` dedup makes
 * at-least-once effectively-once. Present only on hub-originated mail that
 * participates in the ack/retry handshake; agent-to-agent relayed mail omits
 * it.
 *
 * `authenticatedSender` is the hub-verified sender ADDRESS, assigned from a
 * value the hub itself verified — NEVER from the message's own spoofable MIME
 * `From`. The recipient's signature check uses this value as the sender of
 * record.
 */
export const MailInboundFrame = type({
  type: "'mail.inbound'",
  agentAddress: "string",
  rawMessage: "string",
  authenticatedSender: "string",
  "messageId?": "string",
});
export type MailInboundFrame = typeof MailInboundFrame.infer;

/**
 * Sidecar acknowledges durable receipt of a `mail.inbound` (the message is in
 * the on-disk inbox). The hub retries until this ack lands or re-delivers on
 * reconnect, so a message dropped in the connected/reconnecting window is not
 * silently lost. Keyed on the hub-minted `messageId`; sent only after the
 * durable inbox write resolves. At-least-once is made effectively-once by the
 * `RunStarted`/signal dedup guards.
 */
export const MailInboundAckFrame = type({
  type: "'mail.inbound.ack'",
  agentAddress: "string",
  messageId: "string",
});
export type MailInboundAckFrame = typeof MailInboundAckFrame.infer;

/**
 * Deliver a workflow-run signal to a multi-step deployment's supervisor. The
 * sidecar's hub-link routes it to the supervisor's `deliverSignal`, which
 * sends a `signal.deliver` control frame to the workflow-process child. The
 * child commits `SignalReceived` through its own substrate — the single
 * writer of the workflow-run repo — so the pack-push pipeline never sees a
 * concurrent writer at the same ref.
 *
 * `signalId` feeds the run's dedup index (`observedSignalIds`); a fresh value
 * per call is the producer's responsibility.
 */
export const SignalDeliverFrame = type({
  type: "'signal.deliver'",
  agentAddress: "string",
  runId: "string",
  signalName: "string",
  signalId: "string",
  payload: "unknown",
});
export type SignalDeliverFrame = typeof SignalDeliverFrame.infer;

/**
 * A sender address bound to the public key the hub vouches for. `publicKey`
 * is the hex-encoded raw 32-byte Ed25519 key. `address` is the full
 * domain-qualified sender address.
 */
export const SenderIdentity = type({
  address: "string",
  publicKey: "string",
});
export type SenderIdentity = typeof SenderIdentity.infer;

/**
 * Deliver a run's authorization grants to a multi-step deployment's
 * supervisor. The wiring writes them to `runs/<runId>/grants.json` inside
 * the deployment's `workflow-run` repo.
 *
 * `stepGrants` carries the same `WireGrantRule` shape as the deploy frame's
 * `config.grants`. `senderIdentities` co-delivers the resolved public keys
 * of the run's authorized senders on the same barrier; a sender with no
 * resolvable key is omitted rather than carried as null.
 */
export const RunGrantsFrame = type({
  type: "'run.grants'",
  agentAddress: "string",
  runId: "string",
  stepGrants: WireGrantRule.array(),
  "senderIdentities?": SenderIdentity.array(),
});
export type RunGrantsFrame = typeof RunGrantsFrame.infer;

/**
 * Re-push the current public key the hub vouches for a cached sender, keyed
 * by `address`; the sidecar overwrites its cached key and touches nothing
 * else. Sent once per rotatable sender reported on (re)connect, so a
 * rotation that happened while the sidecar was disconnected lands on it.
 *
 * A dedicated frame rather than a reuse of `SenderIdentity` (a fact embedded
 * in `run.grants`) or `run.grants` itself (run-keyed; routing a cross-run
 * key update through it would poison an idle run on a transient fault). One
 * address per frame keeps each cache write independently fallible.
 */
export const SenderKeyRefreshFrame = type({
  type: "'sender.key.refresh'",
  address: "string",
  publicKey: "string",
});
export type SenderKeyRefreshFrame = typeof SenderKeyRefreshFrame.infer;

/**
 * Evict a cached sender key, keyed by `address`; the sidecar durably removes
 * it and touches nothing else. Sent during reconnect reconciliation when a
 * reported cached sender re-resolves to NO durable key (principal deleted
 * while disconnected), so the sidecar stops verifying that sender's mail.
 *
 * A dedicated sibling of `sender.key.refresh`: a refresh always carries a
 * key while an evict never does, so a shared frame would make `publicKey`
 * conditionally present. One address per frame keeps each eviction
 * independently fallible.
 */
export const SenderKeyEvictFrame = type({
  type: "'sender.key.evict'",
  address: "string",
});
export type SenderKeyEvictFrame = typeof SenderKeyEvictFrame.infer;

/**
 * Deliver a workflow-host drain control payload to a multi-step deployment's
 * supervisor, which sends a `drain` control frame to the workflow-process
 * child and arms one `drainTimeout` accumulator per in-flight run. Each
 * accumulator commits a signed `CancelRequested{origin: "supervisor-drain"}`
 * against the workflow-run repo when its deadline expires.
 *
 * `deadlineMs` is a wire-level hint the child echoes in its logs; the
 * accumulator itself runs on the supervisor's own `drainTimeoutMs` setting.
 */
export const DrainDeliverFrame = type({
  type: "'drain.deliver'",
  agentAddress: "string",
  deadlineMs: "number",
});
export type DrainDeliverFrame = typeof DrainDeliverFrame.infer;

import {
  WorkflowProjectionDefinition,
  WorkflowProjectionWithSources,
} from "./wire-workflow";
// Re-export the wire-step/projection contracts that moved to `./wire-workflow`
// so existing `@intx/types/sidecar` consumers keep resolving them here.
export { WorkflowStep } from "./wire-workflow";
export { WorkflowProjectionDefinition, WorkflowProjectionWithSources };

/**
 * Decrypted credential material and per-handle binding descriptors delivered
 * to a running agent. Secrets ride the live channel ONLY (deploy frame,
 * `credentials.update`, the child's in-memory cell): never written to disk
 * and never copied into snapshots, events, or state.
 *
 * `materials` is keyed by `credentialId` (a secret is stored once);
 * `bindings` maps each tool handle to its credential and allowed consumer.
 */
export const CredentialMaterialEntry = type({
  credentialId: "string",
  providerKey: "string",
  origin: "string",
  secret: "string",
});
export type CredentialMaterialEntry = typeof CredentialMaterialEntry.infer;

export const CredentialBindingDescriptor = type({
  handle: "string",
  credentialId: "string",
  consumer: "string",
});
export type CredentialBindingDescriptor =
  typeof CredentialBindingDescriptor.infer;

export const CredentialDelivery = type({
  bindings: CredentialBindingDescriptor.array(),
  materials: CredentialMaterialEntry.array(),
});
export type CredentialDelivery = typeof CredentialDelivery.infer;

/**
 * The source-ref pin: where a code-sourced workflow definition's bytes come
 * from (`source`) plus the frozen dependency closure the hub resolved
 * (`closure`, concrete versions + integrity SRIs). The two ALWAYS travel
 * together — the sidecar re-materializes the closure and re-evaluates the
 * pinned code — so they are one co-required object. The same shape
 * `WorkflowProbeRequestFrame` co-requires.
 */
export const SourceRefPin = type({
  source: WorkflowDefinitionSource,
  closure: ToolPackageManifest,
});
export type SourceRefPin = typeof SourceRefPin.infer;

/**
 * The frozen, fully-serializable record of a code-sourced workflow approval,
 * persisted at prepare time and rehydrated to deploy the exact same
 * definition later: the recovery input for a provisioned workflow.
 *
 * Every field is inert, secret-free data. `approvedGrants` stays a flat list
 * so rows written before the requirement kind existed (plain strings) still
 * parse — a string is an `ApprovalItem`. Per-step inference sources are
 * deliberately NOT frozen: they carry credential secrets and are re-resolved
 * at deploy time.
 */
export const FrozenApprovalBundle = type({
  source: WorkflowDefinitionSource,
  entry: "string > 0",
  projection: WorkflowProjectionDefinition,
  closure: ToolPackageManifest,
  approvedWireHash: "string > 0",
  approvedGrants: ApprovalItem.array(),
});
export type FrozenApprovalBundle = typeof FrozenApprovalBundle.infer;

/**
 * A hub asset delivered inline in a source-ref frame so the sidecar can
 * materialize a closure entry whose bytes live in that asset. `pack` is the
 * base64-encoded git packfile (`createPack` output); the sidecar checks out
 * `commitSha` as plain files under `mountPath`, and the loader resolves
 * `kind:"asset"` closure entries against that mount.
 */
export const WorkflowSourceAssetMount = type({
  assetId: "string",
  mountPath: "string",
  pack: "string",
  ref: "string",
  commitSha: "string",
});
export type WorkflowSourceAssetMount = typeof WorkflowSourceAssetMount.infer;

/**
 * A full workflow deploy frame. The deploy lineage is source-ref only: the
 * runnable definition is the pinned code closure the sidecar re-materializes
 * and evaluates from `sourceRef`, so the frame carries NO inline
 * `definition`. Deliberately NOT built on `WorkflowProjectionWithSources`,
 * which stays the approval/probe projection (each `referencedDefinitions`
 * body still carries its own inert definition).
 */
export const AgentDeployWorkflow = type({
  // Per-step inference-source failover chains, one per step in the closure's
  // `stepOrder`, threaded to the child so it resolves inference without a hub
  // round-trip.
  sources: { "[string]": InferenceSource.array().atLeastLength(1) },
  // The hub-approved wire hash of the frozen projection (the freeze anchor),
  // fed to the child as `DEFINITION_HASH` to re-verify its closure
  // evaluation. Optional on the wire; the production hub always stamps it and
  // the sidecar fails closed if absent.
  "approvedWireHash?": "string > 0",
  // Extracted trigger bodies (onTrigger sections and childWorkflow children,
  // lifted transitively), each carrying its inert definition, per-step source
  // pins, and approved wire hash. The sidecar seals the bodies' sources into
  // the per-run record so an in-process body child resolves inference durably
  // without holding the cipher key. Optional: only deploys with such bodies.
  "referencedDefinitions?": WorkflowProjectionWithSources.array(),
  // Initial credential material for the deployment's tools, resident before
  // any step runs. Run-global: a secret is stored once, keyed by
  // credentialId. Optional — a deploy binding no credentials omits it.
  "credentials?": CredentialDelivery,
  // The source-ref pin the sidecar re-materializes and evaluates from.
  // Required: without it the sidecar has no definition to run.
  sourceRef: SourceRefPin,
  // Source assets a `kind:"asset"` closure entry reads from, delivered inline
  // so the sidecar checks them out into its source store before
  // materializing the pin. Optional: only an asset-sourced deploy carries it.
  "assets?": WorkflowSourceAssetMount.array(),
});
export type AgentDeployWorkflow = typeof AgentDeployWorkflow.infer;

/**
 * Deploy an agent to this sidecar, spawning a supervised workflow-process
 * child to host the deployment.
 *
 * The deploy router discriminates two shapes by field presence without
 * consulting `config`:
 *   - `workflow` set: a workflow deployment that spawns the child.
 *   - `provisionStep` true: a no-spawn per-step provision of a multi-step
 *     deploy — the sidecar initializes the step's agent-state repo and
 *     records the hub key so the follow-up deploy pack applies, but spawns
 *     nothing. The deployment-level `workflow` frame spawns the child once
 *     every step is provisioned.
 * A frame carrying neither is rejected. The two are mutually exclusive.
 */
export const AgentDeployFrame = type({
  type: "'agent.deploy'",
  agentAddress: "string",
  agentId: "string",
  config: HarnessConfig,
  hubPublicKey: "string",
  "workflow?": AgentDeployWorkflow,
  "provisionStep?": "boolean",
});
export type AgentDeployFrame = typeof AgentDeployFrame.infer;

/**
 * Remove an agent from this sidecar: supervisor shut down, state pushed
 * (best-effort), agent directory deleted, then agent.undeploy.ack.
 */
export const AgentUndeployFrame = type({
  type: "'agent.undeploy'",
  agentAddress: "string",
  reason: "string",
});
export type AgentUndeployFrame = typeof AgentUndeployFrame.infer;

export const WorkflowControlFrame = type({
  type: "'workflow.control'",
  requestId: "string",
  agentAddress: "string",
  runId: "string",
  action: "'cancel' | 'stop'",
  reason: "string",
});
export type WorkflowControlFrame = typeof WorkflowControlFrame.infer;

export const WORKFLOW_CONTROL_INITIALIZING_ERROR = "workflow_initializing";

/** The tip of each authoritative workflow-run ref, `null` for an absent ref. */
export const WorkflowRunRefTips = type({ "[string]": "string | null" });
export type WorkflowRunRefTips = typeof WorkflowRunRefTips.infer;

export const WorkflowControlAckFrame = type({
  type: "'workflow.control.ack'",
  requestId: "string",
  "error?": "string",
  /**
   * A stopped worker's ref tips, read after its child exited; the Hub
   * confirms the stop only once it holds the same tips.
   */
  "refTips?": WorkflowRunRefTips,
});
export type WorkflowControlAckFrame = typeof WorkflowControlAckFrame.infer;

/**
 * Keepalive pong sent by the hub in response to a ping frame; the sidecar
 * considers the hub dead if pongs stop arriving.
 */
export const PongFrame = type({ type: "'pong'" });
export type PongFrame = typeof PongFrame.infer;

/**
 * Push an updated inference-source list to a running single-step deployment;
 * the supervisor delivers it to the warm agent, which swaps its sources in
 * place. `sources` is non-empty (mirroring the deploy frame's per-step
 * arrays); element 0 is the active source and the producer sets
 * `defaultSource` to its id. Responds with session.ack or session.error.
 */
export const SourcesUpdateFrame = type({
  type: "'sources.update'",
  requestId: "string",
  agentAddress: "string",
  sources: InferenceSource.array().atLeastLength(1),
  defaultSource: "string",
});
export type SourcesUpdateFrame = typeof SourcesUpdateFrame.infer;

/**
 * Push refreshed credential material to a running deployment; the supervisor
 * forwards it to the child's in-memory cell. The child MERGES `delivery`
 * (materials upsert by credentialId, bindings by consumer-and-handle) and
 * drops each credentialId in `revoke` plus any binding referencing it.
 * Removal is explicit through `revoke` — omitting a material does not evict
 * it, since the cell has several independently-scoped producers. A pure
 * revocation carries an empty `delivery`.
 */
export const CredentialsUpdateFrame = type({
  type: "'credentials.update'",
  requestId: "string",
  agentAddress: "string",
  delivery: CredentialDelivery,
  "revoke?": type("string")
    .array()
    .atMostLength(MAX_CREDENTIAL_REVOCATIONS_FRAME),
});
export type CredentialsUpdateFrame = typeof CredentialsUpdateFrame.infer;

// ---------------------------------------------------------------------------
// Pack transport (bidirectional)
// ---------------------------------------------------------------------------
//
// Git pack data is streamed over the existing JSON WebSocket. Chunks are
// base64-encoded; a transfer is a sequence of repo.pack.push frames followed
// by repo.pack.done, correlated by transferId, answered with repo.pack.ack
// or repo.pack.reject.
//
// Each pack frame carries two complementary addressing fields:
//
//   - `agentAddress` — the destination agent on the receiving sidecar.
//   - `repoId` — the source repo at the hub. For `repoId.kind ===
//     "agent-state"`, `repoId.id` is the run address, so both fields carry
//     the same value.
//
// Flow control is deferred: agent deploy trees are small enough to push all
// chunks without windowing.

/**
 * Tag identifying a kind of repository in the hub's kind-keyed RepoStore.
 * Lives in `@intx/types` because the wire-level pack frames reference it.
 */
export const RepoKind = type.enumerated(
  "agent-state",
  "skill",
  "package-registry",
  "workflow",
  "workflow-run",
);
export type RepoKind = typeof RepoKind.infer;

/**
 * Operations a principal may invoke against a repo in the RepoStore. Lives
 * in `@intx/types` so storage layers can validate persisted action
 * vocabularies without depending on the substrate package.
 */
export const RepoAction = type.enumerated(
  "init",
  "writeTree",
  "receivePack",
  "createPack",
  "resolveRef",
);
export type RepoAction = typeof RepoAction.infer;

/**
 * Hub-side identity of a repository in the RepoStore, carried alongside
 * `agentAddress` so the hub maps a pack back to the originating repo.
 */
export const RepoId = type({
  kind: RepoKind,
  id: "string",
});
export type RepoId = typeof RepoId.infer;

/**
 * A chunk of git pack data, split into chunks of at most 64 KiB (before
 * base64 encoding) and sent in order. `seq` is monotonically increasing per
 * transferId, starting at 0; the receiver must reject the transfer on a gap.
 */
export const PackPushFrame = type({
  type: "'repo.pack.push'",
  agentAddress: "string",
  repoId: RepoId,
  transferId: "string",
  seq: "number",
  data: "string",
});
export type PackPushFrame = typeof PackPushFrame.infer;

/**
 * Signals the end of a pack transfer. The receiver applies the pack and
 * updates `ref` to point at `commitSha`, rejecting with "sha_mismatch" if
 * the post-apply HEAD does not match.
 *
 * When `mountPath` is set, the receiver materializes the pack at
 * `workspace/<mountPath>/` instead of the hardcoded agent deploy tree; the
 * receiver distinguishes the paths by `repoId.kind`.
 */
export const PackDoneFrame = type({
  type: "'repo.pack.done'",
  agentAddress: "string",
  repoId: RepoId,
  transferId: "string",
  ref: "string",
  commitSha: "string",
  "mountPath?": "string",
});
export type PackDoneFrame = typeof PackDoneFrame.infer;

/**
 * Receiver acknowledges successful application of a pack transfer.
 */
export const PackAckFrame = type({
  type: "'repo.pack.ack'",
  agentAddress: "string",
  repoId: RepoId,
  transferId: "string",
});
export type PackAckFrame = typeof PackAckFrame.infer;

export const PackRejectReason = type.enumerated(
  "signature_invalid",
  "path_violation",
  "conflict",
  "corrupt",
  "sha_mismatch",
  "timeout",
);
export type PackRejectReason = typeof PackRejectReason.infer;

/**
 * Receiver rejects a pack transfer.
 */
export const PackRejectFrame = type({
  type: "'repo.pack.reject'",
  agentAddress: "string",
  repoId: RepoId,
  transferId: "string",
  // A plain string, NOT the `PackRejectReason` enum: a reason a newer peer
  // added must still pass `HubFrame` validation and reach the reject handler
  // (which latches the transfer) rather than being dropped and stalling the
  // transfer until the next disconnect. Producers still construct through
  // `PackRejectReason`; the reader treats any reason as a terminal reject.
  reason: "string",
  // Optional human-readable cause alongside the machine reason.
  "detail?": "string",
});
export type PackRejectFrame = typeof PackRejectFrame.infer;

/**
 * Categories of deploy-apply failure surfaced by the sidecar's tool-package
 * loader; each maps one-to-one to a point in the apply pipeline.
 *
 *   tarball.missing — asset-sourced tarball absent at its recorded path.
 *   asset.mount.missing — `kind:"asset"` entry names an `assetId` the
 *     deploy's `deploy/asset-mounts.json` does not cover.
 *   integrity.mismatch — fetched bytes do not match the pinned SRI.
 *   registry.fetch.failed / registry.unknown / registry.auth.failed — the
 *     registry refused, is not configured, or rejected the credentials.
 *   tarball.extract.failed — extraction failed or produced a malformed tree.
 *   git.materialization.failed — a git-sourced entry could not be
 *     materialized, or reached a loader that does not materialize git sources.
 *   manifest.invalid — the manifest failed JSON.parse or arktype validation.
 *   package.entry.missing / package.entry.invalid — package.json lacked
 *     `interchange.tools`, or the module exported no AnnotatedToolFactory.
 *   factory.construct.failed — a factory threw or needed a missing
 *     capability key.
 *   tool.name.duplicate — a tool name registered twice. The cross-bundle
 *     case is rejected at apply time; the intra-bundle case surfaces at
 *     first agent construction with the same category.
 *   apply.swap.failed — DEPRECATED, no longer emitted; retained for wire
 *     compatibility so an older sidecar's frame still validates on a newer
 *     hub.
 *   apply.previous-rotation.failed — the `active-deploy-id` commit degraded
 *     (no-fsync / dirty-marker fallback). The deploy is logically live, so
 *     `previousDeployId` carries the NEW deploy id.
 */
export const DeployApplyErrorCategory = type.enumerated(
  "tarball.missing",
  "asset.mount.missing",
  "integrity.mismatch",
  "registry.fetch.failed",
  "registry.unknown",
  "registry.auth.failed",
  "tarball.extract.failed",
  "git.materialization.failed",
  "manifest.invalid",
  "package.entry.missing",
  "package.entry.invalid",
  "factory.construct.failed",
  "tool.name.duplicate",
  "apply.swap.failed",
  "apply.previous-rotation.failed",
);
export type DeployApplyErrorCategory = typeof DeployApplyErrorCategory.infer;

/**
 * Hub requests the sidecar to push its current agent state: pack.push frames
 * followed by pack.done on the same transferId.
 */
export const SyncRequestFrame = type({
  type: "'sync.request'",
  agentAddress: "string",
  transferId: "string",
});
export type SyncRequestFrame = typeof SyncRequestFrame.infer;

// ---------------------------------------------------------------------------
// Workflow probe (bidirectional)
// ---------------------------------------------------------------------------
//
// A probe asks a connected sidecar to inspect a code-sourced workflow WITHOUT
// deploying it: materialize the frozen closure, evaluate the entry module,
// project it to its inert needs surface, and return the projection plus the
// derived grant set and content hash. Correlated by `requestId`, independent
// of the address maps — a token-authed sidecar can serve a probe with no
// agent deployed.

/**
 * Hub asks a connected sidecar to probe a code-sourced workflow, correlated
 * by `requestId` and answered with `workflow.probe.result` or
 * `workflow.probe.error` on the same id.
 *
 * The frame carries everything the probe child needs with no further hub
 * round-trip: `source` (registry, package-registry asset, or git asset),
 * `closure` (the frozen dependency closure the hub resolved), `entry` (the
 * `interchange.workflow` module path to evaluate), and optional `assets`
 * delivered inline. Inline delivery suits a single-shot request that already
 * buffers the whole frame; the sidecar caps the total inline payload.
 */
export const WorkflowProbeRequestFrame = type({
  type: "'workflow.probe.request'",
  requestId: "string",
  source: WorkflowDefinitionSource,
  closure: ToolPackageManifest,
  entry: "string",
  "assets?": WorkflowSourceAssetMount.array(),
});
export type WorkflowProbeRequestFrame = typeof WorkflowProbeRequestFrame.infer;

/**
 * A connected sidecar's answer to a `workflow.probe.request`, correlated by
 * `requestId`: the inert needs-surface projection, the derived inert grant
 * set, and the projection's content hash.
 *
 * `grants` is the deduped, sorted union of every step's grant strings, for
 * pre-deploy operator inspection. `wireHash` is the hex SHA-256 of the
 * projection's canonical JSON (`computeWireDefinitionHash`).
 *
 * `grantWalkSnapshot` is the UN-flattened capability walk `grants` derives
 * from — per-step declarations, effect data, and full grantRequirements —
 * so a later persist step can record the complete walk, not just the union.
 */
export const WorkflowProbeResultFrame = type({
  type: "'workflow.probe.result'",
  requestId: "string",
  projection: WorkflowProjectionDefinition,
  grants: type("string").array().atMostLength(MAX_PROBE_GRANTS_FRAME),
  grantWalkSnapshot: GrantWalkSnapshot,
  wireHash: "string",
});
export type WorkflowProbeResultFrame = typeof WorkflowProbeResultFrame.infer;

/**
 * A connected sidecar reports that a `workflow.probe.request` failed
 * (materialization, evaluation, projection, or hashing threw). Correlated by
 * `requestId`; `error` describes the failure.
 */
export const WorkflowProbeErrorFrame = type({
  type: "'workflow.probe.error'",
  requestId: "string",
  error: "string",
});
export type WorkflowProbeErrorFrame = typeof WorkflowProbeErrorFrame.infer;

// ---------------------------------------------------------------------------
// Discriminated frame unions
// ---------------------------------------------------------------------------

/** All frame types the sidecar sends to the hub. */
export const SidecarFrame = type.or(
  RegisterFrame,
  ReconnectFrame,
  AgentDeployAckFrame,
  AgentErrorFrame,
  MailOutboundFrame,
  AgentEventFrame,
  ConnectorStateChangedFrame,
  PingFrame,
  SessionAckFrame,
  SessionErrorFrame,
  AgentUndeployAckFrame,
  SignalCorrelationRegisterFrame,
  PackPushFrame,
  PackDoneFrame,
  PackAckFrame,
  PackRejectFrame,
  MailInboundAckFrame,
  WorkflowProbeResultFrame,
  WorkflowProbeErrorFrame,
  WorkflowControlAckFrame,
);
export type SidecarFrame = typeof SidecarFrame.infer;

/** All frame types the hub sends to the sidecar. */
export const HubFrame = type.or(
  MailInboundFrame,
  AgentDeployFrame,
  AgentUndeployFrame,
  PongFrame,
  SourcesUpdateFrame,
  CredentialsUpdateFrame,
  PackPushFrame,
  PackDoneFrame,
  PackAckFrame,
  PackRejectFrame,
  SyncRequestFrame,
  SignalDeliverFrame,
  RunGrantsFrame,
  SenderKeyRefreshFrame,
  SenderKeyEvictFrame,
  SignalCorrelationRegisterAckFrame,
  DrainDeliverFrame,
  WorkflowProbeRequestFrame,
  WorkflowControlFrame,
);
export type HubFrame = typeof HubFrame.infer;

/** Any frame on the wire, regardless of direction. */
export const WireFrame = SidecarFrame.or(HubFrame);
export type WireFrame = typeof WireFrame.infer;
