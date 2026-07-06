// Websocket wire protocol for hub↔sidecar communication.
//
// One websocket connection per sidecar↔hub pair. All traffic is multiplexed
// as JSON frames with a `type` discriminator. The sidecar initiates the
// connection; the hub is the server.
//
// Mail bytes are base64-encoded in JSON frames. Binary frames would be more
// efficient but JSON is simpler to debug and inspect.

import { type } from "arktype";
import {
  ConnectorThreadState,
  HarnessConfig,
  InferenceEvent,
  InferenceSource,
} from "./runtime";

// ---------------------------------------------------------------------------
// Sidecar → Hub
// ---------------------------------------------------------------------------

/**
 * Sent on first connect when the sidecar has no existing agents in its data
 * directory. Identifies the sidecar and declares it ready to receive
 * agent.deploy frames.
 */
export const RegisterFrame = type({
  type: "'register'",
  sidecarId: "string",
  token: "string",
  agentAddresses: "string[]",
  // Live workflow-substrate deployment addresses (ins_dep_...) this sidecar
  // currently hosts. Unlike `agentAddresses`, these are hub-minted and
  // carry no per-address key, so the hub re-registers them for routing
  // directly (no challenge) -- the same way they were first registered at
  // deploy time. Absent on sidecars/paths that host none.
  "workflowAddresses?": "string[]",
});
export type RegisterFrame = typeof RegisterFrame.infer;

/**
 * Sent on connect when the sidecar has existing agent repositories from a
 * previous run. Lists the agent addresses it can serve, triggering the
 * challenge/response verification flow.
 */
export const ReconnectFrame = type({
  type: "'reconnect'",
  sidecarId: "string",
  token: "string",
  agentAddresses: "string[]",
  // See `RegisterFrame.workflowAddresses`. Carried on the reconnect frame too
  // so a sidecar restoring both single-agent sessions and workflow
  // deployments re-registers both in one connect.
  "workflowAddresses?": "string[]",
  "deployRefs?": "Record<string, string>",
});
export type ReconnectFrame = typeof ReconnectFrame.infer;

/**
 * Response to a challenge frame. Contains a signature per agent address
 * proving the sidecar holds the private key. Each signature is computed
 * over `nonce || utf8(agentAddress)`.
 */
export const ChallengeResponseFrame = type({
  type: "'challenge.response'",
  responses: type({ address: "string", signature: "string" }).array(),
});
export type ChallengeResponseFrame = typeof ChallengeResponseFrame.infer;

/**
 * Acknowledges a successful agent deployment. Includes the agent's Ed25519
 * public key (hex-encoded) so the hub can verify ownership on reconnect.
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
 * should route the message to its recipients. When `delivered` is true the
 * message was already delivered locally and is forwarded for audit/projection
 * only — the hub must not re-route it.
 *
 * Structured metadata (senderAddress, messageId, to, cc) is available for
 * audit and projection purposes without parsing the raw MIME bytes.
 */
export const MailOutboundFrame = type({
  type: "'mail.outbound'",
  rawMessage: "string",
  recipients: "string[]",
  "senderAddress?": "string",
  "sessionId?": "string",
  "messageId?": "string",
  "to?": "string[]",
  "cc?": "string[]",
  "delivered?": "boolean",
});
export type MailOutboundFrame = typeof MailOutboundFrame.infer;

/**
 * An InferenceEvent from the reactor, forwarded for UI consumption. Tagged
 * with the agent address so the hub can route to the correct UI client.
 */
export const AgentEventFrame = type({
  type: "'agent.event'",
  agentAddress: "string",
  sessionId: "string",
  event: InferenceEvent,
});
export type AgentEventFrame = typeof AgentEventFrame.infer;

/**
 * Notifies the hub that the agent's connector-thread state has changed.
 * The sidecar emits this when the harness's connector router commits a
 * start/continue decision, when an outbound reply advances the
 * lastMessageId, and when load-time restore brings persisted state into
 * memory. The hub uses the cached state to set threading headers on
 * user-originated mail so the harness routes it as `continue` rather
 * than `passthrough`.
 *
 * `connectorState` is `null` when no active thread exists.
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
 * Acknowledges that an agent has been fully undeployed: harness stopped,
 * state pushed (best-effort), and directory deleted.
 */
export const AgentUndeployAckFrame = type({
  type: "'agent.undeploy.ack'",
  agentAddress: "string",
  statePushed: "boolean",
});
export type AgentUndeployAckFrame = typeof AgentUndeployAckFrame.infer;

// ---------------------------------------------------------------------------
// Hub → Sidecar
// ---------------------------------------------------------------------------

/**
 * A message to deliver to a local agent's INBOX. The hub routes inbound
 * mail (from UI users, from agents on other sidecars) to the correct
 * sidecar connection.
 */
export const MailInboundFrame = type({
  type: "'mail.inbound'",
  agentAddress: "string",
  rawMessage: "string",
});
export type MailInboundFrame = typeof MailInboundFrame.infer;

/**
 * Deliver a workflow-run signal to a multi-step deployment's
 * supervisor. The hub forwards the frame to the sidecar that hosts the
 * deployment named by `agentAddress` (the deployment-level mail
 * address). The sidecar's hub-link routes the frame into the matching
 * supervisor's `deliverSignal`, which sends a `signal.deliver` control
 * IPC frame to the workflow-process child. The child commits the
 * `SignalReceived` event through its own substrate -- the single
 * writer of the workflow-run repo on the sidecar side -- so the
 * pack-push pipeline that propagates the commit to the hub never sees
 * a concurrent writer at the same ref.
 *
 * `signalId` is supplied by the producer so the workflow-run state
 * machine's dedup index (`observedSignalIds`) rejects a duplicate
 * delivery cleanly; a fresh value per call is the producer's
 * responsibility.
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
 * Deliver a workflow-host drain control payload to a multi-step
 * deployment's supervisor. The hub forwards the frame to the sidecar
 * that hosts the deployment named by `agentAddress` (the
 * deployment-level mail address). The sidecar's hub-link routes the
 * frame into the matching supervisor's `drain`, which sends a `drain`
 * control IPC frame to the workflow-process child and arms one
 * `drainTimeout` accumulator per in-flight run. Cancel-mode in-flight
 * steps abort on the child side as the controller's signal flips;
 * wait-mode steps continue. Each accumulator commits a signed
 * `CancelRequested{origin: "supervisor-drain"}` against the
 * workflow-run repo through the supervisor's substrate when the
 * deadline expires.
 *
 * `deadlineMs` is the wire-level policy hint the child echoes in its
 * logs. The supervisor's accumulator is driven by its own bindings'
 * `drainTimeoutMs` -- a per-deployment operator setting -- not by this
 * value; the wire field exists so the child's log reflects the
 * caller's intent.
 */
export const DrainDeliverFrame = type({
  type: "'drain.deliver'",
  agentAddress: "string",
  deadlineMs: "number",
});
export type DrainDeliverFrame = typeof DrainDeliverFrame.infer;

/**
 * Workflow projection carried on an `agent.deploy` frame. Its presence
 * at the deploy router routes the frame to the workflow deploy path --
 * single- or multi-step, both of which spawn the workflow-process child
 * -- as opposed to a per-step provision frame.
 *
 * `definition` is the wire projection of `WorkflowDefinition` from
 * `@intx/workflow`. The arktype validator enforces the structural
 * envelope the workflow-process child re-parses on the sidecar after
 * materialization (`packages/hub-sessions/src/workflow-kind.ts`'s
 * `workflowDefinitionEnvelopeSchema`): `id`, `triggers`, `steps`,
 * `stepOrder`, optional `state`. The wire validator MUST require every
 * field the envelope requires — the sidecar's deploy router serializes
 * `projection.definition` verbatim into `workflow.json` and the child
 * rejects a tree missing any envelope-required field. Deeper validation
 * of authoring-time primitive shape lives on the workflow definition
 * surface in `@intx/workflow`, not on the wire.
 *
 * `sources` pins an ordered, non-empty inference-source list per step in
 * `definition.stepOrder` so the workflow-process child can resolve inference
 * at step invocation without a round trip to the hub. The list is the step's
 * failover chain: element 0 is the active source (its id is the step's
 * `defaultSource`), and the reactor fails over forward through the tail on a
 * transient inference error. A workflow step pins a single-element list (no
 * per-step failover); a single-agent instance pins the instance's full
 * ordered source chain. Every `stepOrder` entry must have a matching
 * `sources` entry; the validator rejects frames that violate this at the
 * boundary.
 */
export const AgentDeployWorkflow = type({
  definition: type({
    id: "string > 0",
    triggers: "unknown[]",
    stepOrder: "string[]",
    steps: { "[string]": "unknown" },
    "state?": "Record<string, unknown>",
    "+": "delete",
  }),
  sources: { "[string]": InferenceSource.array().atLeastLength(1) },
}).narrow((value, ctx) => {
  for (const stepId of value.definition.stepOrder) {
    if (!Object.prototype.hasOwnProperty.call(value.sources, stepId)) {
      return ctx.mustBe(
        `a workflow projection whose sources cover every step in stepOrder; ${JSON.stringify(stepId)} is missing`,
      );
    }
  }
  return true;
});
export type AgentDeployWorkflow = typeof AgentDeployWorkflow.infer;

/**
 * Deploy an agent to this sidecar. The sidecar spawns a supervised
 * workflow-process child to host the deployment.
 *
 * The deploy router discriminates two shapes by field presence without
 * consulting `config`:
 *   - `workflow` set: a workflow deployment (single-step head or multi-step)
 *     that spawns the supervised workflow-process child.
 *   - `provisionStep` true: a no-spawn per-step provision of a multi-step
 *     deploy -- the sidecar initializes the step's agent-state repo and
 *     records the hub key so the follow-up deploy pack applies and verifies,
 *     but spawns nothing. The deployment-level `workflow` frame (sent once
 *     after every step is provisioned) spawns the child.
 * A frame carrying neither is rejected -- there is no in-process
 * fall-through. `workflow` and `provisionStep` are mutually exclusive.
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
 * Remove an agent from this sidecar. The sidecar tears down the harness,
 * pushes state to the hub (best-effort), deletes the agent directory, and
 * responds with agent.undeploy.ack.
 */
export const AgentUndeployFrame = type({
  type: "'agent.undeploy'",
  agentAddress: "string",
  reason: "string",
});
export type AgentUndeployFrame = typeof AgentUndeployFrame.infer;

/**
 * Per-address cryptographic challenge. The sidecar must sign
 * `nonce || utf8(address)` with each agent's private key and respond
 * with a challenge.response frame.
 */
export const ChallengeFrame = type({
  type: "'challenge'",
  challenges: type({ address: "string", nonce: "string" }).array(),
});
export type ChallengeFrame = typeof ChallengeFrame.infer;

/**
 * Sent when challenge verification fails for a specific address.
 */
export const ChallengeFailedFrame = type({
  type: "'challenge.failed'",
  address: "string",
  reason: "string",
});
export type ChallengeFailedFrame = typeof ChallengeFailedFrame.infer;

/**
 * Keepalive pong sent by the hub in response to a ping frame.
 * If the sidecar stops receiving pongs, it considers the hub dead.
 */
export const PongFrame = type({ type: "'pong'" });
export type PongFrame = typeof PongFrame.infer;

/**
 * Push an updated inference-source list to a running single-step
 * deployment. The sidecar routes it to the deployment's supervisor, which
 * delivers it to the warm agent and swaps its sources in place; element 0
 * of `sources` is the active source and must equal `defaultSource`.
 * Responds with session.ack or session.error.
 */
export const SourcesUpdateFrame = type({
  type: "'sources.update'",
  requestId: "string",
  agentAddress: "string",
  sources: InferenceSource.array(),
  defaultSource: "string",
});
export type SourcesUpdateFrame = typeof SourcesUpdateFrame.infer;

// ---------------------------------------------------------------------------
// Pack transport (bidirectional)
// ---------------------------------------------------------------------------
//
// Git pack data is streamed between hub and sidecar over the existing JSON
// WebSocket. Chunks are base64-encoded (matching the mail convention above).
// A transfer is a sequence of repo.pack.push frames followed by a
// repo.pack.done, correlated by transferId. The receiver responds with
// repo.pack.ack or repo.pack.reject.
//
// Each pack frame carries two complementary addressing fields:
//
//   - `agentAddress` identifies the destination agent on the receiving
//     sidecar. The sidecar manages per-agent state and uses this field to
//     route the pack to the correct workspace. For agent-state packs the
//     sidecar applies the pack onto the agent's deploy/state tree.
//
//   - `repoId` identifies the source repo at the hub. The hub maps `repoId`
//     to the originating entry in its kind-keyed RepoStore. For
//     `repoId.kind === "agent-state"`, `repoId.id` is the agent address
//     (the deploy/state repo and the destination agent are the same), so
//     the two fields carry the same value. Future kinds (e.g. assets) use
//     `repoId` to name a non-agent source while `agentAddress` continues
//     to address the destination agent.
//
// Flow control: deferred. Agent deploy trees are small enough that the sender
// can push all chunks without windowing. If this becomes a problem, a credit-
// based mechanism can be added later.

/**
 * Tag identifying a kind of repository in the hub's kind-keyed RepoStore.
 * Lives in `@intx/types` because the wire-level pack frames reference it;
 * the substrate package re-exports it for handler authors.
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
 * Operations a principal may invoke against a repo in the RepoStore.
 * Lives in `@intx/types` so storage layers (e.g. `@intx/db`) can validate
 * persisted action vocabularies without depending on the substrate
 * package. The substrate re-exports it for handler authors.
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
 * Hub-side identity of a repository in the RepoStore. Pack frames carry
 * this alongside `agentAddress` so the hub can map a pack back to the
 * originating repo independently of which sidecar/agent it is destined for.
 */
export const RepoId = type({
  kind: RepoKind,
  id: "string",
});
export type RepoId = typeof RepoId.infer;

/**
 * A chunk of git pack data. The sender splits the packfile into chunks of at
 * most 64 KiB (before base64 encoding) and sends them in order.
 *
 * `seq` is monotonically increasing per transferId, starting at 0. The
 * receiver must reject the transfer if a gap is detected.
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
 * updates `ref` to point at `commitSha`. If the post-apply HEAD does not
 * match `commitSha`, the receiver must reject with reason "sha_mismatch".
 *
 * When `mountPath` is set, the receiver materializes the pack at
 * `workspace/<mountPath>/` instead of the hardcoded agent deploy tree.
 * Absent for the agent-state deploy/state flows, which continue to apply
 * the pack to the agent's repo as before.
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
  reason: PackRejectReason,
});
export type PackRejectFrame = typeof PackRejectFrame.infer;

/**
 * Categories of deploy-apply failure surfaced by the sidecar's
 * tool-package loader. Each value maps one-to-one to a distinct point in
 * the apply pipeline; a single category fires per failed attempt.
 *
 *   tarball.missing          — a manifest entry's asset-sourced tarball
 *                              is not present at the recorded path.
 *   asset.mount.missing      — a `kind: "asset"` manifest entry names
 *                              an `assetId` that the deploy pack's
 *                              `deploy/asset-mounts.json` does not
 *                              cover. Indicates a mismatch between the
 *                              resolver's view of attached assets and
 *                              the materialization fan-out, not a
 *                              missing file on disk.
 *   integrity.mismatch       — fetched tarball bytes do not match the
 *                              manifest's pinned SRI integrity.
 *   registry.fetch.failed    — the configured registry refused or
 *                              dropped the request for a tarball.
 *   registry.unknown         — the manifest entry references a registry
 *                              name not present in the sidecar's
 *                              registry config.
 *   registry.auth.failed     — the registry rejected the sidecar's
 *                              credentials.
 *   tarball.extract.failed   — tar extraction failed or the extracted
 *                              tree was malformed.
 *   manifest.invalid         — the manifest itself did not validate
 *                              at the loader boundary (JSON.parse
 *                              failure or arktype schema failure).
 *                              Peer-dependency violations are caught
 *                              earlier by the hub's resolver and
 *                              surface as a launch failure rather
 *                              than this frame.
 *   package.entry.missing    — a top-level package's package.json had
 *                              no `interchange.tools` field.
 *   package.entry.invalid    — the resolved `interchange.tools` module
 *                              exported nothing that looked like an
 *                              AnnotatedToolFactory.
 *   factory.construct.failed — a factory invocation threw, or required
 *                              a capability key the env did not provide.
 *   tool.name.duplicate      — a tool name is registered more than
 *                              once in the apply's loaded set. The
 *                              cross-bundle case (two pinned packages
 *                              share a bundle id, producing colliding
 *                              prefixed tool names) is rejected at
 *                              apply time, before the caller commits.
 *                              The intra-bundle case (one package
 *                              exports two definitions sharing a raw
 *                              name) surfaces at first agent
 *                              construction with the same category
 *                              instead of apply rejection: the loader
 *                              cannot see `bundle.definitions` without
 *                              invoking the factory, and the `BaseEnv`
 *                              the factory needs is constructed by the
 *                              sidecar harness AFTER the commit. Both
 *                              paths carry the same category so the
 *                              operator-facing failure shape is
 *                              uniform regardless of which check
 *                              fired; only the channel (apply.error
 *                              frame vs runtime construct failure)
 *                              differs.
 *   apply.swap.failed        — DEPRECATED, no longer emitted. The apply
 *                              protocol stages each deploy into a stable
 *                              per-deploy-id directory and commits via a
 *                              single `active-deploy-id` file write, so
 *                              there is no filesystem rename that can
 *                              fail. The value is retained in the enum
 *                              for wire compatibility: during a rolling
 *                              upgrade an older sidecar can still emit
 *                              it, and dropping the member would make a
 *                              newer hub's frame validator reject that
 *                              frame.
 *   apply.previous-rotation.failed
 *                            — every loaded factory validated and the
 *                              new deploy was staged, but persisting the
 *                              instance's `active-deploy-id` file (the
 *                              commit) degraded: the id was written
 *                              through the no-fsync / dirty-marker
 *                              fallback ladder rather than durably
 *                              flushed. The new deploy is logically
 *                              live, so `previousDeployId` on this
 *                              failure carries the NEW deploy id rather
 *                              than the pre-apply one. The next boot
 *                              reconciles the recorded id from the dirty
 *                              marker.
 */
export const DeployApplyErrorCategory = type.enumerated(
  "tarball.missing",
  "asset.mount.missing",
  "integrity.mismatch",
  "registry.fetch.failed",
  "registry.unknown",
  "registry.auth.failed",
  "tarball.extract.failed",
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
 * Sidecar reports a failed deploy-apply attempt back to the hub.
 *
 * `previousDeployId` is the atomicity contract for every category
 * except `apply.previous-rotation.failed`: it names the deploy the
 * instance was running before the rejected attempt, and after this
 * frame is emitted the instance continues to run that deploy untouched.
 *
 * For `apply.previous-rotation.failed` the field has inverted meaning.
 * The new deploy was staged and its `active-deploy-id` commit was
 * written through the degradation ladder (so the new deploy is live)
 * before the persist was found to be non-durable; the field carries
 * the **new** deploy id so the hub can record the on-disk truth rather
 * than a stale pre-apply id. The atomicity contract is preserved in
 * the sense that the field always reflects the deploy id the instance
 * is now running — what shifts is whether "now" is pre-apply or
 * post-apply, depending on whether the commit landed before the
 * failure. Renaming the field to make this explicit would break the
 * wire shape; the category's semantics document the override instead.
 *
 * `package` is set when the failure implicates a specific manifest
 * entry; deploy-wide failures (manifest.invalid) omit it.
 */
export const DeployApplyErrorFrame = type({
  type: "'deploy.apply.error'",
  agentAddress: "string",
  attemptId: "string",
  previousDeployId: "string",
  category: DeployApplyErrorCategory,
  message: "string",
  "package?": type({
    name: "string",
    version: "string",
  }),
  occurredAt: "string",
});
export type DeployApplyErrorFrame = typeof DeployApplyErrorFrame.infer;

/**
 * Hub requests the sidecar to push its current agent state. The sidecar
 * responds by sending pack.push frames followed by pack.done using the
 * same transferId.
 */
export const SyncRequestFrame = type({
  type: "'sync.request'",
  agentAddress: "string",
  transferId: "string",
});
export type SyncRequestFrame = typeof SyncRequestFrame.infer;

// ---------------------------------------------------------------------------
// Discriminated frame unions
// ---------------------------------------------------------------------------

/** All frame types the sidecar sends to the hub. */
export const SidecarFrame = RegisterFrame.or(ReconnectFrame)
  .or(ChallengeResponseFrame)
  .or(AgentDeployAckFrame)
  .or(AgentErrorFrame)
  .or(MailOutboundFrame)
  .or(AgentEventFrame)
  .or(ConnectorStateChangedFrame)
  .or(PingFrame)
  .or(SessionAckFrame)
  .or(SessionErrorFrame)
  .or(AgentUndeployAckFrame)
  .or(PackPushFrame)
  .or(PackDoneFrame)
  .or(PackAckFrame)
  .or(PackRejectFrame)
  .or(DeployApplyErrorFrame);
export type SidecarFrame = typeof SidecarFrame.infer;

/** All frame types the hub sends to the sidecar. */
export const HubFrame = MailInboundFrame.or(AgentDeployFrame)
  .or(AgentUndeployFrame)
  .or(ChallengeFrame)
  .or(ChallengeFailedFrame)
  .or(PongFrame)
  .or(SourcesUpdateFrame)
  .or(PackPushFrame)
  .or(PackDoneFrame)
  .or(PackAckFrame)
  .or(PackRejectFrame)
  .or(SyncRequestFrame)
  .or(SignalDeliverFrame)
  .or(DrainDeliverFrame);
export type HubFrame = typeof HubFrame.infer;

/** Any frame on the wire, regardless of direction. */
export const WireFrame = SidecarFrame.or(HubFrame);
export type WireFrame = typeof WireFrame.infer;
