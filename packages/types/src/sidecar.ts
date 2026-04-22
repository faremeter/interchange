// Websocket wire protocol for hub↔sidecar communication.
//
// One websocket connection per sidecar↔hub pair. All traffic is multiplexed
// as JSON frames with a `type` discriminator. The sidecar initiates the
// connection; the hub is the server.
//
// Mail bytes are base64-encoded in JSON frames. Binary frames would be more
// efficient but JSON is simpler to debug and inspect.

import { type } from "arktype";
import { AbortReason, HarnessConfig, InferenceEvent } from "./runtime";
import { WireGrantRule } from "./grant-wire";

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
 * A message from a local agent to a remote recipient. The sidecar's
 * InMemoryTransport calls onRemoteSend, which serializes the signed MIME
 * bytes and pushes this frame.
 */
export const MailOutboundFrame = type({
  type: "'mail.outbound'",
  rawMessage: "string",
  recipients: "string[]",
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
 * Keepalive ping sent by the sidecar. The hub responds with a pong frame.
 * If the hub stops receiving pings, it considers the sidecar dead.
 */
export const PingFrame = type({ type: "'ping'" });
export type PingFrame = typeof PingFrame.infer;

/**
 * Acknowledges a request from the hub (session.abort, grants.update).
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
 * Acknowledges that the inference session has started for a provisioned
 * agent. Sent in response to a session.start frame after the harness is
 * running.
 */
export const SessionStartAckFrame = type({
  type: "'session.start.ack'",
  agentAddress: "string",
});
export type SessionStartAckFrame = typeof SessionStartAckFrame.infer;

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
 * Deploy an agent to this sidecar. The sidecar initializes a harness from
 * the config.
 */
export const AgentDeployFrame = type({
  type: "'agent.deploy'",
  agentAddress: "string",
  agentId: "string",
  config: HarnessConfig,
  hubPublicKey: "string",
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
 * Start the inference session for a provisioned agent. Sent after the
 * deploy pack has been applied so the harness can read deploy-tree tools
 * and prompt from disk.
 */
export const SessionStartFrame = type({
  type: "'session.start'",
  agentAddress: "string",
});
export type SessionStartFrame = typeof SessionStartFrame.infer;

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
 * Kill switch. Aborts a running agent immediately with the given reason.
 * Responds with session.ack or session.error.
 */
export const SessionAbortFrame = type({
  type: "'session.abort'",
  requestId: "string",
  agentAddress: "string",
  reason: AbortReason,
});
export type SessionAbortFrame = typeof SessionAbortFrame.infer;

/**
 * Keepalive pong sent by the hub in response to a ping frame.
 * If the sidecar stops receiving pongs, it considers the hub dead.
 */
export const PongFrame = type({ type: "'pong'" });
export type PongFrame = typeof PongFrame.infer;

/**
 * Push updated grants to a running agent. The sidecar replaces the agent's
 * grant snapshot and re-persists the config. Responds with session.ack or
 * session.error.
 */
export const GrantsUpdateFrame = type({
  type: "'grants.update'",
  requestId: "string",
  agentAddress: "string",
  grants: WireGrantRule.array(),
});
export type GrantsUpdateFrame = typeof GrantsUpdateFrame.infer;

// ---------------------------------------------------------------------------
// Pack transport (bidirectional)
// ---------------------------------------------------------------------------
//
// Git pack data is streamed between hub and sidecar over the existing JSON
// WebSocket. Chunks are base64-encoded (matching the mail convention above).
// A transfer is a sequence of pack.push frames followed by a pack.done,
// correlated by transferId. The receiver responds with pack.ack or pack.reject.
//
// Flow control: deferred. Agent deploy trees are small enough that the sender
// can push all chunks without windowing. If this becomes a problem, a credit-
// based mechanism can be added later.

/**
 * A chunk of git pack data. The sender splits the packfile into chunks of at
 * most 64 KiB (before base64 encoding) and sends them in order.
 *
 * `seq` is monotonically increasing per transferId, starting at 0. The
 * receiver must reject the transfer if a gap is detected.
 */
export const PackPushFrame = type({
  type: "'pack.push'",
  agentAddress: "string",
  transferId: "string",
  seq: "number",
  data: "string",
});
export type PackPushFrame = typeof PackPushFrame.infer;

/**
 * Signals the end of a pack transfer. The receiver applies the pack and
 * updates `ref` to point at `commitSha`. If the post-apply HEAD does not
 * match `commitSha`, the receiver must reject with reason "sha_mismatch".
 */
export const PackDoneFrame = type({
  type: "'pack.done'",
  agentAddress: "string",
  transferId: "string",
  ref: "string",
  commitSha: "string",
});
export type PackDoneFrame = typeof PackDoneFrame.infer;

/**
 * Receiver acknowledges successful application of a pack transfer.
 */
export const PackAckFrame = type({
  type: "'pack.ack'",
  agentAddress: "string",
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
  type: "'pack.reject'",
  agentAddress: "string",
  transferId: "string",
  reason: PackRejectReason,
});
export type PackRejectFrame = typeof PackRejectFrame.infer;

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
  .or(PingFrame)
  .or(SessionAckFrame)
  .or(SessionErrorFrame)
  .or(SessionStartAckFrame)
  .or(AgentUndeployAckFrame)
  .or(PackPushFrame)
  .or(PackDoneFrame)
  .or(PackAckFrame)
  .or(PackRejectFrame);
export type SidecarFrame = typeof SidecarFrame.infer;

/** All frame types the hub sends to the sidecar. */
export const HubFrame = MailInboundFrame.or(AgentDeployFrame)
  .or(AgentUndeployFrame)
  .or(SessionStartFrame)
  .or(ChallengeFrame)
  .or(ChallengeFailedFrame)
  .or(PongFrame)
  .or(SessionAbortFrame)
  .or(GrantsUpdateFrame)
  .or(PackPushFrame)
  .or(PackDoneFrame)
  .or(PackAckFrame)
  .or(PackRejectFrame)
  .or(SyncRequestFrame);
export type HubFrame = typeof HubFrame.infer;

/** Any frame on the wire, regardless of direction. */
export const WireFrame = SidecarFrame.or(HubFrame);
export type WireFrame = typeof WireFrame.infer;
