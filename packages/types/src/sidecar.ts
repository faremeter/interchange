// Websocket wire protocol for hub↔sidecar communication.
//
// One websocket connection per sidecar↔hub pair. All traffic is multiplexed
// as JSON frames with a `type` discriminator. The sidecar initiates the
// connection; the hub is the server.
//
// Mail bytes are base64-encoded in JSON frames. Binary frames would be more
// efficient but JSON is simpler to debug and inspect.

import type { AbortReason, HarnessConfig, InferenceEvent } from "./runtime";
import type { GrantRule } from "./authz";

// ---------------------------------------------------------------------------
// Sidecar → Hub
// ---------------------------------------------------------------------------

/**
 * Sent on first connect when the sidecar has no existing agents in its data
 * directory. Identifies the sidecar and declares it ready to receive
 * agent.deploy frames.
 */
export type RegisterFrame = {
  type: "register";
  sidecarId: string;
  token: string;
  agentAddresses: string[];
};

/**
 * Sent on connect when the sidecar has existing agent repositories from a
 * previous run. Lists the agent addresses it can serve, triggering the
 * challenge/response verification flow.
 */
export type ReconnectFrame = {
  type: "reconnect";
  sidecarId: string;
  token: string;
  agentAddresses: string[];
  /** Maps agentAddress to the SHA of the currently applied deploy commit.
   * Absent entries mean the agent has never received a deploy pack. */
  deployRefs?: Record<string, string>;
};

/**
 * Response to a challenge frame. Contains a signature per agent address
 * proving the sidecar holds the private key. Each signature is computed
 * over `nonce || utf8(agentAddress)`.
 */
export type ChallengeResponseFrame = {
  type: "challenge.response";
  responses: { address: string; signature: string }[];
};

/**
 * Acknowledges a successful agent deployment. Includes the agent's Ed25519
 * public key (hex-encoded) so the hub can verify ownership on reconnect.
 */
export type AgentDeployAckFrame = {
  type: "agent.deploy.ack";
  agentAddress: string;
  publicKey: string;
};

/**
 * Reports a failed agent deployment.
 */
export type AgentErrorFrame = {
  type: "agent.error";
  agentAddress: string;
  error: string;
};

/**
 * A message from a local agent to a remote recipient. The sidecar's
 * InMemoryTransport calls onRemoteSend, which serializes the signed MIME
 * bytes and pushes this frame.
 */
export type MailOutboundFrame = {
  type: "mail.outbound";
  rawMessage: string;
  recipients: string[];
};

/**
 * An InferenceEvent from the reactor, forwarded for UI consumption. Tagged
 * with the agent address so the hub can route to the correct UI client.
 */
export type AgentEventFrame = {
  type: "agent.event";
  agentAddress: string;
  sessionId: string;
  event: InferenceEvent;
};

/**
 * Keepalive ping sent by the sidecar. The hub responds with a pong frame.
 * If the hub stops receiving pings, it considers the sidecar dead.
 */
export type PingFrame = {
  type: "ping";
};

/**
 * Acknowledges a request from the hub (message.send, session.abort).
 */
export type SessionAckFrame = {
  type: "session.ack";
  requestId: string;
};

/**
 * Reports an error processing a hub request.
 */
export type SessionErrorFrame = {
  type: "session.error";
  requestId: string;
  error: string;
};

/**
 * Acknowledges that the inference session has started for a provisioned
 * agent. Sent in response to a session.start frame after the harness is
 * running.
 */
export type SessionStartAckFrame = {
  type: "session.start.ack";
  agentAddress: string;
};

/**
 * Acknowledges that an agent has been fully undeployed: harness stopped,
 * state pushed (best-effort), and directory deleted.
 */
export type AgentUndeployAckFrame = {
  type: "agent.undeploy.ack";
  agentAddress: string;
  statePushed: boolean;
};

/** All frame types the sidecar sends to the hub. */
export type SidecarFrame =
  | RegisterFrame
  | ReconnectFrame
  | ChallengeResponseFrame
  | AgentDeployAckFrame
  | AgentErrorFrame
  | MailOutboundFrame
  | AgentEventFrame
  | PingFrame
  | SessionAckFrame
  | SessionErrorFrame
  | SessionStartAckFrame
  | AgentUndeployAckFrame
  | PackPushFrame
  | PackDoneFrame
  | PackAckFrame
  | PackRejectFrame;

// ---------------------------------------------------------------------------
// Hub → Sidecar
// ---------------------------------------------------------------------------

/**
 * A message to deliver to a local agent's INBOX. The hub routes inbound
 * mail (from UI users, from agents on other sidecars) to the correct
 * sidecar connection.
 */
export type MailInboundFrame = {
  type: "mail.inbound";
  agentAddress: string;
  rawMessage: string;
};

/**
 * Deploy an agent to this sidecar. The sidecar initializes a harness from
 * the config. When `restored` is true, the sidecar loads existing context
 * from its local isogit repository rather than starting fresh.
 */
export type AgentDeployFrame = {
  type: "agent.deploy";
  agentAddress: string;
  agentId: string;
  config: HarnessConfig;
  restored?: boolean;
};

/**
 * Remove an agent from this sidecar. The sidecar tears down the harness,
 * pushes state to the hub (best-effort), deletes the agent directory, and
 * responds with agent.undeploy.ack.
 */
export type AgentUndeployFrame = {
  type: "agent.undeploy";
  agentAddress: string;
  reason: string;
};

/**
 * Start the inference session for a provisioned agent. Sent after the
 * deploy pack has been applied so the harness can read deploy-tree tools
 * and prompt from disk.
 */
export type SessionStartFrame = {
  type: "session.start";
  agentAddress: string;
};

/**
 * Per-address cryptographic challenge. The sidecar must sign
 * `nonce || utf8(address)` with each agent's private key and respond
 * with a challenge.response frame.
 */
export type ChallengeFrame = {
  type: "challenge";
  challenges: { address: string; nonce: string }[];
};

/**
 * Sent when challenge verification fails for a specific address.
 */
export type ChallengeFailedFrame = {
  type: "challenge.failed";
  address: string;
  reason: string;
};

/**
 * Kill switch. Aborts a running agent immediately with the given reason.
 * Responds with session.ack or session.error.
 */
export type SessionAbortFrame = {
  type: "session.abort";
  requestId: string;
  agentAddress: string;
  reason: AbortReason;
};

export type WireAttachment = {
  type: string;
  url: string;
  mimeType?: string;
};

/**
 * Keepalive pong sent by the hub in response to a ping frame.
 * If the sidecar stops receiving pongs, it considers the hub dead.
 */
export type PongFrame = {
  type: "pong";
};

/**
 * Deliver a user message to the agent. The sidecar feeds the content into
 * the harness. Responds with session.ack or session.error.
 */
export type MessageSendFrame = {
  type: "message.send";
  requestId: string;
  agentAddress: string;
  sessionId: string;
  content: string;
  attachments?: WireAttachment[];
};

/**
 * Push updated grants to a running agent. The sidecar replaces the agent's
 * grant snapshot and re-persists the config. Responds with session.ack or
 * session.error.
 */
export type GrantsUpdateFrame = {
  type: "grants.update";
  requestId: string;
  agentAddress: string;
  grants: GrantRule[];
};

/** All frame types the hub sends to the sidecar. */
export type HubFrame =
  | MailInboundFrame
  | AgentDeployFrame
  | AgentUndeployFrame
  | SessionStartFrame
  | ChallengeFrame
  | ChallengeFailedFrame
  | PongFrame
  | SessionAbortFrame
  | MessageSendFrame
  | GrantsUpdateFrame
  | PackPushFrame
  | PackDoneFrame
  | PackAckFrame
  | PackRejectFrame
  | SyncRequestFrame;

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
export type PackPushFrame = {
  type: "pack.push";
  agentAddress: string;
  transferId: string;
  seq: number;
  data: string;
};

/**
 * Signals the end of a pack transfer. The receiver applies the pack and
 * updates `ref` to point at `commitSha`. If the post-apply HEAD does not
 * match `commitSha`, the receiver must reject with reason "sha_mismatch".
 */
export type PackDoneFrame = {
  type: "pack.done";
  agentAddress: string;
  transferId: string;
  ref: string;
  commitSha: string;
};

/**
 * Receiver acknowledges successful application of a pack transfer.
 */
export type PackAckFrame = {
  type: "pack.ack";
  agentAddress: string;
  transferId: string;
};

export type PackRejectReason =
  | "signature_invalid"
  | "path_violation"
  | "conflict"
  | "corrupt"
  | "sha_mismatch"
  | "timeout";

/**
 * Receiver rejects a pack transfer.
 */
export type PackRejectFrame = {
  type: "pack.reject";
  agentAddress: string;
  transferId: string;
  reason: PackRejectReason;
};

/**
 * Hub requests the sidecar to push its current agent state. The sidecar
 * responds by sending pack.push frames followed by pack.done using the
 * same transferId.
 */
export type SyncRequestFrame = {
  type: "sync.request";
  agentAddress: string;
  transferId: string;
};

/** Any frame on the wire, regardless of direction. */
export type WireFrame = SidecarFrame | HubFrame;
