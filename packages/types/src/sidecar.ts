// Websocket wire protocol for hub↔sidecar communication.
//
// One websocket connection per sidecar↔hub pair. All traffic is multiplexed
// as JSON frames with a `type` discriminator. The sidecar initiates the
// connection; the hub is the server.
//
// Mail bytes are base64-encoded in JSON frames. Binary frames would be more
// efficient but JSON is simpler to debug and inspect.

import type { AbortReason, HarnessConfig, InferenceEvent } from "./runtime";

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

/** All frame types the sidecar sends to the hub. */
export type SidecarFrame =
  | RegisterFrame
  | ReconnectFrame
  | ChallengeResponseFrame
  | AgentDeployAckFrame
  | AgentErrorFrame
  | MailOutboundFrame
  | AgentEventFrame
  | SessionAckFrame
  | SessionErrorFrame;

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
 * Remove an agent from this sidecar. The sidecar tears down the harness.
 */
export type AgentUndeployFrame = {
  type: "agent.undeploy";
  agentAddress: string;
  reason: string;
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

/** All frame types the hub sends to the sidecar. */
export type HubFrame =
  | MailInboundFrame
  | AgentDeployFrame
  | AgentUndeployFrame
  | ChallengeFrame
  | ChallengeFailedFrame
  | SessionAbortFrame
  | MessageSendFrame;

/** Any frame on the wire, regardless of direction. */
export type WireFrame = SidecarFrame | HubFrame;
