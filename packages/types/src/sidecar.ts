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
 * Sent immediately after websocket connect. Identifies the sidecar and
 * declares which agent addresses it hosts. The hub uses this to build its
 * routing table.
 *
 * Must be idempotent — a reconnecting sidecar re-registers with the same
 * ID and the hub updates its routing state rather than creating a duplicate.
 */
export type RegisterFrame = {
  type: "register";
  sidecarId: string;
  token: string;
  agentAddresses: string[];
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
 * Acknowledges a session lifecycle request (create, destroy, abort).
 * Sent after the sidecar has successfully processed the request.
 */
export type SessionAckFrame = {
  type: "session.ack";
  requestId: string;
};

/**
 * Reports a session lifecycle error back to the hub.
 */
export type SessionErrorFrame = {
  type: "session.error";
  requestId: string;
  error: string;
};

/** All frame types the sidecar sends to the hub. */
export type SidecarFrame =
  | RegisterFrame
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
 * Start an agent session. The sidecar constructs a harness from the
 * embedded config and begins the reactor. Responds with session.ack
 * or session.error.
 */
export type SessionCreateFrame = {
  type: "session.create";
  requestId: string;
  config: HarnessConfig;
};

/**
 * Gracefully stop an agent session. The sidecar calls harness.stop()
 * which triggers reactor shutdown. Responds with session.ack or
 * session.error.
 */
export type SessionDestroyFrame = {
  type: "session.destroy";
  requestId: string;
  agentAddress: string;
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

/** All frame types the hub sends to the sidecar. */
export type HubFrame =
  | MailInboundFrame
  | SessionCreateFrame
  | SessionDestroyFrame
  | SessionAbortFrame;

/** Any frame on the wire, regardless of direction. */
export type WireFrame = SidecarFrame | HubFrame;
