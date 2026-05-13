// Runtime definitions for the Interchange agent harness.
//
// Wire-facing data types (AbortReason, ProviderConfig, ToolDefinition,
// HarnessConfig) are arktype validators so they can be composed into
// WebSocket frame validators and used for runtime validation at parse
// boundaries. Behavioral interfaces (ContextStore, MessageTransport,
// ToolRunner, etc.) remain plain TypeScript.

import { type } from "arktype";
import type { AuditRecord, ErrorRecord } from "./audit";
import { WireGrantRule } from "./grant-wire";

// ---------------------------------------------------------------------------
// Cryptographic Identity (ARCHITECTURE.md § Cryptographic Identity,
//                         IMPLEMENTATION.md § Cryptographic Identity: Key Formats)
// ---------------------------------------------------------------------------

/**
 * An Ed25519 key pair as raw bytes. The private key is 32 bytes; the public
 * key is the corresponding 32-byte compressed point.
 *
 * Key material is represented as Uint8Array throughout so it stays
 * runtime-agnostic (Bun, Node, browser) and never accidentally leaks through
 * JSON serialization.
 */
export type KeyPair = {
  privateKey: Uint8Array;
  publicKey: Uint8Array;
};

/**
 * A key-bound cryptographic provider. Each instance is constructed with a
 * specific agent's Ed25519 key pair and holds the private key internally.
 *
 * `sign` uses the instance's own private key — no key parameter is accepted.
 * `verify` accepts a public key parameter so the holder can verify messages
 * from arbitrary senders without constructing a new provider instance.
 *
 * The in-memory transport stores one CryptoProvider per registered agent and
 * calls `crypto.sign(content)` during `send()` without passing keys around.
 *
 * Key formats (IMPLEMENTATION.md):
 * - Ed25519 in SSH format — control plane interactions
 * - Ed25519 in PGP format — message-level signatures over SMTP/IMAP
 * - Ed25519 in X.509 format — TLS mutual auth certificates
 *
 * `getPublicKey` returns the raw public key bytes so callers can publish
 * them to the control plane or embed them in discovery metadata.
 */
export interface CryptoProvider {
  /**
   * Sign `content` with the instance's private key. Returns the Ed25519
   * detached signature as raw bytes.
   */
  sign(content: Uint8Array): Promise<Uint8Array>;

  /**
   * Verify that `signature` over `content` was produced by `publicKey`.
   * Returns true if the signature is valid; false otherwise.
   */
  verify(
    content: Uint8Array,
    signature: Uint8Array,
    publicKey: Uint8Array,
  ): Promise<boolean>;

  /** The public key for this instance, as raw bytes. */
  getPublicKey(): Uint8Array;
}

/**
 * Generate a fresh Ed25519 key pair. The returned pair is used to construct
 * a CryptoProvider instance.
 */
export type GenerateKeyPair = () => Promise<KeyPair>;

// ---------------------------------------------------------------------------
// Message Transport (MESSAGE.md § Transport Interface)
// ---------------------------------------------------------------------------

/**
 * Opaque reference to a message in a specific mailbox. Carries the IMAP UID
 * and the mailbox name. Passed to fetch, flag, and move operations without
 * requiring re-search.
 */
export type MessageRef = {
  uid: number;
  mailbox: string;
};

/**
 * Interchange payload types as defined in MESSAGE.md § Payload Types.
 * The type field in structured messages matches the Interchange-Type header.
 */
export type InterchangeType =
  | "conversation.message"
  | "conversation.join"
  | "conversation.leave"
  | "offering.request"
  | "offering.response"
  | "offering.error"
  | "offering.discover"
  | "offering.catalog"
  | "payment.required"
  | "payment.receipt"
  | "payment.verified"
  | "approval.request"
  | "approval.granted"
  | "approval.denied"
  | "system.health"
  | "system.register"
  | "system.deregister"
  | "system.credential.refresh";

/**
 * Attachment for an outbound message. Content is raw bytes; the transport
 * handles Content-Transfer-Encoding (base64 for binary, quoted-printable
 * for 8-bit text).
 */
export type MessageAttachment = {
  name: string;
  contentType: string;
  data: Uint8Array;
};

/**
 * A message the harness submits for delivery via SMTP. The transport
 * assembles the PGP/MIME multipart structure, signs it with the agent's
 * CryptoProvider, and submits it.
 *
 * Conversation types (conversation.*) carry `content` as text/plain.
 * Structured types carry `payload` as application/vnd.interchange+json.
 * Providing both is an error.
 *
 * (MESSAGE.md § Transport Interface › Outbound)
 */
export type OutboundMessage = {
  to: string | string[];
  cc?: string | string[];
  subject?: string;

  type: InterchangeType;

  /** Plain text body — used when type is a conversation.* type. */
  content?: string;

  /** Structured JSON body — used when type is a non-conversation type. */
  payload?: Record<string, unknown>;

  /** Human-readable summary for structured messages (the text/plain part). */
  summary?: string;

  attachments?: MessageAttachment[];

  /** Message-ID of the message being replied to. */
  inReplyTo?: string;

  /** Correlation ID linking this message to a pending async request. */
  correlationId?: string;

  /** Reactor session ID from the Interchange-Session-ID header. */
  sessionId?: string;

  /** Tenant ID for the Interchange-Tenant-ID header. */
  tenantId?: string;
};

/**
 * Receipt returned by `send()`. Contains the assigned Message-ID and
 * delivery status.
 *
 * (MESSAGE.md § Transport Interface › Outbound)
 */
export type SendReceipt = {
  messageId: string;
  status: "delivered" | "queued";
};

/**
 * Parsed headers from an inbound message. Field names follow RFC 5322 and
 * the Interchange-specific header conventions from MESSAGE.md § Headers.
 */
export type MessageHeaders = {
  from: string;
  to: string[];
  cc?: string[];
  date: string;
  messageId: string;
  inReplyTo?: string;
  references?: string[];
  subject?: string;
  listId?: string;

  interchangeType?: InterchangeType;
  interchangeCorrelationId?: string;
  interchangeTenantId?: string;
  interchangeAgentId?: string;
  interchangeSessionId?: string;
  interchangeOfferingId?: string;
  interchangeSchemaVersion?: string;

  traceparent?: string;
  tracestate?: string;
};

/**
 * Signature verification status of an inbound message.
 *
 * - `valid` — signature verified against the sender's public key
 * - `invalid` — signature check failed (tampering or wrong key)
 * - `unknown` — public key not available for verification
 * - `missing` — message was not signed
 *
 * (MESSAGE.md § Transport Interface › fetchFull)
 */
export type SignatureStatus = "valid" | "invalid" | "unknown" | "missing";

/**
 * A parsed MIME part returned by `fetchPart()`.
 */
export type MessagePart = {
  contentType: string;
  content: Uint8Array;
  encoding?: string;
};

/**
 * MIME tree metadata returned by `fetchStructure()`. Describes content types,
 * sizes, and dispositions without transferring content.
 *
 * (MESSAGE.md § Partial Fetch)
 */
export type BodyStructure = {
  contentType: string;
  size?: number;
  disposition?: string;
  parts?: BodyStructure[];
};

/**
 * A fully parsed inbound message including structured payload, headers,
 * attachments, and signature verification status.
 *
 * (MESSAGE.md § Transport Interface › fetchFull)
 */
export type InboundMessage = {
  ref: MessageRef;
  headers: MessageHeaders;
  flags: string[];

  /** Plain text body for conversation.* types. */
  content?: string;

  /** Parsed JSON payload for structured types. */
  payload?: {
    type: InterchangeType;
    version: string;
    body: Record<string, unknown>;
  };

  attachments?: MessageAttachment[];
  signatureStatus: SignatureStatus;
};

/**
 * IMAP mailbox descriptor.
 *
 * (MESSAGE.md § Inbox Management)
 */
export type Mailbox = {
  name: string;
  role?: string;
  delimiter?: string;
};

/**
 * Current status of an IMAP mailbox, including QRESYNC identifiers.
 *
 * (MESSAGE.md § Inbox Management)
 */
export type MailboxStatus = {
  total: number;
  unseen: number;
  recent: number;
  uidNext: number;
  uidValidity: number;
  highestModSeq: number;
};

/**
 * Structured IMAP search query. Maps the IMAP SEARCH grammar to a typed
 * object. Supports recursive boolean composition via `and`, `or`, `not`.
 *
 * (MESSAGE.md § Search)
 */
export type SearchQuery = {
  from?: string;
  to?: string;
  cc?: string;
  bcc?: string;
  header?: { field: string; contains: string };
  before?: Date;
  after?: Date;
  on?: Date;
  sentBefore?: Date;
  sentAfter?: Date;
  sentOn?: Date;
  hasFlags?: string[];
  missingFlags?: string[];
  body?: string;
  text?: string;
  largerThan?: number;
  smallerThan?: number;
  and?: SearchQuery[];
  or?: SearchQuery[];
  not?: SearchQuery;
};

/**
 * A thread node returned by `thread()`. Carries a message reference and
 * child threads representing replies. Implements the RFC 5256 REFERENCES
 * threading algorithm.
 *
 * (MESSAGE.md § Thread Retrieval)
 */
export type Thread = {
  ref: MessageRef;
  children: Thread[];
};

/**
 * QRESYNC state the harness provides when reconnecting to the transport.
 *
 * (MESSAGE.md § Synchronization)
 */
export type SyncState = {
  uidValidity: number;
  uidNext: number;
  highestModSeq: number;
  knownUids?: number[];
};

/**
 * Result of a QRESYNC-style sync operation.
 *
 * (MESSAGE.md § Synchronization)
 */
export type SyncResult = {
  vanished: number[];
  changed: { uid: number; flags: string[] }[];
  newMessages: MessageRef[];
  fullResyncRequired: boolean;
};

/**
 * Distribution list metadata returned by `createList()`.
 *
 * (MESSAGE.md § Message Topologies)
 */
export type ListInfo = {
  address: string;
  name: string;
  memberCount: number;
  createdAt: string;
};

/**
 * Event emitted by the mailbox watcher callback. Corresponds to IMAP IDLE
 * notifications.
 *
 * (MESSAGE.md § Real-Time Notification)
 */
export type MailboxEvent =
  | { type: "exists"; uid: number; headers: MessageHeaders }
  | { type: "flagsChanged"; uid: number; flags: string[] }
  | { type: "expunged"; uid: number };

/** Unsubscribe function returned by `watch()`. */
export type Unsubscribe = () => void;

/**
 * The message transport interface. Abstracts SMTP and IMAP behind a
 * TypeScript API. Implementations range from real SMTP/IMAP servers to
 * in-process stubs that route messages through memory.
 *
 * All long-running operations accept an AbortSignal for cooperative
 * cancellation.
 *
 * (MESSAGE.md § Transport Interface)
 */
export interface MessageTransport {
  // --- Outbound ---

  /** Compose, sign, and deliver a message via SMTP. */
  send(message: OutboundMessage, signal?: AbortSignal): Promise<SendReceipt>;

  /** Append a raw message to a mailbox (IMAP APPEND). */
  append(
    mailbox: string,
    message: InboundMessage,
    flags?: string[],
    signal?: AbortSignal,
  ): Promise<MessageRef>;

  // --- Mailbox management ---

  listMailboxes(signal?: AbortSignal): Promise<Mailbox[]>;
  createMailbox(name: string, signal?: AbortSignal): Promise<Mailbox>;
  deleteMailbox(name: string, signal?: AbortSignal): Promise<void>;
  getMailboxStatus(name: string, signal?: AbortSignal): Promise<MailboxStatus>;

  // --- Message search and retrieval ---

  search(
    mailbox: string,
    query: SearchQuery,
    signal?: AbortSignal,
  ): Promise<MessageRef[]>;

  thread(
    mailbox: string,
    algorithm: "references" | "orderedsubject",
    query?: SearchQuery,
    signal?: AbortSignal,
  ): Promise<Thread[]>;

  fetchHeaders(ref: MessageRef, signal?: AbortSignal): Promise<MessageHeaders>;
  fetchStructure(ref: MessageRef, signal?: AbortSignal): Promise<BodyStructure>;
  fetchPart(
    ref: MessageRef,
    partPath: string,
    signal?: AbortSignal,
  ): Promise<MessagePart>;
  fetchFull(ref: MessageRef, signal?: AbortSignal): Promise<InboundMessage>;

  // --- Flag management ---

  setFlags(
    ref: MessageRef,
    flags: string[],
    signal?: AbortSignal,
  ): Promise<void>;

  clearFlags(
    ref: MessageRef,
    flags: string[],
    signal?: AbortSignal,
  ): Promise<void>;

  // --- Message organization ---

  move(ref: MessageRef, toMailbox: string, signal?: AbortSignal): Promise<void>;

  copy(ref: MessageRef, toMailbox: string, signal?: AbortSignal): Promise<void>;

  expunge(mailbox: string, signal?: AbortSignal): Promise<void>;

  // --- Real-time notification ---

  /** Monitor a mailbox for new messages and flag changes (IMAP IDLE). */
  watch(mailbox: string, callback: (event: MailboxEvent) => void): Unsubscribe;

  // --- Synchronization ---

  /** Efficient reconnection using QRESYNC semantics. */
  sync(
    mailbox: string,
    knownState: SyncState,
    signal?: AbortSignal,
  ): Promise<SyncResult>;

  // --- Distribution lists ---

  createList(
    address: string,
    name: string,
    signal?: AbortSignal,
  ): Promise<ListInfo>;

  listMembers(address: string, signal?: AbortSignal): Promise<string[]>;

  subscribe(
    listAddress: string,
    agentAddress: string,
    signal?: AbortSignal,
  ): Promise<void>;

  unsubscribe(
    listAddress: string,
    agentAddress: string,
    signal?: AbortSignal,
  ): Promise<void>;
}

// ---------------------------------------------------------------------------
// Tool Execution (ARCHITECTURE.md § Tools, INFERENCE.md § Tool Execution)
// ---------------------------------------------------------------------------

/**
 * A tool call as requested by the model. Carries the provider-assigned call
 * ID, the tool name, and the parsed arguments.
 *
 * (INFERENCE.md § Message Format › Content Types)
 */
export const ToolCall = type({
  id: "string",
  name: "string",
  arguments: "Record<string, unknown>",
});
export type ToolCall = typeof ToolCall.infer;

/**
 * Result of a tool execution. `content` is text or structured data the model
 * sees as the tool result. `detail` is additional data that the harness may
 * use (e.g., for validation or audit) but that is not shown to the model.
 *
 * When `isError` is true the model sees the result as an error. When
 * `pendingMarker` is present the tool is async — the reactor registers the
 * correlation ID and waits for a matching inbound message.
 *
 * (INFERENCE.md § Tool Execution Semantics)
 */
export const ToolResult = type({
  callId: "string",
  content: "string | Record<string, unknown>",
  "detail?": "unknown",
  "isError?": "boolean",
  "pendingMarker?": {
    status: "'pending'",
    correlationId: "string",
    "expectedFrom?": "string",
  },
});
export type ToolResult = typeof ToolResult.infer;

/**
 * The tool runner interface. The harness implements this; the reactor calls
 * it when the director requests tool execution.
 *
 * Parallel execution is modeled by calling `run` concurrently for each call
 * in a batch — the interface is per-call, not per-batch.
 *
 * (ARCHITECTURE.md § Agent Harness › Tools)
 */
export interface ToolRunner {
  /**
   * Execute a single tool call. Resolves with the result. Must not throw —
   * errors are returned as `ToolResult` with `isError: true`.
   */
  run(call: ToolCall, signal: AbortSignal): Promise<ToolResult>;
}

// ---------------------------------------------------------------------------
// Inference Event Building Blocks (INFERENCE.md § Event Protocol)
// ---------------------------------------------------------------------------

/**
 * Partial assistant message accumulated during streaming. Carries all
 * content blocks seen so far so late-joining subscribers receive current
 * state without replaying deltas.
 *
 * (INFERENCE.md § Event Protocol › Partial State)
 */
export const PartialMessage = type({
  text: "string",
  "thinking?": "string",
  "toolCalls?": type({
    id: "string",
    name: "string",
    partialArguments: "string",
  }).array(),
});
export type PartialMessage = typeof PartialMessage.infer;

/**
 * Token usage for a single inference call. Cache read/write counts are
 * provider-specific and may be zero when the provider does not report them.
 *
 * (INFERENCE.md § Token Accounting)
 */
export const TokenUsage = type({
  input: "number",
  output: "number",
  cacheRead: "number",
  cacheWrite: "number",
  thinking: "number",
});
export type TokenUsage = typeof TokenUsage.infer;

// ---------------------------------------------------------------------------
// Internal Turn Format (INFERENCE.md § Message Format)
// ---------------------------------------------------------------------------

/**
 * A single content block within a conversation turn. Provider-agnostic.
 *
 * (INFERENCE.md § Message Format › Content Types)
 */
const TextBlock = type({ type: "'text'", text: "string" });
const ImageBlock = type({
  type: "'image'",
  mimeType: "string",
  data: "string",
});

const ThinkingBlock = type({
  type: "'thinking'",
  thinking: "string",
  "signature?": "string",
  "redacted?": "boolean",
});
const ToolCallBlock = type({
  type: "'tool_call'",
  id: "string",
  name: "string",
  arguments: "Record<string, unknown>",
});
const ToolResultBlock = type({
  type: "'tool_result'",
  callId: "string",
  content: TextBlock.or(ImageBlock).array(),
  "detail?": "unknown",
  "isError?": "boolean",
});

export const ContentBlock = TextBlock.or(ThinkingBlock)
  .or(ImageBlock)
  .or(ToolCallBlock)
  .or(ToolResultBlock);
export type ContentBlock = typeof ContentBlock.infer;

/**
 * A turn in the internal conversation history. The `model` field records
 * which provider model produced this turn (present only on assistant
 * turns). Used by cross-provider transformation to strip or preserve
 * thinking blocks.
 *
 * (INFERENCE.md § Message Format)
 */
export type ConversationTurn = {
  role: "user" | "assistant" | "system";
  content: ContentBlock[];
  model?: string;
  timestamp: number;
};

/**
 * A completed assistant turn returned in `inference.done`. Narrower type
 * than ConversationTurn to make the inference boundary explicit.
 */
export const AssistantTurn = type({
  role: "'assistant'",
  content: ContentBlock.array(),
  model: "string",
  timestamp: "number",
});
export type AssistantTurn = typeof AssistantTurn.infer;

// ---------------------------------------------------------------------------
// Error Classification (INFERENCE.md § Error Classification)
// ---------------------------------------------------------------------------

/**
 * Classified inference error. The category determines the reactor's default
 * response; the director can override per its policy.
 *
 * (INFERENCE.md § Error Classification)
 */
export const InferenceError = type({
  category: type.enumerated(
    "retryable",
    "context_overflow",
    "credential_failure",
    "quota_exhausted",
    "fatal",
    "aborted",
  ),
  message: "string",
  "statusCode?": "number",
  "retryAfterMs?": "number",
  "raw?": "unknown",
});
export type InferenceError = typeof InferenceError.infer;

// ---------------------------------------------------------------------------
// Agent Reactor (INFERENCE.md § Agent Reactor)
// ---------------------------------------------------------------------------

/**
 * Gate types that can block the reactor.
 *
 * (INFERENCE.md § Gates)
 */
export const GateType = type.enumerated(
  "approval",
  "payment",
  "credential",
  "budget",
  "child_completion",
  "message_response",
);
export type GateType = typeof GateType.infer;

/**
 * Fork mode. `independent` creates a divergent reactor with its own context.
 * `child` creates a reactor that reports results back to the parent.
 *
 * (INFERENCE.md § Forking)
 */
export const ForkMode = type.enumerated("independent", "child");
export type ForkMode = typeof ForkMode.infer;

// ---------------------------------------------------------------------------
// Inference Event Protocol (INFERENCE.md § Event Protocol)
// ---------------------------------------------------------------------------

/**
 * Wire-safe representation of InboundMessage for use in InferenceEvent
 * variants. The runtime InboundMessage type contains Uint8Array fields
 * (MessageAttachment.data) that cannot survive JSON serialization, so the
 * wire validator uses `unknown` for attachment data and accepts whatever
 * JSON.parse produces.
 */
const WireInboundMessage = type({
  ref: { uid: "number", mailbox: "string" },
  headers: "Record<string, unknown>",
  flags: "string[]",
  "content?": "string",
  "payload?": "object",
  "attachments?": "unknown[]",
  signatureStatus: type.enumerated("valid", "invalid", "unknown", "missing"),
});

/**
 * A single event in the inference event protocol. Every event carries a
 * monotonic session-scoped sequence number.
 *
 * Event types are namespaced: `inference.*`, `tool.*`, `reactor.*`,
 * `fork.*`, `message.*`, `custom.*`.
 *
 * (INFERENCE.md § Event Protocol)
 */
export const InferenceEvent = type({
  type: "'inference.start'",
  seq: "number",
  data: { model: "string" },
})
  .or({
    type: "'inference.thinking.delta'",
    seq: "number",
    data: { token: "string", partial: PartialMessage },
  })
  .or({
    type: "'inference.text.delta'",
    seq: "number",
    data: { token: "string", partial: PartialMessage },
  })
  .or({
    type: "'inference.tool_call.start'",
    seq: "number",
    data: { callId: "string", name: "string", partial: PartialMessage },
  })
  .or({
    type: "'inference.tool_call.delta'",
    seq: "number",
    data: {
      callId: "string",
      argumentFragment: "string",
      partial: PartialMessage,
    },
  })
  .or({
    type: "'inference.tool_call.end'",
    seq: "number",
    data: {
      callId: "string",
      name: "string",
      arguments: "Record<string, unknown>",
      partial: PartialMessage,
    },
  })
  .or({
    type: "'inference.usage'",
    seq: "number",
    data: { usage: TokenUsage },
  })
  .or({
    type: "'inference.done'",
    seq: "number",
    data: {
      turn: AssistantTurn,
      usage: TokenUsage,
      "pacingDelayMs?": "number",
    },
  })
  .or({
    type: "'inference.error'",
    seq: "number",
    data: { error: InferenceError, partial: PartialMessage },
  })
  .or({
    type: "'tool.start'",
    seq: "number",
    data: { call: ToolCall },
  })
  .or({
    type: "'tool.update'",
    seq: "number",
    data: { callId: "string", partial: "string" },
  })
  .or({
    type: "'tool.done'",
    seq: "number",
    data: { result: ToolResult },
  })
  .or({
    type: "'message.queued'",
    seq: "number",
    data: { message: WireInboundMessage },
  })
  .or({
    type: "'message.correlated'",
    seq: "number",
    data: { message: WireInboundMessage, correlationId: "string" },
  })
  .or({
    type: "'connector.reply'",
    seq: "number",
    data: { content: "string", "checkpointHash?": "string" },
  })
  .or({
    type: "'reactor.start'",
    seq: "number",
    data: "object",
  })
  .or({
    type: "'reactor.gate.blocked'",
    seq: "number",
    data: { reason: GateType, gateId: "string" },
  })
  .or({
    type: "'reactor.gate.cleared'",
    seq: "number",
    data: {
      gateId: "string",
      reason: type.enumerated("resolved", "timeout", "shutdown"),
    },
  })
  .or({
    type: "'reactor.done'",
    seq: "number",
    data: "object",
  })
  .or({
    type: "'reactor.error'",
    seq: "number",
    data: { error: "string", fatal: "boolean" },
  })
  .or({
    type: "'fork.created'",
    seq: "number",
    data: { forkId: "string", parentId: "string", mode: ForkMode },
  })
  .or({
    type: "'fork.done'",
    seq: "number",
    data: { forkId: "string", "result?": "unknown" },
  })
  .or({
    type: "'fork.error'",
    seq: "number",
    data: { forkId: "string", error: "string" },
  })
  .or({
    type: "'fork.aborted'",
    seq: "number",
    data: { forkId: "string" },
  })
  .or({
    type: /^custom\./,
    seq: "number",
    data: "Record<string, unknown>",
  });
// The TypeScript type is defined manually rather than inferred from the
// validator because the `custom.*` variant uses a regex pattern which
// arktype infers as `string`. A bare `string` in the discriminant position
// prevents TypeScript from narrowing the union in switch statements.
// The manually defined type uses a `custom.${string}` template literal
// for that variant, preserving the narrowing behavior downstream code
// relies on.
export type InferenceEvent =
  | { type: "inference.start"; seq: number; data: { model: string } }
  | {
      type: "inference.thinking.delta";
      seq: number;
      data: { token: string; partial: PartialMessage };
    }
  | {
      type: "inference.text.delta";
      seq: number;
      data: { token: string; partial: PartialMessage };
    }
  | {
      type: "inference.tool_call.start";
      seq: number;
      data: { callId: string; name: string; partial: PartialMessage };
    }
  | {
      type: "inference.tool_call.delta";
      seq: number;
      data: {
        callId: string;
        argumentFragment: string;
        partial: PartialMessage;
      };
    }
  | {
      type: "inference.tool_call.end";
      seq: number;
      data: {
        callId: string;
        name: string;
        arguments: Record<string, unknown>;
        partial: PartialMessage;
      };
    }
  | {
      type: "inference.usage";
      seq: number;
      data: { usage: TokenUsage };
    }
  | {
      type: "inference.done";
      seq: number;
      data: { turn: AssistantTurn; usage: TokenUsage; pacingDelayMs?: number };
    }
  | {
      type: "inference.error";
      seq: number;
      data: { error: InferenceError; partial: PartialMessage };
    }
  | { type: "tool.start"; seq: number; data: { call: ToolCall } }
  | {
      type: "tool.update";
      seq: number;
      data: { callId: string; partial: string };
    }
  | { type: "tool.done"; seq: number; data: { result: ToolResult } }
  | {
      type: "message.queued";
      seq: number;
      data: { message: InboundMessage };
    }
  | {
      type: "message.correlated";
      seq: number;
      data: { message: InboundMessage; correlationId: string };
    }
  | {
      type: "connector.reply";
      seq: number;
      data: { content: string; checkpointHash?: string };
    }
  | { type: "reactor.start"; seq: number; data: Record<string, never> }
  | {
      type: "reactor.gate.blocked";
      seq: number;
      data: { reason: GateType; gateId: string };
    }
  | {
      type: "reactor.gate.cleared";
      seq: number;
      data: {
        gateId: string;
        reason: "resolved" | "timeout" | "shutdown";
      };
    }
  | { type: "reactor.done"; seq: number; data: Record<string, never> }
  | {
      type: "reactor.error";
      seq: number;
      data: { error: string; fatal: boolean };
    }
  | {
      type: "fork.created";
      seq: number;
      data: { forkId: string; parentId: string; mode: ForkMode };
    }
  | {
      type: "fork.done";
      seq: number;
      data: { forkId: string; result?: unknown };
    }
  | {
      type: "fork.error";
      seq: number;
      data: { forkId: string; error: string };
    }
  | { type: "fork.aborted"; seq: number; data: { forkId: string } }
  | {
      type: `custom.${string}`;
      seq: number;
      data: Record<string, unknown>;
    };

/**
 * Validate unknown data as an InferenceEvent. ArkType's regex-based validator
 * infers `custom.*` event types as `string`, but the manual InferenceEvent type
 * uses a `custom.${string}` template literal for switch narrowing. This function
 * centralizes that single unavoidable cast.
 */
export function parseInferenceEvent(
  data: unknown,
): InferenceEvent | type.errors {
  const result = InferenceEvent(data);
  if (result instanceof type.errors) return result;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- arktype regex infers as string; manual type uses template literal
  return result as InferenceEvent;
}

/**
 * A pending async operation registered in the reactor's async state.
 * Correlates an outbound message (or payment/approval request) to the
 * expected inbound response.
 *
 * (INFERENCE.md § Correlation)
 */
export type PendingOperation = {
  correlationId: string;
  expectedFrom?: string;
  registeredAt: number;
  gateId: string;
};

/**
 * Complete reactor state visible to the director decision function.
 *
 * `tokenUsage` is the cumulative usage across the session; `lastCycleUsage`
 * is the usage reported by the most recent inference call, or null if no
 * inference call has completed yet. The per-cycle value supports
 * compaction triggers that key off recent input cost rather than session
 * totals.
 *
 * (INFERENCE.md § Agent Reactor › Director Decision Function)
 */
export type ReactorState = {
  turns: ConversationTurn[];
  activeForks: { forkId: string; mode: ForkMode }[];
  pendingOperations: PendingOperation[];
  activeGates: { gateId: string; type: GateType; timeoutAt: number }[];
  tokenUsage: TokenUsage;
  lastCycleUsage: TokenUsage | null;
  sessionId: string;
};

/**
 * Actions the director can direct the reactor to take.
 *
 * (INFERENCE.md § Agent Reactor › Actions)
 */
export type ReactorAction =
  | {
      type: "infer";
      model: string;
      options?: InferenceOptions;
    }
  | {
      type: "execute_tools";
      calls: ToolCall[];
      parallel?: boolean;
      addToHistory?: boolean;
    }
  | {
      type: "suspend";
      gate: {
        type: GateType;
        gateId: string;
        timeoutMs: number;
        correlationId?: string;
      };
    }
  | {
      type: "fork";
      mode: ForkMode;
      forkId: string;
    }
  | {
      type: "emit";
      eventType: `custom.${string}`;
      data: Record<string, unknown>;
    }
  | {
      type: "reply";
      content: string;
    }
  | { type: "checkpoint"; message: string }
  | { type: "compact"; compactor: string; reason: string }
  | { type: "wait" }
  | { type: "done" };

/**
 * The capabilities object passed to the director. Mirrors the `ReactorAction`
 * union — provides a type-safe way for the director to construct actions.
 *
 * (INFERENCE.md § Agent Reactor › Director Decision Function)
 */
export type ReactorCapabilities = {
  infer(model: string, options?: InferenceOptions): ReactorAction;
  executeTools(
    calls: ToolCall[],
    parallel?: boolean,
    addToHistory?: boolean,
  ): ReactorAction;
  suspend(gate: {
    type: GateType;
    gateId: string;
    timeoutMs: number;
    correlationId?: string;
  }): ReactorAction;
  fork(mode: ForkMode, forkId: string): ReactorAction;
  emit(
    eventType: `custom.${string}`,
    data: Record<string, unknown>,
  ): ReactorAction;
  reply(content: string): ReactorAction;
  checkpoint(message?: string): ReactorAction;
  compact(compactor: string, reason: string): ReactorAction;
  wait(): ReactorAction;
  done(): ReactorAction;
};

/**
 * The inbound events delivered to the director decision function.
 *
 * (INFERENCE.md § Agent Reactor › Reactor Structure)
 */
export type ReactorInboundEvent =
  | { type: "message.received"; message: InboundMessage }
  | { type: "inference.done"; turn: AssistantTurn; usage: TokenUsage }
  | { type: "inference.error"; error: InferenceError; partial: PartialMessage }
  | { type: "tool.done"; result: ToolResult }
  | {
      type: "reactor.gate.cleared";
      gateId: string;
      reason: "resolved" | "timeout" | "shutdown";
    }
  | { type: "abort"; reason: AbortReason };

/**
 * The core director is a single decision function: given an event and the
 * current reactor state, return one or more actions.
 *
 * If the director throws, the reactor catches the exception, emits
 * `reactor.error`, and initiates graceful shutdown.
 *
 * (INFERENCE.md § Reactor Director › Core Director)
 */
export interface ReactorDirector {
  decide(
    event: ReactorInboundEvent,
    state: ReactorState,
    capabilities: ReactorCapabilities,
  ): Promise<ReactorAction | ReactorAction[]>;
}

// ---------------------------------------------------------------------------
// Director Extension Hooks (INFERENCE.md § Reactor Director › Extension Hooks)
// ---------------------------------------------------------------------------

/**
 * Extension that runs before a tool call is executed. Returning a string
 * blocks the call with that reason. Returning `undefined` allows it.
 */
export interface BeforeToolExtension {
  beforeTool(
    call: ToolCall,
    state: ReactorState,
    signal: AbortSignal,
  ): Promise<string | undefined>;
}

/**
 * Extension that runs after a tool result is produced. Can modify the result
 * (redaction, enrichment, audit logging). Extensions run in order.
 */
export interface AfterToolExtension {
  afterTool(
    result: ToolResult,
    call: ToolCall,
    state: ReactorState,
    signal: AbortSignal,
  ): Promise<ToolResult>;
}

// ---------------------------------------------------------------------------
// Context Strategies: Transforms and Compactors
// (INFERENCE.md § Context Management, § Tool Result Lifecycle)
// ---------------------------------------------------------------------------

/**
 * Durable description of a single strategy invocation. Written to the
 * per-cycle manifest in the context store so that future operators can
 * reconstruct exactly which strategy made which change, with what
 * parameters, and why.
 *
 * - `strategy` is the implementation name (e.g. `"size-cap"`).
 * - `version` is the implementation version. Changes to the strategy's
 *   behavior bump the version so old manifest entries remain unambiguous.
 * - `parameters` records the configuration the strategy ran with.
 * - `reason` is a short machine-readable cause label
 *   (e.g. `"exceeded-cap"`, `"overflow-recovery"`).
 * - `decisions` records strategy-specific details about what was actually
 *   done (e.g. the keep count, the spill key, the original byte size).
 */
export const TransformRecord = type({
  strategy: "string",
  version: "string",
  parameters: "Record<string, unknown>",
  reason: "string",
  decisions: "Record<string, unknown>",
});
export type TransformRecord = typeof TransformRecord.infer;

/**
 * Per-invocation context passed to every `ContextStrategy.apply` call.
 * `state` is the reactor's snapshot at the moment the strategy runs;
 * `trigger` is a short label describing why the strategy was invoked
 * (e.g. `"tool-result-ingest"`, `"pre-inference"`, `"director-request"`).
 */
export interface StrategyContext {
  readonly state: ReactorState;
  readonly trigger: string;
}

/**
 * Optional blob attachment emitted by a strategy. The reactor writes each
 * blob to the context store's working tree via `ContextStore.writeBlob`
 * (Phase 2) so the data is durable and migrates with the conversation.
 */
export type StrategyBlob = {
  key: string;
  bytes: Uint8Array;
  contentType?: string;
};

/**
 * Result returned by `ContextStrategy.apply`. Carries the transformed
 * output, a `TransformRecord` describing what happened, and any blobs
 * that should be persisted in the context store.
 */
export interface StrategyResult<O> {
  output: O;
  record: TransformRecord;
  blobs?: StrategyBlob[];
}

/**
 * Generic base interface for content-mutating strategies. The role-specific
 * aliases below specialize `I` and `O` for tool-result ingestion, pre-
 * inference context shaping, and explicit compaction.
 *
 * Strategies are pure with respect to the context store: they describe what
 * should change via their return value. The reactor decides where to write
 * the result (history, prompt, manifest) and which blobs to persist.
 */
export interface ContextStrategy<I, O> {
  readonly name: string;
  readonly version: string;
  apply(input: I, ctx: StrategyContext): Promise<StrategyResult<O>>;
}

/**
 * Runs on each tool result entering history. Output is appended to the
 * conversation; any emitted blobs are written to the context store's
 * `tool-output/` directory.
 */
export type ToolResultTransform = ContextStrategy<
  { call: ToolCall; result: ToolResult },
  ToolResult
>;

/**
 * Runs in order before every inference call, producing the materialized
 * prompt. Output is written to `prompt.jsonl` for that cycle; the durable
 * history in `turns.jsonl` is left untouched.
 *
 * (INFERENCE.md § Async State Awareness › Pending Status Injection)
 */
export type ContextTransform = ContextStrategy<
  ConversationTurn[],
  ConversationTurn[]
>;

/**
 * Named compaction strategy. Registered in a registry on the reactor and
 * invoked explicitly via the director's `compact` action. Output overwrites
 * `turns.jsonl`; a `TransformRecord` is appended to the manifest.
 */
export type Compactor = ContextStrategy<ConversationTurn[], ConversationTurn[]>;

// ---------------------------------------------------------------------------
// Abort Reasons (INFERENCE.md § Abort Handling)
// ---------------------------------------------------------------------------

/**
 * Reason codes for the `abort` reactor event. The reason determines the
 * appropriate cleanup action.
 *
 * (INFERENCE.md § Abort Handling › Abort Reasons)
 */
export const AbortReason = type.enumerated(
  "user_disconnect",
  "wallet_exhaustion",
  "admin_kill",
  "session_timeout",
  "credential_revocation",
);
export type AbortReason = typeof AbortReason.infer;

// ---------------------------------------------------------------------------
// Provider Configuration (INFERENCE.md § Providers)
// ---------------------------------------------------------------------------

/**
 * Configuration for a single inference provider. Identifies the provider and
 * carries the endpoint URL and API credentials.
 *
 * Supported providers correspond to the day-one set defined in INFERENCE.md.
 * Custom providers may be registered under any string identifier.
 *
 * (INFERENCE.md § Providers)
 */
export const ProviderConfig = type({
  provider: "string",
  baseURL: "string",
  apiKey: "string",
  "model?": "string",
});
export type ProviderConfig = typeof ProviderConfig.infer;

/**
 * Options for a single inference call. Override the defaults from the agent
 * configuration on a per-call basis.
 *
 * (INFERENCE.md § Providers › Streaming Harness)
 */
export type InferenceOptions = {
  model?: string;
  maxTokens?: number;
  temperature?: number;
  thinking?: { enabled: boolean; budgetTokens?: number };
  systemPrompt?: string;
  tools?: ToolDefinition[];
};

// ---------------------------------------------------------------------------
// Context Store (INFERENCE.md § Context Management › Context Store,
//                ARCHITECTURE.md § Change History)
// ---------------------------------------------------------------------------

/**
 * A named commit point in the context store. Corresponds to a git commit.
 *
 * (ARCHITECTURE.md § Change History › Named Checkpoints)
 */
export type ContextCommit = {
  hash: string;
  message: string;
  timestamp: number;
  parentHash?: string;
};

/**
 * The state of an active connector thread. Persisted alongside the
 * conversation context so that reply threading survives sidecar restarts.
 */
export type ConnectorThreadState = {
  threadRoot: string;
  lastMessageId: string;
  replyTo: string;
  subject?: string;
};

/**
 * The context store interface. Implementations back the store with git
 * (filesystem, in-memory, or virtual) depending on the execution environment.
 * The reactor accepts any implementation that satisfies this interface.
 *
 * The store holds the turn history and reactor metadata. Forking creates
 * a git branch. Compaction commits the compacted history.
 *
 * (INFERENCE.md § Context Management › Context Store)
 */
export interface ContextStore {
  /**
   * Load the current turn history and reactor metadata from the store.
   * Called during reactor initialization.
   */
  load(signal?: AbortSignal): Promise<{
    turns: ConversationTurn[];
    pendingOperations: PendingOperation[];
    tokenUsage: TokenUsage;
    connectorState: ConnectorThreadState | null;
  }>;

  /**
   * Buffer connector thread state for the next commit. The harness calls
   * this before each checkpoint so that connector state is persisted
   * atomically with the conversation context.
   */
  setConnectorState(state: ConnectorThreadState | null): void;

  /**
   * Commit the current turn history and reactor metadata to the store.
   * May be called during a checkpoint, suspension, compaction, or shutdown.
   */
  commit(
    turns: ConversationTurn[],
    pendingOperations: PendingOperation[],
    tokenUsage: TokenUsage,
    message: string,
    signal?: AbortSignal,
  ): Promise<ContextCommit>;

  /**
   * Create a branch for a fork operation. The branch starts from the current
   * HEAD commit.
   */
  branch(name: string, signal?: AbortSignal): Promise<void>;

  /**
   * List recent commits. Used by the agent's history query tools.
   */
  log(limit?: number, signal?: AbortSignal): Promise<ContextCommit[]>;

  /**
   * Read the turn history at a specific commit hash. Used for history
   * inspection and rollback.
   */
  readAt(hash: string, signal?: AbortSignal): Promise<ConversationTurn[]>;
}

// ---------------------------------------------------------------------------
// Audit Store (INTR-4 § Audit Trail)
// ---------------------------------------------------------------------------

/**
 * Persistent store for tool invocation audit records. Separated from
 * ContextStore so the audit capability is opt-in at the composition
 * layer. The isogit implementation writes audit records as individual
 * JSON files in the same git repo used for context storage.
 */
export interface AuditStore {
  /**
   * Persist a batch of audit records. Called at checkpoint boundaries
   * with all records accumulated since the last checkpoint.
   */
  commitAudit(records: AuditRecord[], signal?: AbortSignal): Promise<void>;

  /**
   * Load audit records for a session. Returns all records matching
   * the given sessionId, ordered by seq.
   */
  loadAudit(sessionId: string, signal?: AbortSignal): Promise<AuditRecord[]>;

  /**
   * Persist a batch of error records. Called at checkpoint boundaries
   * and shutdown with all error records accumulated since the last flush.
   */
  commitErrors(records: ErrorRecord[], signal?: AbortSignal): Promise<void>;
}

// ---------------------------------------------------------------------------
// Agent / Harness Configuration (ARCHITECTURE.md § Agent Harness)
// ---------------------------------------------------------------------------

/**
 * Configured tool definition exposed to the model. The harness registers
 * available tools; the reactor passes this list to the inference provider as
 * part of each request.
 *
 * (ARCHITECTURE.md § Agent Harness › Tools)
 */
export const ToolDefinition = type({
  name: "string",
  description: "string",
  inputSchema: "Record<string, unknown>",
});
export type ToolDefinition = typeof ToolDefinition.infer;

/**
 * Agent harness configuration. Assembled from the agent definition package
 * and capability grants during harness initialization.
 *
 * `principalId` is the agent's principal in the hub's authorization model.
 * The sidecar needs it to reconstruct the in-memory grant store on restart
 * (the store's `collectGrants` filters by principal).
 *
 * `grants` uses `WireGrantRule` because this type arrives over JSON where
 * `GrantRule.expiresAt` is serialized as a string. The wire validator
 * coerces strings back to Date instances.
 *
 * (ARCHITECTURE.md § Agent Harness)
 */
export const HarnessConfig = type({
  sessionId: "string",
  agentId: "string",
  tenantId: "string",
  principalId: "string",
  agentAddress: "string",
  systemPrompt: "string",
  tools: ToolDefinition.array(),
  grants: WireGrantRule.array(),
  providers: ProviderConfig.array(),
  defaultModel: "string",
  "sessionChannelEnabled?": "boolean",
});
export type HarnessConfig = typeof HarnessConfig.infer;
