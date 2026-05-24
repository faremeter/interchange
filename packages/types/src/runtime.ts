// Runtime definitions for the Interchange agent harness.
//
// Wire-facing data types (AbortReason, InferenceSource, ToolDefinition,
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
 *
 * Exposed as both an arktype validator (for runtime validation at parse
 * boundaries and tool-argument schemas) and a derived TypeScript union.
 */
export const InterchangeType = type.enumerated(
  "conversation.message",
  "conversation.join",
  "conversation.leave",
  "offering.request",
  "offering.response",
  "offering.error",
  "offering.discover",
  "offering.catalog",
  "payment.required",
  "payment.receipt",
  "payment.verified",
  "approval.request",
  "approval.granted",
  "approval.denied",
  "system.health",
  "system.register",
  "system.deregister",
  "system.credential.refresh",
);
export type InterchangeType = typeof InterchangeType.infer;

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
export const SignatureStatus = type.enumerated(
  "valid",
  "invalid",
  "unknown",
  "missing",
);
export type SignatureStatus = typeof SignatureStatus.infer;

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
    subscriberAddress: string,
    signal?: AbortSignal,
  ): Promise<void>;

  unsubscribe(
    listAddress: string,
    subscriberAddress: string,
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

/**
 * How a media payload is carried by a content block. Either inline as a
 * base64-encoded string, or by reference to an opaque provider-native
 * handle (e.g. a Gemini fileUri, an Anthropic file_id). The wire shape
 * each provider expects is built by the provider adapter; MediaSource is
 * the internal, provider-agnostic representation.
 *
 * (INFERENCE.md § Generalized Multimodal Taxonomy)
 */
const MediaSourceBase64 = type({
  kind: "'base64'",
  mimeType: "string",
  data: "string",
});

const MediaSourceFileReference = type({
  kind: "'file-reference'",
  mimeType: "string",
  reference: "string",
});

export const MediaSource = MediaSourceBase64.or(MediaSourceFileReference);
export type MediaSource = typeof MediaSource.infer;

// Exported because `inference.image_output` events reference it by
// name, following the same pattern as `CitationBlock`,
// `CodeExecutionRequestBlock`, and `RedactedThinkingBlock`.
export const ImageBlock = type({
  type: "'image'",
  source: MediaSource,
});
export type ImageBlock = typeof ImageBlock.infer;

const AudioBlock = type({
  type: "'audio'",
  source: MediaSource,
});

const VideoBlock = type({
  type: "'video'",
  source: MediaSource,
});

const DocumentBlock = type({
  type: "'document'",
  source: MediaSource,
});

const ThinkingBlock = type({
  type: "'thinking'",
  thinking: "string",
  "signature?": "string",
});

/**
 * A thinking block whose content the provider has filtered. The
 * opaque `data` blob must echo back verbatim on every follow-up turn
 * — Anthropic 400s the request if it changes or goes missing. Treat
 * the bytes as opaque: do not log them and do not render them to
 * users.
 *
 * Exported because `inference.thinking.redacted` events reference it
 * by name.
 */
export const RedactedThinkingBlock = type({
  type: "'redacted_thinking'",
  data: "string",
});
export type RedactedThinkingBlock = typeof RedactedThinkingBlock.infer;
const ToolCallBlock = type({
  type: "'tool_call'",
  id: "string",
  name: "string",
  arguments: "Record<string, unknown>",
});
/**
 * Location of a citation's cited span within its source document.
 * The unit of `start` and `end` varies by `kind`:
 *   - "page": 1-indexed page numbers (Anthropic `page_location`).
 *   - "char": UTF-16 character offsets, matching JS string semantics
 *     (Anthropic `char_location`; Gemini `groundingSupports[].segment`).
 *   - "content-block": index into a structured source's content blocks
 *     (Anthropic `content_block_location`).
 */
const CitationLocation = type({
  kind: "'page' | 'char' | 'content-block'",
  start: "number",
  end: "number",
});

const CitationSource = type({
  "title?": "string",
  // Self-contained dereferenceable URL — populated by providers whose
  // citations carry URLs directly (Gemini `groundingChunks[].web.uri`).
  "uri?": "string",
  // Back-pointer into the request's `documents` array, populated by
  // providers that cite uploaded documents by position (Anthropic
  // `document_index`).
  "documentRef?": type({ index: "number" }),
});

/**
 * A citation that supports a span of assistant text. Emitted in
 * conversation-turn order immediately following the TextBlock it
 * annotates. Consumers MUST attribute trailing CitationBlocks to the
 * nearest preceding TextBlock in the same turn.
 *
 * Citations are deliberately excluded from ToolResultBlock.content
 * — they annotate model output, not tool output.
 *
 * Exported because `inference.citation` events reference it by name,
 * following the same pattern as `AssistantTurn`, `ToolCall`, and
 * `ToolResult`.
 */
export const CitationBlock = type({
  type: "'citation'",
  // The exact substring of the preceding TextBlock this citation
  // supports. Both providers emit it; required for inspection and
  // for fallback offset reconstruction.
  citedText: "string",
  source: CitationSource,
  "location?": CitationLocation,
  // UTF-16 character offsets into the preceding TextBlock's text.
  // Providers that emit offsets natively populate these directly;
  // adapters that derive offsets from a cited substring populate
  // them only when the substring appears unambiguously in the
  // preceding text. Omitted when the offset cannot be determined.
  "textOffset?": type({ start: "number", end: "number" }),
});
export type CitationBlock = typeof CitationBlock.infer;

/**
 * The model's request to execute code via a server-side execution tool.
 * Paired with a CodeExecutionResultBlock carrying the same `id` as the
 * result's `requestId`. Streaming order within a single execution is
 * `inference.code_execution.start` → zero or more
 * `inference.code_execution.delta` → `inference.code_execution.result`,
 * uninterrupted by other events that share the same `requestId`; events
 * with different `requestId`s or for other block kinds at distinct
 * `index`es may interleave.
 *
 * Exported because `inference.code_execution.start` references it by
 * name.
 */
export const CodeExecutionRequestBlock = type({
  type: "'code_execution_request'",
  // Identifier for the execution request. Populated from the
  // provider's call id where one exists (Anthropic
  // `srvtoolu_...`); synthesized by the adapter for providers that
  // don't emit one (Gemini), using a deterministic per-response
  // position-based scheme so replays match.
  id: "string",
  // Source code the model is asking to execute.
  code: "string",
  // Language hint. Absent when the provider does not emit one;
  // adapters MUST NOT default this — callers narrow on its
  // presence rather than fall through to a guessed language.
  "language?": "string",
});
export type CodeExecutionRequestBlock = typeof CodeExecutionRequestBlock.infer;

/**
 * The result of executing a CodeExecutionRequestBlock. The `requestId`
 * back-points to the request block's `id`. Status is normalized across
 * providers; raw provider signals (return code, native outcome string,
 * abort reason) are preserved on optional fields for callers that need
 * them.
 *
 * File outputs from code execution (e.g. generated plots that
 * Anthropic returns in `code_execution_tool_result.content`) are NOT
 * modeled by this block today. The block carries no field for them;
 * surfacing file outputs is a separate concern.
 *
 * Exported because `inference.code_execution.result` references it by
 * name.
 */
export const CodeExecutionResultBlock = type({
  type: "'code_execution_result'",
  // Back-pointer to the originating CodeExecutionRequestBlock.id.
  requestId: "string",
  // Normalized outcome. Translated from provider-specific signals:
  //   - Anthropic: derived from `return_code` (0 → "ok", non-zero →
  //     "error") and `abort_reason` (non-null → "aborted" or
  //     "timeout" per the reason).
  //   - Gemini: derived from the `outcome` enum
  //     (OUTCOME_OK → "ok", OUTCOME_FAILED → "error",
  //     OUTCOME_DEADLINE_EXCEEDED → "timeout", etc.).
  status: "'ok' | 'error' | 'aborted' | 'timeout'",
  // Standard output. Providers that don't split stdout from stderr
  // (Gemini) map their combined `output` here and leave `stderr` empty.
  "stdout?": "string",
  // Standard error. Empty for providers that don't split.
  "stderr?": "string",
  // Provider-native numeric return code when available
  // (Anthropic `return_code`). Absent for providers whose outcome
  // is enum-only (Gemini).
  "returnCode?": "number",
  // Provider-native outcome string preserved verbatim for callers
  // that need the raw signal (Gemini `OUTCOME_OK` /
  // `OUTCOME_FAILED` / `OUTCOME_DEADLINE_EXCEEDED` / ...). Absent
  // when the provider does not emit one (Anthropic).
  "providerOutcome?": "string",
  // Human-readable reason populated when status is "aborted"
  // (Anthropic `abort_reason`). Absent otherwise.
  "abortReason?": "string",
});
export type CodeExecutionResultBlock = typeof CodeExecutionResultBlock.infer;

const ToolResultBlock = type({
  type: "'tool_result'",
  callId: "string",
  // Deliberately narrow: tool results carry user-facing media, not
  // CitationBlocks (citations annotate the model's text output) and
  // not CodeExecution blocks (server-side code execution is a
  // distinct lifecycle from the user-tool round-trip).
  content: TextBlock.or(ImageBlock)
    .or(AudioBlock)
    .or(VideoBlock)
    .or(DocumentBlock)
    .array(),
  "detail?": "unknown",
  "isError?": "boolean",
});

export const ContentBlock = TextBlock.or(ThinkingBlock)
  .or(RedactedThinkingBlock)
  .or(ImageBlock)
  .or(AudioBlock)
  .or(VideoBlock)
  .or(DocumentBlock)
  .or(CitationBlock)
  .or(CodeExecutionRequestBlock)
  .or(CodeExecutionResultBlock)
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
    "timeout",
    "protocol_mismatch",
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
    data: {
      token: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  })
  .or({
    type: "'inference.thinking.signature'",
    seq: "number",
    data: { signature: "string", "index?": "number" },
  })
  .or({
    type: "'inference.thinking.redacted'",
    seq: "number",
    data: { redactedThinking: RedactedThinkingBlock, "index?": "number" },
  })
  .or({
    type: "'inference.text.delta'",
    seq: "number",
    data: {
      token: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  })
  .or({
    type: "'inference.tool_call.start'",
    seq: "number",
    data: {
      callId: "string",
      name: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  })
  .or({
    type: "'inference.tool_call.delta'",
    seq: "number",
    data: {
      callId: "string",
      argumentFragment: "string",
      partial: PartialMessage,
      "index?": "number",
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
      "index?": "number",
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
    type: "'inference.citation'",
    seq: "number",
    data: { citation: CitationBlock },
  })
  .or({
    type: "'inference.code_execution.start'",
    seq: "number",
    data: { request: CodeExecutionRequestBlock, "index?": "number" },
  })
  .or({
    type: "'inference.code_execution.delta'",
    seq: "number",
    // requestId correlates fragments back to the originating
    // CodeExecutionRequestBlock; index is the positional hint into
    // the response's content-block stream. They are independent: a
    // single response may stream code execution for multiple
    // requests interleaved, distinguished by requestId; index lets
    // the harness's per-block accumulator route the fragment to
    // the correct block when the array isn't yet finalized.
    data: {
      requestId: "string",
      codeFragment: "string",
      "index?": "number",
    },
  })
  .or({
    type: "'inference.code_execution.result'",
    seq: "number",
    data: { result: CodeExecutionResultBlock, "index?": "number" },
  })
  .or({
    type: "'inference.image_output'",
    seq: "number",
    // Fires mid-stream when an adapter finalizes an image-output
    // block, signaling that the image is ready for downstream
    // handoff before the full inference.done lands. The wrapped
    // ImageBlock typically carries a base64 MediaSource — the
    // payload can be large (Gemini's image-output captures show
    // ~1MB inline blobs); consumers that subscribe to this event
    // should treat it as a non-trivial transport size.
    data: { image: ImageBlock, "index?": "number" },
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
      data: { token: string; partial: PartialMessage; index?: number };
    }
  | {
      type: "inference.thinking.signature";
      seq: number;
      data: { signature: string; index?: number };
    }
  | {
      type: "inference.thinking.redacted";
      seq: number;
      data: { redactedThinking: RedactedThinkingBlock; index?: number };
    }
  | {
      type: "inference.text.delta";
      seq: number;
      data: { token: string; partial: PartialMessage; index?: number };
    }
  | {
      type: "inference.tool_call.start";
      seq: number;
      data: {
        callId: string;
        name: string;
        partial: PartialMessage;
        index?: number;
      };
    }
  | {
      type: "inference.tool_call.delta";
      seq: number;
      data: {
        callId: string;
        argumentFragment: string;
        partial: PartialMessage;
        index?: number;
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
        index?: number;
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
  | {
      type: "inference.citation";
      seq: number;
      data: { citation: CitationBlock };
    }
  | {
      type: "inference.code_execution.start";
      seq: number;
      data: { request: CodeExecutionRequestBlock; index?: number };
    }
  | {
      type: "inference.code_execution.delta";
      seq: number;
      data: { requestId: string; codeFragment: string; index?: number };
    }
  | {
      type: "inference.code_execution.result";
      seq: number;
      data: { result: CodeExecutionResultBlock; index?: number };
    }
  | {
      type: "inference.image_output";
      seq: number;
      data: { image: ImageBlock; index?: number };
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
  infer(options?: InferenceOptions): ReactorAction;
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
// Blob Reader (INFERENCE.md § Tool Result Lifecycle)
// ---------------------------------------------------------------------------

/**
 * Read-only capability for resolving `tool-output:///{callId}` URIs to the
 * underlying blob bytes. A `ToolResultTransform` that spills oversized tool
 * output writes a blob via `ContextStore.writeBlob` and returns a pointer of
 * the form `tool-output:///{callId}`; the agent's read tool reaches the spill
 * by calling `BlobReader.read(uri)`.
 *
 * The URI scheme is deliberately rigid:
 *
 * - Scheme: `tool-output`
 * - Authority: empty (the `///` makes pathname carry the callId)
 * - Path: `/{callId}` — preserves case so provider-assigned callIds with
 *   uppercase letters survive parsing
 * - Query and fragment: rejected
 *
 * Any deviation (different scheme, missing or non-empty hostname, extra path
 * segments, search string, or fragment) throws. Missing blobs throw.
 * `BlobReader` never accepts a filesystem path; the agent has no direct view
 * of the context store's working tree.
 */
export interface BlobReader {
  /**
   * Resolve `uri` to the underlying blob bytes. Throws if the URI is not a
   * well-formed `tool-output:///{callId}` reference or if no blob exists for
   * the extracted callId.
   */
  read(uri: string): Promise<Uint8Array>;
}

/** Source for blob bytes used by `createBlobReader`. */
export interface BlobSource {
  readBlob(key: string, signal?: AbortSignal): Promise<Uint8Array>;
}

/**
 * Parse a `tool-output:///{callId}` URI and return the callId. Throws on any
 * deviation from the documented shape: wrong scheme, non-empty authority,
 * missing or extra path components, search string, or fragment.
 *
 * The two-slash form `tool-output://abc` is rejected because the URL parser
 * lowercases the hostname, which silently corrupts provider-assigned callIds
 * that contain uppercase letters. The three-slash form puts the callId in
 * `pathname`, where case is preserved.
 */
export function parseToolOutputURI(uri: string): string {
  let parsed: URL;
  try {
    parsed = new URL(uri);
  } catch (cause) {
    throw new Error(`invalid tool-output URI: ${uri}`, { cause });
  }
  if (parsed.protocol !== "tool-output:") {
    throw new Error(
      `invalid tool-output URI scheme: expected "tool-output:", got "${parsed.protocol}"`,
    );
  }
  if (parsed.hostname !== "") {
    throw new Error(
      `invalid tool-output URI: authority must be empty (use the form tool-output:///{callId}), got "${parsed.hostname}"`,
    );
  }
  if (parsed.search !== "") {
    throw new Error(
      `invalid tool-output URI: query string is not allowed, got "${parsed.search}"`,
    );
  }
  if (parsed.hash !== "") {
    throw new Error(
      `invalid tool-output URI: fragment is not allowed, got "${parsed.hash}"`,
    );
  }
  const path = parsed.pathname;
  if (!path.startsWith("/")) {
    throw new Error(`invalid tool-output URI: empty path: ${uri}`);
  }
  const callId = path.slice(1);
  if (callId === "") {
    throw new Error(`invalid tool-output URI: missing callId: ${uri}`);
  }
  if (callId.includes("/")) {
    throw new Error(
      `invalid tool-output URI: path must contain a single callId segment, got "${callId}"`,
    );
  }
  return callId;
}

/**
 * Construct a `BlobReader` that resolves `tool-output:///{callId}` URIs by
 * delegating to `source.readBlob(callId)`. The most common source is a
 * `ContextStore` (Phase 2 added `readBlob` to that interface), but any object
 * implementing `BlobSource` works — this keeps tests trivial.
 *
 * URI parsing is performed in this layer; the source only ever sees the
 * extracted callId. Missing blobs surface as whatever error the source
 * raises (`ContextStore.readBlob` already throws for unknown keys).
 */
export function createBlobReader(source: BlobSource): BlobReader {
  return {
    async read(uri: string): Promise<Uint8Array> {
      const callId = parseToolOutputURI(uri);
      return source.readBlob(callId);
    },
  };
}

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
// Inference Source (INFERENCE.md § Providers)
// ---------------------------------------------------------------------------

/**
 * Model-bound default knobs for an inference source. Per-call
 * `InferenceOptions.X` overrides `defaults.X`; the merge happens once at
 * the top of `runInference` before the adapter sees anything. New fields
 * land here as separately-scoped issues.
 */
export const InferenceSourceDefaults = type({
  "maxTokens?": "number",
});
export type InferenceSourceDefaults = typeof InferenceSourceDefaults.infer;

/**
 * A specific (provider, model) bundle the agent runtime can route to.
 * Carries wire reachability, credentials, the model identity at the
 * provider, and the model-bound default knobs.
 *
 * Pre-catalog, `id` is synthesized by the resolver as `${provider}:${model}`.
 * When the catalog ships, `id` becomes the catalog primary key and the
 * resolver looks the source up rather than synthesizing it. `id` is the
 * routing key used by `AgentConfig.defaultSource` and `Agent.setSource`.
 *
 * Multi-model providers become multiple sources — `model` is part of the
 * identity, not an optional override.
 *
 * `capabilities` is carried for the selection-policy layer (the model
 * selector consumes it). The runtime ignores it; populating the field
 * later is not a wire-format change.
 *
 * (INFERENCE.md § Providers)
 */
export const InferenceSource = type({
  id: "string",
  provider: "string",
  baseURL: "string",
  apiKey: "string",
  model: "string",
  "defaults?": InferenceSourceDefaults,
  "capabilities?": "string[]",
});
export type InferenceSource = typeof InferenceSource.infer;

/**
 * Replace every field on `active` with the corresponding field from
 * `next`, in place. Optional fields (`defaults`, `capabilities`) are
 * `delete`d from `active` when absent on `next` so the swap is exact —
 * no stale value from a previous rotation can survive.
 *
 * Used by both the agent's source registry and the harness's source
 * hot-swap path to mutate the single shared `InferenceSource` object the
 * reactor reads lazily at the start of each inference call. Putting the
 * field list in one place means the next field added to
 * `InferenceSource` only has to be remembered here.
 */
export function applyInferenceSourceFields(
  active: InferenceSource,
  next: InferenceSource,
): void {
  active.id = next.id;
  active.provider = next.provider;
  active.baseURL = next.baseURL;
  active.apiKey = next.apiKey;
  active.model = next.model;
  if (next.defaults !== undefined) {
    active.defaults = next.defaults;
  } else {
    delete active.defaults;
  }
  if (next.capabilities !== undefined) {
    active.capabilities = next.capabilities;
  } else {
    delete active.capabilities;
  }

  // Compile-time exhaustiveness check. `Required<>` forces optional
  // keys to also be required in the guard — so a future optional field
  // (e.g. `region?: string`) added to `InferenceSource` without being
  // handled above is flagged by TypeScript, not silently dropped.
  const _handled: { readonly [K in keyof Required<InferenceSource>]: true } = {
    id: true,
    provider: true,
    baseURL: true,
    apiKey: true,
    model: true,
    defaults: true,
    capabilities: true,
  };
  void _handled;
}

/**
 * Options for a single inference call. Override the defaults from the agent
 * configuration on a per-call basis.
 *
 * (INFERENCE.md § Providers › Streaming Harness)
 */
export type InferenceOptions = {
  maxTokens?: number;
  temperature?: number;
  thinking?: { enabled: boolean; budgetTokens?: number };
  systemPrompt?: string;
  tools?: ToolDefinition[];
  /**
   * Modalities the caller wants the model to emit. Adapters translate
   * to the provider-native shape (Gemini's
   * `generationConfig.responseModalities` accepts `"TEXT"` / `"IMAGE"`
   * uppercase; see `packages/inference-testing/wire/google-genai/
   * gemini-2.5-flash-image/image-output/request.json` for the captured
   * shape). Providers that do not expose a modality switch ignore the
   * field. When omitted the provider's default modalities apply.
   */
  responseModalities?: ("text" | "image" | "audio")[];
  /**
   * Per-call inactivity timeout in milliseconds. If the harness yields no
   * event (other than `inference.start`) for this many ms, the underlying
   * fetch is aborted and the call ends with `inference.error` of category
   * `"timeout"`. Default 120_000 (2 min). Tune higher for reasoning models
   * that exhibit long silent-thinking stretches between token bursts; tune
   * lower to fail fast. `0` arms the timer to fire on the next tick (a
   * "fail-fast even if the fetch is instant" mode useful in tests).
   */
  inactivityTimeoutMs?: number;
  /**
   * Per-call total wall-clock cap in milliseconds. Starts at fetch.
   * Default 600_000 (10 min). Backstop for streams that keep emitting
   * forever without terminating. Same error category as `inactivityTimeoutMs`.
   * `0` arms the timer to fire on the next tick.
   */
  totalTimeoutMs?: number;
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
   * Commit whatever currently lives in the working tree, using the supplied
   * commit message. The reactor's per-cycle checkpoint routes through this
   * overload after writing the per-cycle files via `writeTurns`,
   * `writePrompt`, `writeResponse`, `writeManifest`, and any `writeBlob`
   * calls produced by transforms.
   */
  commit(
    options: { message: string },
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

  /**
   * Write an opaque blob to the working tree under `tool-output/`. Used by
   * `ToolResultTransform`s that spill oversized payloads out of the inline
   * conversation. The file is staged at the next `commit({ message })`.
   *
   * `key` is sanitized for filesystem safety; callers should pass the tool
   * call id. `contentType` selects a file extension when known.
   */
  writeBlob(
    key: string,
    bytes: Uint8Array,
    contentType?: string,
    signal?: AbortSignal,
  ): Promise<void>;

  /**
   * Read a blob previously written via `writeBlob`. Throws if no blob with
   * that key exists.
   */
  readBlob(key: string, signal?: AbortSignal): Promise<Uint8Array>;

  /**
   * Overwrite `prompt.jsonl` with the materialized prompt for the current
   * inference cycle. One `ConversationTurn` per line. Staged at the next
   * `commit({ message })`.
   */
  writePrompt(turns: ConversationTurn[], signal?: AbortSignal): Promise<void>;

  /**
   * Overwrite `response.jsonl` with the assistant turn returned for the
   * current cycle. Single-line JSONL for consistency with the per-cycle file
   * conventions. Staged at the next `commit({ message })`.
   */
  writeResponse(turn: AssistantTurn, signal?: AbortSignal): Promise<void>;

  /**
   * Overwrite `manifest.jsonl` with the ordered transform records produced
   * for the current cycle. One `TransformRecord` per line. Staged at the
   * next `commit({ message })`.
   */
  writeManifest(
    records: TransformRecord[],
    signal?: AbortSignal,
  ): Promise<void>;

  /**
   * Overwrite `turns.jsonl` with the durable conversation history. One
   * `ConversationTurn` per line. Staged at the next `commit({ message })`.
   */
  writeTurns(turns: ConversationTurn[], signal?: AbortSignal): Promise<void>;

  /**
   * Overwrite `metadata.json` with non-turn-shaped reactor state needed for
   * restart: pending async operations and cumulative token usage. The store
   * combines this with the most recently buffered connector state (from
   * `setConnectorState`) and writes the merged payload. Staged at the next
   * `commit({ message })`.
   */
  writeMetadata(
    metadata: {
      pendingOperations: PendingOperation[];
      tokenUsage: TokenUsage;
    },
    signal?: AbortSignal,
  ): Promise<void>;

  /**
   * Read manifest entries from the most recent `limit` commits that contain
   * a `manifest.jsonl`. Newest commit first; records within a commit are
   * returned in their natural in-file order (chronological per-cycle).
   */
  readManifestHistory(
    limit: number,
    signal?: AbortSignal,
  ): Promise<TransformRecord[]>;
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
  sources: InferenceSource.array(),
  defaultSource: "string",
  "sessionChannelEnabled?": "boolean",
});
export type HarnessConfig = typeof HarnessConfig.infer;
