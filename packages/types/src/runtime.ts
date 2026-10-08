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
import type { SignalKind } from "./signals";

// ---------------------------------------------------------------------------
// Cryptographic Identity (ARCHITECTURE.md § Cryptographic Identity,
//                         IMPLEMENTATION.md § Cryptographic Identity: Key Formats)
// ---------------------------------------------------------------------------

/**
 * An Ed25519 key pair as raw bytes. Uint8Array throughout so key material
 * stays runtime-agnostic (Bun, Node, browser) and never leaks through JSON
 * serialization.
 */
export type KeyPair = {
  privateKey: Uint8Array;
  publicKey: Uint8Array;
};

/**
 * A key-bound cryptographic provider. Each instance is constructed with a
 * specific agent's Ed25519 key pair and holds the private key internally.
 *
 * `sign` uses the instance's own private key; `verify` accepts a public key
 * parameter so the holder can verify messages from arbitrary senders.
 *
 * Key formats (IMPLEMENTATION.md):
 * - Ed25519 in SSH format — control plane interactions
 * - Ed25519 in PGP format — message-level signatures over SMTP/IMAP
 * - Ed25519 in X.509 format — TLS mutual auth certificates
 */
export interface CryptoProvider {
  /**
   * Sign `content` with the instance's private key, returning the Ed25519
   * detached signature as raw bytes.
   */
  sign(content: Uint8Array): Promise<Uint8Array>;

  /**
   * Sign `payload` with the SSH signature envelope (sshsig), returning an
   * ASCII-armored SSH SIGNATURE block for `gpgsig`-style consumers. Framing
   * differs from `sign`'s raw output; pick the matching method per format.
   */
  signSSH(payload: string): Promise<string>;

  /**
   * Verify that `signature` over `content` was produced by `publicKey`.
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

/** Opaque reference to a message in a specific mailbox. */
export type MessageRef = {
  uid: number;
  mailbox: string;
};

/**
 * Interchange payload types (MESSAGE.md § Payload Types). The type field in
 * structured messages matches the Interchange-Type header. Exposed as both
 * an arktype validator and a derived TypeScript union.
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

const CONVERSATION_TYPES: ReadonlySet<InterchangeType> = new Set([
  "conversation.message",
  "conversation.join",
  "conversation.leave",
]);

/**
 * Membership is enumerated rather than derived from the `conversation.` name
 * prefix, so a member added to InterchangeType is not a conversation type until
 * it is listed in CONVERSATION_TYPES.
 */
export function isConversationType(value: InterchangeType): boolean {
  return CONVERSATION_TYPES.has(value);
}

/** Attachment for an outbound message. */
export type MessageAttachment = {
  name: string;
  contentType: string;
  data: Uint8Array;
  /**
   * IMAP BODY section of this part, stamped by `extractAttachments` from
   * the parsed sibling numbering. Absent on outbound attachments.
   */
  part?: string;
};

/**
 * A message the harness submits for delivery via SMTP. Conversation types
 * (conversation.*) carry `content` as text/plain; structured types carry
 * `payload` as application/vnd.interchange+json. Providing both is an error.
 *
 * (MESSAGE.md § Transport Interface › Outbound)
 */
export type OutboundMessage = {
  to: string | string[];
  cc?: string | string[];
  subject?: string;

  type: InterchangeType;

  /** Plain text body for conversation.* types. */
  content?: string;

  /** Structured JSON body for non-conversation types. */
  payload?: Record<string, unknown>;

  /** Human-readable summary for structured messages (the text/plain part). */
  summary?: string;

  attachments?: MessageAttachment[];

  /** Message-ID of the message being replied to. */
  inReplyTo?: string;

  /**
   * RFC 5322 References chain for a threaded reply: the parent's References
   * plus its Message-ID, in order. When present the transport ships it
   * verbatim (appending `inReplyTo` if not already the tail).
   */
  references?: string[];

  /** Correlation ID linking this message to a pending async request. */
  correlationId?: string;

  /** Reactor session ID from the Interchange-Session-ID header. */
  sessionId?: string;

  /** Tenant ID for the Interchange-Tenant-ID header. */
  tenantId?: string;
};

/**
 * Receipt returned by `send()`: the assigned Message-ID and delivery status.
 *
 * (MESSAGE.md § Transport Interface › Outbound)
 */
export type SendReceipt = {
  messageId: string;
  status: "delivered" | "queued";
};

/**
 * Parsed headers from an inbound message. Field names follow RFC 5322 and
 * the header conventions from MESSAGE.md § Headers. `from` is optional: a
 * message can arrive carrying no originator.
 */
export type MessageHeaders = {
  from?: string;
  to: string[];
  cc?: string[];
  date?: string;
  messageId?: string;
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
 * Signature verification status of an inbound message: `valid`, `invalid`,
 * `unknown` (no key available), or `missing` (not signed).
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
 * The admission outcome of an inbound message, the single vocabulary a
 * delivery decision keys on. `clean` is always admitted; `error` (a fault
 * stopped the check) is always rejected.
 */
export const InboundMailOutcome = type.enumerated(
  "clean",
  "untrustedFrom",
  "mismatchedFrom",
  "absentFrom",
  "invalid",
  "missing",
  "unknown",
  "error",
);
export type InboundMailOutcome = typeof InboundMailOutcome.infer;

/**
 * The subset of {@link InboundMailOutcome} a workflow author may relax to
 * admit a message that would otherwise be rejected. Omits `clean` (always
 * admitted) and `error` (pinned to reject).
 */
export const AuthorControllableOutcome = type.enumerated(
  "untrustedFrom",
  "mismatchedFrom",
  "absentFrom",
  "invalid",
  "missing",
  "unknown",
);
export type AuthorControllableOutcome = typeof AuthorControllableOutcome.infer;

/**
 * A per-workflow inbound-mail admission policy: for each
 * {@link AuthorControllableOutcome}, whether a message raising that outcome
 * is `reject`ed or `admit`ted. Sparse and undeclared-key-rejecting: omitted
 * keys are not defaults, and sparse keys keep the content hash covering only
 * what the author declared.
 */
export const InboundMailPolicy = type({
  "untrustedFrom?": "'reject' | 'admit'",
  "mismatchedFrom?": "'reject' | 'admit'",
  "absentFrom?": "'reject' | 'admit'",
  "invalid?": "'reject' | 'admit'",
  "missing?": "'reject' | 'admit'",
  "unknown?": "'reject' | 'admit'",
}).onUndeclaredKey("reject");
export type InboundMailPolicy = typeof InboundMailPolicy.infer;

type AssertEqual<A, B> = [A] extends [B]
  ? [B] extends [A]
    ? true
    : false
  : false;

/** The three outcome vocabularies must move together. */
const _policyKeysMatchAuthorControllable: AssertEqual<
  keyof InboundMailPolicy,
  AuthorControllableOutcome
> = true;

const _authorControllableAreOutcomes: AssertEqual<
  Exclude<AuthorControllableOutcome, InboundMailOutcome>,
  never
> = true;

/**
 * A parsed MIME part. `content` is the DECODED bytes in memory (transfer
 * encoding already undone); `filename`/`disposition` are surfaced from the
 * part's headers so a consumer need not re-parse them.
 */
export type MessagePart = {
  contentType: string;
  content: Uint8Array;
  filename?: string;
  disposition?: "inline" | "attachment";
  /**
   * The declared Content-Transfer-Encoding. Not set for a decoded part --
   * `content` is already decoded, so the wire encoding is spent metadata.
   */
  encoding?: string;
};

/**
 * A single part of a persisted `Mail`. The bytes live in the durable store;
 * `ref` resolves them via a `MailPartReader`. Small UTF-8 text parts also
 * carry their decoded `text` inline.
 */
export type MailPart = {
  contentType: string;
  filename?: string;
  disposition?: "inline" | "attachment";
  ref: string;
  text?: string;
};

/**
 * Environment-agnostic reader for a persisted mail part's bytes. A consumer
 * resolves a `MailPart.ref` without knowing where the bytes live; the `ref`
 * is opaque and the reader owns its scheme.
 */
export interface MailPartReader {
  /** Resolve a `MailPart.ref` to the part's decoded bytes. Throws if the ref
   * is unrecognized or its bytes are missing. */
  read(ref: string): Promise<Uint8Array>;
}

/**
 * A fully decoded mail message: the typed header subset, a raw catch-all with
 * nothing dropped, and the flat list of decoded leaf parts. JSON-safe: each
 * part carries a `ref` (not raw bytes), so binary content never enters the
 * event log; the runtime resolves a `ref` into a `MessagePart` on demand.
 */
export type Mail = {
  headers: MessageHeaders;
  /** Every header, lowercased name to its ordered values; nothing dropped. */
  rawHeaders: Record<string, string[]>;
  parts: MailPart[];
};

const MailShape = type({
  // Require the recipient list consumers dereference unconditionally; other
  // headers stay optional and lossless in `rawHeaders`. `from` stays
  // optional: mail with no usable originator is still mail.
  headers: {
    "from?": "string",
    to: "string[]",
  },
  rawHeaders: {
    "[string]": "string[]",
  },
  parts: type({
    contentType: "string",
    ref: "string",
    "filename?": "string",
    "disposition?": "'inline' | 'attachment'",
    "text?": "string",
  })
    .onUndeclaredKey("reject")
    .array(),
}).onUndeclaredKey("reject");

/**
 * Narrow an opaque value (a workflow step input) to a `Mail`, used at the
 * `agent.send` boundary to decide whether its parts must be projected into
 * content blocks. Strict undeclared-key rejection keeps an arbitrary value
 * that merely carries a `parts` field from matching.
 */
export function isMail(value: unknown): value is Mail {
  return !(MailShape(value) instanceof type.errors);
}

/**
 * MIME tree metadata returned by `fetchStructure()`: content types, sizes,
 * and dispositions, without transferring content. (MESSAGE.md § Partial Fetch)
 */
export type BodyStructure = {
  contentType: string;
  size?: number;
  disposition?: string;
  parts?: BodyStructure[];
};

/**
 * A fully parsed inbound message: structured payload, headers, attachments,
 * and signature verification status. (MESSAGE.md § Transport Interface › fetchFull)
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

/** IMAP mailbox descriptor. (MESSAGE.md § Inbox Management) */
export type Mailbox = {
  name: string;
  role?: string;
  delimiter?: string;
};

/** Current status of an IMAP mailbox, including QRESYNC identifiers. (MESSAGE.md § Inbox Management) */
export type MailboxStatus = {
  total: number;
  unseen: number;
  recent: number;
  uidNext: number;
  uidValidity: number;
  highestModSeq: number;
};

/** Structured IMAP search query mapping the SEARCH grammar to a typed object. (MESSAGE.md § Search) */
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

/** A thread node returned by `thread()`: a message reference plus child reply threads. (MESSAGE.md § Thread Retrieval) */
export type Thread = {
  ref: MessageRef;
  children: Thread[];
};

/** QRESYNC state the harness provides when reconnecting. (MESSAGE.md § Synchronization) */
export type SyncState = {
  uidValidity: number;
  uidNext: number;
  highestModSeq: number;
  knownUids?: number[];
};

/** Result of a QRESYNC-style sync operation. (MESSAGE.md § Synchronization) */
export type SyncResult = {
  vanished: number[];
  changed: { uid: number; flags: string[] }[];
  newMessages: MessageRef[];
  fullResyncRequired: boolean;
};

/** Distribution list metadata returned by `createList()`. (MESSAGE.md § Message Topologies) */
export type ListInfo = {
  address: string;
  name: string;
  memberCount: number;
  createdAt: string;
};

/** Event emitted by the mailbox watcher callback (IMAP IDLE). (MESSAGE.md § Real-Time Notification) */
export type MailboxEvent =
  | { type: "exists"; uid: number; headers: MessageHeaders }
  | { type: "flagsChanged"; uid: number; flags: string[] }
  | { type: "expunged"; uid: number };

/** Unsubscribe function returned by `watch()`. */
export type Unsubscribe = () => void;

/**
 * The message transport interface, abstracting SMTP and IMAP behind a
 * TypeScript API (real servers to in-memory stubs). All long-running
 * operations accept an AbortSignal for cooperative cancellation.
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

  /** Permanently remove every `\Deleted` message; returns the expunged uids. */
  expunge(
    mailbox: string,
    signal?: AbortSignal,
  ): Promise<{ expungedUids: number[] }>;

  // --- Real-time notification ---

  /**
   * Monitor a mailbox for new messages and flag changes (IMAP IDLE). The
   * promise resolves once the watch is accepted; a refusal rejects with the
   * transport's condition. The unsubscribe function stays synchronous.
   */
  watch(
    mailbox: string,
    callback: (event: MailboxEvent) => void,
  ): Promise<Unsubscribe>;

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

/**
 * The condition a `MessageTransport` operation failed under. A transport that
 * grows a condition this union does not carry takes its name from RFC 5530 § 3
 * rather than coining one (https://www.rfc-editor.org/rfc/rfc5530.html).
 */
export const MessageTransportCondition = type(
  "'NONEXISTENT' | 'CANNOT' | 'SERVERBUG'",
);
export type MessageTransportCondition = typeof MessageTransportCondition.infer;

export class MessageTransportError extends Error {
  readonly condition: MessageTransportCondition;

  constructor(condition: MessageTransportCondition, message: string) {
    super(message);
    this.name = "MessageTransportError";
    this.condition = condition;
  }
}

/**
 * `instanceof` is not usable for this: a tool package is published as a bundle
 * with workspace imports inlined, so a bundle-loaded consumer holds its own
 * copy of MessageTransportError and `instanceof` against the host's answers
 * false. The check is structural instead.
 */
export function isMessageTransportError(
  value: unknown,
): value is Error & { readonly condition: MessageTransportCondition } {
  if (!(value instanceof Error)) return false;
  if (!("condition" in value)) return false;
  return MessageTransportCondition.allows(value.condition);
}

// ---------------------------------------------------------------------------
// Tool Execution (ARCHITECTURE.md § Tools, INFERENCE.md § Tool Execution)
// ---------------------------------------------------------------------------

/**
 * A tool call as requested by the model: provider-assigned call ID, tool
 * name, and parsed arguments. (INFERENCE.md § Message Format › Content Types)
 */
export const ToolCall = type({
  id: "string",
  name: "string",
  arguments: "Record<string, unknown>",
});
export type ToolCall = typeof ToolCall.infer;

/**
 * Approver-facing snapshot of the tool call awaiting approval, built at the
 * authz `ask` branch and threaded to the hub co-write. `name`,
 * `description`, and `inputSchema` mirror the {@link ToolDefinition};
 * `arguments` is the live call's; never folded into {@link ToolCall}.
 */
export const ApprovalSnapshot = type({
  name: "string",
  description: "string",
  inputSchema: "Record<string, unknown>",
  arguments: "Record<string, unknown>",
});
export type ApprovalSnapshot = typeof ApprovalSnapshot.infer;

/**
 * The kind of a control-plane park: a step suspended awaiting an external
 * event.
 *
 * - `"approval"` -- parked on a tool/authz gate; carries an
 *   {@link ApprovalSnapshot} and notifies the host (`env.onPark`).
 * - `"input"` -- parked awaiting its next input; carries no snapshot, does
 *   not notify the host, and is deliberately not a {@link SignalKind}.
 * - `"signal-relay"` -- an onTrigger section container parked on an
 *   author-named signal; the name is not a reserved
 *   `signalName(correlationId)`, so recovery must branch on this kind first.
 *
 * Kinds are discriminated by an explicit discriminant, never by snapshot
 * presence, which would silently reclassify a malformed snapshot-less
 * approval as another park kind.
 */
export const ControlParkKind = type.enumerated(
  "approval",
  "input",
  "signal-relay",
);
export type ControlParkKind = typeof ControlParkKind.infer;

/**
 * Maximum serialized size, in UTF-8 bytes, of an {@link ApprovalSnapshot}
 * crossing a trust boundary. A snapshot approaching this bound is malformed
 * or hostile and is rejected at the parse boundary.
 */
export const APPROVAL_SNAPSHOT_MAX_BYTES = 131072;

/**
 * {@link ApprovalSnapshot} bounded to {@link APPROVAL_SNAPSHOT_MAX_BYTES}.
 * Parse the snapshot through this validator at trust boundaries (the
 * `park.notify` and `parked-correlations.response` IPC frames, the
 * sidecar→hub register frame); internal hops use the unbounded
 * {@link ApprovalSnapshot}.
 */
export const BoundedApprovalSnapshot = ApprovalSnapshot.narrow(
  (snapshot, ctx) => {
    const bytes = Buffer.byteLength(JSON.stringify(snapshot), "utf8");
    return (
      bytes <= APPROVAL_SNAPSHOT_MAX_BYTES ||
      ctx.mustBe(`at most ${APPROVAL_SNAPSHOT_MAX_BYTES} bytes when serialized`)
    );
  },
);
export type BoundedApprovalSnapshot = typeof BoundedApprovalSnapshot.infer;

/**
 * Result of a tool execution. `content` is what the model sees; `detail` is
 * harness-only. `isError` surfaces an error to the model; `pendingMarker`
 * marks an async tool whose correlation ID awaits a matching inbound
 * message.
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
  },
});
export type ToolResult = typeof ToolResult.infer;

/**
 * The tool runner interface, implemented by the harness and called by the
 * reactor. Parallel execution is modeled by calling `run` concurrently per
 * call in a batch. (ARCHITECTURE.md § Agent Harness › Tools)
 */
export interface ToolRunner {
  /**
   * Execute a single tool call. Must not throw — errors are returned as
   * `ToolResult` with `isError: true`.
   */
  run(call: ToolCall, signal: AbortSignal): Promise<ToolResult>;
}

// ---------------------------------------------------------------------------
// Inference Event Building Blocks (INFERENCE.md § Event Protocol)
// ---------------------------------------------------------------------------

/**
 * Partial assistant message accumulated during streaming, carrying all
 * content blocks seen so far so late-joining subscribers get current state
 * without replaying deltas. `text` and `thinking` are cumulative across
 * every delta of that kind; per-block structure lives in the finalized
 * `inference.done` turn's content[].
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
 * provider-specific and may be zero when unreported. (INFERENCE.md § Token Accounting)
 */
export const TokenUsage = type({
  input: "number",
  output: "number",
  cacheRead: "number",
  cacheWrite: "number",
  thinking: "number",
});
export type TokenUsage = typeof TokenUsage.infer;

/**
 * Slim source descriptor stamped onto `inference.usage` / `inference.done`
 * events and `ReactorState.lastCycleSource`, so state-aware policies can
 * attribute usage without re-reading the live `InferenceSource`. Strict
 * subset: credentials and endpoints must not leak to policy code; full
 * sources go through the harness's source registry.
 */
export const LastCycleSource = type({
  sourceId: "string",
  provider: "string",
  model: "string",
});
export type LastCycleSource = typeof LastCycleSource.infer;

// ---------------------------------------------------------------------------
// Internal Turn Format (INFERENCE.md § Message Format)
// ---------------------------------------------------------------------------

/**
 * A single content block within a conversation turn. Provider-agnostic.
 *
 * (INFERENCE.md § Message Format › Content Types)
 */
const TextBlock = type({
  type: "'text'",
  text: "string",
  // Opaque provider signature; echo back verbatim on follow-up turns (Gemini `thoughtSignature`, including plain text).
  "signature?": "string",
});

/**
 * How a media payload is carried by a content block: inline base64, an
 * opaque provider-native handle (Gemini fileUri, Anthropic file_id), or a
 * public URL the provider fetches itself. Provider-agnostic internal form.
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

const MediaSourceUrl = type({
  kind: "'url'",
  mimeType: "string",
  url: "string",
});

export const MediaSource = type.or(
  MediaSourceBase64,
  MediaSourceFileReference,
  MediaSourceUrl,
);
export type MediaSource = typeof MediaSource.infer;

// Exported because `inference.image_output` events reference it by
// name, following the same pattern as `CitationBlock`,
// `CodeExecutionRequestBlock`, and `RedactedThinkingBlock`.
export const ImageBlock = type({
  type: "'image'",
  source: MediaSource,
  // Opaque provider signature; echo back verbatim on follow-up turns (Gemini rides a `thoughtSignature` on the inlineData part).
  "signature?": "string",
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
  "title?": "string",
  "context?": "string",
});

const ThinkingBlock = type({
  type: "'thinking'",
  thinking: "string",
  "signature?": "string",
});

/**
 * A thinking block whose content the provider filtered. The opaque `data`
 * blob must echo back verbatim on every follow-up turn — Anthropic 400s if
 * it changes or goes missing. Do not log or render it.
 *
 * Exported because `inference.thinking.redacted` events reference it by name.
 */
export const RedactedThinkingBlock = type({
  type: "'redacted_thinking'",
  data: "string",
});
export type RedactedThinkingBlock = typeof RedactedThinkingBlock.infer;

/**
 * A model-emitted refusal: the provider's strict-mode structured-outputs
 * path declined to satisfy the requested schema (OpenAI `delta.refusal` /
 * `message.refusal`). Distinct from `inference.error`: the HTTP call
 * succeeded, and `reason` is the model's refusal text in lieu of
 * schema-conformant content.
 *
 * Exported because `inference.refusal.delta` events reference it by name.
 */
export const RefusalBlock = type({
  type: "'refusal'",
  // A zero-length reason would be indistinguishable from a lost payload; the
  // adapter's empty-chunk filter is belt-and-braces alongside this.
  reason: "string > 0",
});
export type RefusalBlock = typeof RefusalBlock.infer;
const ToolCallBlock = type({
  type: "'tool_call'",
  id: "string",
  name: "string",
  arguments: "Record<string, unknown>",
  // Opaque provider signature; echo back verbatim on follow-up turns (Gemini rides a `thoughtSignature` on the functionCall part).
  "signature?": "string",
});
/**
 * Location of a citation's cited span within its source document. The unit
 * of `start`/`end` varies by `kind`: "page" (Anthropic `page_location`),
 * "char" (UTF-16 offsets; Anthropic `char_location`, Gemini
 * `groundingSupports[].segment`), or "content-block" (Anthropic
 * `content_block_location`).
 */
const CitationLocation = type({
  kind: "'page' | 'char' | 'content-block'",
  start: "number",
  end: "number",
});

const CitationSource = type({
  "title?": "string",
  // URL populated by providers whose citations carry one directly (Gemini
  // `groundingChunks[].web.uri`).
  "uri?": "string",
  // Index into the request's `documents` array (Anthropic `document_index`).
  "documentRef?": type({ index: "number" }),
});

/**
 * A citation supporting a span of assistant text. Without a paired
 * source-block index, consumers MUST attribute it by adjacency to the
 * nearest preceding TextBlock in the same turn. Deliberately excluded from
 * ToolResultBlock.content — citations annotate model output, not tool
 * output.
 *
 * Exported because `inference.citation` events reference it by name.
 */
export const CitationBlock = type({
  type: "'citation'",
  // The exact substring of the preceding TextBlock this citation supports;
  // required for inspection and fallback offset reconstruction.
  citedText: "string",
  source: CitationSource,
  "location?": CitationLocation,
  // UTF-16 character offsets into the preceding TextBlock's text, populated
  // by the provider or derived by the adapter when the cited substring
  // appears unambiguously. Omitted when they cannot be determined.
  "textOffset?": type({ start: "number", end: "number" }),
});
export type CitationBlock = typeof CitationBlock.infer;

/**
 * A structured safety signal on model output or request filtering. The
 * payload mirrors the first real Gemini capture (2026-07-28), which was
 * prompt-level only: `promptFeedback: { blockReason: "PROHIBITED_CONTENT" }`,
 * no candidates, no per-category ratings. Carries `blockReason` only.
 *
 * Deliberately excluded from ToolResultBlock.content — safety signals
 * annotate model/request filtering, not tool output.
 *
 * Exported because `inference.safety_rating` events reference it by name.
 */
export const SafetyRatingBlock = type({
  type: "'safety_rating'",
  // Provider-native reason string (observed: "PROHIBITED_CONTENT"); open so a
  // new token does not force a type bump.
  blockReason: "string > 0",
});
export type SafetyRatingBlock = typeof SafetyRatingBlock.infer;

/**
 * Human-readable rendering of a SafetyRatingBlock for reply text,
 * timeline summaries, and request-history rewrites when a provider
 * has no input wire shape for safety_rating. Single owner of the
 * display string so reply / history / transform stay in lockstep.
 */
export function formatSafetyRatingText(block: SafetyRatingBlock): string {
  return `Request blocked: ${block.blockReason}`;
}

/**
 * The model's request to execute code via a server-side execution tool.
 * Paired with a CodeExecutionResultBlock whose `requestId` matches this
 * block's `id`. Streaming order for one execution is
 * `inference.code_execution.start` → zero or more `...delta` →
 * `inference.code_execution.result`, uninterrupted by events sharing the
 * `requestId`.
 *
 * Exported because `inference.code_execution.start` references it by name.
 */
export const CodeExecutionRequestBlock = type({
  type: "'code_execution_request'",
  // Provider call id where one exists (Anthropic `srvtoolu_...`); otherwise
  // synthesized deterministically per response position so replays match.
  id: "string",
  // Source code the model is asking to execute.
  code: "string",
  // Absent when the provider does not emit one; adapters MUST NOT default it
  // — callers narrow on presence rather than fall through to a guess.
  "language?": "string",
  // Opaque provider signature; echo back verbatim on follow-up turns (Gemini rides a `thoughtSignature` on the executableCode part).
  "signature?": "string",
});
export type CodeExecutionRequestBlock = typeof CodeExecutionRequestBlock.infer;

/**
 * The result of executing a CodeExecutionRequestBlock; `requestId`
 * back-points to the request block's `id`. Status is normalized across
 * providers; raw provider signals (return code, native outcome string,
 * abort reason) are preserved on optional fields. File outputs are not
 * modeled today.
 *
 * Exported because `inference.code_execution.result` references it by name.
 */
export const CodeExecutionResultBlock = type({
  type: "'code_execution_result'",
  // Back-pointer to the originating CodeExecutionRequestBlock.id.
  requestId: "string",
  // Normalized outcome. Anthropic: derived from `return_code` and
  // `abort_reason`; Gemini: from the `outcome` enum.
  status: "'ok' | 'error' | 'aborted' | 'timeout'",
  // Providers that don't split (Gemini) map their combined `output` to
  // `stdout` and leave `stderr` empty.
  "stdout?": "string",
  "stderr?": "string",
  // Provider-native numeric return code (Anthropic `return_code`).
  "returnCode?": "number",
  // Provider-native outcome string kept verbatim for callers that need the
  // raw signal (Gemini `OUTCOME_OK` / `OUTCOME_FAILED` / ...).
  "providerOutcome?": "string",
  // Human-readable reason, populated when status is "aborted"
  // (Anthropic `abort_reason`).
  "abortReason?": "string",
});
export type CodeExecutionResultBlock = typeof CodeExecutionResultBlock.infer;

const ToolResultBlock = type({
  type: "'tool_result'",
  callId: "string",
  // Deliberately narrow: tool results carry user-facing media only — not
  // citations (model output), safety ratings (filtering), or code execution.
  content: type
    .or(TextBlock, ImageBlock, AudioBlock, VideoBlock, DocumentBlock)
    .array(),
  "detail?": "unknown",
  "isError?": "boolean",
});

export const ContentBlock = type.or(
  TextBlock,
  ThinkingBlock,
  RedactedThinkingBlock,
  RefusalBlock,
  ImageBlock,
  AudioBlock,
  VideoBlock,
  DocumentBlock,
  CitationBlock,
  SafetyRatingBlock,
  CodeExecutionRequestBlock,
  CodeExecutionResultBlock,
  ToolCallBlock,
  ToolResultBlock,
);
export type ContentBlock = typeof ContentBlock.infer;

/**
 * A turn in the internal conversation history. `model` records the provider
 * model that produced the turn (assistant turns only). (INFERENCE.md § Message Format)
 */
export type ConversationTurn = {
  role: "user" | "assistant" | "system";
  content: ContentBlock[];
  model?: string;
  timestamp: number;
};

/**
 * A completed assistant turn returned in `inference.done` — a narrower type
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
 * response; the director can override per policy. (INFERENCE.md § Error Classification)
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

/** Gate types that can block the reactor. (INFERENCE.md § Gates) */
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
 * Fork mode. `independent` forks a divergent reactor with its own context;
 * `child` forks one that reports results back to the parent.
 * (INFERENCE.md § Forking)
 */
export const ForkMode = type.enumerated("independent", "child");
export type ForkMode = typeof ForkMode.infer;

// ---------------------------------------------------------------------------
// Inference Event Protocol (INFERENCE.md § Event Protocol)
// ---------------------------------------------------------------------------

/**
 * Wire-safe representation of InboundMessage for InferenceEvent variants:
 * the runtime type carries Uint8Array fields that cannot survive JSON
 * serialization, so attachment data is validated as `unknown`.
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
 * A single event in the inference event protocol: a monotonic session-scoped
 * sequence number plus a namespaced type (`inference.*`, `tool.*`,
 * `reactor.*`, `fork.*`, `message.*`, `custom.*`).
 *
 * (INFERENCE.md § Event Protocol)
 */
export const InferenceEvent = type.or(
  {
    type: "'inference.start'",
    seq: "number",
    data: { model: "string" },
  },
  {
    type: "'inference.thinking.delta'",
    seq: "number",
    data: {
      token: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.block.signature'",
    seq: "number",
    data: { signature: "string", "index?": "number" },
  },
  {
    type: "'inference.thinking.redacted'",
    seq: "number",
    data: { redactedThinking: RedactedThinkingBlock, "index?": "number" },
  },
  {
    type: "'inference.text.delta'",
    seq: "number",
    data: {
      token: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.refusal.delta'",
    seq: "number",
    data: {
      token: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.tool_call.start'",
    seq: "number",
    data: {
      callId: "string",
      name: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.tool_call.delta'",
    seq: "number",
    data: {
      callId: "string",
      argumentFragment: "string",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.tool_call.end'",
    seq: "number",
    data: {
      callId: "string",
      name: "string",
      arguments: "Record<string, unknown>",
      partial: PartialMessage,
      "index?": "number",
    },
  },
  {
    type: "'inference.usage'",
    seq: "number",
    data: { usage: TokenUsage, source: LastCycleSource },
  },
  {
    type: "'inference.done'",
    seq: "number",
    data: {
      turn: AssistantTurn,
      usage: TokenUsage,
      source: LastCycleSource,
      "pacingDelayMs?": "number",
    },
  },
  {
    type: "'inference.error'",
    seq: "number",
    data: { error: InferenceError, partial: PartialMessage },
  },
  {
    type: "'inference.retry'",
    seq: "number",
    data: {
      attempt: "number",
      delayMs: "number",
      previousError: InferenceError,
    },
  },
  {
    type: "'inference.citation'",
    seq: "number",
    // `index` names the cited source content block so the harness can
    // interleave it into the finalized turn; absent when the adapter has no
    // per-citation index (the harness appends at `content[]` end).
    data: { citation: CitationBlock, "index?": "number" },
  },
  {
    type: "'inference.safety_rating'",
    seq: "number",
    // Prompt-level signal (observed Gemini `promptFeedback.blockReason`);
    // the first capture had zero candidates, so there is no candidate index.
    data: { safetyRating: SafetyRatingBlock },
  },
  {
    type: "'inference.code_execution.start'",
    seq: "number",
    data: { request: CodeExecutionRequestBlock, "index?": "number" },
  },
  {
    type: "'inference.code_execution.delta'",
    seq: "number",
    // requestId correlates fragments to the originating request; index is a
    // positional hint. One response may interleave several requests.
    data: {
      requestId: "string",
      codeFragment: "string",
      "index?": "number",
    },
  },
  {
    type: "'inference.code_execution.result'",
    seq: "number",
    data: { result: CodeExecutionResultBlock, "index?": "number" },
  },
  {
    type: "'inference.image_output'",
    seq: "number",
    // Fires mid-stream when an adapter finalizes an image-output block, so
    // the image is ready before inference.done lands.
    data: { image: ImageBlock, "index?": "number" },
  },
  {
    type: "'tool.start'",
    seq: "number",
    data: { call: ToolCall },
  },
  {
    type: "'tool.update'",
    seq: "number",
    data: { callId: "string", partial: "string" },
  },
  {
    type: "'tool.done'",
    seq: "number",
    data: { result: ToolResult },
  },
  {
    type: "'message.queued'",
    seq: "number",
    data: { message: WireInboundMessage },
  },
  {
    type: "'message.run.started'",
    seq: "number",
    data: {
      "messageId?": "string",
      messageRunId: "string",
      receivedAt: "number",
    },
  },
  {
    type: "'message.run.ended'",
    seq: "number",
    data: {
      messageRunId: "string",
      "messageId?": "string",
      status: type.enumerated("completed", "failed"),
      "error?": {
        message: "string",
        "kind?": "string",
      },
    },
  },
  {
    type: "'message.correlated'",
    seq: "number",
    data: { message: WireInboundMessage, correlationId: "string" },
  },
  {
    type: "'connector.reply'",
    seq: "number",
    data: { content: "string", "checkpointHash?": "string" },
  },
  {
    type: "'reactor.start'",
    seq: "number",
    data: "object",
  },
  {
    type: "'reactor.gate.blocked'",
    seq: "number",
    data: {
      reason: GateType,
      gateId: "string",
      "correlationId?": "string",
      "approvalSnapshot?": ApprovalSnapshot,
    },
  },
  {
    type: "'reactor.gate.cleared'",
    seq: "number",
    data: {
      gateId: "string",
      reason: type.enumerated("resolved", "timeout", "shutdown"),
    },
  },
  {
    type: "'reactor.done'",
    seq: "number",
    data: "object",
  },
  {
    type: "'reactor.error'",
    seq: "number",
    data: { error: "string", fatal: "boolean" },
  },
  {
    type: "'fork.created'",
    seq: "number",
    data: { forkId: "string", parentId: "string", mode: ForkMode },
  },
  {
    type: "'fork.done'",
    seq: "number",
    data: { forkId: "string", "result?": "unknown" },
  },
  {
    type: "'fork.error'",
    seq: "number",
    data: { forkId: "string", error: "string" },
  },
  {
    type: "'fork.aborted'",
    seq: "number",
    data: { forkId: "string" },
  },
  {
    type: /^custom\./,
    seq: "number",
    data: "Record<string, unknown>",
  },
);
// The TypeScript type is manual rather than inferred: arktype infers the
// `custom.*` regex variant as `string`, which would prevent narrowing in
// switch statements. The manual type uses a `custom.${string}` template
// literal to preserve it.
export type InferenceEvent =
  | { type: "inference.start"; seq: number; data: { model: string } }
  | {
      type: "inference.thinking.delta";
      seq: number;
      data: { token: string; partial: PartialMessage; index?: number };
    }
  | {
      type: "inference.block.signature";
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
      type: "inference.refusal.delta";
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
      data: { usage: TokenUsage; source: LastCycleSource };
    }
  | {
      type: "inference.done";
      seq: number;
      data: {
        turn: AssistantTurn;
        usage: TokenUsage;
        source: LastCycleSource;
        pacingDelayMs?: number;
      };
    }
  | {
      type: "inference.error";
      seq: number;
      data: { error: InferenceError; partial: PartialMessage };
    }
  | {
      /**
       * Emitted when the per-call retry policy decides to retry. `attempt`
       * is the 1-indexed attempt that just failed; `delayMs` is the delay
       * before the next attempt; `previousError` is what triggered the
       * retry. Not emitted when the policy aborts.
       */
      type: "inference.retry";
      seq: number;
      data: {
        attempt: number;
        delayMs: number;
        previousError: InferenceError;
      };
    }
  | {
      type: "inference.citation";
      seq: number;
      data: { citation: CitationBlock; index?: number };
    }
  | {
      type: "inference.safety_rating";
      seq: number;
      data: { safetyRating: SafetyRatingBlock };
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
      /**
       * Per-message run-bracket open, emitted when the reactor dequeues an
       * inbound mail message. `messageRunId` is reactor-minted, unique per
       * dequeue, and required for crash-replay correlation: the same
       * `messageId` can be dequeued more than once across a crash + replay.
       */
      type: "message.run.started";
      seq: number;
      data: {
        messageId?: string;
        messageRunId: string;
        receivedAt: number;
      };
    }
  | {
      /**
       * Per-message run-bracket close, pairing with `message.run.started` by
       * `messageRunId`. `messageId` is carried redundantly for log
       * correlation; absent for a message with no id of its own.
       *
       * `status` is `"completed" | "failed"` only; cancellation is a harness
       * abort with `error.kind` `"inference_error" | "tool_error" |
       * "reactor_fatal" | "harness_aborted" | "doom_loop"`. `"doom_loop"`
       * marks a protective break on a repeated identical tool batch.
       */
      type: "message.run.ended";
      seq: number;
      data: {
        messageRunId: string;
        messageId?: string;
        status: "completed" | "failed";
        error?: {
          message: string;
          kind?: string;
        };
      };
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
      data: {
        reason: GateType;
        gateId: string;
        correlationId?: string;
        approvalSnapshot?: ApprovalSnapshot;
      };
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

// Drift guards for the dual-maintained `reactor.gate.blocked` event: the
// arktype validator and the manual type are kept in lockstep by hand, and
// arktype passes undeclared keys through at runtime, so a schema that dropped
// `approvalSnapshot` would not fail at runtime. Projecting the field off each
// shape makes it load-bearing: `tsc` errors if either mirror stops carrying it.
const _arkGateBlockedApprovalSnapshot = (
  data: Extract<
    typeof InferenceEvent.infer,
    { type: "reactor.gate.blocked" }
  >["data"],
): ApprovalSnapshot | undefined => data.approvalSnapshot;
void _arkGateBlockedApprovalSnapshot;

const _tsGateBlockedApprovalSnapshot = (
  data: Extract<InferenceEvent, { type: "reactor.gate.blocked" }>["data"],
): ApprovalSnapshot | undefined => data.approvalSnapshot;
void _tsGateBlockedApprovalSnapshot;

/**
 * Validate unknown data as an InferenceEvent, centralizing the single
 * unavoidable cast: arktype infers `custom.*` as `string` while the manual
 * type uses a `custom.${string}` template literal for switch narrowing.
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
 * A pending async operation in the reactor's async state, correlating an
 * outbound message (or payment/approval request) to its expected inbound
 * response. (INFERENCE.md § Correlation)
 */
export type PendingOperation = {
  correlationId: string;
  kind: SignalKind;
  registeredAt: number;
  gateId: string;
  /**
   * Absolute deadline (epoch ms) for the parking gate, persisted so a restart
   * re-arms the gate against the original deadline. Absent when parked with
   * none.
   */
  timeoutAt?: number;
  /**
   * The tool call suspended when this operation parked, for `kind:
   * "approval"` operations so the approved call re-runs on resume. Absent
   * for director-path async-tool pending markers.
   */
  suspendedCall?: ToolCall;
  /**
   * Approver-facing snapshot of `suspendedCall`: what the approver decides
   * on, as distinct from the re-dispatch artifact. Present only for ask-rail
   * operations; threaded to the hub co-write.
   */
  approvalSnapshot?: ApprovalSnapshot;
};

/**
 * Complete reactor state visible to the director decision function.
 *
 * `tokenUsage` is cumulative session usage. `lastCycleUsage` and
 * `lastCycleSource` describe the most recent *successful* inference call
 * and move together: both null before the first completion, set atomically
 * on `inference.done`, not cleared by `inference.error`.
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
  lastCycleSource: LastCycleSource | null;
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
 * `resume.execute_tools` fires when an approval resolves and a parked tool
 * call must re-run; it carries the calls so the director can seed its
 * outstanding tool-result count before their `tool.done` events arrive
 * (without the seed the first `tool.done` drives an accidental
 * re-inference). `resume.tool_result` fires when a parked approval ends
 * without running its tool, carrying a synthetic error result that answers
 * the parked call; no tool runs, so no count seed.
 *
 * (INFERENCE.md § Agent Reactor › Reactor Structure)
 */
export type ReactorInboundEvent =
  | { type: "message.received"; message: InboundMessage }
  | {
      type: "inference.done";
      turn: AssistantTurn;
      usage: TokenUsage;
      source: LastCycleSource;
    }
  | { type: "inference.error"; error: InferenceError; partial: PartialMessage }
  | { type: "tool.done"; result: ToolResult }
  | {
      type: "reactor.gate.cleared";
      gateId: string;
      reason: "resolved" | "timeout" | "shutdown";
    }
  | { type: "resume.execute_tools"; calls: ToolCall[] }
  | { type: "resume.tool_result"; result: ToolResult }
  | { type: "abort"; reason: AbortReason };

/**
 * The core director is a single decision function: given an event and the
 * current reactor state, return one or more actions; a throw emits
 * `reactor.error` and shuts the reactor down gracefully.
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
 * Decision returned by a `BeforeToolExtension`.
 *
 * - `allow` — the tool proceeds.
 * - `block` — the tool is answered with an error result carrying `reason`.
 * - `suspend` — the call is parked: the reactor registers `gate`, persists
 *   `pendingOp`, and neither runs nor error-completes the call.
 */
export type BeforeToolDecision =
  | { type: "allow" }
  | { type: "block"; reason: string }
  | {
      type: "suspend";
      gate: {
        type: GateType;
        gateId: string;
        correlationId: string;
        timeoutAt: number;
      };
      pendingOp: PendingOperation;
    };

/**
 * Extension that runs before a tool call is executed, returning a
 * `BeforeToolDecision`. `grantOneShot` registers a within-cycle bypass token
 * keyed on a `ToolCall.id`: the next `beforeTool` for that id skips a
 * suspension, consuming the token. Optional; only extensions that can
 * suspend a call have anything to bypass.
 */
export interface BeforeToolExtension {
  beforeTool(
    call: ToolCall,
    state: ReactorState,
    signal: AbortSignal,
  ): Promise<BeforeToolDecision>;
  grantOneShot?(id: string): void;
}

/**
 * Extension that runs after a tool result is produced, in order, and may
 * modify it (redaction, enrichment, audit logging).
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
 * Durable description of a single strategy invocation, written to the
 * per-cycle manifest so operators can reconstruct which strategy made which
 * change, with what parameters, and why. `version` bumps when behavior
 * changes so old manifest entries stay unambiguous.
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
 * Per-invocation context passed to every `ContextStrategy.apply` call:
 * the reactor's snapshot at the moment the strategy runs plus a short label
 * describing why it was invoked.
 */
export interface StrategyContext {
  readonly state: ReactorState;
  readonly trigger: string;
}

/**
 * Optional blob attachment emitted by a strategy. The reactor writes each
 * blob to the context store's working tree via `ContextStore.writeBlob`.
 */
export type StrategyBlob = {
  key: string;
  bytes: Uint8Array;
  contentType?: string;
};

/**
 * Result returned by `ContextStrategy.apply`: the transformed output, a
 * `TransformRecord`, and any blobs to persist in the context store.
 */
export interface StrategyResult<O> {
  output: O;
  record: TransformRecord;
  blobs?: StrategyBlob[];
}

/**
 * Generic base interface for content-mutating strategies; the role-specific
 * aliases below specialize `I`/`O`. Strategies are pure with respect to the
 * context store: they describe what should change via their return value, and
 * the reactor decides where to write it.
 */
export interface ContextStrategy<I, O> {
  readonly name: string;
  readonly version: string;
  apply(input: I, ctx: StrategyContext): Promise<StrategyResult<O>>;
}

/**
 * Runs on each tool result entering history; output is appended to the
 * conversation and any blobs are written to `tool-output/`.
 */
export type ToolResultTransform = ContextStrategy<
  { call: ToolCall; result: ToolResult },
  ToolResult
>;

/**
 * Runs in order before every inference call, producing the materialized
 * prompt written to `prompt.jsonl`; the durable history in `turns.jsonl` is
 * left untouched. (INFERENCE.md § Async State Awareness › Pending Status Injection)
 */
export type ContextTransform = ContextStrategy<
  ConversationTurn[],
  ConversationTurn[]
>;

/**
 * Named compaction strategy, registered on the reactor and invoked via the
 * director's `compact` action. Output overwrites `turns.jsonl`; a
 * `TransformRecord` is appended to the manifest.
 */
export type Compactor = ContextStrategy<ConversationTurn[], ConversationTurn[]>;

// ---------------------------------------------------------------------------
// Blob Reader (INFERENCE.md § Tool Result Lifecycle)
// ---------------------------------------------------------------------------

/**
 * Read-only capability for resolving `tool-output:///{callId}` URIs to blob
 * bytes. A transform that spills oversized tool output writes a blob via
 * `ContextStore.writeBlob` and returns a pointer of that form; the agent's
 * read tool reaches the spill via `BlobReader.read(uri)`.
 *
 * The URI scheme is rigid: `tool-output` scheme, empty authority (the `///`
 * keeps the callId in pathname so case survives URL parsing), `/{callId}`
 * path, no query or fragment. Any deviation or missing blob throws; the
 * reader never accepts a filesystem path.
 */
export interface BlobReader {
  /** Resolve `uri` to the blob bytes; throws on a malformed URI or missing blob. */
  read(uri: string): Promise<Uint8Array>;
}

/** Source for blob bytes used by `createBlobReader`. */
export interface BlobSource {
  readBlob(key: string, signal?: AbortSignal): Promise<Uint8Array>;
}

/**
 * Parse a `tool-output:///{callId}` URI and return the callId. Throws on any
 * deviation. The two-slash form is rejected: the URL parser lowercases the
 * hostname, silently corrupting callIds with uppercase letters; the
 * three-slash form keeps the callId in `pathname`, where case survives.
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
 * delegating to `source.readBlob(callId)`; the source only ever sees the
 * extracted callId. The most common source is a `ContextStore`, but any
 * `BlobSource` works.
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

/** Reason codes for the `abort` reactor event. (INFERENCE.md § Abort Handling › Abort Reasons) */
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
 * `InferenceOptions.X` overrides `defaults.X`, merged once at the top of
 * `runInference` before the adapter sees anything.
 */
export const InferenceSourceDefaults = type({
  "maxTokens?": "number",
  // Provider-native knobs merged into the outbound request body (Anthropic
  // `metadata.user_id`, OpenAI `user`, Gemini `safetySettings`, ...); the
  // per-call merge is shallow: a per-call object replaces the source one.
  "providerOptions?": "Record<string, unknown>",
});
export type InferenceSourceDefaults = typeof InferenceSourceDefaults.infer;

/**
 * A specific (provider, model) bundle the agent runtime can route to. `id`
 * is the catalog offering's primary key and the routing key for
 * `AgentConfig.defaultSource` / `Agent.setSource`; multi-model providers
 * become multiple sources. `quirks` is an opaque bag of provider-adapter
 * accommodations, read once at adapter instantiation; present-and-populated
 * or absent, never `null` (no quirks stores SQL `NULL`, resolved to an
 * omitted key).
 *
 * (INFERENCE.md § Providers)
 */
export const InferenceSource = type({
  id: "string",
  provider: "string",
  baseURL: "string",
  // Reference into the run's credential-material cell; the secret is resolved
  // by `credentialId` at call time, so the source config carries no secret.
  credentialId: "string",
  model: "string",
  "defaults?": InferenceSourceDefaults,
  "capabilities?": "string[]",
  "quirks?": "Record<string, unknown>",
});
export type InferenceSource = typeof InferenceSource.infer;

/**
 * Replace every field on `active` with the corresponding field from `next`,
 * in place; optional fields absent on `next` are `delete`d so the swap is
 * exact. Used by the source registry and hot-swap path to mutate the single
 * shared `InferenceSource` the reactor reads lazily per call.
 */
export function applyInferenceSourceFields(
  active: InferenceSource,
  next: InferenceSource,
): void {
  active.id = next.id;
  active.provider = next.provider;
  active.baseURL = next.baseURL;
  active.credentialId = next.credentialId;
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
  if (next.quirks !== undefined) {
    active.quirks = next.quirks;
  } else {
    delete active.quirks;
  }

  // Compile-time exhaustiveness: `Required<>` forces optional keys into the
  // guard, so a future optional field added to `InferenceSource` without
  // handling here is flagged by TypeScript.
  const _handled: { readonly [K in keyof Required<InferenceSource>]: true } = {
    id: true,
    provider: true,
    baseURL: true,
    credentialId: true,
    model: true,
    defaults: true,
    capabilities: true,
    quirks: true,
  };
  void _handled;
}

/**
 * Outcome of a `RetryPolicy` consultation: abort the call (surfacing the
 * most recent `inference.error`) or retry after `delayMs` milliseconds
 * against the harness Scheduler. (INFERENCE.md § Providers › Streaming Harness)
 */
export type RetryDecision =
  | { kind: "abort" }
  | { kind: "retry"; delayMs: number };

/**
 * Context supplied to a `RetryPolicy` each time an attempt produces an
 * `inference.error`. (INFERENCE.md § Providers › Streaming Harness)
 */
export type RetrySituation = {
  /** The classified error the most recent attempt produced. */
  readonly error: InferenceError;
  /** 1-indexed attempt counter (the first failure is 1). */
  readonly attempt: number;
  /**
   * Milliseconds since the first attempt of this call started, via the
   * harness `Scheduler.now()`. May be fractional (performance.now) or
   * integer (virtual-clock test schedulers).
   */
  readonly elapsedMs: number;
};

/**
 * Per-call retry policy, invoked once per `inference.error`, in 1-indexed
 * attempt order. `{ kind: "abort" }` ends the call; `{ kind: "retry",
 * delayMs }` discards the failed attempt's events, sleeps `delayMs` against
 * the Scheduler, and re-issues the request with the same body. May be async.
 *
 * (INFERENCE.md § Providers › Streaming Harness)
 */
export type RetryPolicy = (
  situation: RetrySituation,
) => RetryDecision | Promise<RetryDecision>;

/**
 * Options for a single inference call, overriding the agent-configuration
 * defaults per call. (INFERENCE.md § Providers › Streaming Harness)
 */
export type InferenceOptions = {
  maxTokens?: number;
  temperature?: number;
  thinking?: { enabled: boolean; budgetTokens?: number };
  systemPrompt?: string;
  tools?: ToolDefinition[];
  /**
   * Modalities the caller wants the model to emit; adapters translate to the
   * provider-native shape (e.g. Gemini `generationConfig.responseModalities`
   * accepts uppercase `"TEXT"` / `"IMAGE"`). Providers without a switch
   * ignore it; omitted means the provider default.
   */
  responseModalities?: ("text" | "image" | "audio")[];
  /**
   * Structured-output constraint: free-form text, JSON, or JSON conforming
   * to a schema. Adapters translate to the provider-native wire shape —
   * OpenAI `response_format` (strict-mode refusals surface as
   * RefusalBlocks), Gemini `responseMimeType` / `responseSchema` (a JSON
   * Schema subset, forwarded verbatim). Anthropic has none: `text` is a
   * no-op, `json` / `json-schema` throw. Omitted means the provider default.
   */
  responseFormat?:
    | { kind: "text" }
    | { kind: "json" }
    | {
        kind: "json-schema";
        name: string;
        schema: unknown;
        strict?: boolean;
      };
  /**
   * Provider-native knobs merged into the outbound request body, overriding
   * `InferenceSourceDefaults.providerOptions` per call. The merge is
   * shallow: a per-call object replaces the source-bound one wholesale.
   */
  providerOptions?: Record<string, unknown>;
  /**
   * Per-call inactivity timeout in ms: no event (other than `inference.start`)
   * for this long ends the call with an `inference.error` of category
   * `"timeout"`. Default 120_000; `0` fires on the next tick (fail-fast,
   * useful in tests).
   */
  inactivityTimeoutMs?: number;
  /**
   * Per-call total wall-clock cap in ms, starting at fetch. Default 600_000;
   * backstop for streams that never terminate. Same error category as
   * `inactivityTimeoutMs`. `0` arms the timer to fire on the next tick.
   */
  totalTimeoutMs?: number;
  /** Per-call retry policy (see `RetryPolicy`); a built-in default applies if omitted. */
  retryPolicy?: RetryPolicy;
};

// ---------------------------------------------------------------------------
// Context Store (INFERENCE.md § Context Management › Context Store,
//                ARCHITECTURE.md § Change History)
// ---------------------------------------------------------------------------

/** A named commit point in the context store, corresponding to a git commit. (ARCHITECTURE.md § Change History › Named Checkpoints) */
export type ContextCommit = {
  hash: string;
  message: string;
  timestamp: number;
  parentHash?: string;
};

/**
 * State of an active connector thread (one durable thread per agent,
 * persisted across sidecar restarts). `replyTo` is the most recent speaker,
 * the primary recipient on the next outbound reply; `cc` is every other
 * participant who has spoken, deduplicated in arrival order. An arktype so
 * the wire layer can validate snapshots.
 */
export const ConnectorThreadState = type({
  "threadRoot?": "string",
  "lastMessageId?": "string",
  replyTo: "string",
  cc: "string[]",
  "subject?": "string",
});
export type ConnectorThreadState = typeof ConnectorThreadState.infer;

/**
 * The context store interface, backed by git (filesystem, in-memory, or
 * virtual) depending on the execution environment. Holds the turn history
 * and reactor metadata; forking creates a branch, compaction commits the
 * compacted history. (INFERENCE.md § Context Management › Context Store)
 */
export interface ContextStore {
  /** Load the current turn history and reactor metadata (reactor init). */
  load(signal?: AbortSignal): Promise<{
    turns: ConversationTurn[];
    pendingOperations: PendingOperation[];
    tokenUsage: TokenUsage;
    connectorState: ConnectorThreadState | null;
  }>;

  /** Buffer connector thread state for the next commit, persisted atomically with the context. */
  setConnectorState(state: ConnectorThreadState | null): void;

  /** Commit the working tree with the supplied message. */
  commit(
    options: { message: string },
    signal?: AbortSignal,
  ): Promise<ContextCommit>;

  /** Create a branch for a fork, starting from the current HEAD commit. */
  branch(name: string, signal?: AbortSignal): Promise<void>;

  /** List recent commits, for history query tools. */
  log(limit?: number, signal?: AbortSignal): Promise<ContextCommit[]>;

  /** Read the turn history at a specific commit hash. */
  readAt(hash: string, signal?: AbortSignal): Promise<ConversationTurn[]>;

  /**
   * Write an opaque blob to the working tree under `tool-output/`, staged at
   * the next commit. `key` is sanitized for filesystem safety; callers
   * should pass the tool call id.
   */
  writeBlob(
    key: string,
    bytes: Uint8Array,
    contentType?: string,
    signal?: AbortSignal,
  ): Promise<void>;

  /** Read a blob previously written via `writeBlob`; throws for an unknown key. */
  readBlob(key: string, signal?: AbortSignal): Promise<Uint8Array>;

  /** Overwrite `prompt.jsonl` with the materialized prompt, staged at the next commit. */
  writePrompt(turns: ConversationTurn[], signal?: AbortSignal): Promise<void>;

  /** Overwrite `response.jsonl` with the current cycle's assistant turn. */
  writeResponse(turn: AssistantTurn, signal?: AbortSignal): Promise<void>;

  /** Overwrite `manifest.jsonl` with the current cycle's transform records. */
  writeManifest(
    records: TransformRecord[],
    signal?: AbortSignal,
  ): Promise<void>;

  /** Overwrite `turns.jsonl` with the durable conversation history. */
  writeTurns(turns: ConversationTurn[], signal?: AbortSignal): Promise<void>;

  /**
   * Overwrite `metadata.json` with non-turn-shaped restart state: pending
   * operations and token usage, merged with the buffered connector state.
   */
  writeMetadata(
    metadata: {
      pendingOperations: PendingOperation[];
      tokenUsage: TokenUsage;
    },
    signal?: AbortSignal,
  ): Promise<void>;

  /**
   * Read manifest entries from the most recent `limit` commits containing a
   * `manifest.jsonl`, newest first; records keep their in-file order.
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
 * Persistent store for tool invocation audit records, separated from
 * ContextStore so the capability is opt-in at the composition layer.
 */
export interface AuditStore {
  /** Persist a batch of audit records accumulated since the last checkpoint. */
  commitAudit(records: AuditRecord[], signal?: AbortSignal): Promise<void>;

  /** Load a session's audit records, ordered by seq. */
  loadAudit(sessionId: string, signal?: AbortSignal): Promise<AuditRecord[]>;

  /** Persist a batch of error records accumulated since the last flush. */
  commitErrors(records: ErrorRecord[], signal?: AbortSignal): Promise<void>;
}

// ---------------------------------------------------------------------------
// Agent / Harness Configuration (ARCHITECTURE.md § Agent Harness)
// ---------------------------------------------------------------------------

/**
 * Configured tool definition exposed to the model, registered by the harness
 * and passed to the inference provider with each request.
 * (ARCHITECTURE.md § Agent Harness › Tools)
 */
export const ToolDefinition = type({
  name: "string",
  description: "string",
  inputSchema: "Record<string, unknown>",
});
export type ToolDefinition = typeof ToolDefinition.infer;

/**
 * Agent harness configuration, assembled from the agent definition package
 * and capability grants during harness initialization. `principalId`
 * reconstructs the in-memory grant store on restart; `grants` uses
 * `WireGrantRule` because it arrives over JSON where `GrantRule.expiresAt`
 * is a serialized string.
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
