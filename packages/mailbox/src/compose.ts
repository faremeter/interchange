// Compose an `OutboundMessage` into signed RFC 2822 bytes.
//
// Signing and the stored envelope belong to the message, not to the mailbox
// that delivers it. This module owns the body checks, the signed content, the
// header set, and the References chain; the caller keeps delivery.
//
// It sits beside `verifyMimeSignature` deliberately: composing a signed
// message and verifying one are peers over the same byte contract.

import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  generateMessageId,
  isMessageId,
  type ConversationContent,
  type MessageHeaders as MimeMessageHeaders,
  type StructuredContent,
} from "@intx/mime";
import type { CryptoProvider, OutboundMessage } from "@intx/types/runtime";
import { isConversationType } from "@intx/types/runtime";

import type { StoredEnvelope } from "./mailbox";

/**
 * The composed message: wire bytes, the message id stamped into them, the
 * pre-parsed envelope a mailbox stores beside them, and the address lists the
 * caller routes on. `recipients` is the deduplicated union of `to` and `cc`;
 * `to` and `cc` stay apart because they are separate header fields and audit
 * facts.
 */
export type ComposedMessage = {
  messageId: string;
  rawBytes: Uint8Array;
  envelope: StoredEnvelope;
  to: string[];
  cc: string[];
  recipients: string[];
};

/**
 * Assemble the wire `References` chain. When the caller supplies a full
 * ancestry (`existingReferences` -- the parent's References plus its
 * Message-Id, built by the threaded reply path), use it and append `inReplyTo`
 * only when it is not already the tail; otherwise derive the chain from
 * `inReplyTo` alone.
 *
 * The caller-supplied chain is filtered to RFC 2822 message identifiers:
 * inbound mail can carry a headerless-derived (sha256) Message-Id that is a
 * valid claim-check key but not a valid `<id@host>` identifier, and it must
 * not leak into a `References` header. `inReplyTo` is appended without
 * filtering, as for a bare reply.
 */
function buildReferences(
  inReplyTo: string | undefined,
  existingReferences: string[] | undefined,
): string[] | undefined {
  const refs = (existingReferences ?? []).filter(isMessageId);
  if (inReplyTo === undefined) return refs.length > 0 ? refs : undefined;
  if (!refs.includes(inReplyTo)) {
    return [...refs, inReplyTo];
  }
  return refs;
}

/**
 * Split a `to`/`cc` field that takes one address or several into a fresh list
 * so a caller cannot alias the message's own.
 */
function addressList(field: string | string[] | undefined): string[] {
  if (field === undefined) return [];
  return Array.isArray(field) ? [...field] : [field];
}

/**
 * Compose an `OutboundMessage` into signed RFC 2822 bytes.
 *
 * Throws when the message contradicts itself: a conversation type carrying a
 * structured payload, a structured type carrying text content or attachments,
 * a blank `inReplyTo`, or no recipient at all. These make the message
 * unsendable whatever the transport, so they are refused here rather than once
 * per transport.
 */
export async function composeOutbound(
  senderAddress: string,
  message: OutboundMessage,
  senderCrypto: CryptoProvider,
): Promise<ComposedMessage> {
  const to = addressList(message.to);
  if (to.length === 0) {
    throw new Error("OutboundMessage must have at least one recipient");
  }
  const cc = addressList(message.cc);
  const recipients = [...new Set([...to, ...cc])];

  const isConversation = isConversationType(message.type);
  if (isConversation && message.payload !== undefined) {
    throw new Error(
      "Conversation messages must not carry a structured payload",
    );
  }
  if (!isConversation && message.content !== undefined) {
    throw new Error("Structured messages must not carry a text content field");
  }
  if (!isConversation && message.attachments !== undefined) {
    throw new Error("Structured messages must not carry attachments");
  }

  // RFC 5322 §3.6.4 defines `In-Reply-To` as `1*msg-id`, so a blank value
  // names nothing rather than naming a shorter parent.
  if (
    message.inReplyTo !== undefined &&
    message.inReplyTo.trim().length === 0
  ) {
    throw new Error(
      "OutboundMessage inReplyTo, when provided, must name a message identifier",
    );
  }

  const messageId = generateMessageId(senderAddress);
  const now = new Date();

  let content: ConversationContent | StructuredContent;
  if (isConversation) {
    const conversation: ConversationContent = {
      kind: "conversation",
      text: message.content ?? "",
    };
    if (message.attachments !== undefined) {
      conversation.attachments = message.attachments;
    }
    content = conversation;
  } else {
    const structured: StructuredContent = {
      kind: "structured",
      json: { type: message.type, version: "1", body: message.payload ?? {} },
    };
    if (message.summary !== undefined) structured.summary = message.summary;
    content = structured;
  }

  const signedContentBytes = assembleSignedContent(content);
  const signatureBytes = await createDetachedSignatureFromProvider(
    signedContentBytes,
    senderCrypto,
  );

  const references = buildReferences(message.inReplyTo, message.references);

  const mimeHeaders: MimeMessageHeaders = {
    from: senderAddress,
    to,
    cc: cc.length > 0 ? cc : undefined,
    date: now,
    messageId,
    subject: message.subject,
    inReplyTo: message.inReplyTo,
    references,
    mimeVersion: "1.0",
    interchangeType: message.type,
    interchangeCorrelationId: message.correlationId,
    interchangeTenantId: message.tenantId,
    interchangeAgentId: undefined,
    interchangeSessionId: message.sessionId,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
  };

  const rawBytes = assembleMessage(
    mimeHeaders,
    signedContentBytes,
    signatureBytes,
  );

  const envelope: StoredEnvelope = {
    messageId,
    from: senderAddress,
    to,
    subject: message.subject ?? "",
    date: now,
    inReplyTo: message.inReplyTo,
    references: references ?? [],
    interchangeType: message.type,
    interchangeCorrelationId: message.correlationId,
  };

  return { messageId, rawBytes, envelope, to, cc, recipients };
}
