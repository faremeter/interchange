/* eslint-disable @typescript-eslint/no-non-null-assertion -- Map.get()! after validation that agent is registered */
import type {
  OutboundMessage,
  SendReceipt,
  CryptoProvider,
  MailboxEvent,
} from "@interchange/types/runtime";
import type { AgentMailboxEntry, StoredEnvelope } from "./mailbox";
import { appendToMailbox } from "./mailbox";
import {
  assembleSignedContent,
  assembleMessage,
  generateMessageId,
  parseHeaderSection,
  createDetachedSignatureFromProvider,
  type MessageHeaders as MimeMessageHeaders,
  type ConversationContent,
  type StructuredContent,
} from "@interchange/mime";
import { buildMessageHeaders } from "./headers";

const CONVERSATION_TYPES = new Set([
  "conversation.message",
  "conversation.join",
  "conversation.leave",
]);

/**
 * Callback for delivering messages to recipients not registered on this
 * transport. The federation layer provides this to forward messages to
 * the hub for remote routing.
 */
export type RemoteSendHandler = (
  rawMessage: Uint8Array,
  recipients: string[],
) => Promise<void>;

/**
 * Context passed to MessageSentHandler callbacks after a message is fully
 * assembled and delivered.
 */
export type MessageSentContext = {
  senderAddress: string;
  rawMessage: Uint8Array;
  messageId: string;
  /** Deduplicated union of to and cc — the full routing set. */
  recipients: string[];
  /** To addresses only (before merging with cc). */
  to: string[];
  /** CC addresses only. Empty array when no CC recipients. */
  cc: string[];
  /** True when all recipients were delivered locally (no remote leg). */
  localOnly: boolean;
};

/**
 * Callback fired after a message is fully assembled and delivered. The
 * send is already complete when this fires — a handler rejection does
 * not mean the message was not delivered.
 *
 * Used by the sidecar to commit outbound wire messages to the git audit
 * trail and forward metadata to the hub.
 */
export type MessageSentHandler = (ctx: MessageSentContext) => Promise<void>;

/**
 * Execute the send() flow:
 * 1. Validate sender registration, split recipients into local/remote
 * 2. Build signed content part (MIME bytes to sign)
 * 3. Sign with sender's CryptoProvider
 * 4. Assemble the complete RFC 2822 message
 * 5. Append to each local recipient's INBOX and sender's Sent mailbox
 * 6. Forward to remote recipients via onRemoteSend
 * 7. Schedule watch callbacks asynchronously via queueMicrotask
 * 8. Fire onMessageSent callback (fire-and-forget)
 *
 * If onRemoteSend is not provided and there are remote recipients, send()
 * throws. If onRemoteSend rejects, the error propagates — local delivery
 * that already completed is not rolled back. This is a known limitation:
 * partial delivery is possible when a message has both local and remote
 * recipients and the remote leg fails.
 */
export async function executeSend(
  senderAddress: string,
  message: OutboundMessage,
  agentMailboxes: Map<string, AgentMailboxEntry>,
  cryptoProviders: Map<string, CryptoProvider>,
  onRemoteSend?: RemoteSendHandler,
  onMessageSent?: MessageSentHandler,
): Promise<SendReceipt> {
  const senderCrypto = cryptoProviders.get(senderAddress);
  if (senderCrypto === undefined) {
    throw new Error(
      `Sender "${senderAddress}" is not registered with this transport`,
    );
  }

  const recipients = Array.isArray(message.to) ? message.to : [message.to];
  if (recipients.length === 0) {
    throw new Error("OutboundMessage must have at least one recipient");
  }

  const ccAddressList =
    message.cc !== undefined
      ? Array.isArray(message.cc)
        ? message.cc
        : [message.cc]
      : [];

  const allAddressees = [...new Set([...recipients, ...ccAddressList])];
  const remoteRecipients = allAddressees.filter(
    (addr) => !agentMailboxes.has(addr),
  );

  if (remoteRecipients.length > 0 && onRemoteSend === undefined) {
    throw new Error(
      `Recipient "${remoteRecipients[0]}" is not registered with this transport`,
    );
  }

  const isConversation = CONVERSATION_TYPES.has(message.type);

  if (isConversation && message.payload !== undefined) {
    throw new Error(
      "Conversation messages must not carry a structured payload",
    );
  }
  if (!isConversation && message.content !== undefined) {
    throw new Error("Structured messages must not carry a text content field");
  }

  const messageId = generateMessageId(senderAddress);
  const now = new Date();

  let content: ConversationContent | StructuredContent;
  if (isConversation) {
    content = {
      kind: "conversation",
      text: message.content ?? "",
    };
  } else {
    const payload = message.payload ?? {};
    const envelope = {
      type: message.type,
      version: "1",
      body: payload,
    };
    const structured: StructuredContent = {
      kind: "structured",
      json: envelope,
    };
    if (message.summary !== undefined) structured.summary = message.summary;
    content = structured;
  }

  const signedContentBytes = assembleSignedContent(content);
  const signatureBytes = await createDetachedSignatureFromProvider(
    signedContentBytes,
    senderCrypto,
  );

  const ccAddresses = ccAddressList.length > 0 ? ccAddressList : undefined;

  const refs = buildReferences(message.inReplyTo, undefined);

  const mimeHeaders: MimeMessageHeaders = {
    from: senderAddress,
    to: recipients,
    cc: ccAddresses,
    date: now,
    messageId,
    subject: message.subject,
    inReplyTo: message.inReplyTo,
    references: refs,
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
    to: recipients,
    subject: message.subject ?? "",
    date: now,
    inReplyTo: message.inReplyTo,
    references: refs ?? [],
    interchangeType: message.type,
    interchangeCorrelationId: message.correlationId,
  };

  // Deliver to each local recipient's INBOX.
  const deliveredUids: { address: string; uid: number }[] = [];
  for (const recipient of allAddressees) {
    if (!agentMailboxes.has(recipient)) continue;
    const entry = agentMailboxes.get(recipient)!;
    const store = entry.mailboxes.get("INBOX")!;
    const uid = appendToMailbox(store, rawBytes, envelope, []);
    deliveredUids.push({ address: recipient, uid });
  }

  // Append copy to sender's Sent mailbox.
  {
    const senderEntry = agentMailboxes.get(senderAddress)!;
    const sentStore = senderEntry.mailboxes.get("Sent")!;
    appendToMailbox(sentStore, rawBytes, envelope, ["\\Seen"]);
  }

  // Fire local recipient watch callbacks ASYNCHRONOUSLY (per MESSAGE.md
  // requirement). queueMicrotask ensures callbacks never run synchronously
  // on the sender's call stack, preserving real IMAP IDLE async delivery
  // semantics. Scheduled before the remote send so local delivery
  // notifications are not delayed by network latency.
  const { headers: parsedHeaders } = parseHeaderSection(rawBytes);
  const msgHeaders = buildMessageHeaders(parsedHeaders);

  for (const { address, uid } of deliveredUids) {
    const entry = agentMailboxes.get(address)!;
    const callbacks = entry.watchCallbacks.get("INBOX");
    if (callbacks === undefined || callbacks.size === 0) continue;

    const event: MailboxEvent = {
      type: "exists",
      uid,
      headers: msgHeaders,
    };

    for (const cb of callbacks) {
      queueMicrotask(() => cb(event));
    }
  }

  // Forward to remote recipients via federation hook.
  if (remoteRecipients.length > 0 && onRemoteSend !== undefined) {
    await onRemoteSend(rawBytes, remoteRecipients);
  }

  if (onMessageSent !== undefined) {
    const localOnly = remoteRecipients.length === 0;
    onMessageSent({
      senderAddress,
      rawMessage: rawBytes,
      messageId,
      recipients: allAddressees,
      to: recipients,
      cc: ccAddressList,
      localOnly,
    }).catch((err: unknown) => {
      queueMicrotask(() => {
        throw err instanceof Error
          ? err
          : new Error(`MessageSentHandler failed: ${String(err)}`);
      });
    });
  }

  return {
    messageId,
    status: remoteRecipients.length > 0 ? "queued" : "delivered",
  };
}

function buildReferences(
  inReplyTo: string | undefined,
  existingReferences: string[] | undefined,
): string[] | undefined {
  if (inReplyTo === undefined) return existingReferences;
  const refs = existingReferences ?? [];
  if (!refs.includes(inReplyTo)) {
    return [...refs, inReplyTo];
  }
  return refs;
}
