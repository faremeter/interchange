import type {
  OutboundMessage,
  SendReceipt,
  MailboxEvent,
} from "@intx/types/runtime";
import { composeOutbound } from "@intx/mailbox";
import { buildMessageHeaders, parseHeaderSection } from "@intx/mime";
import type { AddressEntry } from "./mailbox";

/**
 * Callback for delivering messages to recipients not registered on this
 * transport. The federation layer provides this to forward messages and the
 * transport-validated sender address to the hub for remote routing.
 */
export type RemoteSendHandler = (
  rawMessage: Uint8Array,
  recipients: string[],
  senderAddress: string,
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
 * 1. Validate sender registration
 * 2. Compose the signed message
 * 3. Split recipients into local and remote
 * 4. Append to each local recipient's INBOX and the sender's Sent mailbox
 * 5. Forward to remote recipients via onRemoteSend
 * 6. Schedule watch callbacks asynchronously via queueMicrotask
 * 7. Fire onMessageSent callback (fire-and-forget)
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
  entries: Map<string, AddressEntry>,
  onRemoteSend?: RemoteSendHandler,
  onMessageSent?: MessageSentHandler,
): Promise<SendReceipt> {
  const senderEntry = entries.get(senderAddress);
  if (senderEntry === undefined) {
    throw new Error(
      `Sender "${senderAddress}" is not registered with this transport`,
    );
  }
  const senderCrypto = senderEntry.crypto;

  // Local recipients are classified before signing yields, so one
  // unregistered meanwhile fails the send instead of being routed as remote.
  const registeredBeforeSigning = new Set(entries.keys());
  const composed = await composeOutbound(senderAddress, message, senderCrypto);
  const { messageId, rawBytes, envelope } = composed;
  const recipients = composed.to;
  const ccAddressList = composed.cc;
  const allAddressees = composed.recipients;
  const localRecipients = allAddressees.filter((addr) =>
    registeredBeforeSigning.has(addr),
  );
  const remoteRecipients = allAddressees.filter(
    (addr) => !registeredBeforeSigning.has(addr),
  );

  if (remoteRecipients.length > 0 && onRemoteSend === undefined) {
    throw new Error(
      `Recipient "${remoteRecipients[0]}" is not registered with this transport`,
    );
  }

  // Every recipient's INBOX is resolved before any is appended to, so a
  // recipient unregistered while the message was being signed fails the send
  // before the others receive it.
  const inboxes = localRecipients.map((recipient) => {
    const entry = entries.get(recipient);
    if (entry === undefined) {
      throw new Error(
        `Recipient "${recipient}" was unregistered while the message was being signed`,
      );
    }
    const inbox = entry.mailboxes.get("INBOX");
    if (inbox === undefined) {
      throw new Error(
        `Mailbox "INBOX" does not exist for recipient "${recipient}"`,
      );
    }
    return { address: recipient, inbox };
  });
  const deliveredUids = inboxes.map(({ address, inbox }) => ({
    address,
    uid: inbox.append(rawBytes, envelope, []),
  }));

  // Append copy to sender's Sent mailbox.
  const sentStore = senderEntry.mailboxes.get("Sent");
  if (sentStore === undefined) {
    throw new Error(
      `Mailbox "Sent" does not exist for sender "${senderAddress}"`,
    );
  }
  sentStore.append(rawBytes, envelope, ["\\Seen"]);

  // Fire local recipient watch callbacks ASYNCHRONOUSLY (per MESSAGE.md
  // requirement). queueMicrotask ensures callbacks never run synchronously
  // on the sender's call stack, preserving real IMAP IDLE async delivery
  // semantics. Scheduled before the remote send so local delivery
  // notifications are not delayed by network latency.
  const { headers: parsedHeaders } = parseHeaderSection(rawBytes);
  const msgHeaders = buildMessageHeaders(parsedHeaders);

  for (const { address, uid } of deliveredUids) {
    const entry = entries.get(address);
    if (entry === undefined) {
      throw new Error(
        `Entry for "${address}" disappeared between delivery and callback dispatch`,
      );
    }
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
    await onRemoteSend(rawBytes, remoteRecipients, senderAddress);
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
