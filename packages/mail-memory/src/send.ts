import type {
  MailboxEvent,
  MessageSentHandler,
  OutboundMessage,
  RemoteSendHandler,
  SendReceipt,
} from "@intx/types/runtime";
import { composeOutbound } from "@intx/mailbox";
import { buildMessageHeaders, parseHeaderSection } from "@intx/mime";
import type { AddressEntry } from "./mailbox";

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

  const composed = await composeOutbound(senderAddress, message, senderCrypto);
  const { messageId, rawBytes, envelope } = composed;
  const recipients = composed.to;
  const ccAddressList = composed.cc;
  const allAddressees = composed.recipients;
  const remoteRecipients = allAddressees.filter((addr) => !entries.has(addr));

  if (remoteRecipients.length > 0 && onRemoteSend === undefined) {
    throw new Error(
      `Recipient "${remoteRecipients[0]}" is not registered with this transport`,
    );
  }

  // Deliver to each local recipient's INBOX.
  const deliveredUids: { address: string; uid: number }[] = [];
  for (const recipient of allAddressees) {
    const entry = entries.get(recipient);
    if (entry === undefined) continue;
    const inbox = entry.mailboxes.get("INBOX");
    if (inbox === undefined) {
      throw new Error(
        `Mailbox "INBOX" does not exist for recipient "${recipient}"`,
      );
    }
    const uid = inbox.append(rawBytes, envelope, []);
    deliveredUids.push({ address: recipient, uid });
  }

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
