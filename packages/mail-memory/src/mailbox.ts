import type { CryptoProvider, MailboxEvent } from "@interchange/types/runtime";

/**
 * Pre-parsed envelope extracted from MIME headers at delivery time.
 * Avoids re-parsing raw bytes for every search operation.
 */
export type StoredEnvelope = {
  messageId: string;
  from: string;
  to: string[];
  subject: string;
  date: Date;
  inReplyTo: string | undefined;
  references: string[];
  interchangeType: string | undefined;
  interchangeCorrelationId: string | undefined;
};

/**
 * A single message stored in memory. The `raw` field contains the complete
 * RFC 2822 message bytes (headers + MIME body). All fetch operations parse
 * from these bytes, guaranteeing byte-exact signature verification.
 */
export type StoredMessage = {
  uid: number;
  modseq: number;
  flags: Set<string>;
  raw: Uint8Array;
  envelope: StoredEnvelope;
};

export type MailboxStore = {
  messages: StoredMessage[];
  uidCounter: number;
  modseqCounter: number;
  uidValidity: number;
};

export type AddressEntry = {
  mailboxes: Map<string, MailboxStore>;
  watchCallbacks: Map<string, Set<(event: MailboxEvent) => void>>;
  crypto: CryptoProvider;
};

export const DEFAULT_MAILBOXES = [
  "INBOX",
  "Sent",
  "Drafts",
  "Archive",
  "Trash",
] as const;

export function createMailboxStore(): MailboxStore {
  return {
    messages: [],
    uidCounter: 1,
    modseqCounter: 1,
    uidValidity: Date.now(),
  };
}

export function createAddressEntry(crypto: CryptoProvider): AddressEntry {
  const mailboxes = new Map<string, MailboxStore>();
  for (const name of DEFAULT_MAILBOXES) {
    mailboxes.set(name, createMailboxStore());
  }
  return {
    mailboxes,
    watchCallbacks: new Map(),
    crypto,
  };
}

/**
 * Append a message to a mailbox store. Assigns UID and MODSEQ, returns the
 * assigned UID.
 */
export function appendToMailbox(
  store: MailboxStore,
  raw: Uint8Array,
  envelope: StoredEnvelope,
  flags: string[],
): number {
  const uid = store.uidCounter++;
  const modseq = store.modseqCounter++;
  store.messages.push({
    uid,
    modseq,
    flags: new Set(flags),
    raw,
    envelope,
  });
  return uid;
}

export function findMessage(
  store: MailboxStore,
  uid: number,
): StoredMessage | undefined {
  return store.messages.find((m) => m.uid === uid);
}

export function requireMessage(
  store: MailboxStore,
  uid: number,
  mailboxName: string,
): StoredMessage {
  const msg = findMessage(store, uid);
  if (msg === undefined) {
    throw new Error(`Message UID ${uid} not found in mailbox "${mailboxName}"`);
  }
  return msg;
}
