/**
 * Pre-parsed envelope extracted from MIME headers at delivery time, so search
 * and thread never re-parse raw bytes.
 */
export type StoredEnvelope = {
  messageId: string;
  from: string | undefined;
  to: string[];
  subject: string;
  /** The date the sender put on the message, not the instant it arrived. */
  date: Date | undefined;
  inReplyTo: string | undefined;
  references: string[];
  interchangeType: string | undefined;
  interchangeCorrelationId: string | undefined;
};

/**
 * A stored message's resident model: uid, IMAP counters, flags, and the
 * pre-parsed envelope. The RFC 2822 bytes are not resident; they are read on
 * demand through `MailboxStore.readRaw`, so a backing can keep them on disk,
 * and the projections route through `readRaw` so signature verification stays
 * byte-exact.
 */
export type StoredMessage = {
  uid: number;
  modseq: number;
  flags: Set<string>;
  envelope: StoredEnvelope;
};

/**
 * Storage-agnostic per-mailbox model. A backing owns how the message list and
 * the uid/modseq/uidValidity counters are stored; the pure query and
 * projection functions read the snapshot through `messages` and raw bytes on
 * demand through `readRaw`.
 *
 * Counters follow IMAP semantics: `uidNext` is the UID the next `append`
 * assigns (UIDNEXT), `highestModSeq` is the largest MODSEQ assigned
 * (HIGHESTMODSEQ), and `uidValidity` is stable for the mailbox lifetime
 * (UIDVALIDITY).
 */
export interface MailboxStore {
  readonly uidValidity: number;
  readonly uidNext: number;
  readonly highestModSeq: number;
  readonly messages: readonly StoredMessage[];

  /**
   * Store a message, assigning the next UID and MODSEQ. The backing decides
   * whether to retain `raw` in memory or serve it from disk through `readRaw`.
   */
  append(raw: Uint8Array, envelope: StoredEnvelope, flags: string[]): number;

  /**
   * Read a stored message's verbatim RFC 2822 bytes from wherever the backing
   * keeps them. Throws if no message has the UID.
   */
  readRaw(uid: number): Promise<Uint8Array>;

  /** Locate a stored message by UID, or `undefined` if none matches. */
  find(uid: number): StoredMessage | undefined;

  /** Add flags and advance the MODSEQ. Throws if no message has the UID. */
  addFlags(uid: number, flags: string[]): StoredMessage;

  /** Remove flags and advance the MODSEQ. Throws if no message has the UID. */
  removeFlags(uid: number, flags: string[]): StoredMessage;

  /** Drop a stored message by UID. Throws if absent. */
  remove(uid: number): void;
}

/**
 * The default set of mailboxes created for a freshly registered address.
 */
export const DEFAULT_MAILBOXES = [
  "INBOX",
  "Sent",
  "Drafts",
  "Archive",
  "Trash",
] as const;

/** Create an in-memory `MailboxStore` backing; state lives in process memory. */
export function createInMemoryMailboxStore(): MailboxStore {
  const messages: StoredMessage[] = [];
  // The in-memory backing is its own durable store, so it retains every
  // message's raw bytes while the metadata mirror in `messages` stays free of
  // them, matching the disk-backed backing.
  const rawByUid = new Map<number, Uint8Array>();
  let uidCounter = 1;
  let modseqCounter = 1;
  const uidValidity = Date.now();

  function find(uid: number): StoredMessage | undefined {
    return messages.find((m) => m.uid === uid);
  }

  function require(uid: number): StoredMessage {
    const msg = find(uid);
    if (msg === undefined) {
      throw new Error(`Message UID ${uid} not found`);
    }
    return msg;
  }

  return {
    uidValidity,
    get uidNext() {
      return uidCounter;
    },
    get highestModSeq() {
      return modseqCounter - 1;
    },
    get messages() {
      return messages;
    },
    append(raw, envelope, flags) {
      const uid = uidCounter++;
      const modseq = modseqCounter++;
      messages.push({ uid, modseq, flags: new Set(flags), envelope });
      rawByUid.set(uid, raw);
      return uid;
    },
    readRaw(uid) {
      const raw = rawByUid.get(uid);
      if (raw === undefined) {
        return Promise.reject(new Error(`Message UID ${uid} not found`));
      }
      return Promise.resolve(raw);
    },
    find,
    addFlags(uid, flags) {
      const msg = require(uid);
      for (const flag of flags) {
        msg.flags.add(flag);
      }
      msg.modseq = modseqCounter++;
      return msg;
    },
    removeFlags(uid, flags) {
      const msg = require(uid);
      for (const flag of flags) {
        msg.flags.delete(flag);
      }
      msg.modseq = modseqCounter++;
      return msg;
    },
    remove(uid) {
      const idx = messages.findIndex((m) => m.uid === uid);
      if (idx === -1) {
        throw new Error(`Message UID ${uid} not found`);
      }
      messages.splice(idx, 1);
      rawByUid.delete(uid);
    },
  };
}

/**
 * Locate a stored message by UID, throwing a mailbox-qualified error when
 * absent. Used by the fetch projections.
 */
export function requireMessage(
  store: MailboxStore,
  uid: number,
  mailboxName: string,
): StoredMessage {
  const msg = store.find(uid);
  if (msg === undefined) {
    throw new Error(`Message UID ${uid} not found in mailbox "${mailboxName}"`);
  }
  return msg;
}
