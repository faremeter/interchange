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
 * A single stored message's resident model: its uid, the IMAP counters, its
 * flags, and the pre-parsed envelope. The complete RFC 2822 bytes are NOT
 * resident here; they are read on demand through `MailboxStore.readRaw`, so a
 * backing can bound its in-memory footprint to metadata and keep the raw bytes
 * on disk (the substrate backing) or retain them itself (the in-memory
 * backing). The projections that need the bytes -- `fetchFull`, `fetchPart`,
 * `fetchStructure`, `fetchHeaders`, and the raw-scanning search predicates --
 * route through `readRaw`, which returns the verbatim bytes so signature
 * verification stays byte-exact.
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
 * projection functions (search, thread, fetch, bodystructure, headers) read
 * the message snapshot the backing exposes through `messages`, and read a
 * message's raw bytes on demand through `readRaw`.
 *
 * The counters follow IMAP semantics: `uidNext` is the UID that the next
 * `append` will assign (UIDNEXT), `highestModSeq` is the largest MODSEQ
 * currently assigned (HIGHESTMODSEQ), and `uidValidity` is stable for the
 * lifetime of the mailbox (UIDVALIDITY).
 */
export interface MailboxStore {
  readonly uidValidity: number;
  readonly uidNext: number;
  readonly highestModSeq: number;
  readonly messages: readonly StoredMessage[];

  /**
   * Store a message, assigning it the next UID and MODSEQ. Returns the
   * assigned UID. The backing decides whether to retain `raw` in memory or
   * persist it and serve it from disk through `readRaw`.
   */
  append(raw: Uint8Array, envelope: StoredEnvelope, flags: string[]): number;

  /**
   * Read a stored message's verbatim RFC 2822 bytes. Resolves the bytes from
   * wherever the backing keeps them (memory or disk). Throws if no message has
   * the given UID.
   */
  readRaw(uid: number): Promise<Uint8Array>;

  /** Locate a stored message by UID, or `undefined` if none matches. */
  find(uid: number): StoredMessage | undefined;

  /**
   * Add flags to a stored message and advance its MODSEQ. Returns the updated
   * message. Throws if no message has the given UID.
   */
  addFlags(uid: number, flags: string[]): StoredMessage;

  /**
   * Remove flags from a stored message and advance its MODSEQ. Returns the
   * updated message. Throws if no message has the given UID.
   */
  removeFlags(uid: number, flags: string[]): StoredMessage;

  /** Drop a stored message by UID. Throws if no message has the given UID. */
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

/**
 * Create an in-memory `MailboxStore` backing. Messages, counters, and
 * uidValidity live in process memory for the lifetime of the returned store.
 */
export function createInMemoryMailboxStore(): MailboxStore {
  const messages: StoredMessage[] = [];
  // The in-memory backing is its own durable store, so it legitimately retains
  // every message's raw bytes. `readRaw` returns them; the metadata mirror in
  // `messages` stays free of the bytes so the read model matches the
  // disk-backed backing.
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
 * Locate a stored message by UID, throwing a mailbox-qualified error when it
 * is absent. Used by the fetch projections, which resolve a `MessageRef`
 * against a specific mailbox.
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
