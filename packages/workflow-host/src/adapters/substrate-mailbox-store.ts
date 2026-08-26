// Workflow-run-substrate backing for the `@intx/mailbox` `MailboxStore`.
//
// The `MailboxStore` surface is SYNCHRONOUS, but the workflow-run substrate is
// an async git store. This backing follows the shape of an IMAP client with a
// local cache: the async `createSubstrateMailboxStore` factory loads the
// committed `mailbox/INBOX/` subtree into an in-memory mirror on open, exposes
// the synchronous `MailboxStore` surface over that mirror, and persists the
// current state back to the substrate through `flush`. Callers mutate
// synchronously (append / addFlags / removeFlags / remove) and `await flush()`
// at a boundary.
//
// On-disk layout, a top-level subtree of the workflow-run repo (one mailbox per
// deployment repo):
//
//   mailbox/INBOX/index.json      committed metadata: uidValidity, the
//                                 uid/modseq counters, one entry per live
//                                 message (uid, modseq, flags, pre-parsed
//                                 envelope), and the expunged-uid tombstones
//                                 QRESYNC answers `vanished` from.
//   mailbox/INBOX/<uid>.eml       the verbatim raw RFC 2822 bytes of each live
//                                 message, so `fetchFull` verifies signatures
//                                 byte-exactly. Write-once per uid.
//
// Reads resolve from the committed substrate (`openCommittedReads`), never the
// lagging working tree, matching the sibling `mail-part-store` reader. Writes
// go through `writeTreeDelta`: `index.json` changes on every mutation and is
// always put; each `<uid>.eml` is immutable, so a flush puts only the blobs
// appended since the last successful flush and deletes only those whose
// message was removed since then, and the substrate carries every untouched
// `.eml` forward by object id. This keeps a flush O(delta) rather than
// O(mailbox), so a long-lived warm conversational mailbox does not re-hash its
// whole history on each append or flag change. The committed subtree the delta
// leaves behind is byte-identical in shape to a full rewrite of the same live
// set. The kind handler's push-time validation of this subtree is owned
// separately by the hub replication layer; this module owns the on-disk shape
// it validates.

import { type } from "arktype";
import type {
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";
import type {
  MailboxStore,
  StoredEnvelope,
  StoredMessage,
} from "@intx/mailbox";

/** Top-level subtree of the workflow-run repo that holds the mailbox. */
export const MAILBOX_PREFIX = "mailbox";
/** The single mailbox this backing persists, an IMAP INBOX. */
export const MAILBOX_INBOX_DIR = "INBOX";
/** Committed metadata blob name directly under `mailbox/INBOX/`. */
export const MAILBOX_INDEX_FILE = "index.json";
/** Suffix of a per-message raw-bytes blob (`<uid>.eml`). */
export const MAILBOX_EML_SUFFIX = ".eml";

/**
 * The `mailbox/INBOX/` prefix, ending in `/` as
 * `writeTreePreservingPrefix` requires. Every blob this backing writes is a
 * direct child of it.
 */
export const MAILBOX_INBOX_PREFIX = `${MAILBOX_PREFIX}/${MAILBOX_INBOX_DIR}/`;

/** Relative directory path of the INBOX, for `CommittedReads.listDir`. */
const MAILBOX_INBOX_DIR_PATH = `${MAILBOX_PREFIX}/${MAILBOX_INBOX_DIR}`;

/** Current on-disk schema version of `index.json`. */
const INDEX_VERSION = 1;

const decoder = new TextDecoder();
const encoder = new TextEncoder();

/**
 * On-disk envelope shape. Mirrors `StoredEnvelope` but serializes `date` as an
 * ISO string and the three nullable header fields as `string | null` (JSON has
 * no `undefined`); the loader maps `null` back to `undefined`.
 */
const StoredEnvelopeJson = type({
  messageId: "string",
  from: "string",
  to: "string[]",
  subject: "string",
  date: "string",
  inReplyTo: "string | null",
  references: "string[]",
  interchangeType: "string | null",
  interchangeCorrelationId: "string | null",
});

/**
 * On-disk `index.json` shape. Validated on every open: the committed tree is
 * durable but external to this process, so it is parsed at the boundary rather
 * than trusted. `expunged` records the uid and the modseq at which each
 * message vanished so a QRESYNC `sync` can answer `vanished` since a client's
 * known modseq.
 */
const MailboxIndexJson = type({
  version: `${INDEX_VERSION}`,
  uidValidity: "number >= 0",
  uidNext: "number >= 1",
  highestModSeq: "number >= 0",
  messages: type({
    uid: "number >= 1",
    modseq: "number >= 1",
    flags: "string[]",
    envelope: StoredEnvelopeJson,
  }).array(),
  expunged: type({
    uid: "number >= 1",
    modseq: "number >= 1",
  }).array(),
});

type MailboxIndexJson = typeof MailboxIndexJson.infer;

/** A message the client no longer holds, with the modseq at which it vanished. */
type ExpungedRecord = { uid: number; modseq: number };

/**
 * The client's last-known synchronization state, per QRESYNC (RFC 7162). A
 * mismatched `uidValidity` forces a full resync; otherwise `highestModSeq`
 * bounds the changed / vanished deltas.
 */
export type MailboxSyncKnownState = {
  uidValidity: number;
  highestModSeq: number;
};

/**
 * The result of a QRESYNC `sync`. `resync: true` signals the client's
 * `uidValidity` no longer matches the mailbox, so it must discard its cache
 * and take the full `messages` snapshot. `resync: false` carries the deltas
 * since the client's `highestModSeq`: `changed` is every live message whose
 * modseq advanced past it (new arrivals and flag changes alike), and
 * `vanished` is every uid expunged past it.
 */
export type MailboxSyncResult =
  | {
      resync: true;
      uidValidity: number;
      uidNext: number;
      highestModSeq: number;
      messages: readonly StoredMessage[];
    }
  | {
      resync: false;
      uidValidity: number;
      uidNext: number;
      highestModSeq: number;
      changed: readonly StoredMessage[];
      vanished: readonly number[];
    };

/**
 * A `MailboxStore` whose state is durable in the workflow-run substrate. The
 * synchronous `MailboxStore` surface reads and mutates an in-memory mirror;
 * `flush` persists that mirror to `mailbox/INBOX/`; `sync` answers a QRESYNC
 * delta against a client's known state.
 */
export interface SubstrateMailboxStore extends MailboxStore {
  /** True when a mutation has occurred that `flush` has not yet persisted. */
  readonly pendingWrites: boolean;
  /**
   * Persist the current mirror to the substrate through a delta write:
   * `index.json` (always), the `<uid>.eml` blobs appended since the last
   * successful flush, and deletions for the blobs whose message was removed
   * since then. Every untouched `<uid>.eml` is carried forward by object id. A
   * no-op when no mutation is pending.
   */
  flush(): Promise<void>;
  /** Compute the QRESYNC delta between the mailbox and a client's known state. */
  sync(known: MailboxSyncKnownState): MailboxSyncResult;
}

export type SubstrateMailboxStoreOpts = {
  substrate: SubstrateRepoStore;
  repoId: RepoId;
  principal: Principal;
  ref: string;
};

function serializeEnvelope(envelope: StoredEnvelope) {
  return {
    messageId: envelope.messageId,
    from: envelope.from,
    to: envelope.to,
    subject: envelope.subject,
    date: envelope.date.toISOString(),
    inReplyTo: envelope.inReplyTo ?? null,
    references: envelope.references,
    interchangeType: envelope.interchangeType ?? null,
    interchangeCorrelationId: envelope.interchangeCorrelationId ?? null,
  };
}

function deserializeEnvelope(
  raw: MailboxIndexJson["messages"][number]["envelope"],
): StoredEnvelope {
  return {
    messageId: raw.messageId,
    from: raw.from,
    to: raw.to,
    subject: raw.subject,
    date: new Date(raw.date),
    inReplyTo: raw.inReplyTo === null ? undefined : raw.inReplyTo,
    references: raw.references,
    interchangeType:
      raw.interchangeType === null ? undefined : raw.interchangeType,
    interchangeCorrelationId:
      raw.interchangeCorrelationId === null
        ? undefined
        : raw.interchangeCorrelationId,
  };
}

/** The `<uid>.eml` blob name for a message. */
function emlName(uid: number): string {
  return `${String(uid)}${MAILBOX_EML_SUFFIX}`;
}

type LoadedState = {
  uidValidity: number;
  uidNext: number;
  highestModSeq: number;
  messages: StoredMessage[];
  expunged: ExpungedRecord[];
};

/**
 * Load the committed `mailbox/INBOX/` subtree into an in-memory state, or the
 * empty state (a fresh `uidValidity`) when the repo, the ref, or the subtree
 * does not yet exist. Every read resolves against the committed object store,
 * so an open observes committed state even when the working tree lags.
 */
async function loadCommittedState(
  opts: SubstrateMailboxStoreOpts,
): Promise<LoadedState> {
  const empty = (): LoadedState => ({
    uidValidity: Date.now(),
    uidNext: 1,
    highestModSeq: 0,
    messages: [],
    expunged: [],
  });

  const reads = await opts.substrate.openCommittedReads(
    opts.principal,
    opts.repoId,
    opts.ref,
  );
  if (reads === null) return empty();

  const entries = await reads.listDir(MAILBOX_INBOX_DIR_PATH);
  const indexEntry = entries.find(
    (e) => e.name === MAILBOX_INDEX_FILE && e.type === "blob",
  );
  if (indexEntry === undefined) return empty();

  const indexBytes = await reads.readBlobByOid(indexEntry.oid);
  let parsedJson: unknown;
  try {
    parsedJson = JSON.parse(decoder.decode(indexBytes));
  } catch (cause) {
    throw new Error(
      `substrate mailbox store: ${MAILBOX_INBOX_PREFIX}${MAILBOX_INDEX_FILE} is not valid JSON`,
      { cause },
    );
  }
  const index = MailboxIndexJson(parsedJson);
  if (index instanceof type.errors) {
    throw new Error(
      `substrate mailbox store: invalid ${MAILBOX_INBOX_PREFIX}${MAILBOX_INDEX_FILE}: ${index.summary}`,
    );
  }

  const emlByName = new Map(
    entries
      .filter((e) => e.type === "blob" && e.name.endsWith(MAILBOX_EML_SUFFIX))
      .map((e) => [e.name, e.oid]),
  );

  const messages: StoredMessage[] = [];
  for (const entry of index.messages) {
    const oid = emlByName.get(emlName(entry.uid));
    if (oid === undefined) {
      throw new Error(
        `substrate mailbox store: index references message uid ${String(
          entry.uid,
        )} but ${MAILBOX_INBOX_PREFIX}${emlName(entry.uid)} is absent`,
      );
    }
    const raw = await reads.readBlobByOid(oid);
    messages.push({
      uid: entry.uid,
      modseq: entry.modseq,
      flags: new Set(entry.flags),
      raw,
      envelope: deserializeEnvelope(entry.envelope),
    });
  }

  return {
    uidValidity: index.uidValidity,
    uidNext: index.uidNext,
    highestModSeq: index.highestModSeq,
    messages,
    expunged: index.expunged.map((e) => ({ uid: e.uid, modseq: e.modseq })),
  };
}

/**
 * Create a workflow-run-substrate-backed `MailboxStore`. Loads the committed
 * `mailbox/INBOX/` subtree into an in-memory mirror, then serves the
 * synchronous `MailboxStore` surface over that mirror. Mutations stay in
 * memory until `flush` persists them.
 */
export async function createSubstrateMailboxStore(
  opts: SubstrateMailboxStoreOpts,
): Promise<SubstrateMailboxStore> {
  const state = await loadCommittedState(opts);

  const messages = state.messages;
  const expunged = state.expunged;
  const uidValidity = state.uidValidity;
  let uidCounter = state.uidNext;
  // The next modseq to assign. `highestModSeq` is the largest assigned, so the
  // next is one past it; a fresh mailbox (highestModSeq 0) starts at 1.
  let modseqCounter = state.highestModSeq + 1;
  let dirty = false;

  // Delta tracking for `flush`. `index.json` changes on every mutation, so it
  // is put unconditionally; each `<uid>.eml` is immutable and written once, so
  // a flush need only put the blobs appended since the last successful flush
  // and delete the blobs whose message was removed since then. Both sets clear
  // on a successful flush; a flush that throws leaves them intact so the next
  // flush re-attempts the same delta.
  const appendedSinceFlush = new Set<number>();
  const removedSinceFlush = new Set<number>();

  function find(uid: number): StoredMessage | undefined {
    return messages.find((m) => m.uid === uid);
  }

  function require(uid: number): StoredMessage {
    const msg = find(uid);
    if (msg === undefined) {
      throw new Error(`Message UID ${String(uid)} not found`);
    }
    return msg;
  }

  async function flush(): Promise<void> {
    if (!dirty) return;

    const index = {
      version: INDEX_VERSION,
      uidValidity,
      uidNext: uidCounter,
      highestModSeq: modseqCounter - 1,
      messages: messages.map((m) => ({
        uid: m.uid,
        modseq: m.modseq,
        flags: Array.from(m.flags),
        envelope: serializeEnvelope(m.envelope),
      })),
      expunged: expunged.map((e) => ({ uid: e.uid, modseq: e.modseq })),
    };

    // `index.json` is put on every flush. Each `<uid>.eml` is immutable, so
    // only the blobs appended since the last successful flush are put and only
    // those whose message was removed are deleted; every other `.eml` is
    // carried forward by object id, so the flush never re-hashes the mailbox's
    // whole history.
    const puts: Record<string, string | Uint8Array> = {
      [`${MAILBOX_INBOX_PREFIX}${MAILBOX_INDEX_FILE}`]: encoder.encode(
        JSON.stringify(index),
      ),
    };
    for (const uid of appendedSinceFlush) {
      const msg = find(uid);
      if (msg === undefined) {
        throw new Error(
          `substrate mailbox store: flush cannot persist uid ${String(
            uid,
          )}: no live message holds its raw bytes`,
        );
      }
      puts[`${MAILBOX_INBOX_PREFIX}${emlName(uid)}`] = msg.raw;
    }
    const deletes = Array.from(
      removedSinceFlush,
      (uid) => `${MAILBOX_INBOX_PREFIX}${emlName(uid)}`,
    );

    await opts.substrate.writeTreeDelta(opts.principal, opts.repoId, opts.ref, {
      computeDelta: async () => ({ puts, deletes }),
      changedPathPrefixes: new Set([MAILBOX_INBOX_PREFIX]),
      message: `persist mailbox INBOX (${String(messages.length)} message(s))`,
    });
    appendedSinceFlush.clear();
    removedSinceFlush.clear();
    dirty = false;
  }

  function sync(known: MailboxSyncKnownState): MailboxSyncResult {
    const highestModSeq = modseqCounter - 1;
    if (known.uidValidity !== uidValidity) {
      return {
        resync: true,
        uidValidity,
        uidNext: uidCounter,
        highestModSeq,
        messages: messages.slice(),
      };
    }
    const changed = messages
      .filter((m) => m.modseq > known.highestModSeq)
      .sort((a, b) => a.uid - b.uid);
    const vanished = expunged
      .filter((e) => e.modseq > known.highestModSeq)
      .map((e) => e.uid)
      .sort((a, b) => a - b);
    return {
      resync: false,
      uidValidity,
      uidNext: uidCounter,
      highestModSeq,
      changed,
      vanished,
    };
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
    get pendingWrites() {
      return dirty;
    },
    append(raw, envelope, flags) {
      const uid = uidCounter++;
      const modseq = modseqCounter++;
      messages.push({ uid, modseq, flags: new Set(flags), raw, envelope });
      appendedSinceFlush.add(uid);
      dirty = true;
      return uid;
    },
    find,
    addFlags(uid, flags) {
      const msg = require(uid);
      for (const flag of flags) {
        msg.flags.add(flag);
      }
      msg.modseq = modseqCounter++;
      dirty = true;
      return msg;
    },
    removeFlags(uid, flags) {
      const msg = require(uid);
      for (const flag of flags) {
        msg.flags.delete(flag);
      }
      msg.modseq = modseqCounter++;
      dirty = true;
      return msg;
    },
    remove(uid) {
      const idx = messages.findIndex((m) => m.uid === uid);
      if (idx === -1) {
        throw new Error(`Message UID ${String(uid)} not found`);
      }
      messages.splice(idx, 1);
      // Record the expunge with a fresh modseq so a QRESYNC `sync` can report
      // this uid as `vanished` to a client whose known modseq predates it. The
      // in-memory reference backing does not advance modseq on remove; this
      // backing does, because it must answer QRESYNC across reopens.
      expunged.push({ uid, modseq: modseqCounter++ });
      // A message appended and removed within the same flush window was never
      // committed, so its `.eml` must be neither put nor deleted: drop it from
      // the append set. Otherwise the blob is already committed and the next
      // flush deletes it.
      if (appendedSinceFlush.has(uid)) {
        appendedSinceFlush.delete(uid);
      } else {
        removedSinceFlush.add(uid);
      }
      dirty = true;
    },
    flush,
    sync,
  };
}
