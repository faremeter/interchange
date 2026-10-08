// Workflow-run-substrate backing for the `@intx/mailbox` `MailboxStore`.
//
// The `MailboxStore` mutation surface is SYNCHRONOUS while the substrate is
// an async git store, so this backing follows an IMAP-client-with-local-cache
// shape: load the committed `mailbox/INBOX/index.json` metadata into an
// in-memory mirror on open, serve the synchronous surface over it, and
// persist through `flush`.
//
// Raw RFC 2822 bytes are never held resident: `readRaw(uid)` reads the
// message's `<uid>.eml` blob on demand from the pinned committed snapshot;
// only an appended-but-not-yet-flushed message keeps its raw in memory. So a
// long-lived mailbox's footprint is bounded to metadata.
//
// On-disk layout (a top-level subtree of the workflow-run repo):
//
//   mailbox/INBOX/index.json   metadata: uidValidity, uid/modseq counters,
//                              one entry per live message, expunged-uid
//                              tombstones for QRESYNC `vanished`.
//   mailbox/INBOX/<uid>.eml    the verbatim raw bytes, write-once per uid,
//                              so `fetchFull` verifies signatures exactly.
//
// Reads resolve from the committed substrate, never the lagging working tree.
// Writes go through `writeTreeDelta`: `index.json` is always put; each
// `.eml` is immutable, so a flush puts only blobs appended since the last
// successful flush and deletes only removed ones -- O(delta), not O(mailbox).

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
 * On-disk envelope shape. Mirrors `StoredEnvelope` but serializes
 * `date` as an ISO string and nullable header fields as `string | null`
 * (JSON has no `undefined`); the loader maps `null` back to `undefined`.
 * Widening a field needs no `INDEX_VERSION` bump: the version is matched
 * exactly, so a bump rejects every existing mailbox instead of migrating it.
 */
const StoredEnvelopeJson = type({
  messageId: "string",
  from: "string | null",
  to: "string[]",
  subject: "string",
  date: "string | null",
  inReplyTo: "string | null",
  references: "string[]",
  interchangeType: "string | null",
  interchangeCorrelationId: "string | null",
});

/**
 * On-disk `index.json` shape, validated on every open (the committed
 * tree is external to this process). `expunged` records uid + modseq at
 * which each message vanished, so QRESYNC `sync` can answer `vanished`.
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
 * The client's last-known synchronization state, per QRESYNC (RFC 7162).
 * A mismatched `uidValidity` forces a full resync; otherwise
 * `highestModSeq` bounds the changed / vanished deltas.
 */
export type MailboxSyncKnownState = {
  uidValidity: number;
  highestModSeq: number;
};

/**
 * Result of a QRESYNC `sync`. `resync: true` signals the client's
 * `uidValidity` no longer matches, so it must discard its cache and
 * take the full `messages` snapshot. `resync: false` carries the deltas
 * since the client's `highestModSeq`: `changed` (modseq advanced past
 * it) and `vanished` (uid expunged past it).
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
 * A `MailboxStore` whose state is durable in the workflow-run substrate:
 * the synchronous surface reads and mutates an in-memory mirror; `flush`
 * persists it; `sync` answers a QRESYNC delta.
 */
export interface SubstrateMailboxStore extends MailboxStore {
  /** True when a mutation has occurred that `flush` has not yet persisted. */
  readonly pendingWrites: boolean;
  /** Persist the mirror via a delta write; a no-op when nothing is pending. */
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
    from: envelope.from ?? null,
    to: envelope.to,
    subject: envelope.subject,
    date: envelope.date === undefined ? null : envelope.date.toISOString(),
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
    from: raw.from === null ? undefined : raw.from,
    to: raw.to,
    subject: raw.subject,
    date: raw.date === null ? undefined : new Date(raw.date),
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

/**
 * Pinned committed-read snapshot the store opened against, the source
 * `readRaw` resolves a live `<uid>.eml` from; `null` when the repo,
 * ref, or subtree did not exist at open.
 */
type CommittedReads = Awaited<
  ReturnType<SubstrateRepoStore["openCommittedReads"]>
>;

type LoadedState = {
  uidValidity: number;
  uidNext: number;
  highestModSeq: number;
  messages: StoredMessage[];
  expunged: ExpungedRecord[];
  /** Pinned committed-read snapshot, for on-demand `readRaw`. */
  reads: CommittedReads;
  /** The `<uid>.eml` object id per committed uid. */
  oidByUid: Map<number, string>;
};

/**
 * Load the committed `mailbox/INBOX/` metadata into an in-memory state,
 * or the empty state (a fresh `uidValidity`) when the repo, ref, or
 * subtree does not exist yet. Only `index.json` is read; the `<uid>.eml`
 * blobs stay on disk and are read lazily by `readRaw`. The committed-read
 * snapshot and uid->oid map are retained so `readRaw` resolves against
 * the same pinned commit the open observed.
 */
async function loadCommittedState(
  opts: SubstrateMailboxStoreOpts,
): Promise<LoadedState> {
  const empty = (reads: CommittedReads): LoadedState => ({
    uidValidity: Date.now(),
    uidNext: 1,
    highestModSeq: 0,
    messages: [],
    expunged: [],
    reads,
    oidByUid: new Map(),
  });

  const reads = await opts.substrate.openCommittedReads(
    opts.principal,
    opts.repoId,
    opts.ref,
  );
  if (reads === null) return empty(reads);

  const entries = await reads.listDir(MAILBOX_INBOX_DIR_PATH);
  const indexEntry = entries.find(
    (e) => e.name === MAILBOX_INDEX_FILE && e.type === "blob",
  );
  if (indexEntry === undefined) return empty(reads);

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
  const oidByUid = new Map<number, string>();
  for (const entry of index.messages) {
    const oid = emlByName.get(emlName(entry.uid));
    if (oid === undefined) {
      throw new Error(
        `substrate mailbox store: index references message uid ${String(
          entry.uid,
        )} but ${MAILBOX_INBOX_PREFIX}${emlName(entry.uid)} is absent`,
      );
    }
    // Presence is asserted by the object id; bytes are read on demand.
    oidByUid.set(entry.uid, oid);
    messages.push({
      uid: entry.uid,
      modseq: entry.modseq,
      flags: new Set(entry.flags),
      envelope: deserializeEnvelope(entry.envelope),
    });
  }

  return {
    uidValidity: index.uidValidity,
    uidNext: index.uidNext,
    highestModSeq: index.highestModSeq,
    messages,
    expunged: index.expunged.map((e) => ({ uid: e.uid, modseq: e.modseq })),
    reads,
    oidByUid,
  };
}

/**
 * Create a workflow-run-substrate-backed `MailboxStore`: load the
 * committed subtree into an in-memory mirror, serve the synchronous
 * surface over it; mutations stay in memory until `flush`.
 */
export async function createSubstrateMailboxStore(
  opts: SubstrateMailboxStoreOpts,
): Promise<SubstrateMailboxStore> {
  const state = await loadCommittedState(opts);

  const messages = state.messages;
  const expunged = state.expunged;
  const uidValidity = state.uidValidity;
  const reads = state.reads;
  const oidByUid = state.oidByUid;
  let uidCounter = state.uidNext;
  // Next modseq: one past the largest assigned (fresh mailbox starts at 1).
  let modseqCounter = state.highestModSeq + 1;
  let dirty = false;

  // Delta tracking for `flush`: `index.json` is put unconditionally;
  // each `.eml` is immutable, so only blobs appended since the last
  // successful flush are put and only removed ones are deleted. The
  // removed set clears on success; a throwing flush leaves it intact
  // for a retry. `pendingRawByUid` holds appended-but-not-yet-flushed
  // raws (the only resident raw) and doubles as the appended-since-flush
  // set.
  const pendingRawByUid = new Map<number, Uint8Array>();
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

    // Only appended and removed blobs are touched; every other `.eml`
    // carries forward by object id, so the flush never re-hashes the
    // mailbox's whole history.
    const puts: Record<string, string | Uint8Array> = {
      [`${MAILBOX_INBOX_PREFIX}${MAILBOX_INDEX_FILE}`]: encoder.encode(
        JSON.stringify(index),
      ),
    };
    const flushedUids: number[] = [];
    for (const [uid, raw] of pendingRawByUid) {
      puts[`${MAILBOX_INBOX_PREFIX}${emlName(uid)}`] = raw;
      flushedUids.push(uid);
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
    // Committed raws leave memory; a post-open append resolves only while
    // pending (the pinned snapshot predates the commit), which is fine:
    // the writer never reads its own appends back, and readers open a
    // fresh snapshot.
    for (const uid of flushedUids) {
      pendingRawByUid.delete(uid);
    }
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
      messages.push({ uid, modseq, flags: new Set(flags), envelope });
      pendingRawByUid.set(uid, raw);
      dirty = true;
      return uid;
    },
    async readRaw(uid) {
      if (find(uid) === undefined) {
        throw new Error(`Message UID ${String(uid)} not found`);
      }
      // Pending appends keep their raw in memory; committed messages
      // read from the pinned snapshot on demand.
      const pending = pendingRawByUid.get(uid);
      if (pending !== undefined) return pending;
      const oid = oidByUid.get(uid);
      if (oid === undefined || reads === null) {
        throw new Error(
          `substrate mailbox store: no committed blob for message uid ${String(
            uid,
          )}; its raw bytes are not resolvable from this snapshot`,
        );
      }
      return reads.readBlobByOid(oid);
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
      // Record the expunge with a fresh modseq so a QRESYNC `sync` can
      // report this uid as `vanished`; unlike the in-memory reference
      // backing, this one must answer QRESYNC across reopens.
      expunged.push({ uid, modseq: modseqCounter++ });
      // Appended and removed within one flush window, the `.eml` was
      // never committed: drop its pending raw instead of deleting.
      if (pendingRawByUid.has(uid)) {
        pendingRawByUid.delete(uid);
      } else {
        removedSinceFlush.add(uid);
      }
      dirty = true;
    },
    flush,
    sync,
  };
}
