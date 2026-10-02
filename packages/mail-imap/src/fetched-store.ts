// A `MailboxStore` holding a set of already-fetched messages.
//
// The `@intx/mailbox` query and projection functions (`executeSearch`,
// `executeThread`, `fetchHeaders`, `fetchStructure`, `fetchPart`, `fetchFull`)
// are written against a `MailboxStore`: they read the metadata snapshot through
// `messages` and a message's bytes through `readRaw`. Wrapping a FETCH result in
// this store reuses every one of them against a real IMAP server, so the MIME
// walk, the part addressing, and the PGP/MIME signature check are the same code
// that runs over the in-memory and substrate backings. A message therefore reads
// identically whichever backing holds it.
//
// The mutation surface throws. A projection never mutates, and a flag write here
// would be lost: the server owns flag and message state, so the transport routes
// `setFlags`, `clearFlags`, and `expunge` to it rather than to this view.

import type {
  MailboxStore,
  StoredEnvelope,
  StoredMessage,
} from "@intx/mailbox";

export type FetchedMessage = {
  uid: number;
  modseq: number;
  flags: Set<string>;
  envelope: StoredEnvelope;
  raw: Uint8Array;
};

/** The mailbox counters a SELECT reported, carried through unchanged. */
export type MailboxCounters = {
  uidValidity: number;
  uidNext: number;
  highestModSeq: number;
};

/**
 * Wrap fetched messages as a read-only `MailboxStore`, ordered by uid as IMAP
 * reports them. `counters` carry the mailbox's real SELECT values so a caller
 * reading them off the store is not given invented ones.
 */
export function createFetchedStore(
  messages: readonly FetchedMessage[],
  counters: MailboxCounters,
): MailboxStore {
  const byUid = new Map(messages.map((m) => [m.uid, m]));
  const stored: StoredMessage[] = [...messages]
    .sort((a, b) => a.uid - b.uid)
    .map((m) => ({
      uid: m.uid,
      modseq: m.modseq,
      flags: m.flags,
      envelope: m.envelope,
    }));

  function refuseMutation(op: string): never {
    throw new Error(
      `fetched store: ${op} is not available; the IMAP server owns flag and message state, so the transport routes mutations to it rather than to this read-only view`,
    );
  }

  return {
    uidValidity: counters.uidValidity,
    uidNext: counters.uidNext,
    highestModSeq: counters.highestModSeq,
    messages: stored,
    append: () => refuseMutation("append"),
    readRaw(uid) {
      const found = byUid.get(uid);
      if (found === undefined) {
        return Promise.reject(
          new Error(
            `fetched store holds uids [${stored.map((m) => String(m.uid)).join(", ")}]; uid ${String(uid)} was not fetched`,
          ),
        );
      }
      return Promise.resolve(found.raw);
    },
    find(uid) {
      return stored.find((m) => m.uid === uid);
    },
    addFlags: () => refuseMutation("addFlags"),
    removeFlags: () => refuseMutation("removeFlags"),
    remove: () => refuseMutation("remove"),
  };
}
