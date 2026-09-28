// On-repo format of a workflow-run repo's mailbox index,
// `mailbox/INBOX/index.json`: the mailbox's `uidValidity`, its uid and modseq
// counters, one entry per live message (uid, modseq, flags, pre-parsed
// envelope), and the expunged-uid tombstones QRESYNC answers `vanished` from.
// The workflow host's substrate mailbox store owns the mailbox and reads and
// writes the index; the format lives here, beside the repo's other formats, so
// every reader and writer shares it.

import { type } from "arktype";

/** Current on-disk schema version of the mailbox index. */
export const MAILBOX_INDEX_VERSION = 1;

/**
 * On-disk envelope shape. Mirrors `StoredEnvelope` but serializes `date` as an
 * ISO string and the three nullable header fields as `string | null` (JSON has
 * no `undefined`); the loader maps `null` back to `undefined`.
 */
const MailboxIndexEnvelope = type({
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
 * On-disk `index.json` shape. Validated on every read: the committed tree is
 * durable but external to the reading process, so it is parsed at the boundary
 * rather than trusted. `expunged` records the uid and the modseq at which each
 * message vanished so a QRESYNC `sync` can answer `vanished` since a client's
 * known modseq.
 */
export const MailboxIndex = type({
  version: `${MAILBOX_INDEX_VERSION}`,
  uidValidity: "number >= 0",
  uidNext: "number >= 1",
  highestModSeq: "number >= 0",
  messages: type({
    uid: "number >= 1",
    modseq: "number >= 1",
    flags: "string[]",
    envelope: MailboxIndexEnvelope,
  }).array(),
  expunged: type({
    uid: "number >= 1",
    modseq: "number >= 1",
  }).array(),
});
export type MailboxIndex = typeof MailboxIndex.infer;
