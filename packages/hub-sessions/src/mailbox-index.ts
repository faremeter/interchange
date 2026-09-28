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
 * ISO string and the nullable header fields as `string | null` (JSON has no
 * `undefined`); the loader maps `null` back to `undefined`.
 *
 * Widening a field needs no `MAILBOX_INDEX_VERSION` bump: the version is
 * matched exactly, so a bump rejects every existing mailbox instead of
 * migrating it.
 */
const MailboxIndexEnvelope = type({
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
 * On-disk `index.json` shape. Validated on every read: the committed tree is
 * durable but external to the reading process, so it is parsed at the boundary
 * rather than trusted. `expunged` records the uid and the modseq at which each
 * message vanished so a QRESYNC `sync` can answer `vanished` since a client's
 * known modseq.
 */
export const MailboxIndex = type({
  version: `${MAILBOX_INDEX_VERSION}`,
  uidValidity: "number >= 0",
  uidNext: "1 <= number.integer <= 4294967295",
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

/** Parse an index file's text, naming the file as `label` in any error. */
export function parseMailboxIndex(raw: string, label: string): MailboxIndex {
  let body: unknown;
  try {
    body = JSON.parse(raw);
  } catch (cause) {
    throw new Error(`${label} is not valid JSON`, { cause });
  }
  const index = MailboxIndex(body);
  if (index instanceof type.errors) {
    throw new Error(`${label} is not valid: ${index.summary}`);
  }
  return index;
}
