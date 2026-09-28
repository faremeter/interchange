import type { SearchQuery, MessageRef } from "@intx/types/runtime";
import type { MailboxStore, StoredMessage } from "./mailbox";
import { parseHeaderSection } from "@intx/mime";

/**
 * Execute an IMAP SEARCH-equivalent query over a mailbox.
 *
 * Supports: from, to, cc, bcc, header (field match), before/after/on,
 * sentBefore/sentAfter/sentOn, hasFlags, missingFlags, body, text,
 * largerThan, smallerThan, and boolean and/or/not composition.
 *
 * The envelope- and flag-based predicates (from, to, dates, flags, boolean
 * composition) resolve from metadata alone. The predicates that inspect
 * headers the envelope does not carry (cc, bcc, arbitrary `header`), the body,
 * or the raw size (body, text, largerThan, smallerThan) read a message's raw
 * bytes on demand through `store.readRaw`, memoized per message so a query that
 * touches raw reads each candidate's blob at most once. A query with no
 * raw-scanning predicate never reads a blob.
 *
 * Returns MessageRef[] for all matching messages, ordered by UID.
 */
export async function executeSearch(
  mailboxName: string,
  store: MailboxStore,
  query: SearchQuery,
): Promise<MessageRef[]> {
  const results: MessageRef[] = [];
  for (const msg of store.messages) {
    if (await matchMessage(msg, query, makeRawReader(store, msg.uid))) {
      results.push({ uid: msg.uid, mailbox: mailboxName });
    }
  }
  return results;
}

/**
 * A per-message memoized reader for the raw bytes. The first raw-scanning
 * predicate reads the blob through `store.readRaw`; every later predicate on
 * the same message reuses the resolved bytes.
 */
function makeRawReader(
  store: MailboxStore,
  uid: number,
): () => Promise<Uint8Array> {
  let pending: Promise<Uint8Array> | undefined;
  return () => {
    if (pending === undefined) pending = store.readRaw(uid);
    return pending;
  };
}

async function matchMessage(
  msg: StoredMessage,
  query: SearchQuery,
  readRaw: () => Promise<Uint8Array>,
): Promise<boolean> {
  if (query.from !== undefined) {
    const sender = msg.envelope.from;
    if (
      sender === undefined ||
      !sender.toLowerCase().includes(query.from.toLowerCase())
    ) {
      return false;
    }
  }

  if (query.to !== undefined) {
    const queryTo = query.to;
    const toMatch = msg.envelope.to.some((addr) =>
      addr.toLowerCase().includes(queryTo.toLowerCase()),
    );
    if (!toMatch) return false;
  }

  if (query.cc !== undefined) {
    const headers = await lazyHeaders(msg, readRaw);
    const ccHeader = headers.get("cc") ?? "";
    if (!ccHeader.toLowerCase().includes(query.cc.toLowerCase())) {
      return false;
    }
  }

  if (query.bcc !== undefined) {
    const headers = await lazyHeaders(msg, readRaw);
    const bccHeader = headers.get("bcc") ?? "";
    if (!bccHeader.toLowerCase().includes(query.bcc.toLowerCase())) {
      return false;
    }
  }

  if (query.header !== undefined) {
    const { field, contains } = query.header;
    const headers = await lazyHeaders(msg, readRaw);
    const value = headers.get(field.toLowerCase()) ?? "";
    if (!value.toLowerCase().includes(contains.toLowerCase())) {
      return false;
    }
  }

  // A message that named no date placed itself nowhere in time, so it falls
  // outside every date window below rather than inside one.
  const sent = msg.envelope.date;

  if (query.before !== undefined) {
    if (sent === undefined || sent >= query.before) return false;
  }
  if (query.after !== undefined) {
    if (sent === undefined || sent <= query.after) return false;
  }
  if (query.on !== undefined) {
    if (sent === undefined) return false;
    const q = query.on;
    if (
      sent.getUTCFullYear() !== q.getUTCFullYear() ||
      sent.getUTCMonth() !== q.getUTCMonth() ||
      sent.getUTCDate() !== q.getUTCDate()
    ) {
      return false;
    }
  }

  // Sent date filters use the Date header (same as envelope date here).
  if (query.sentBefore !== undefined) {
    if (sent === undefined || sent >= query.sentBefore) return false;
  }
  if (query.sentAfter !== undefined) {
    if (sent === undefined || sent <= query.sentAfter) return false;
  }
  if (query.sentOn !== undefined) {
    if (sent === undefined) return false;
    const q = query.sentOn;
    if (
      sent.getUTCFullYear() !== q.getUTCFullYear() ||
      sent.getUTCMonth() !== q.getUTCMonth() ||
      sent.getUTCDate() !== q.getUTCDate()
    ) {
      return false;
    }
  }

  if (query.hasFlags !== undefined) {
    for (const flag of query.hasFlags) {
      if (!msg.flags.has(flag)) return false;
    }
  }

  if (query.missingFlags !== undefined) {
    for (const flag of query.missingFlags) {
      if (msg.flags.has(flag)) return false;
    }
  }

  if (query.largerThan !== undefined) {
    if ((await readRaw()).length <= query.largerThan) return false;
  }
  if (query.smallerThan !== undefined) {
    if ((await readRaw()).length >= query.smallerThan) return false;
  }

  if (query.body !== undefined || query.text !== undefined) {
    const raw = await readRaw();
    const rawText = new TextDecoder("utf-8", { fatal: false }).decode(raw);
    if (query.body !== undefined) {
      const { bodyOffset } = parseHeaderSection(raw);
      const bodyText = new TextDecoder("utf-8", { fatal: false }).decode(
        raw.slice(bodyOffset),
      );
      if (!bodyText.toLowerCase().includes(query.body.toLowerCase())) {
        return false;
      }
    }
    if (query.text !== undefined) {
      if (!rawText.toLowerCase().includes(query.text.toLowerCase())) {
        return false;
      }
    }
  }

  if (query.and !== undefined) {
    for (const sub of query.and) {
      if (!(await matchMessage(msg, sub, readRaw))) return false;
    }
  }

  if (query.or !== undefined) {
    if (query.or.length > 0) {
      let anyMatch = false;
      for (const sub of query.or) {
        if (await matchMessage(msg, sub, readRaw)) {
          anyMatch = true;
          break;
        }
      }
      if (!anyMatch) return false;
    }
  }

  if (query.not !== undefined) {
    if (await matchMessage(msg, query.not, readRaw)) return false;
  }

  return true;
}

const headerCache = new WeakMap<StoredMessage, Map<string, string>>();

async function lazyHeaders(
  msg: StoredMessage,
  readRaw: () => Promise<Uint8Array>,
): Promise<Map<string, string>> {
  const cached = headerCache.get(msg);
  if (cached !== undefined) return cached;
  const { headers } = parseHeaderSection(await readRaw());
  headerCache.set(msg, headers);
  return headers;
}
