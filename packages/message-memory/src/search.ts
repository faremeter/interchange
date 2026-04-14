import type { SearchQuery, MessageRef } from "@interchange/types/runtime";
import type { MailboxStore, StoredMessage } from "./mailbox";
import { parseHeaderSection } from "./mime";

/**
 * Execute an IMAP SEARCH-equivalent query over an in-memory mailbox.
 *
 * Supports: from, to, cc, bcc, header (field match), before/after/on,
 * sentBefore/sentAfter/sentOn, hasFlags, missingFlags, body, text,
 * largerThan, smallerThan, and boolean and/or/not composition.
 *
 * Returns MessageRef[] for all matching messages, ordered by UID.
 */
export function executeSearch(
  mailboxName: string,
  store: MailboxStore,
  query: SearchQuery,
): MessageRef[] {
  const results: MessageRef[] = [];
  for (const msg of store.messages) {
    if (matchMessage(msg, query)) {
      results.push({ uid: msg.uid, mailbox: mailboxName });
    }
  }
  return results;
}

function matchMessage(msg: StoredMessage, query: SearchQuery): boolean {
  if (query.from !== undefined) {
    if (!msg.envelope.from.toLowerCase().includes(query.from.toLowerCase())) {
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
    const headers = lazyHeaders(msg);
    const ccHeader = headers.get("cc") ?? "";
    if (!ccHeader.toLowerCase().includes(query.cc.toLowerCase())) {
      return false;
    }
  }

  if (query.bcc !== undefined) {
    const headers = lazyHeaders(msg);
    const bccHeader = headers.get("bcc") ?? "";
    if (!bccHeader.toLowerCase().includes(query.bcc.toLowerCase())) {
      return false;
    }
  }

  if (query.header !== undefined) {
    const { field, contains } = query.header;
    const headers = lazyHeaders(msg);
    const value = headers.get(field.toLowerCase()) ?? "";
    if (!value.toLowerCase().includes(contains.toLowerCase())) {
      return false;
    }
  }

  if (query.before !== undefined) {
    if (msg.envelope.date >= query.before) return false;
  }
  if (query.after !== undefined) {
    if (msg.envelope.date <= query.after) return false;
  }
  if (query.on !== undefined) {
    const d = msg.envelope.date;
    const q = query.on;
    if (
      d.getUTCFullYear() !== q.getUTCFullYear() ||
      d.getUTCMonth() !== q.getUTCMonth() ||
      d.getUTCDate() !== q.getUTCDate()
    ) {
      return false;
    }
  }

  // Sent date filters use the Date header (same as envelope date here).
  if (query.sentBefore !== undefined) {
    if (msg.envelope.date >= query.sentBefore) return false;
  }
  if (query.sentAfter !== undefined) {
    if (msg.envelope.date <= query.sentAfter) return false;
  }
  if (query.sentOn !== undefined) {
    const d = msg.envelope.date;
    const q = query.sentOn;
    if (
      d.getUTCFullYear() !== q.getUTCFullYear() ||
      d.getUTCMonth() !== q.getUTCMonth() ||
      d.getUTCDate() !== q.getUTCDate()
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
    if (msg.raw.length <= query.largerThan) return false;
  }
  if (query.smallerThan !== undefined) {
    if (msg.raw.length >= query.smallerThan) return false;
  }

  if (query.body !== undefined || query.text !== undefined) {
    const rawText = new TextDecoder("utf-8", { fatal: false }).decode(msg.raw);
    if (query.body !== undefined) {
      const { bodyOffset } = parseHeaderSection(msg.raw);
      const bodyText = new TextDecoder("utf-8", { fatal: false }).decode(
        msg.raw.slice(bodyOffset),
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
      if (!matchMessage(msg, sub)) return false;
    }
  }

  if (query.or !== undefined) {
    if (query.or.length > 0) {
      const anyMatch = query.or.some((sub) => matchMessage(msg, sub));
      if (!anyMatch) return false;
    }
  }

  if (query.not !== undefined) {
    if (matchMessage(msg, query.not)) return false;
  }

  return true;
}

const headerCache = new WeakMap<StoredMessage, Map<string, string>>();

function lazyHeaders(msg: StoredMessage): Map<string, string> {
  const cached = headerCache.get(msg);
  if (cached !== undefined) return cached;
  const { headers } = parseHeaderSection(msg.raw);
  headerCache.set(msg, headers);
  return headers;
}
