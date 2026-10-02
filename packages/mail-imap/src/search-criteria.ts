// Translation of the transport's `SearchQuery` into an IMAP SEARCH.
//
// `SearchQuery` was written against the IMAP SEARCH grammar, so most of it maps
// one-to-one. Three things do not, and this module names them rather than hiding
// them:
//
//   1. IMAP SEARCH carries ONE custom keyword per KEYWORD key, while `hasFlags`
//      and `missingFlags` are lists. The first custom keyword in each direction
//      goes to the server; later ones stay behind.
//   2. The `SearchObject` shape imapflow accepts has `or` and `not` but no `and`
//      -- its keys are implicitly ANDed, so an `and` branch merges into the
//      parent only while its keys do not collide. A collision stays behind.
//   3. IMAP date keys are date-granular and inclusive: SINCE matches a message
//      whose INTERNALDATE falls ON the date as well as after it. `after` reads
//      as strictly later, so the server's answer is a superset and the predicate
//      is re-checked locally.
//
// What stays behind is the `residue`: a list of `SearchQuery` the caller applies
// with `executeSearch` over the messages the server returned. The server's
// answer is always a SUPERSET of the true match set, so a local re-check narrows
// it and never has to widen it.

import type { SearchQuery } from "@intx/types/runtime";
import type { SearchObject } from "imapflow";

/** IMAP system flags, and the `SearchObject` key each one is spelled as. */
const SYSTEM_FLAG_KEYS: Readonly<Record<string, keyof SearchObject>> = {
  "\\Seen": "seen",
  "\\Answered": "answered",
  "\\Flagged": "flagged",
  "\\Deleted": "deleted",
  "\\Draft": "draft",
};

export type TranslatedSearch = {
  /** What the server is asked. Matches a superset of the query. */
  criteria: SearchObject;
  /**
   * Predicates IMAP SEARCH could not carry, to re-check locally over the
   * messages the server returned. Empty when the translation was complete.
   */
  residue: SearchQuery[];
};

/**
 * True when `date` names midnight local time, the only case where an IMAP
 * date-granular comparison agrees exactly with an instant comparison. A query
 * built from a date string (which is how the mail tools parse one) lands here;
 * one built from an arbitrary instant does not.
 */
function isMidnight(date: Date): boolean {
  return (
    date.getHours() === 0 &&
    date.getMinutes() === 0 &&
    date.getSeconds() === 0 &&
    date.getMilliseconds() === 0
  );
}

type Accumulator = {
  criteria: Record<string, unknown>;
  residue: SearchQuery[];
};

/**
 * Set `key` on the accumulator, or push `fallback` to the residue when the key
 * is already taken. Two branches of an `and` that constrain the same IMAP key
 * cannot both reach the server in one SEARCH.
 */
function setOrDefer(
  acc: Accumulator,
  key: string,
  value: unknown,
  fallback: SearchQuery,
): void {
  if (key in acc.criteria) {
    acc.residue.push(fallback);
    return;
  }
  acc.criteria[key] = value;
}

function translateInto(query: SearchQuery, acc: Accumulator): void {
  if (query.from !== undefined) {
    setOrDefer(acc, "from", query.from, { from: query.from });
  }
  if (query.to !== undefined) {
    setOrDefer(acc, "to", query.to, { to: query.to });
  }
  if (query.cc !== undefined) {
    setOrDefer(acc, "cc", query.cc, { cc: query.cc });
  }
  if (query.bcc !== undefined) {
    setOrDefer(acc, "bcc", query.bcc, { bcc: query.bcc });
  }
  if (query.body !== undefined) {
    setOrDefer(acc, "body", query.body, { body: query.body });
  }
  if (query.text !== undefined) {
    setOrDefer(acc, "text", query.text, { text: query.text });
  }
  if (query.largerThan !== undefined) {
    setOrDefer(acc, "larger", query.largerThan, {
      largerThan: query.largerThan,
    });
  }
  if (query.smallerThan !== undefined) {
    setOrDefer(acc, "smaller", query.smallerThan, {
      smallerThan: query.smallerThan,
    });
  }

  if (query.header !== undefined) {
    const header = query.header;
    setOrDefer(acc, "header", { [header.field]: header.contains }, { header });
  }

  // Date keys. `on`/`sentOn` are date-granular on both sides, so they translate
  // exactly. `before`/`after` are instant-valued here and date-valued in IMAP,
  // so a non-midnight bound keeps a local re-check.
  const dateKeys = [
    ["before", "before", "before"],
    ["after", "since", "after"],
    ["on", "on", "on"],
    ["sentBefore", "sentBefore", "sentBefore"],
    ["sentAfter", "sentSince", "sentAfter"],
    ["sentOn", "sentOn", "sentOn"],
  ] as const;
  for (const [field, imapKey, residueKey] of dateKeys) {
    const value = query[field];
    if (value === undefined) continue;
    setOrDefer(acc, imapKey, value, { [residueKey]: value });
    const exact = residueKey === "on" || residueKey === "sentOn";
    if (!exact && !isMidnight(value)) {
      acc.residue.push({ [residueKey]: value });
    }
  }

  // Flags. System flags each have their own boolean key, so every one of them
  // reaches the server. Custom keywords share a single KEYWORD key, so only the
  // first in each direction does.
  for (const [field, keywordKey, booleanValue] of [
    ["hasFlags", "keyword", true],
    ["missingFlags", "unKeyword", false],
  ] as const) {
    const flags = query[field];
    if (flags === undefined) continue;
    const deferred: string[] = [];
    for (const flag of flags) {
      const systemKey = SYSTEM_FLAG_KEYS[flag];
      if (systemKey !== undefined) {
        setOrDefer(acc, systemKey, booleanValue, { [field]: [flag] });
        continue;
      }
      if (keywordKey in acc.criteria) {
        deferred.push(flag);
        continue;
      }
      acc.criteria[keywordKey] = flag;
    }
    if (deferred.length > 0) acc.residue.push({ [field]: deferred });
  }

  if (query.not !== undefined) {
    const inner = translate(query.not);
    if (inner.residue.length > 0) {
      // The negated branch did not translate completely, so negating the
      // partial criteria would be wrong -- NOT of a superset is a subset, which
      // would drop true matches. Defer the whole branch instead.
      acc.residue.push({ not: query.not });
    } else {
      setOrDefer(acc, "not", inner.criteria, { not: query.not });
    }
  }

  if (query.or !== undefined && query.or.length > 0) {
    const branches = query.or.map(translate);
    if (branches.some((b) => b.residue.length > 0)) {
      // A partially-translated branch would narrow the union, so the whole
      // disjunction is re-checked locally.
      acc.residue.push({ or: query.or });
    } else {
      setOrDefer(
        acc,
        "or",
        branches.map((b) => b.criteria),
        {
          or: query.or,
        },
      );
    }
  }

  if (query.and !== undefined) {
    for (const branch of query.and) translateInto(branch, acc);
  }
}

/**
 * Translate a `SearchQuery` into the SEARCH the server is asked, plus whatever
 * has to be re-checked locally. The returned `criteria` always match a superset
 * of the query, so the caller narrows with `residue` and never widens.
 */
export function translate(query: SearchQuery): TranslatedSearch {
  const acc: Accumulator = { criteria: {}, residue: [] };
  translateInto(query, acc);
  // `all: true` keeps an empty query a legal SEARCH rather than a syntax error.
  if (Object.keys(acc.criteria).length === 0) acc.criteria.all = true;
  return { criteria: acc.criteria as SearchObject, residue: acc.residue };
}
