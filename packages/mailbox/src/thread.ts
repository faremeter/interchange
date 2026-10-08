/* eslint-disable @typescript-eslint/no-non-null-assertion -- Map.get()! after has() checks in threading algorithm */
import type { Thread, SearchQuery } from "@intx/types/runtime";
import type { MailboxStore, StoredMessage } from "./mailbox";
import { executeSearch } from "./search";

/**
 * RFC 5256 REFERENCES threading algorithm: build parent-child relationships
 * from In-Reply-To and References headers. Only the parent/child linking
 * portion is implemented; subject-based gathering (RFC 5256 step 5) is not
 * needed by this transport. RFC 5256 also defines an ORDEREDSUBJECT variant
 * that sorts by subject and date without reference tracking.
 */

type Container = {
  messageId: string;
  message: StoredMessage | null;
  parent: Container | null;
  children: Container[];
};

export async function executeThread(
  mailboxName: string,
  store: MailboxStore,
  algorithm: "references" | "orderedsubject",
  query?: SearchQuery,
): Promise<Thread[]> {
  let messages: StoredMessage[];

  if (query !== undefined) {
    const refs = await executeSearch(mailboxName, store, query);
    const uidSet = new Set(refs.map((r) => r.uid));
    messages = store.messages.filter((m) => uidSet.has(m.uid));
  } else {
    messages = [...store.messages];
  }

  if (messages.length === 0) return [];

  if (algorithm === "orderedsubject") {
    return orderedSubjectThread(mailboxName, messages);
  }

  return referencesThread(mailboxName, messages);
}

/**
 * RFC 5256 ORDEREDSUBJECT: sort by base subject, then date. The earliest
 * message in each subject group is the root; the rest are direct children.
 */
function orderedSubjectThread(
  mailboxName: string,
  messages: StoredMessage[],
): Thread[] {
  const bySubject = new Map<string, StoredMessage[]>();

  for (const msg of messages) {
    const base = baseSubject(msg.envelope.subject);
    const bucket = bySubject.get(base);
    if (bucket === undefined) {
      bySubject.set(base, [msg]);
    } else {
      bucket.push(msg);
    }
  }

  const threads: Thread[] = [];
  for (const [, msgs] of bySubject) {
    const sorted = msgs.sort((a, b) => messageDate(a) - messageDate(b));
    const root = sorted[0]!;
    const rootThread: Thread = {
      ref: { uid: root.uid, mailbox: mailboxName },
      children: sorted.slice(1).map((m) => ({
        ref: { uid: m.uid, mailbox: mailboxName },
        children: [],
      })),
    };
    threads.push(rootThread);
  }

  return threads.sort((a, b) => {
    const aMsg = messages.find((m) => m.uid === a.ref.uid)!;
    const bMsg = messages.find((m) => m.uid === b.ref.uid)!;
    return messageDate(aMsg) - messageDate(bMsg);
  });
}

/**
 * RFC 5256 REFERENCES algorithm: create one container per message, link
 * containers by References/In-Reply-To chains, prune dummies, and sort by
 * date.
 */
function referencesThread(
  mailboxName: string,
  messages: StoredMessage[],
): Thread[] {
  const idTable = new Map<string, Container>();

  function getOrCreate(msgId: string): Container {
    const existing = idTable.get(msgId);
    if (existing !== undefined) return existing;
    const c: Container = {
      messageId: msgId,
      message: null,
      parent: null,
      children: [],
    };
    idTable.set(msgId, c);
    return c;
  }

  // Build containers and link parent-child relationships.
  for (const msg of messages) {
    const container = getOrCreate(msg.envelope.messageId);
    container.message = msg;

    // References + In-Reply-To, deduplicated.
    const refs = buildRefList(msg.envelope.references, msg.envelope.inReplyTo);

    // Link: refs[i] is the parent of refs[i+1]; the last ref is the parent of
    // this message.
    let prevContainer: Container | null = null;
    for (const refId of refs) {
      const refContainer = getOrCreate(refId);

      if (
        prevContainer !== null &&
        refContainer.parent === null &&
        !isAncestor(refContainer, prevContainer)
      ) {
        prevContainer.children.push(refContainer);
        refContainer.parent = prevContainer;
      }

      prevContainer = refContainer;
    }

    // Link the last reference as parent of this message if that creates no
    // cycle.
    if (
      prevContainer !== null &&
      container.parent === null &&
      !isAncestor(container, prevContainer)
    ) {
      prevContainer.children.push(container);
      container.parent = prevContainer;
    }
  }

  // Find root containers (no parent).
  const roots: Container[] = [];
  for (const [, c] of idTable) {
    if (c.parent === null) {
      roots.push(c);
    }
  }

  // Prune dummy containers (containers with no message): drop a childless
  // dummy; promote the children of one that has children.
  const prunedRoots = pruneContainers(roots);

  // Sort and convert to Thread[].
  return containersToThreads(mailboxName, prunedRoots);
}

function buildRefList(references: string[], inReplyTo?: string): string[] {
  const seen = new Set<string>();
  const result: string[] = [];

  for (const ref of references) {
    if (ref && !seen.has(ref)) {
      seen.add(ref);
      result.push(ref);
    }
  }

  if (inReplyTo !== undefined && !seen.has(inReplyTo)) {
    result.push(inReplyTo);
  }

  return result;
}

function isAncestor(potentialAncestor: Container, of: Container): boolean {
  let cur: Container | null = of;
  while (cur !== null) {
    if (cur === potentialAncestor) return true;
    cur = cur.parent;
  }
  return false;
}

function pruneContainers(containers: Container[]): Container[] {
  const result: Container[] = [];
  for (const c of containers) {
    if (c.message === null && c.children.length === 0) {
      // Dummy with no children: drop it.
      continue;
    }
    if (c.message === null && c.children.length > 0) {
      // Dummy with children: promote children (skip the dummy).
      const promotedChildren = pruneContainers(c.children);
      result.push(...promotedChildren);
    } else {
      // Real message: recurse into children.
      c.children = pruneContainers(c.children);
      result.push(c);
    }
  }
  return result;
}

/**
 * Sort key for a message that named no date. `Number.MAX_SAFE_INTEGER` exceeds
 * the largest time value a `Date` can hold (8.64e15), so undated messages sort
 * after every dated one. The epoch would hand the root of an ascending-sorted
 * thread to a message that placed itself nowhere in time.
 */
const UNDATED_SORT_KEY = Number.MAX_SAFE_INTEGER;

/** The sort key for a message's position in a thread (RFC 5256 orders by date). */
function messageDate(msg: StoredMessage): number {
  const date = msg.envelope.date;
  return date === undefined ? UNDATED_SORT_KEY : date.getTime();
}

function containerDate(c: Container): number {
  if (c.message !== null) {
    return messageDate(c.message);
  }
  // A dummy container borrows the earliest date among its descendants; with
  // none dated it keeps `UNDATED_SORT_KEY` and sorts last.
  let earliest = UNDATED_SORT_KEY;
  for (const child of c.children) {
    const d = containerDate(child);
    if (d < earliest) earliest = d;
  }
  return earliest;
}

function containersToThreads(
  mailboxName: string,
  containers: Container[],
): Thread[] {
  // Sort by date of the container (or earliest descendant for dummies).
  const sorted = containers.sort((a, b) => containerDate(a) - containerDate(b));

  return sorted
    .filter((c) => c.message !== null)
    .map((c) => ({
      ref: { uid: c.message!.uid, mailbox: mailboxName },
      children: containersToThreads(mailboxName, c.children),
    }));
}

function baseSubject(subject: string): string {
  // Strip "Re:", "Fwd:", "Fw:" prefixes (case-insensitive) repeatedly.
  let s = subject.trim();
  let changed = true;
  while (changed) {
    changed = false;
    const m = s.match(/^(?:re|fwd?)\s*:\s*/i);
    if (m !== null) {
      s = s.slice(m[0].length).trim();
      changed = true;
    }
  }
  return s;
}
