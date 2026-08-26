/* eslint-disable @typescript-eslint/no-non-null-assertion -- Map.get()! after has() checks in threading algorithm */
import type { Thread, SearchQuery } from "@intx/types/runtime";
import type { MailboxStore, StoredMessage } from "./mailbox";
import { executeSearch } from "./search";

/**
 * RFC 5256 REFERENCES threading algorithm.
 *
 * Builds parent-child relationships from In-Reply-To and References headers.
 * The algorithm:
 * 1. For each message, collect its References chain (oldest → newest ancestor).
 * 2. Link messages into a tree using these chains.
 * 3. Create dummy containers for referenced messages not present in the set.
 * 4. Prune dummy containers with no children; promote children of childless dummies.
 * 5. Gather root-level containers with the same base subject (skipped here —
 *    we implement only the parent/child linking portion which is what this
 *    transport needs; subject-based gathering is optional for our use case).
 * 6. Sort threads at each level.
 *
 * Note: RFC 5256 also defines an ORDEREDSUBJECT algorithm. For that, messages
 * are sorted by subject and date without reference tracking.
 */

type Container = {
  messageId: string;
  message: StoredMessage | null;
  parent: Container | null;
  children: Container[];
};

export function executeThread(
  mailboxName: string,
  store: MailboxStore,
  algorithm: "references" | "orderedsubject",
  query?: SearchQuery,
): Thread[] {
  let messages: StoredMessage[];

  if (query !== undefined) {
    const refs = executeSearch(mailboxName, store, query);
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
 * RFC 5256 ORDEREDSUBJECT: sort by base subject, then date.
 * All messages with the same base subject form one thread; the first by date
 * is the root, the rest are direct children.
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
    const sorted = msgs.sort(
      (a, b) => a.envelope.date.getTime() - b.envelope.date.getTime(),
    );
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
    return aMsg.envelope.date.getTime() - bMsg.envelope.date.getTime();
  });
}

/**
 * RFC 5256 REFERENCES algorithm.
 *
 * Step 1: For each message, create a container. Walk its References list
 *   (and In-Reply-To if not already in References) and link containers
 *   as parent-child in left-to-right order.
 *
 * Step 2: Build the id_table mapping Message-IDs to containers.
 *
 * Step 3: Prune empty containers (those with no message).
 *
 * Step 4: Collect root containers.
 *
 * Step 5: Sort each container's children by date.
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

  // Step 1 & 2: Build containers and link parent-child relationships.
  for (const msg of messages) {
    const container = getOrCreate(msg.envelope.messageId);
    container.message = msg;

    // Build the reference list: References + In-Reply-To (deduplicated).
    const refs = buildRefList(msg.envelope.references, msg.envelope.inReplyTo);

    // Link: refs[i] is parent of refs[i+1], last ref is parent of this message.
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

    // Link the last reference as parent of this message (if no circular reference).
    if (
      prevContainer !== null &&
      container.parent === null &&
      !isAncestor(container, prevContainer)
    ) {
      prevContainer.children.push(container);
      container.parent = prevContainer;
    }
  }

  // Step 3: Find root containers (no parent).
  const roots: Container[] = [];
  for (const [, c] of idTable) {
    if (c.parent === null) {
      roots.push(c);
    }
  }

  // Step 4: Prune dummy containers (containers with no message).
  // A dummy with no children is dropped.
  // A dummy with children: the children are promoted to the dummy's parent level.
  const prunedRoots = pruneContainers(roots);

  // Step 5: Sort and convert to Thread[].
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

  if (inReplyTo !== undefined && inReplyTo !== "" && !seen.has(inReplyTo)) {
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

function containerDate(c: Container): number {
  if (c.message !== null) {
    return c.message.envelope.date.getTime();
  }
  // For dummy containers, use the earliest child date.
  let earliest = Infinity;
  for (const child of c.children) {
    const d = containerDate(child);
    if (d < earliest) earliest = d;
  }
  return earliest === Infinity ? 0 : earliest;
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
