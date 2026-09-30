import { describe, test, expect } from "bun:test";
import {
  createInMemoryMailboxStore,
  executeThread,
  type MailboxStore,
  type StoredEnvelope,
} from "./index";

const encoder = new TextEncoder();

/**
 * Threading reads only the stored envelope, never the raw bytes, so every
 * message in this file shares the same placeholder body.
 */
const rawBody = encoder.encode("body");

function envelopeFor(
  overrides: Partial<StoredEnvelope> & { messageId: string },
): StoredEnvelope {
  return {
    from: "alice@x",
    to: ["bob@y"],
    subject: "Shared",
    date: undefined,
    inReplyTo: undefined,
    references: [],
    interchangeType: undefined,
    interchangeCorrelationId: undefined,
    ...overrides,
  };
}

function append(
  store: MailboxStore,
  overrides: Partial<StoredEnvelope> & { messageId: string },
): number {
  return store.append(rawBody, envelopeFor(overrides), []);
}

const jan1 = new Date("2026-01-01T00:00:00Z");
const jan2 = new Date("2026-01-02T00:00:00Z");
const jan3 = new Date("2026-01-03T00:00:00Z");

describe("orderedsubject threading", () => {
  test("roots the thread at the earliest dated message, not at an undated one", async () => {
    // A message that names no date says nothing about where it belongs in the
    // conversation, so it cannot claim the root slot from a message that does
    // name one. The undated message is appended between the two dated ones, so
    // neither append order nor a date decides its position by accident.
    const store = createInMemoryMailboxStore();
    const originalUid = append(store, {
      messageId: "<original@corp>",
      subject: "Invoice",
      date: jan1,
    });
    const undatedUid = append(store, {
      messageId: "<undated@elsewhere>",
      subject: "Re: Invoice",
    });
    const replyUid = append(store, {
      messageId: "<reply@corp>",
      subject: "Re: Invoice",
      date: jan2,
    });

    const threads = await executeThread("INBOX", store, "orderedsubject");

    expect(threads).toHaveLength(1);
    expect(threads[0]?.ref.uid).toBe(originalUid);
    expect(threads[0]?.children.map((c) => c.ref.uid)).toEqual([
      replyUid,
      undatedUid,
    ]);
  });

  test("orders a fully dated thread by date", async () => {
    const store = createInMemoryMailboxStore();
    const lastUid = append(store, {
      messageId: "<last@corp>",
      subject: "Re: Invoice",
      date: jan3,
    });
    const firstUid = append(store, {
      messageId: "<first@corp>",
      subject: "Invoice",
      date: jan1,
    });
    const middleUid = append(store, {
      messageId: "<middle@corp>",
      subject: "Re: Invoice",
      date: jan2,
    });

    const threads = await executeThread("INBOX", store, "orderedsubject");

    expect(threads).toHaveLength(1);
    expect(threads[0]?.ref.uid).toBe(firstUid);
    expect(threads[0]?.children.map((c) => c.ref.uid)).toEqual([
      middleUid,
      lastUid,
    ]);
  });

  test("orders a dated thread ahead of a thread that is entirely undated", async () => {
    const store = createInMemoryMailboxStore();
    const undatedFirstUid = append(store, {
      messageId: "<undated-1@elsewhere>",
      subject: "Nowhere",
    });
    const undatedSecondUid = append(store, {
      messageId: "<undated-2@elsewhere>",
      subject: "Re: Nowhere",
    });
    const datedUid = append(store, {
      messageId: "<dated@corp>",
      subject: "Invoice",
      date: jan1,
    });

    const threads = await executeThread("INBOX", store, "orderedsubject");

    expect(threads.map((t) => t.ref.uid)).toEqual([datedUid, undatedFirstUid]);
    // Undated messages share one sort key, so the stable sort leaves them in
    // the order the mailbox holds them.
    expect(threads[1]?.children.map((c) => c.ref.uid)).toEqual([
      undatedSecondUid,
    ]);
  });
});

describe("references threading", () => {
  test("sorts an undated top-level message after every dated one", async () => {
    const store = createInMemoryMailboxStore();
    const laterUid = append(store, { messageId: "<later@corp>", date: jan2 });
    const undatedUid = append(store, { messageId: "<undated@elsewhere>" });
    const earlierUid = append(store, {
      messageId: "<earlier@corp>",
      date: jan1,
    });

    const threads = await executeThread("INBOX", store, "references");

    expect(threads.map((t) => t.ref.uid)).toEqual([
      earlierUid,
      laterUid,
      undatedUid,
    ]);
  });

  test("sorts an undated reply after every dated sibling", async () => {
    const store = createInMemoryMailboxStore();
    const rootUid = append(store, { messageId: "<root@corp>", date: jan1 });
    const undatedReplyUid = append(store, {
      messageId: "<undated-reply@elsewhere>",
      inReplyTo: "<root@corp>",
    });
    const datedReplyUid = append(store, {
      messageId: "<dated-reply@corp>",
      date: jan2,
      inReplyTo: "<root@corp>",
    });

    const threads = await executeThread("INBOX", store, "references");

    expect(threads).toHaveLength(1);
    expect(threads[0]?.ref.uid).toBe(rootUid);
    expect(threads[0]?.children.map((c) => c.ref.uid)).toEqual([
      datedReplyUid,
      undatedReplyUid,
    ]);
  });

  test("orders fully dated threads by date", async () => {
    const store = createInMemoryMailboxStore();
    const laterUid = append(store, { messageId: "<later@corp>", date: jan3 });
    const earlierUid = append(store, {
      messageId: "<earlier@corp>",
      date: jan1,
    });
    const replyUid = append(store, {
      messageId: "<reply@corp>",
      date: jan2,
      inReplyTo: "<earlier@corp>",
    });

    const threads = await executeThread("INBOX", store, "references");

    expect(threads.map((t) => t.ref.uid)).toEqual([earlierUid, laterUid]);
    expect(threads[0]?.children.map((c) => c.ref.uid)).toEqual([replyUid]);
  });

  test("promotes children of an absent parent and still sorts the undated one last", async () => {
    // Nothing in the set carries `<absent@corp>`, so the container standing in
    // for it is dropped and its children rise to the top level, where the same
    // ordering rule applies to them.
    const store = createInMemoryMailboxStore();
    const undatedOrphanUid = append(store, {
      messageId: "<undated-orphan@elsewhere>",
      references: ["<absent@corp>"],
    });
    const datedOrphanUid = append(store, {
      messageId: "<dated-orphan@corp>",
      date: jan1,
      references: ["<absent@corp>"],
    });

    const threads = await executeThread("INBOX", store, "references");

    expect(threads.map((t) => t.ref.uid)).toEqual([
      datedOrphanUid,
      undatedOrphanUid,
    ]);
  });
});
