import { describe, test, expect } from "bun:test";
import {
  createInMemoryMailboxStore,
  executeSearch,
  fetchHeaders,
  fetchStructure,
  fetchPart,
  type MailboxStore,
  type StoredEnvelope,
} from "./index";

const encoder = new TextEncoder();

function envelopeFor(overrides: Partial<StoredEnvelope> = {}): StoredEnvelope {
  return {
    messageId: "<1@x>",
    from: "alice@x",
    to: ["bob@y"],
    subject: "Hello",
    date: new Date("2026-01-01T00:00:00Z"),
    inReplyTo: undefined,
    references: [],
    interchangeType: undefined,
    interchangeCorrelationId: undefined,
    ...overrides,
  };
}

/** A minimal single-part RFC 2822 message with the given subject and body. */
function rawMessage(subject: string, body: string): Uint8Array {
  return encoder.encode(
    [
      "From: alice@x",
      "To: bob@y",
      `Subject: ${subject}`,
      "Message-ID: <1@x>",
      "Date: Thu, 01 Jan 2026 00:00:00 +0000",
      "Content-Type: text/plain",
      "",
      body,
    ].join("\r\n"),
  );
}

/** A two-part multipart/mixed message; part 1 is a text/plain body. */
function rawMultipart(body: string): Uint8Array {
  const boundary = "b0undary";
  return encoder.encode(
    [
      "From: alice@x",
      "To: bob@y",
      "Subject: Multipart",
      "Message-ID: <1@x>",
      "Date: Thu, 01 Jan 2026 00:00:00 +0000",
      `Content-Type: multipart/mixed; boundary="${boundary}"`,
      "",
      `--${boundary}`,
      "Content-Type: text/plain",
      "",
      body,
      `--${boundary}--`,
      "",
    ].join("\r\n"),
  );
}

/**
 * Wrap a store so every `readRaw` is counted. Proves the pure functions read
 * raw only when a projection or predicate needs the bytes.
 */
function countingStore(inner: MailboxStore): {
  store: MailboxStore;
  readRawCount: () => number;
} {
  let count = 0;
  const store: MailboxStore = {
    get uidValidity() {
      return inner.uidValidity;
    },
    get uidNext() {
      return inner.uidNext;
    },
    get highestModSeq() {
      return inner.highestModSeq;
    },
    get messages() {
      return inner.messages;
    },
    append: (raw, envelope, flags) => inner.append(raw, envelope, flags),
    readRaw: (uid) => {
      count++;
      return inner.readRaw(uid);
    },
    find: (uid) => inner.find(uid),
    addFlags: (uid, flags) => inner.addFlags(uid, flags),
    removeFlags: (uid, flags) => inner.removeFlags(uid, flags),
    remove: (uid) => inner.remove(uid),
  };
  return { store, readRawCount: () => count };
}

describe("in-memory readRaw", () => {
  test("returns the appended bytes and the model carries no raw", async () => {
    const store = createInMemoryMailboxStore();
    const raw = rawMessage("Hello", "body text");
    const uid = store.append(raw, envelopeFor(), []);

    expect(await store.readRaw(uid)).toEqual(raw);
    // The resident message model never carries the raw bytes.
    expect("raw" in (store.find(uid) ?? {})).toBe(false);
  });

  test("throws for an absent uid", async () => {
    const store = createInMemoryMailboxStore();
    await expect(store.readRaw(9999)).rejects.toThrow(/not found/);
  });

  test("drops the bytes on remove", async () => {
    const store = createInMemoryMailboxStore();
    const uid = store.append(rawMessage("Hello", "b"), envelopeFor(), []);
    store.remove(uid);
    await expect(store.readRaw(uid)).rejects.toThrow(/not found/);
  });
});

describe("executeSearch reads raw only when a predicate needs it", () => {
  test("envelope and flag predicates never read raw", async () => {
    const { store, readRawCount } = countingStore(createInMemoryMailboxStore());
    store.append(rawMessage("Hello", "body text"), envelopeFor(), ["\\Seen"]);

    const byFrom = await executeSearch("INBOX", store, { from: "alice" });
    const byFlag = await executeSearch("INBOX", store, {
      hasFlags: ["\\Seen"],
    });

    expect(byFrom).toHaveLength(1);
    expect(byFlag).toHaveLength(1);
    expect(readRawCount()).toBe(0);
  });

  test("a header predicate reads raw, memoized once per message", async () => {
    const { store, readRawCount } = countingStore(createInMemoryMailboxStore());
    store.append(rawMessage("Hello", "body text"), envelopeFor(), []);

    const bySubject = await executeSearch("INBOX", store, {
      header: { field: "Subject", contains: "hello" },
    });
    expect(bySubject).toHaveLength(1);
    expect(readRawCount()).toBe(1);

    // Two raw-scanning predicates over one message still read the blob once.
    const combined = await executeSearch("INBOX", store, {
      and: [
        { header: { field: "Subject", contains: "hello" } },
        { text: "body" },
      ],
    });
    expect(combined).toHaveLength(1);
    // One additional read for the single candidate, memoized across both subs.
    expect(readRawCount()).toBe(2);
  });

  test("a body/text predicate reads raw and matches on content", async () => {
    const store = createInMemoryMailboxStore();
    store.append(rawMessage("Hello", "the needle is here"), envelopeFor(), []);

    const hit = await executeSearch("INBOX", store, { text: "needle" });
    const miss = await executeSearch("INBOX", store, { text: "haystack" });
    expect(hit).toHaveLength(1);
    expect(miss).toHaveLength(0);
  });
});

describe("async fetch projections route through readRaw", () => {
  test("fetchHeaders parses the full header set from raw", async () => {
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      rawMessage("Subject Line", "b"),
      envelopeFor(),
      [],
    );

    const headers = await fetchHeaders({ uid, mailbox: "INBOX" }, store);
    expect(headers.from).toBe("alice@x");
    expect(headers.to).toContain("bob@y");
    expect(headers.subject).toBe("Subject Line");
  });

  test("fetchStructure describes a single text part", async () => {
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      rawMessage("Hello", "part body"),
      envelopeFor(),
      [],
    );

    const structure = await fetchStructure({ uid, mailbox: "INBOX" }, store);
    expect(structure.contentType).toBe("text/plain");
  });

  test("fetchStructure and fetchPart read a multipart body", async () => {
    const store = createInMemoryMailboxStore();
    const uid = store.append(rawMultipart("part body"), envelopeFor(), []);

    const structure = await fetchStructure({ uid, mailbox: "INBOX" }, store);
    expect(structure.contentType).toContain("multipart/mixed");

    const part = await fetchPart({ uid, mailbox: "INBOX" }, "1", store);
    expect(new TextDecoder().decode(part.content)).toContain("part body");
  });

  test("fetch projections reject an absent uid", async () => {
    const store = createInMemoryMailboxStore();
    await expect(
      fetchHeaders({ uid: 42, mailbox: "INBOX" }, store),
    ).rejects.toThrow(/not found/);
  });
});
