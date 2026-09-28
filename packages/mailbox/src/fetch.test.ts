import { describe, test, expect } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  decodeMail,
  type MessageHeaders,
} from "@intx/mime";
import type { CryptoProvider } from "@intx/types/runtime";
import {
  createInMemoryMailboxStore,
  executeSearch,
  fetchHeaders,
  fetchStructure,
  fetchPart,
  fetchFull,
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

function signedHeaders(): MessageHeaders {
  return {
    from: "alice@x",
    to: ["bob@y"],
    cc: undefined,
    date: new Date("2026-01-01T00:00:00Z"),
    messageId: "<1@x>",
    subject: "Hello",
    inReplyTo: undefined,
    references: undefined,
    mimeVersion: "1.0",
    interchangeType: "conversation.message",
    interchangeCorrelationId: undefined,
    interchangeTenantId: undefined,
    interchangeAgentId: undefined,
    interchangeSessionId: undefined,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
  };
}

/** A `multipart/signed` conversation message validly signed by `crypto`. */
async function signedRawMessage(crypto: CryptoProvider): Promise<Uint8Array> {
  const content = assembleSignedContent({
    kind: "conversation",
    text: "signed body",
  });
  const signature = await createDetachedSignatureFromProvider(content, crypto);
  return assembleMessage(signedHeaders(), content, signature);
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
 * A multipart/mixed message whose single part declares `encoding`. The part
 * declares a bare `text/plain` with no parameters, so the type `fetchPart`
 * reports (which keeps any parameters) and the type `decodeMail` reports
 * (which drops them) are directly comparable.
 */
function rawMultipartWithEncoding(encoding: string, body: string): Uint8Array {
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
      `Content-Transfer-Encoding: ${encoding}`,
      "",
      body,
      `--${boundary}--`,
      "",
    ].join("\r\n"),
  );
}

/** A single-part message whose own headers declare `encoding`. */
function rawMessageWithEncoding(encoding: string, body: string): Uint8Array {
  return encoder.encode(
    [
      "From: alice@x",
      "To: bob@y",
      "Subject: Encoded",
      "Message-ID: <1@x>",
      "Date: Thu, 01 Jan 2026 00:00:00 +0000",
      "Content-Type: text/plain",
      `Content-Transfer-Encoding: ${encoding}`,
      "",
      body,
    ].join("\r\n"),
  );
}

/**
 * A `multipart/signed` message built by hand, so a test can choose the transfer
 * encoding of the signed content. The assembler only ever emits `7bit`, and the
 * shape under test is the one a peer's mail client produces. `signedPart` is the
 * whole signed content part, headers included.
 *
 * The signature part is a placeholder: `fetchFull` resolves no key for this
 * sender in these tests, so it reports `unknown` without reading it.
 */
function handBuiltSigned(
  signedPart: string[],
  interchangeType = "conversation.message",
): Uint8Array {
  return encoder.encode(
    [
      "From: alice@x",
      "To: bob@y",
      "Subject: Encoded",
      "Message-ID: <1@x>",
      "Date: Thu, 01 Jan 2026 00:00:00 +0000",
      `Interchange-Type: ${interchangeType}`,
      'Content-Type: multipart/signed; protocol="application/pgp-signature"; ' +
        'micalg=pgp-sha512; boundary="outer"',
      "",
      "--outer",
      ...signedPart,
      "--outer",
      "Content-Type: application/pgp-signature",
      "",
      "placeholder-signature",
      "--outer--",
      "",
    ].join("\r\n"),
  );
}

/** The conversation shape: a multipart/mixed whose part 1.1 is the text body. */
function signedConversationWithEncoding(
  encoding: string,
  body: string,
): Uint8Array {
  return handBuiltSigned([
    'Content-Type: multipart/mixed; boundary="inner"',
    "",
    "--inner",
    "Content-Type: text/plain; charset=utf-8",
    `Content-Transfer-Encoding: ${encoding}`,
    "",
    body,
    "--inner--",
    "",
  ]);
}

/**
 * A signed content part that is a bare text/plain part with no multipart/mixed
 * wrapper -- the shape a plain mail client signs.
 */
function signedBareTextWithEncoding(
  encoding: string,
  body: string,
): Uint8Array {
  return handBuiltSigned([
    "Content-Type: text/plain; charset=utf-8",
    `Content-Transfer-Encoding: ${encoding}`,
    "",
    body,
  ]);
}

/** The structured shape: the JSON payload at part 1.1. */
function signedStructuredWithEncoding(
  encoding: string,
  body: string,
): Uint8Array {
  return handBuiltSigned(
    [
      'Content-Type: multipart/mixed; boundary="inner"',
      "",
      "--inner",
      "Content-Type: application/vnd.interchange+json; charset=utf-8",
      `Content-Transfer-Encoding: ${encoding}`,
      "",
      body,
      "--inner--",
      "",
    ],
    "offering.catalog",
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

  // A run of CFWS between two lexical tokens is semantically a single space
  // (RFC 2822 section 3.2.3), so a comment separates the tokens either side.
  // `ba(c)se64` is therefore the two tokens `ba` and `se64` and names no
  // mechanism, where a reading that deletes the comment sees `base64`. Each
  // row is a message read two ways -- this projection and the whole-message
  // decode -- and the point is that both ways give one answer, because a
  // sender that can pick which reading a consumer gets can smuggle content
  // past whichever one inspects it.
  const transferEncodingCases: {
    declared: string;
    body: string;
    content: string;
    contentType: string;
    encoding: string | undefined;
  }[] = [
    {
      declared: "base64",
      body: "aGVsbG8=",
      content: "hello",
      contentType: "text/plain",
      encoding: "base64",
    },
    {
      declared: "ba(c)se64",
      body: "aGVsbG8=",
      content: "aGVsbG8=",
      contentType: "application/octet-stream",
      encoding: "ba se64",
    },
    {
      declared: "base(x)64",
      body: "aGVsbG8=",
      content: "aGVsbG8=",
      contentType: "application/octet-stream",
      encoding: "base 64",
    },
    {
      declared: "b(1)a(2)s(3)e(4)6(5)4",
      body: "aGVsbG8=",
      content: "aGVsbG8=",
      contentType: "application/octet-stream",
      encoding: "b a s e 6 4",
    },
    {
      declared: "7bit (default)",
      body: "hello",
      content: "hello",
      contentType: "text/plain",
      encoding: undefined,
    },
    {
      declared: "(just a comment)",
      body: "hello",
      content: "hello",
      contentType: "text/plain",
      encoding: undefined,
    },
  ];

  test("fetchPart reads a transfer encoding as the whole-message decode does", async () => {
    for (const c of transferEncodingCases) {
      const raw = rawMultipartWithEncoding(c.declared, c.body);
      const store = createInMemoryMailboxStore();
      const uid = store.append(raw, envelopeFor(), []);

      const part = await fetchPart({ uid, mailbox: "INBOX" }, "1", store);
      expect(new TextDecoder().decode(part.content)).toBe(c.content);
      expect(part.contentType).toBe(c.contentType);
      expect(part.encoding).toBe(c.encoding);

      // Pinning both sides to the same row proves the expectation, not just
      // that two readers are wrong together.
      const whole = decodeMail(raw).parts[0];
      expect(new TextDecoder().decode(whole?.content)).toBe(c.content);
      expect(whole?.contentType).toBe(c.contentType);
    }
  });

  test("fetch projections reject an absent uid", async () => {
    const store = createInMemoryMailboxStore();
    await expect(
      fetchHeaders({ uid: 42, mailbox: "INBOX" }, store),
    ).rejects.toThrow(/not found/);
  });

  test("fetchFull verifies a signed message against its sender's key", async () => {
    // The positive control for the no-originator case below: this fixture
    // does verify, so an `unknown` there is the guard's doing and not a
    // fixture that could never have verified.
    const crypto = createEd25519Crypto(await generateKeyPair());
    const store = createInMemoryMailboxStore();
    const uid = store.append(await signedRawMessage(crypto), envelopeFor(), []);

    const full = await fetchFull(
      { uid, mailbox: "INBOX" },
      store,
      () => crypto,
    );
    expect(full.signatureStatus).toBe("valid");
  });

  // `fetchFull` reads the same `Content-Transfer-Encoding` the whole-message
  // decoders read. Each row is a body whose declared mechanism has to be undone
  // before the bytes are the message's text; reading the part bytes directly
  // would hand a caller the source form of the encoding.
  const contentEncodingCases: { encoding: string; body: string }[] = [
    { encoding: "base64", body: "aGVsbG8gd29ybGQ=" },
    { encoding: "quoted-printable", body: "hello=20world" },
    { encoding: "7bit", body: "hello world" },
    // RFC 2045 section 1: a comment carries no meaning, so this names base64.
    { encoding: "base64 (RFC 2045)", body: "aGVsbG8gd29ybGQ=" },
  ];

  test("fetchFull undoes the conversation body's transfer encoding", async () => {
    for (const c of contentEncodingCases) {
      const store = createInMemoryMailboxStore();
      const uid = store.append(
        signedConversationWithEncoding(c.encoding, c.body),
        envelopeFor(),
        [],
      );
      const full = await fetchFull(
        { uid, mailbox: "INBOX" },
        store,
        () => undefined,
      );
      expect(full.content).toBe("hello world");
    }
  });

  test("fetchFull undoes a bare signed text part's transfer encoding", async () => {
    for (const c of contentEncodingCases) {
      const store = createInMemoryMailboxStore();
      const uid = store.append(
        signedBareTextWithEncoding(c.encoding, c.body),
        envelopeFor(),
        [],
      );
      const full = await fetchFull(
        { uid, mailbox: "INBOX" },
        store,
        () => undefined,
      );
      expect(full.content).toBe("hello world");
    }
  });

  test("fetchFull undoes a structured payload's transfer encoding", async () => {
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      signedStructuredWithEncoding(
        "base64",
        "eyJ0eXBlIjoib2ZmZXJpbmcuY2F0YWxvZyIsInZlcnNpb24iOiIxIiwiYm9keSI6eyJrIjoxfX0=",
      ),
      envelopeFor({ interchangeType: "offering.catalog" }),
      [],
    );
    const full = await fetchFull(
      { uid, mailbox: "INBOX" },
      store,
      () => undefined,
    );
    expect(full.payload?.type).toBe("offering.catalog");
    expect(full.payload?.body).toEqual({ k: 1 });
  });

  test("fetchFull reads a non-multipart message's own body as its content", async () => {
    // An ordinary unsigned email is a single part, so it has no part 1 to index
    // into and the message is its own leaf part. Returning it with no content
    // would drop the body of every sender that writes this shape.
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      rawMessage("Hello", "the whole body"),
      envelopeFor(),
      [],
    );
    const full = await fetchFull(
      { uid, mailbox: "INBOX" },
      store,
      () => undefined,
    );
    expect(full.content).toBe("the whole body");
    expect(full.signatureStatus).toBe("unknown");
  });

  test("fetchFull undoes a non-multipart message's transfer encoding", async () => {
    // The message's own headers are the part's headers on this shape, so the
    // mechanism it declares is the one that applies to its body.
    for (const c of contentEncodingCases) {
      const store = createInMemoryMailboxStore();
      const uid = store.append(
        rawMessageWithEncoding(c.encoding, c.body),
        envelopeFor(),
        [],
      );
      const full = await fetchFull(
        { uid, mailbox: "INBOX" },
        store,
        () => undefined,
      );
      expect(full.content).toBe("hello world");
    }
  });

  test("fetchFull propagates a sender whose getPublicKey throws", async () => {
    // A CryptoProvider that cannot produce its own public key is a local
    // fault, not a bad signature: the error surfaces rather than being
    // masked as a signature status. The key is resolved for every inbound
    // from a known sender, so even this non-signed message reaches it.
    const store = createInMemoryMailboxStore();
    const uid = store.append(rawMessage("Hello", "b"), envelopeFor(), []);

    const brokenSender: CryptoProvider = {
      sign: () => Promise.reject(new Error("unused")),
      signSSH: () => Promise.reject(new Error("unused")),
      verify: () => Promise.resolve(false),
      getPublicKey: () => {
        throw new Error("no public key");
      },
    };

    await expect(
      fetchFull({ uid, mailbox: "INBOX" }, store, () => brokenSender),
    ).rejects.toThrow(/no public key/);
  });
});
