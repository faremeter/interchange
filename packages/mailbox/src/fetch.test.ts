import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  decodeMail,
  parseHeaderSection,
  type MessageHeaders,
} from "@intx/mime";
import type { CryptoProvider } from "@intx/types/runtime";
import {
  createInMemoryMailboxStore,
  executeSearch,
  executeThread,
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

/**
 * Drop the `From` line from a message's header section, leaving every other
 * byte alone. The detached signature covers the signed-content part inside the
 * body, not the outer headers, so a message stripped this way still verifies
 * -- which is what makes it a fixture for a message that carries no
 * originator and would otherwise have a valid signature.
 */
function stripFromHeader(raw: Uint8Array): Uint8Array {
  const { headerEnd } = parseHeaderSection(raw);
  const kept = new TextDecoder()
    .decode(raw.subarray(0, headerEnd))
    .split("\r\n")
    .filter((line) => !/^from:/i.test(line))
    .join("\r\n");
  const keptBytes = encoder.encode(kept);
  const rest = raw.subarray(headerEnd);
  const out = new Uint8Array(keptBytes.length + rest.length);
  out.set(keptBytes, 0);
  out.set(rest, keptBytes.length);
  return out;
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

/** Conversation mail whose text/plain attachment is quoted-printable `caf=E9`. */
function rawQuotedPrintableAttachment(): Uint8Array {
  return encoder.encode(
    [
      "From: alice@x",
      "To: bob@y",
      "Subject: QP",
      "Message-ID: <qp@x>",
      "Date: Thu, 01 Jan 2026 00:00:00 +0000",
      "Interchange-Type: conversation.message",
      `Content-Type: multipart/signed; protocol="application/pgp-signature"; micalg=pgp-sha512; boundary="outer"`,
      "",
      "--outer",
      `Content-Type: multipart/mixed; boundary="inner"`,
      "",
      "--inner",
      "Content-Type: text/plain; charset=utf-8",
      "Content-Transfer-Encoding: 7bit",
      "",
      "see attached",
      "--inner",
      "Content-Type: text/plain",
      "Content-Transfer-Encoding: quoted-printable",
      `Content-Disposition: attachment; filename="cafe.txt"`,
      "",
      "caf=E9",
      "--inner--",
      "--outer",
      "Content-Type: application/pgp-signature",
      "",
      "FAKE",
      "--outer--",
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

  test("fetchPart undoes quoted-printable the same way listing does", async () => {
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      rawQuotedPrintableAttachment(),
      envelopeFor({
        messageId: "<qp@x>",
        subject: "QP",
        interchangeType: "conversation.message",
      }),
      [],
    );
    const ref = { uid, mailbox: "INBOX" };
    const listed = await fetchFull(ref, store, () => undefined);
    const attachment = listed.attachments?.[0];
    if (attachment === undefined)
      throw new Error("expected a listed attachment");
    expect(Array.from(attachment.data)).toEqual([0x63, 0x61, 0x66, 0xe9]);

    const fetched = await fetchPart(ref, attachment.part ?? "1.2", store);
    expect(Array.from(fetched.content)).toEqual(Array.from(attachment.data));
  });

  test("fetchFull undoes the text body's transfer encoding like fetchPart", async () => {
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      encoder.encode(
        [
          "From: alice@x",
          "To: bob@y",
          "Subject: QP body",
          "Message-ID: <qpbody@x>",
          "Date: Thu, 01 Jan 2026 00:00:00 +0000",
          "Interchange-Type: conversation.message",
          `Content-Type: multipart/signed; protocol="application/pgp-signature"; micalg=pgp-sha512; boundary="outer"`,
          "",
          "--outer",
          `Content-Type: multipart/mixed; boundary="inner"`,
          "",
          "--inner",
          "Content-Type: text/plain; charset=utf-8",
          "Content-Transfer-Encoding: quoted-printable",
          "",
          "Bonjour, caf=C3=A9",
          "--inner--",
          "--outer",
          "Content-Type: application/pgp-signature",
          "",
          "FAKE",
          "--outer--",
          "",
        ].join("\r\n"),
      ),
      envelopeFor({
        messageId: "<qpbody@x>",
        interchangeType: "conversation.message",
      }),
      [],
    );
    const ref = { uid, mailbox: "INBOX" };
    const full = await fetchFull(ref, store, () => undefined);
    expect(full.content).toBe("Bonjour, café");

    const part = await fetchPart(ref, "1.1", store);
    expect(new TextDecoder().decode(part.content)).toBe("Bonjour, café");
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

  // A body that does not decode into text: RFC 2045 section 6.4 makes the
  // first opaque octets, and the second is base64 that will not decode.
  const undecodableBodyCases: { encoding: string; body: string }[] = [
    { encoding: "x-uuencode", body: "begin 644 x" },
    { encoding: "base64", body: "!!! not base64 !!!" },
  ];

  test("fetchFull delivers a message whose body it cannot decode", async () => {
    // Refusing the message is not a behaviour any mail client has: one that
    // cannot render a body still shows the message. `content` is text, so an
    // undecodable body is carried as an absent one rather than as invented
    // text; the octets remain reachable through `fetchPart`.
    for (const c of undecodableBodyCases) {
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
      expect(full.content).toBeUndefined();
      expect(full.payload).toBeUndefined();
      expect(full.headers.subject).toBe("Encoded");
      expect(full.headers.from).toBe("alice@x");
      expect(full.flags).toEqual([]);
      expect(full.signatureStatus).toBe("unknown");
      expect(full.ref).toEqual({ uid, mailbox: "INBOX" });
    }
  });

  test("fetchFull omits a structured payload it cannot decode", async () => {
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      signedStructuredWithEncoding("x-uuencode", "begin 644 x"),
      envelopeFor({ interchangeType: "offering.catalog" }),
      [],
    );
    const full = await fetchFull(
      { uid, mailbox: "INBOX" },
      store,
      () => undefined,
    );
    expect(full.payload).toBeUndefined();
    expect(full.content).toBeUndefined();
    expect(full.headers.interchangeType).toBe("offering.catalog");
  });

  test("fetchPart reports an undecodable body as application/octet-stream", async () => {
    // The two paths agree about the same bytes: what `fetchFull` declines to
    // call text, `fetchPart` hands back as octets under the section 6.4
    // relabel. A part path resolves against the signed content, so "1.1" is
    // the body part `fetchFull` reads.
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      signedConversationWithEncoding("x-uuencode", "begin 644 x"),
      envelopeFor(),
      [],
    );
    const ref = { uid, mailbox: "INBOX" };

    expect(
      (await fetchFull(ref, store, () => undefined)).content,
    ).toBeUndefined();

    const part = await fetchPart(ref, "1.1", store);
    expect(part.contentType).toBe("application/octet-stream");
    expect(part.encoding).toBe("x-uuencode");
    expect(new TextDecoder().decode(part.content)).toBe("begin 644 x");
  });

  test("fetchFull still refuses a structured payload that is not valid JSON", async () => {
    // A decode failure and a payload failure are different conditions: here the
    // bytes decoded and the sender's JSON is wrong, which is a fault to surface
    // rather than a body this transport cannot read.
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      signedStructuredWithEncoding("7bit", "{ not json"),
      envelopeFor({ interchangeType: "offering.catalog" }),
      [],
    );
    await expect(
      fetchFull({ uid, mailbox: "INBOX" }, store, () => undefined),
    ).rejects.toThrow();
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

describe("a message that carries no originator", () => {
  test("matches no sender query, including the empty one", async () => {
    // There is no sender for the substring to be found in, so no `from`
    // query can match. The empty query is the assertion that matters: every
    // string contains "", so it catches a stand-in substituted for the
    // absent sender whatever address that stand-in names.
    const store = createInMemoryMailboxStore();
    const orphanUid = store.append(
      stripFromHeader(rawMessage("Hello", "body text")),
      envelopeFor({ from: undefined, messageId: "<orphan@x>" }),
      [],
    );
    const namedUid = store.append(
      rawMessage("Hello", "body text"),
      envelopeFor(),
      [],
    );

    const byEmpty = await executeSearch("INBOX", store, { from: "" });
    // The named message proves the empty query is a query that matches, so
    // the orphan's absence from the result is the guard and not a query that
    // matches nothing.
    expect(byEmpty.map((r) => r.uid)).toEqual([namedUid]);

    const byAddress = await executeSearch("INBOX", store, { from: "alice@x" });
    expect(byAddress.map((r) => r.uid)).toEqual([namedUid]);

    // Every other predicate still sees the orphan, so it is in the mailbox
    // and reachable -- it is the sender predicate alone that excludes it.
    const byRecipient = await executeSearch("INBOX", store, { to: "bob@y" });
    expect(byRecipient.map((r) => r.uid)).toEqual([orphanUid, namedUid]);
  });

  test("fetchFull reports its signature unknown without consulting the key lookup", async () => {
    // A message with no originator names no key to verify against. Asking
    // the lookup for a stand-in address would return whichever key that
    // address happens to have and verify the message against it, so the
    // lookup must not be asked at all.
    const crypto = createEd25519Crypto(await generateKeyPair());
    const store = createInMemoryMailboxStore();
    const raw = stripFromHeader(await signedRawMessage(crypto));
    const uid = store.append(raw, envelopeFor({ from: undefined }), []);

    const asked: string[] = [];
    // Answers with the signing key for any address, so a stand-in would
    // verify and report `valid`.
    const getCrypto = (fromAddress: string): CryptoProvider => {
      asked.push(fromAddress);
      return crypto;
    };

    const full = await fetchFull({ uid, mailbox: "INBOX" }, store, getCrypto);

    expect(full.headers.from).toBeUndefined();
    expect(full.signatureStatus).toBe("unknown");
    expect(asked).toEqual([]);
  });
});

describe("a message that carries no date", () => {
  test("matches no date window, in either direction", async () => {
    // Every date predicate asks where the sender placed the message in time. A
    // message that named no date placed itself nowhere, so each window has to
    // exclude it. The dated message in the same mailbox proves each query is
    // one that matches, so the undated message's absence is the guard rather
    // than a query matching nothing.
    const store = createInMemoryMailboxStore();
    const undatedUid = store.append(
      rawMessage("Hello", "body text"),
      envelopeFor({ date: undefined, messageId: "<undated@x>" }),
      [],
    );
    const datedUid = store.append(
      rawMessage("Hello", "body text"),
      envelopeFor(),
      [],
    );

    const on = new Date("2026-01-01T00:00:00Z");
    const before = new Date("2026-06-01T00:00:00Z");
    const after = new Date("2025-06-01T00:00:00Z");

    for (const query of [
      { on },
      { sentOn: on },
      { before },
      { sentBefore: before },
      { after },
      { sentAfter: after },
    ]) {
      const hits = await executeSearch("INBOX", store, query);
      expect(hits.map((r) => r.uid)).toEqual([datedUid]);
    }

    // Every other predicate still sees the undated message, so it is in the
    // mailbox and reachable -- the date predicates alone exclude it.
    const byRecipient = await executeSearch("INBOX", store, { to: "bob@y" });
    expect(byRecipient.map((r) => r.uid)).toEqual([undatedUid, datedUid]);
  });

  test("sorts behind a dated message in a thread rather than taking a date", async () => {
    // Threading orders by date, and an undated message supplies no key. It
    // sorts after every dated message, so the dated message keeps the root
    // slot instead of hanging off a message that placed itself nowhere in
    // time. The undated message is appended first, so append order cannot be
    // what puts it last.
    const store = createInMemoryMailboxStore();
    const undatedUid = store.append(
      rawMessage("Shared", "second"),
      envelopeFor({
        messageId: "<undated@x>",
        subject: "Shared",
        date: undefined,
      }),
      [],
    );
    const datedUid = store.append(
      rawMessage("Shared", "first"),
      envelopeFor({ messageId: "<dated@x>", subject: "Shared" }),
      [],
    );

    const threads = await executeThread("INBOX", store, "orderedsubject");

    expect(threads).toHaveLength(1);
    expect(threads[0]?.ref.uid).toBe(datedUid);
    expect(threads[0]?.children.map((c) => c.ref.uid)).toEqual([undatedUid]);
  });
});

describe("an envelope whose declared structure is not there", () => {
  // A sender chooses its own `Content-Type`, so it chooses whether the part
  // `fetchFull` reads can be found at all. These three shapes are the ones
  // that leave the projection with no content part, and it refuses the
  // message rather than inventing one. Refusing is not deleting: the caller
  // that reads the refusal owns what happens to the mail, and the INBOX watch
  // in `@intx/harness` keeps it.

  /** A `multipart/*` message whose declared boundary appears nowhere. */
  function boundaryDeclaredButAbsent(): Uint8Array {
    return encoder.encode(
      [
        "From: alice@x",
        "To: bob@y",
        "Subject: Encoded",
        "Message-ID: <1@x>",
        "Date: Thu, 01 Jan 2026 00:00:00 +0000",
        "Interchange-Type: conversation.message",
        'Content-Type: multipart/signed; protocol="application/pgp-signature"; ' +
          'micalg=pgp-sha512; boundary="outer"',
        "",
        "nothing here delimits a part",
        "",
      ].join("\r\n"),
    );
  }

  /** A `multipart/*` message that declares no `boundary` parameter. */
  function noBoundaryDeclared(): Uint8Array {
    return encoder.encode(
      [
        "From: alice@x",
        "To: bob@y",
        "Subject: Encoded",
        "Message-ID: <1@x>",
        "Date: Thu, 01 Jan 2026 00:00:00 +0000",
        "Interchange-Type: conversation.message",
        "Content-Type: multipart/signed",
        "",
        "body",
        "",
      ].join("\r\n"),
    );
  }

  /** A signed content part that declares `multipart/mixed` with no boundary. */
  function signedPartWithoutBoundary(): Uint8Array {
    return handBuiltSigned(["Content-Type: multipart/mixed", "", "orphan"]);
  }

  const malformed: { name: string; raw: () => Uint8Array }[] = [
    {
      name: "a declared boundary that appears nowhere",
      raw: boundaryDeclaredButAbsent,
    },
    { name: "a multipart type declaring no boundary", raw: noBoundaryDeclared },
    {
      name: "a signed content part that is multipart with no boundary",
      raw: signedPartWithoutBoundary,
    },
  ];

  for (const shape of malformed) {
    test(`fetchFull refuses ${shape.name}`, async () => {
      const store = createInMemoryMailboxStore();
      const uid = store.append(shape.raw(), envelopeFor(), []);

      await expect(
        fetchFull({ uid, mailbox: "INBOX" }, store, () => undefined),
      ).rejects.toThrow();

      // The refusal leaves the mailbox as it found it: the projections are
      // pure reads, so the bytes a caller may still want are all there.
      expect(store.find(uid)).toBeDefined();
      expect(await store.readRaw(uid)).toEqual(shape.raw());
    });
  }
});

describe("a body that will not decode reports itself", () => {
  // The default sink routes `warning` and above to `console.warn`, so spying
  // on it asserts what an operator actually sees rather than an internal
  // logger call. The development formatter colours every interpolated value,
  // so the escape sequences come off before anything is matched.
  const ansiEscape = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, "g");

  /* eslint-disable no-console -- intentional spy on console.warn */
  const warned: string[] = [];
  let originalWarn: typeof console.warn;

  beforeEach(() => {
    warned.length = 0;
    originalWarn = console.warn;
    console.warn = (...args: unknown[]) => {
      warned.push(
        args
          .map((a) => String(a))
          .join(" ")
          .replace(ansiEscape, ""),
      );
    };
  });

  afterEach(() => {
    console.warn = originalWarn;
  });
  /* eslint-enable no-console */

  test("fetchFull records the decode failure it delivers the message without", async () => {
    // Absent `content` is the whole of what the caller learns, and "not text"
    // and "would not decode" reach it as the same value. Without a record the
    // second is indistinguishable from the first and nobody learns the peer is
    // sending bodies its own declared encoding does not describe.
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      signedConversationWithEncoding("base64", "!!! not base64 !!!"),
      envelopeFor(),
      [],
    );

    const full = await fetchFull(
      { uid, mailbox: "INBOX" },
      store,
      () => undefined,
    );
    expect(full.content).toBeUndefined();

    // The record has to name the message and the encoding that failed, so an
    // operator can read the octets back through `fetchPart` and can tell which
    // peer to ask about.
    const record = warned.find((line) => line.includes("body did not decode"));
    expect(record).toBeDefined();
    expect(record).toContain(`uid=${String(uid)}`);
    expect(record).toContain("INBOX");
    expect(record).toContain("base64");
  });

  test("an unrecognized encoding is not reported as a decode failure", async () => {
    // The RFC 2045 section 6.4 relabel is the declared handling of an encoding
    // this transport does not implement, not a fault. Reporting it would cry
    // wolf on every opaque part a peer legitimately sends.
    const store = createInMemoryMailboxStore();
    const uid = store.append(
      signedConversationWithEncoding("x-uuencode", "begin 644 x"),
      envelopeFor(),
      [],
    );

    const full = await fetchFull(
      { uid, mailbox: "INBOX" },
      store,
      () => undefined,
    );
    expect(full.content).toBeUndefined();
    expect(
      warned.filter((line) => line.includes("body did not decode")),
    ).toEqual([]);
  });
});
