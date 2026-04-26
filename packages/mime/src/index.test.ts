import { describe, test, expect } from "bun:test";
import {
  generateKeyPair,
  createNodeCrypto,
  verifyDetachedSignature,
} from "@interchange/crypto-node";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  formatRFC2822Date,
  generateMessageId,
  parseHeaderSection,
  parseMimePart,
  parseMultipart,
  extractBoundary,
  extractPartByPath,
  parseMailToEmail,
  type MessageHeaders,
} from "./index";

const enc = new TextEncoder();
const dec = new TextDecoder();

function defined<T>(value: T | undefined | null): T {
  expect(value).toBeDefined();
  return value as T;
}

function makeHeaders(overrides?: Partial<MessageHeaders>): MessageHeaders {
  return {
    from: "alice@test.interchange",
    to: ["bob@test.interchange"],
    cc: undefined,
    date: new Date("2026-04-21T12:00:00Z"),
    messageId: "<test-1@test.interchange>",
    subject: undefined,
    inReplyTo: undefined,
    references: undefined,
    mimeVersion: "1.0",
    interchangeType: undefined,
    interchangeCorrelationId: undefined,
    interchangeTenantId: undefined,
    interchangeAgentId: undefined,
    interchangeSessionId: undefined,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// generateMessageId
// ---------------------------------------------------------------------------

describe("generateMessageId", () => {
  test("extracts domain from address", () => {
    const id = generateMessageId("alice@example.com");
    expect(id).toMatch(/^<[0-9a-f-]+@example\.com>$/);
  });

  test("uses local when address has no domain", () => {
    const id = generateMessageId("alice");
    expect(id).toMatch(/^<[0-9a-f-]+@local>$/);
  });

  test("produces unique IDs", () => {
    const a = generateMessageId("x@y");
    const b = generateMessageId("x@y");
    expect(a).not.toBe(b);
  });
});

// ---------------------------------------------------------------------------
// formatRFC2822Date
// ---------------------------------------------------------------------------

describe("formatRFC2822Date", () => {
  test("formats a known date correctly", () => {
    const date = new Date("2026-04-21T14:30:05Z");
    expect(formatRFC2822Date(date)).toBe("Tue, 21 Apr 2026 14:30:05 +0000");
  });

  test("zero-pads single-digit day and time components", () => {
    const date = new Date("2026-01-05T03:04:09Z");
    expect(formatRFC2822Date(date)).toBe("Mon, 05 Jan 2026 03:04:09 +0000");
  });
});

// ---------------------------------------------------------------------------
// assembleSignedContent — conversation
// ---------------------------------------------------------------------------

describe("assembleSignedContent", () => {
  test("conversation produces text/plain with CRLF", () => {
    const bytes = assembleSignedContent({
      kind: "conversation",
      text: "Hello\nWorld",
    });
    const text = dec.decode(bytes);
    expect(text).toContain("Content-Type: text/plain; charset=utf-8\r\n");
    expect(text).toContain("Content-Transfer-Encoding: 7bit\r\n");
    expect(text).toContain("\r\nHello\r\nWorld");
  });

  test("conversation strips trailing whitespace but preserves leading", () => {
    const bytes = assembleSignedContent({
      kind: "conversation",
      text: "  leading   \nindented   ",
    });
    const text = dec.decode(bytes);
    expect(text).toContain("\r\n  leading\r\nindented");
  });

  test("conversation with empty text produces headers only", () => {
    const bytes = assembleSignedContent({
      kind: "conversation",
      text: "",
    });
    const text = dec.decode(bytes);
    expect(text).toMatch(/Content-Transfer-Encoding: 7bit\r\n\r\n$/);
  });

  test("conversation normalizes CRLF input without doubling", () => {
    const bytes = assembleSignedContent({
      kind: "conversation",
      text: "line1\r\nline2",
    });
    const text = dec.decode(bytes);
    expect(text).toContain("line1\r\nline2");
    expect(text).not.toContain("line1\r\n\r\nline2");
  });

  test("structured produces multipart/mixed with JSON part", () => {
    const bytes = assembleSignedContent({
      kind: "structured",
      json: { action: "deploy" },
    });
    const text = dec.decode(bytes);
    expect(text).toContain("Content-Type: multipart/mixed;");
    expect(text).toContain(
      "Content-Type: application/vnd.interchange+json; charset=utf-8",
    );
    expect(text).toContain('{"action":"deploy"}');
  });

  test("structured without summary produces only the JSON part", () => {
    const bytes = assembleSignedContent({
      kind: "structured",
      json: { x: 1 },
    });
    const text = dec.decode(bytes);
    expect(text).toContain('{"x":1}');
    expect(text).not.toContain("Content-Type: text/plain");
  });

  test("structured includes optional summary as text/plain part", () => {
    const bytes = assembleSignedContent({
      kind: "structured",
      json: { x: 1 },
      summary: "A summary",
    });
    const text = dec.decode(bytes);
    const plainMatches = text.match(
      /Content-Type: text\/plain; charset=utf-8/g,
    );
    expect(plainMatches).toHaveLength(1);
    expect(text).toContain("A summary");
  });
});

// ---------------------------------------------------------------------------
// assembleMessage
// ---------------------------------------------------------------------------

describe("assembleMessage", () => {
  test("produces multipart/signed with correct headers", () => {
    const content = assembleSignedContent({
      kind: "conversation",
      text: "test",
    });
    const fakeSig = enc.encode("FAKE-SIGNATURE");
    const msg = assembleMessage(makeHeaders(), content, fakeSig);
    const text = dec.decode(msg);

    expect(text).toContain("From: alice@test.interchange\r\n");
    expect(text).toContain("To: bob@test.interchange\r\n");
    expect(text).toContain("Message-ID: <test-1@test.interchange>\r\n");
    expect(text).toContain("MIME-Version: 1.0\r\n");
    expect(text).toContain(
      'multipart/signed; protocol="application/pgp-signature"',
    );
    expect(text).toContain("micalg=pgp-sha512");
  });

  test("includes optional headers when provided", () => {
    const content = assembleSignedContent({
      kind: "conversation",
      text: "test",
    });
    const headers = makeHeaders({
      subject: "Test Subject",
      cc: ["charlie@test.interchange"],
      inReplyTo: "<prev@test.interchange>",
      references: ["<first@test.interchange>", "<prev@test.interchange>"],
      interchangeType: "conversation.message",
      interchangeSessionId: "sess-123",
    });
    const msg = assembleMessage(headers, content, enc.encode("SIG"));
    const text = dec.decode(msg);

    expect(text).toContain("Subject: Test Subject\r\n");
    expect(text).toContain("Cc: charlie@test.interchange\r\n");
    expect(text).toContain("In-Reply-To: <prev@test.interchange>\r\n");
    expect(text).toContain(
      "References: <first@test.interchange> <prev@test.interchange>\r\n",
    );
    expect(text).toContain("Interchange-Type: conversation.message\r\n");
    expect(text).toContain("Interchange-Session-ID: sess-123\r\n");
  });

  test("body has exactly two multipart/signed parts", () => {
    const content = assembleSignedContent({
      kind: "conversation",
      text: "test",
    });
    const msg = assembleMessage(makeHeaders(), content, enc.encode("FAKE-SIG"));
    const { headers, bodyOffset } = parseHeaderSection(msg);
    const ct = defined(headers.get("content-type"));
    const boundary = defined(extractBoundary(ct));
    const body = msg.slice(bodyOffset);
    const parts = parseMultipart(body, boundary);
    expect(parts).toHaveLength(2);

    const sigPart = parseMimePart(defined(parts[1]));
    expect(sigPart.contentType).toBe("application/pgp-signature");
  });
});

// ---------------------------------------------------------------------------
// parseHeaderSection
// ---------------------------------------------------------------------------

describe("parseHeaderSection", () => {
  test("parses CRLF-terminated headers", () => {
    const raw = enc.encode("From: alice@test\r\nTo: bob@test\r\n\r\nBody here");
    const { headers, bodyOffset } = parseHeaderSection(raw);
    expect(headers.get("from")).toBe("alice@test");
    expect(headers.get("to")).toBe("bob@test");
    expect(dec.decode(raw.slice(bodyOffset))).toBe("Body here");
  });

  test("parses LF-terminated headers", () => {
    const raw = enc.encode("From: alice@test\nTo: bob@test\n\nBody");
    const { headers, bodyOffset } = parseHeaderSection(raw);
    expect(headers.get("from")).toBe("alice@test");
    expect(dec.decode(raw.slice(bodyOffset))).toBe("Body");
  });

  test("unfolds continuation lines", () => {
    const raw = enc.encode("References: <a@test>\r\n <b@test>\r\n\r\nBody");
    const { headers } = parseHeaderSection(raw);
    expect(headers.get("references")).toBe("<a@test> <b@test>");
  });

  test("keeps first value for repeated headers", () => {
    const raw = enc.encode("Received: first\r\nReceived: second\r\n\r\nBody");
    const { headers } = parseHeaderSection(raw);
    expect(headers.get("received")).toBe("first");
  });

  test("lowercases header names", () => {
    const raw = enc.encode("Content-Type: text/plain\r\n\r\n");
    const { headers } = parseHeaderSection(raw);
    expect(headers.has("content-type")).toBe(true);
    expect(headers.has("Content-Type")).toBe(false);
  });

  test("bodyOffset is byte-accurate with 2-byte UTF-8 characters", () => {
    const raw = enc.encode("Subject: héllo\r\n\r\nBody here");
    const { bodyOffset } = parseHeaderSection(raw);
    expect(dec.decode(raw.slice(bodyOffset))).toBe("Body here");
  });

  test("bodyOffset is byte-accurate with 3-byte UTF-8 characters", () => {
    const raw = enc.encode("Subject: \u20ACuro\r\n\r\nBody here");
    const { bodyOffset } = parseHeaderSection(raw);
    expect(dec.decode(raw.slice(bodyOffset))).toBe("Body here");
  });

  test("bodyOffset is byte-accurate with 4-byte UTF-8 characters", () => {
    const raw = enc.encode("Subject: \u{1F600}face\r\n\r\nBody here");
    const { bodyOffset } = parseHeaderSection(raw);
    expect(dec.decode(raw.slice(bodyOffset))).toBe("Body here");
  });

  test("no separator treats entire input as headers", () => {
    const raw = enc.encode("From: alice\r\nTo: bob");
    const { headers, bodyOffset } = parseHeaderSection(raw);
    expect(bodyOffset).toBe(raw.length);
    expect(dec.decode(raw.slice(bodyOffset))).toBe("");
    expect(headers.get("from")).toBe("alice");
  });

  test("empty input returns empty headers and zero offset", () => {
    const raw = enc.encode("");
    const { headers, bodyOffset } = parseHeaderSection(raw);
    expect(bodyOffset).toBe(0);
    expect(headers.size).toBe(0);
  });

  test("separator-only input returns empty headers", () => {
    const raw = enc.encode("\r\n\r\n");
    const { headers, bodyOffset } = parseHeaderSection(raw);
    expect(bodyOffset).toBe(4);
    expect(headers.size).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// extractBoundary
// ---------------------------------------------------------------------------

describe("extractBoundary", () => {
  test("extracts quoted boundary", () => {
    const ct = 'multipart/signed; boundary="----=_Part_abc123"';
    expect(extractBoundary(ct)).toBe("----=_Part_abc123");
  });

  test("extracts unquoted boundary", () => {
    const ct = "multipart/mixed; boundary=simple_boundary";
    expect(extractBoundary(ct)).toBe("simple_boundary");
  });

  test("returns undefined when no boundary", () => {
    expect(extractBoundary("text/plain")).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// parseMultipart
// ---------------------------------------------------------------------------

describe("parseMultipart", () => {
  test("splits two parts correctly", () => {
    const body = enc.encode(
      [
        "--boundary",
        "Content-Type: text/plain",
        "",
        "Part one",
        "--boundary",
        "Content-Type: text/html",
        "",
        "<p>Part two</p>",
        "--boundary--",
      ].join("\r\n"),
    );
    const parts = parseMultipart(body, "boundary");
    expect(parts).toHaveLength(2);
    expect(dec.decode(defined(parts[0]))).toContain("Part one");
    expect(dec.decode(defined(parts[1]))).toContain("<p>Part two</p>");
  });

  test("handles LF-only line endings", () => {
    const body = enc.encode(
      "--boundary\nContent-Type: text/plain\n\nPart one\n--boundary\nContent-Type: text/html\n\n<p>Part two</p>\n--boundary--\n",
    );
    const parts = parseMultipart(body, "boundary");
    expect(parts).toHaveLength(2);
    const p1 = parseMimePart(defined(parts[0]));
    const p2 = parseMimePart(defined(parts[1]));
    expect(dec.decode(p1.body)).toContain("Part one");
    expect(dec.decode(p2.body)).toContain("<p>Part two</p>");
  });
});

// ---------------------------------------------------------------------------
// parseMimePart
// ---------------------------------------------------------------------------

describe("parseMimePart", () => {
  test("separates headers from body", () => {
    const raw = enc.encode("Content-Type: text/plain\r\n\r\nThe body text");
    const part = parseMimePart(raw);
    expect(part.contentType).toBe("text/plain");
    expect(dec.decode(part.body)).toBe("The body text");
  });

  test("defaults to application/octet-stream", () => {
    const raw = enc.encode("X-Custom: value\r\n\r\ndata");
    const part = parseMimePart(raw);
    expect(part.contentType).toBe("application/octet-stream");
  });
});

// ---------------------------------------------------------------------------
// extractPartByPath
// ---------------------------------------------------------------------------

describe("extractPartByPath", () => {
  test("extracts parts from an assembled message", () => {
    const content = assembleSignedContent({
      kind: "conversation",
      text: "Hello world",
    });
    const msg = assembleMessage(makeHeaders(), content, enc.encode("SIG"));

    const part1 = extractPartByPath(msg, "1");
    expect(dec.decode(part1)).toContain("Hello world");

    const part2 = extractPartByPath(msg, "2");
    const sigPart = parseMimePart(part2);
    expect(sigPart.contentType).toBe("application/pgp-signature");
  });

  test("throws on invalid path segment", () => {
    const msg = assembleMessage(
      makeHeaders(),
      assembleSignedContent({ kind: "conversation", text: "x" }),
      enc.encode("SIG"),
    );
    expect(() => extractPartByPath(msg, "0")).toThrow(/Invalid part path/);
    expect(() => extractPartByPath(msg, "abc")).toThrow(/Invalid part path/);
  });

  test("throws when part index exceeds part count", () => {
    const msg = assembleMessage(
      makeHeaders(),
      assembleSignedContent({ kind: "conversation", text: "x" }),
      enc.encode("SIG"),
    );
    expect(() => extractPartByPath(msg, "5")).toThrow(/does not exist/);
  });

  test("throws when indexing into a non-multipart message", () => {
    const raw = enc.encode("Content-Type: text/plain\r\n\r\nJust a body");
    expect(() => extractPartByPath(raw, "1")).toThrow(/non-multipart/);
  });
});

// ---------------------------------------------------------------------------
// createDetachedSignatureFromProvider — round-trip with verify
// ---------------------------------------------------------------------------

describe("createDetachedSignatureFromProvider", () => {
  test("signature verifies against the signed content", async () => {
    const kp = await generateKeyPair();
    const provider = createNodeCrypto(kp);
    const content = assembleSignedContent({
      kind: "conversation",
      text: "Round-trip test",
    });

    const sig = await createDetachedSignatureFromProvider(content, provider);
    const valid = await verifyDetachedSignature(
      content,
      sig,
      provider.getPublicKey(),
    );
    expect(valid).toBe(true);
  });

  test("signature is ASCII-armored", async () => {
    const kp = await generateKeyPair();
    const provider = createNodeCrypto(kp);
    const content = assembleSignedContent({
      kind: "conversation",
      text: "test",
    });

    const sig = await createDetachedSignatureFromProvider(content, provider);
    const text = dec.decode(sig);
    expect(text).toContain("-----BEGIN PGP SIGNATURE-----");
    expect(text).toContain("-----END PGP SIGNATURE-----");
  });

  test("verification fails with wrong public key", async () => {
    const kp1 = await generateKeyPair();
    const kp2 = await generateKeyPair();
    const provider = createNodeCrypto(kp1);
    const wrongKey = createNodeCrypto(kp2);
    const content = assembleSignedContent({
      kind: "conversation",
      text: "test",
    });

    const sig = await createDetachedSignatureFromProvider(content, provider);
    const valid = await verifyDetachedSignature(
      content,
      sig,
      wrongKey.getPublicKey(),
    );
    expect(valid).toBe(false);
  });

  test("verification fails with tampered content", async () => {
    const kp = await generateKeyPair();
    const provider = createNodeCrypto(kp);
    const content = assembleSignedContent({
      kind: "conversation",
      text: "original",
    });

    const sig = await createDetachedSignatureFromProvider(content, provider);
    const tampered = assembleSignedContent({
      kind: "conversation",
      text: "modified",
    });
    const valid = await verifyDetachedSignature(
      tampered,
      sig,
      provider.getPublicKey(),
    );
    expect(valid).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// parseMailToEmail
// ---------------------------------------------------------------------------

describe("parseMailToEmail", () => {
  test("parses a simple text/plain message", () => {
    const raw = enc.encode(
      [
        "From: Alice <alice@example.com>",
        "To: Bob <bob@example.com>",
        "Subject: Hello",
        "Date: Tue, 21 Apr 2026 12:00:00 +0000",
        "MIME-Version: 1.0",
        "Content-Type: text/plain; charset=utf-8",
        "",
        "Hello from Alice",
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_abc123");

    expect(email.from).toEqual([{ name: "Alice", email: "alice@example.com" }]);
    expect(email.to).toEqual([{ name: "Bob", email: "bob@example.com" }]);
    expect(email.subject).toBe("Hello");
    expect(email.sentAt).toBe("2026-04-21T12:00:00.000Z");
    expect(Object.keys(email.bodyValues)).toHaveLength(1);
    expect(email.bodyValues["1"]?.value).toContain("Hello from Alice");
    expect(email.textBody).toEqual([{ partId: "1", type: "text/plain" }]);
    expect(email.htmlBody).toHaveLength(0);
    expect(email.attachments).toHaveLength(0);
  });

  test("parses from/to with bare email addresses", () => {
    const raw = enc.encode(
      [
        "From: alice@example.com",
        "To: bob@example.com, charlie@example.com",
        "Date: Tue, 21 Apr 2026 12:00:00 +0000",
        "Content-Type: text/plain",
        "",
        "body",
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_1");

    expect(email.from).toEqual([{ name: null, email: "alice@example.com" }]);
    expect(email.to).toEqual([
      { name: null, email: "bob@example.com" },
      { name: null, email: "charlie@example.com" },
    ]);
  });

  test("returns null subject when header is absent", () => {
    const raw = enc.encode(
      [
        "From: alice@example.com",
        "To: bob@example.com",
        "Date: Tue, 21 Apr 2026 12:00:00 +0000",
        "Content-Type: text/plain",
        "",
        "body",
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_1");
    expect(email.subject).toBeNull();
  });

  test("returns null sentAt when Date header is absent", () => {
    const raw = enc.encode(
      [
        "From: alice@example.com",
        "To: bob@example.com",
        "Content-Type: text/plain",
        "",
        "body",
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_1");
    expect(email.sentAt).toBeNull();
  });

  test("returns null sentAt when Date header is unparseable", () => {
    const raw = enc.encode(
      [
        "From: alice@example.com",
        "To: bob@example.com",
        "Date: not-a-date",
        "Content-Type: text/plain",
        "",
        "body",
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_1");
    expect(email.sentAt).toBeNull();
  });

  test("parses multipart/mixed with text and attachment", () => {
    const boundary = "test_boundary_xyz";
    const raw = enc.encode(
      [
        "From: alice@example.com",
        "To: bob@example.com",
        "Date: Tue, 21 Apr 2026 12:00:00 +0000",
        `Content-Type: multipart/mixed; boundary="${boundary}"`,
        "",
        `--${boundary}`,
        "Content-Type: text/plain; charset=utf-8",
        "",
        "The message body",
        `--${boundary}`,
        "Content-Type: application/pdf",
        'Content-Disposition: attachment; filename="report.pdf"',
        "",
        "PDF-BYTES-HERE",
        `--${boundary}--`,
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_multi");

    expect(email.textBody).toEqual([{ partId: "1", type: "text/plain" }]);
    expect(email.bodyValues["1"]?.value).toContain("The message body");
    expect(email.attachments).toHaveLength(1);
    expect(email.attachments[0]).toEqual({
      blobId: "blob_sml_multi_2",
      name: "report.pdf",
      type: "application/pdf",
      size: "PDF-BYTES-HERE".length,
    });
  });

  test("parses multipart/mixed with html part", () => {
    const boundary = "mixed_html_boundary";
    const raw = enc.encode(
      [
        "From: alice@example.com",
        "To: bob@example.com",
        "Date: Tue, 21 Apr 2026 12:00:00 +0000",
        `Content-Type: multipart/mixed; boundary="${boundary}"`,
        "",
        `--${boundary}`,
        "Content-Type: text/html; charset=utf-8",
        "",
        "<p>Hello</p>",
        `--${boundary}--`,
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_html");

    expect(email.htmlBody).toEqual([{ partId: "1", type: "text/html" }]);
    expect(email.bodyValues["1"]?.value).toContain("<p>Hello</p>");
    expect(email.textBody).toHaveLength(0);
    expect(email.attachments).toHaveLength(0);
  });

  test("parses a multipart/signed conversation message assembled by this library", () => {
    const content = assembleSignedContent({
      kind: "conversation",
      text: "Hello from a signed message",
    });
    const fakeSig = enc.encode("FAKE-SIGNATURE");
    const msg = assembleMessage(
      makeHeaders({
        subject: "Signed Convo",
        interchangeType: "conversation.message",
        interchangeSessionId: "sess-42",
      }),
      content,
      fakeSig,
    );

    const email = parseMailToEmail(msg, "sml_signed_plain");

    expect(email.from).toEqual([
      { name: null, email: "alice@test.interchange" },
    ]);
    expect(email.to).toEqual([{ name: null, email: "bob@test.interchange" }]);
    expect(email.subject).toBe("Signed Convo");
    expect(email.sentAt).toBe("2026-04-21T12:00:00.000Z");
    expect(email.textBody).toHaveLength(1);
    expect(
      email.bodyValues[defined(email.textBody[0]).partId]?.value,
    ).toContain("Hello from a signed message");
    expect(email.attachments).toHaveLength(0);
    expect(email.headers["interchange-type"]).toBe("conversation.message");
    expect(email.headers["interchange-session-id"]).toBe("sess-42");
  });

  test("parses a multipart/signed structured message assembled by this library", () => {
    const payload = { action: "deploy", env: "staging" };
    const content = assembleSignedContent({
      kind: "structured",
      json: payload,
      summary: "Deploying to staging",
    });
    const fakeSig = enc.encode("FAKE-SIG");
    const msg = assembleMessage(
      makeHeaders({ interchangeType: "structured.message" }),
      content,
      fakeSig,
    );

    const email = parseMailToEmail(msg, "sml_signed_structured");

    // The structured message is multipart/mixed inside multipart/signed.
    // textBody should include the summary text/plain part.
    expect(email.textBody).toHaveLength(1);
    const textPartId = defined(email.textBody[0]).partId;
    expect(email.bodyValues[textPartId]?.value).toContain(
      "Deploying to staging",
    );
    // The application/vnd.interchange+json part is a non-text blob attachment.
    expect(email.attachments).toHaveLength(1);
    expect(email.attachments[0]?.type).toBe("application/vnd.interchange+json");
    expect(email.headers["interchange-type"]).toBe("structured.message");
  });

  test("blob IDs use the correct scheme", () => {
    const boundary = "blob_id_boundary";
    const raw = enc.encode(
      [
        "From: alice@example.com",
        "To: bob@example.com",
        "Date: Tue, 21 Apr 2026 12:00:00 +0000",
        `Content-Type: multipart/mixed; boundary="${boundary}"`,
        "",
        `--${boundary}`,
        "Content-Type: text/plain",
        "",
        "Text body",
        `--${boundary}`,
        "Content-Type: image/png",
        'Content-Disposition: attachment; filename="photo.png"',
        "",
        "PNG-DATA",
        `--${boundary}`,
        "Content-Type: application/zip",
        'Content-Disposition: attachment; filename="archive.zip"',
        "",
        "ZIP-DATA",
        `--${boundary}--`,
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_xyz");

    expect(email.attachments[0]?.blobId).toBe("blob_sml_xyz_2");
    expect(email.attachments[1]?.blobId).toBe("blob_sml_xyz_3");
  });

  test("extracts Interchange-specific headers into headers field", () => {
    const raw = enc.encode(
      [
        "From: alice@example.com",
        "To: bob@example.com",
        "Date: Tue, 21 Apr 2026 12:00:00 +0000",
        "Content-Type: text/plain",
        "Interchange-Type: conversation.message",
        "Interchange-Tenant-ID: tenant-99",
        "Interchange-Agent-ID: agent-42",
        "X-Custom: should-not-appear",
        "",
        "body",
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_hdrs");

    expect(email.headers["interchange-type"]).toBe("conversation.message");
    expect(email.headers["interchange-tenant-id"]).toBe("tenant-99");
    expect(email.headers["interchange-agent-id"]).toBe("agent-42");
    expect(email.headers["x-custom"]).toBeUndefined();
  });

  test("non-text non-attachment parts are treated as attachments", () => {
    const boundary = "mixed_types";
    const raw = enc.encode(
      [
        "From: alice@example.com",
        "To: bob@example.com",
        "Date: Tue, 21 Apr 2026 12:00:00 +0000",
        `Content-Type: multipart/mixed; boundary="${boundary}"`,
        "",
        `--${boundary}`,
        "Content-Type: text/plain",
        "",
        "Text here",
        `--${boundary}`,
        "Content-Type: application/octet-stream",
        'Content-Disposition: attachment; filename="data.bin"',
        "",
        "BINARY",
        `--${boundary}--`,
      ].join("\r\n"),
    );

    const email = parseMailToEmail(raw, "sml_bin");

    expect(email.textBody).toHaveLength(1);
    expect(email.attachments).toHaveLength(1);
    expect(email.attachments[0]?.type).toBe("application/octet-stream");
    expect(email.attachments[0]?.name).toBe("data.bin");
  });
});

// ---------------------------------------------------------------------------
// Full round-trip: assemble → parse → verify
// ---------------------------------------------------------------------------

describe("assemble then parse round-trip", () => {
  test("conversation message survives assemble/parse cycle", async () => {
    const kp = await generateKeyPair();
    const provider = createNodeCrypto(kp);
    const content = assembleSignedContent({
      kind: "conversation",
      text: "Hello from the round-trip test",
    });
    const sig = await createDetachedSignatureFromProvider(content, provider);
    const msg = assembleMessage(
      makeHeaders({ interchangeType: "conversation.message" }),
      content,
      sig,
    );

    const { headers, bodyOffset } = parseHeaderSection(msg);
    expect(headers.get("from")).toBe("alice@test.interchange");
    expect(headers.get("interchange-type")).toBe("conversation.message");

    const ct = defined(headers.get("content-type"));
    expect(ct).toContain("multipart/signed");
    const boundary = defined(extractBoundary(ct));

    const body = msg.slice(bodyOffset);
    const parts = parseMultipart(body, boundary);
    expect(parts).toHaveLength(2);

    const signedPart = defined(parts[0]);
    const sigPart = parseMimePart(defined(parts[1]));
    expect(sigPart.contentType).toBe("application/pgp-signature");

    const valid = await verifyDetachedSignature(
      signedPart,
      sigPart.body,
      provider.getPublicKey(),
    );
    expect(valid).toBe(true);

    const parsed = parseMimePart(signedPart);
    expect(parsed.contentType).toContain("text/plain");
    expect(dec.decode(parsed.body)).toContain("Hello from the round-trip test");
  });

  test("structured message survives assemble/parse cycle", async () => {
    const kp = await generateKeyPair();
    const provider = createNodeCrypto(kp);
    const payload = { action: "deploy", target: "prod" };
    const content = assembleSignedContent({
      kind: "structured",
      json: payload,
      summary: "Deploying to prod",
    });
    const sig = await createDetachedSignatureFromProvider(content, provider);
    const msg = assembleMessage(makeHeaders(), content, sig);

    const { headers, bodyOffset } = parseHeaderSection(msg);
    const outerBoundary = defined(
      extractBoundary(defined(headers.get("content-type"))),
    );
    const outerParts = parseMultipart(msg.slice(bodyOffset), outerBoundary);
    expect(outerParts).toHaveLength(2);

    const signedPart = defined(outerParts[0]);
    const innerParsed = parseMimePart(signedPart);
    expect(innerParsed.contentType).toContain("multipart/mixed");

    const innerBoundary = defined(extractBoundary(innerParsed.contentType));
    const innerParts = parseMultipart(innerParsed.body, innerBoundary);
    expect(innerParts).toHaveLength(2);

    const jsonPart = parseMimePart(defined(innerParts[0]));
    expect(jsonPart.contentType).toContain("application/vnd.interchange+json");
    const parsed = JSON.parse(dec.decode(jsonPart.body));
    expect(parsed).toEqual(payload);

    const summaryPart = parseMimePart(defined(innerParts[1]));
    expect(dec.decode(summaryPart.body)).toContain("Deploying to prod");
  });
});
