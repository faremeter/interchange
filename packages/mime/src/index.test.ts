import { describe, test, expect } from "bun:test";
import {
  generateKeyPair,
  createEd25519Crypto,
  verifyDetachedSignature,
} from "@intx/crypto";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  extractAddrSpec,
  formatRFC2822Date,
  generateMessageId,
  parseHeaderSection,
  parseMimePart,
  parseMultipart,
  extractBoundary,
  extractPartByPath,
  parseMailToEmail,
  extractAttachments,
  type MessageHeaders,
} from "./index";
import type { MessageAttachment } from "@intx/types/runtime";

const enc = new TextEncoder();
const dec = new TextDecoder();

function defined<T>(value: T | undefined | null): T {
  if (value === undefined || value === null) {
    throw new Error("Expected a defined value but got undefined/null");
  }
  return value;
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

describe("extractAddrSpec", () => {
  test("strips quoted display name and angle brackets", () => {
    expect(extractAddrSpec('"Alice Doe" <alice@example.com>')).toBe(
      "alice@example.com",
    );
  });

  test("strips unquoted display name and angle brackets", () => {
    expect(extractAddrSpec("Alice Doe <alice@example.com>")).toBe(
      "alice@example.com",
    );
  });

  test("strips bare angle brackets", () => {
    expect(extractAddrSpec("<alice@example.com>")).toBe("alice@example.com");
  });

  test("passes through a bare addr-spec", () => {
    expect(extractAddrSpec("alice@example.com")).toBe("alice@example.com");
  });

  test("lowercases local-part and domain", () => {
    expect(extractAddrSpec("Alice@Example.COM")).toBe("alice@example.com");
  });

  test("trims surrounding whitespace", () => {
    expect(extractAddrSpec("   Alice@Example.com   ")).toBe(
      "alice@example.com",
    );
  });

  test("throws on empty input", () => {
    expect(() => extractAddrSpec("   ")).toThrow();
  });

  test("throws on input with no '@'", () => {
    expect(() => extractAddrSpec("Alice Doe")).toThrow();
  });

  test("throws on missing local-part", () => {
    expect(() => extractAddrSpec("<@example.com>")).toThrow();
  });

  test("throws on missing domain", () => {
    expect(() => extractAddrSpec("<alice@>")).toThrow();
  });

  test("throws on trailing content after the closing '>'", () => {
    expect(() =>
      extractAddrSpec("Alice <alice@example.com> (comment)"),
    ).toThrow();
  });

  test("throws on a trailing comment in a bare form", () => {
    expect(() => extractAddrSpec("alice@example.com (comment)")).toThrow();
  });

  test("throws on a trailing token in a bare form", () => {
    expect(() => extractAddrSpec("alice@example.com foo")).toThrow();
  });

  test("throws on a quoted local-part", () => {
    expect(() => extractAddrSpec('"a@b"@example.com')).toThrow();
  });

  test("throws on multiple '@' in an unquoted form", () => {
    expect(() => extractAddrSpec("a@b@example.com")).toThrow();
  });

  test("keeps a comma inside a quoted display name", () => {
    expect(extractAddrSpec('"Doe, John" <john@example.com>')).toBe(
      "john@example.com",
    );
  });

  test("throws on a comma-separated bare list", () => {
    expect(() => extractAddrSpec("a@b.example, c@d.example")).toThrow(
      /address lists are not supported/,
    );
  });

  test("throws on an angle-bracketed list rather than taking one member", () => {
    // The reduction this refuses picked the last member: a `From` naming a
    // victim first and the sender second read as the sender alone while every
    // full-header parser still saw both.
    expect(() =>
      extractAddrSpec(
        '"CEO" <ceo@victim.example>, "x" <attacker@evil.example>',
      ),
    ).toThrow(/address lists are not supported/);
  });

  test("throws on adjacent angle-bracketed addresses separated by a space", () => {
    // A space is not the only spelling of a list, so a rule about commas
    // alone left this reducing to the last member.
    expect(() =>
      extractAddrSpec("<ceo@victim.example> <attacker@evil.example>"),
    ).toThrow(/address lists are not supported/);
  });

  test("throws on adjacent angle-bracketed addresses separated by a tab", () => {
    expect(() =>
      extractAddrSpec("<ceo@victim.example>\t<attacker@evil.example>"),
    ).toThrow(/address lists are not supported/);
  });

  test("throws on angle-bracketed addresses with nothing between them", () => {
    expect(() =>
      extractAddrSpec("<ceo@victim.example><attacker@evil.example>"),
    ).toThrow(/address lists are not supported/);
  });

  test("throws on a bare address ahead of an angle-bracketed one", () => {
    // A display name holds no unquoted '@', so the leading addr-spec is a
    // second address and not a name for the one in brackets.
    expect(() =>
      extractAddrSpec("ceo@victim.example <attacker@evil.example>"),
    ).toThrow(/address lists are not supported/);
  });

  test("throws on a display name followed by two angle-bracketed addresses", () => {
    expect(() => extractAddrSpec("Name <a@b.example> <c@d.example>")).toThrow(
      /address lists are not supported/,
    );
  });

  test("throws on quoted display names between adjacent addresses", () => {
    // Quoting each member hides the separator but not the second '<'.
    expect(() =>
      extractAddrSpec('"CEO" <ceo@victim.example>"x"<attacker@evil.example>'),
    ).toThrow(/address lists are not supported/);
  });

  test("throws on an address carried in a leading comment", () => {
    // A comment may hold '@' as text; extractAddrSpec refuses comments
    // wherever they appear, so reading it as a second address costs nothing.
    expect(() =>
      extractAddrSpec("(ceo@victim.example) <attacker@evil.example>"),
    ).toThrow(/address lists are not supported/);
  });

  test("throws on a group rather than taking its one member", () => {
    expect(() =>
      extractAddrSpec("Team: <a@b.example>, <c@d.example>;"),
    ).toThrow(/address lists are not supported/);
  });

  test("throws on a '>' that closes the address early", () => {
    expect(() => extractAddrSpec("<a@b.example>>")).toThrow(/stray '>'/);
  });

  test("keeps an empty quoted display name", () => {
    expect(extractAddrSpec('"" <a@b.example>')).toBe("a@b.example");
  });

  test("keeps an escaped quote inside a quoted display name", () => {
    expect(extractAddrSpec('"a\\"b, c" <x@y.example>')).toBe("x@y.example");
  });

  test("keeps a dot in an unquoted display name", () => {
    expect(extractAddrSpec("John Q. Public <john@example.com>")).toBe(
      "john@example.com",
    );
  });

  test("keeps a domain literal", () => {
    expect(extractAddrSpec("<user@[192.168.0.1]>")).toBe("user@[192.168.0.1]");
  });

  test("throws on an unterminated quoted string", () => {
    // Leaving the quote open would otherwise hide the separator from the scan.
    expect(() =>
      extractAddrSpec('"CEO <ceo@victim.example>, "x" <attacker@evil.example>'),
    ).toThrow(/unterminated quoted string/);
  });

  test("reads a fully quoted prefix as one display name", () => {
    expect(
      extractAddrSpec('"CEO <ceo@victim.example>, x" <attacker@evil.example>'),
    ).toBe("attacker@evil.example");
  });
});

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
  test("conversation wraps text/plain in multipart/mixed with CRLF", () => {
    const bytes = assembleSignedContent({
      kind: "conversation",
      text: "Hello\nWorld",
    });
    const text = dec.decode(bytes);
    expect(text).toContain("Content-Type: multipart/mixed;");
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

  test("conversation with empty text produces an empty text part", () => {
    const bytes = assembleSignedContent({
      kind: "conversation",
      text: "",
    });
    const text = dec.decode(bytes);
    expect(text).toContain("Content-Type: multipart/mixed;");
    // text/plain part headers, blank line, empty body CRLF, then a boundary
    expect(text).toMatch(
      /Content-Type: text\/plain; charset=utf-8\r\nContent-Transfer-Encoding: 7bit\r\n\r\n\r\n--/,
    );
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

  test("rejects a subject containing CRLF (header injection)", () => {
    // A reply copies the subject from the inbound peer message, so this is
    // the remotely reachable path: a doubled line ending ends the header
    // block and turns the signed envelope into inert body text.
    const content = assembleSignedContent({
      kind: "conversation",
      text: "test",
    });
    const headers = makeHeaders({
      subject: "hi\r\nInterchange-Type: session.accept\r\n\r\nplanted body",
    });
    expect(() => assembleMessage(headers, content, enc.encode("SIG"))).toThrow(
      /CR or LF/,
    );
  });

  test("rejects a bare LF and a bare CR in any emitted header", () => {
    const content = assembleSignedContent({
      kind: "conversation",
      text: "test",
    });
    expect(() =>
      assembleMessage(
        makeHeaders({ from: "alice@test.interchange\nCc: mallory@evil" }),
        content,
        enc.encode("SIG"),
      ),
    ).toThrow(/CR or LF/);
    expect(() =>
      assembleMessage(
        makeHeaders({
          interchangeAgentId: "agt-1\rInterchange-Tenant-ID: t-2",
        }),
        content,
        enc.encode("SIG"),
      ),
    ).toThrow(/CR or LF/);
  });

  test("emits a double quote in a header value verbatim", () => {
    // Unstructured text may carry a quote, and the serializer emits its own
    // quoted boundary parameter on every message, so the emission guard must
    // reject line breaks only.
    const content = assembleSignedContent({
      kind: "conversation",
      text: "test",
    });
    const headers = makeHeaders({
      subject: 'Re: the "urgent" request',
      from: '"Doe, Jane" <jane@test.interchange>',
    });
    const text = dec.decode(
      assembleMessage(headers, content, enc.encode("SIG")),
    );
    expect(text).toContain('Subject: Re: the "urgent" request\r\n');
    expect(text).toContain('From: "Doe, Jane" <jane@test.interchange>\r\n');
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

  test("refuses an LF-terminated message", () => {
    // RFC 5321 §2.3.8 forbids recognizing anything but CRLF as a line
    // terminator; §4.1.1.4 refuses the lone-LF ending by name. Folding the
    // breaks instead leaves one field whose value swallows every later field
    // and the body.
    const raw = enc.encode("From: alice@test\nTo: bob@test\n\nBody");
    expect(() => parseHeaderSection(raw)).toThrow(
      /must break its lines with CRLF/,
    );
  });

  test("refuses a CR-terminated message", () => {
    const raw = enc.encode("From: alice@test\rTo: bob@test\r\rBody");
    expect(() => parseHeaderSection(raw)).toThrow(
      /must break its lines with CRLF/,
    );
  });

  test("refuses an LF-only header section that a CRLF CRLF terminates", () => {
    // The section's own line breaks decide conformity, not the flavour of the
    // blank line that ends it. Folding these leaves one field whose value
    // swallows `Interchange-Type` and `Subject`.
    const raw = enc.encode(
      "From: alice@x\nInterchange-Type: conversation.message\n" +
        "Subject: Hi\r\n\r\nBody",
    );
    expect(() => parseHeaderSection(raw)).toThrow(
      /must break its lines with CRLF/,
    );
  });

  test("refuses an LF-only header section when the body carries a CRLF CRLF", () => {
    // Searching the whole message for the separator finds the one in the body
    // and reads the header section from there, which both suppresses every
    // field after the first and truncates the body to what followed it.
    const raw = enc.encode(
      "From: alice@x\nInterchange-Type: conversation.message\n" +
        "Subject: Hi\n\nBody\r\n\r\ntail\n",
    );
    expect(() => parseHeaderSection(raw)).toThrow(
      /must break its lines with CRLF/,
    );
  });

  test("refuses an LF LF separator after a CRLF-only section", () => {
    // The blank line is a pair of line breaks of its own, so honouring a bare
    // pair lets a sender who controls one field body end the section early and
    // strip the fields after it.
    const raw = enc.encode(
      "From: alice@x\r\nSubject: Hi\n\n" +
        "Interchange-Type: conversation.message\r\n\r\nBody",
    );
    expect(() => parseHeaderSection(raw)).toThrow(
      /must break its lines with CRLF/,
    );
  });

  test("takes the first blank line when the body carries another", () => {
    const raw = enc.encode("Subject: Hi\r\n\r\nBody\r\n\r\nmore");
    const { headers, bodyOffset } = parseHeaderSection(raw);
    expect(headers.get("subject")).toBe("Hi");
    expect(dec.decode(raw.slice(bodyOffset))).toBe("Body\r\n\r\nmore");
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

  // A bare CR or LF in a field body is refused for every row below: RFC 5322
  // §2.2 admits neither inside a field body and §2.3 requires the two to occur
  // only as CRLF. Splitting on the bare character resolves a field the sender
  // smuggled into one it controls; folding it suppresses whatever followed.
  const bareBreaks = ["\r", "\n"];
  const bareBreakHeaders: string[] = bareBreaks.flatMap((brk) => [
    `Subject: hello${brk}Bcc: attacker@evil.test`,
    `From: alice@example.com${brk}Bcc: attacker@evil.test`,
    `Message-ID: <p@example.com>${brk}Bcc: attacker@evil.test`,
    `Interchange-Correlation-ID: corr-1${brk}Bcc: attacker@evil.test`,
    `References: <a@example.com>${brk}Bcc: attacker@evil.test`,
    `Subject: benign${brk}Interchange-Agent-Id: victim`,
  ]);

  test("refuses a bare carriage return or line feed in a field body", () => {
    for (const header of bareBreakHeaders) {
      expect(() =>
        parseHeaderSection(
          enc.encode(`${header}\r\nTo: bob@example.com\r\n\r\nbody`),
        ),
      ).toThrow(/must break its lines with CRLF/);
    }
  });

  test("leaves CRLF folding and line termination alone", () => {
    // The fold targets a CR that is not part of a CRLF, so a real terminator
    // and a real continuation line still mean what they meant.
    const { headers } = parseHeaderSection(
      enc.encode(
        "References: <a@test>\r\n <b@test>\r\nSubject: kept\r\n\r\nBody",
      ),
    );
    expect(headers.get("references")).toBe("<a@test> <b@test>");
    expect(headers.get("subject")).toBe("kept");
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

  test("splits parts whose delimiter lines are LF-terminated", () => {
    // The boundary scan tolerates an LF-terminated delimiter line. Each part's
    // own fields are CRLF-terminated (the only terminator the header parse
    // recognizes, RFC 5322 §2.3), so the two tolerances stay apart.
    const body = enc.encode(
      "--boundary\nContent-Type: text/plain\r\n\r\nPart one\n" +
        "--boundary\nContent-Type: text/html\r\n\r\n<p>Part two</p>\n" +
        "--boundary--\n",
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
    const provider = createEd25519Crypto(kp);
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
    const provider = createEd25519Crypto(kp);
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
    const provider = createEd25519Crypto(kp1);
    const wrongKey = createEd25519Crypto(kp2);
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
    const provider = createEd25519Crypto(kp);
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
    const provider = createEd25519Crypto(kp);
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

    // The signed content is multipart/mixed; the text lives at part 1.1.
    const parsed = parseMimePart(signedPart);
    expect(parsed.contentType).toContain("multipart/mixed");
    const innerBoundary = defined(extractBoundary(parsed.contentType));
    const innerParts = parseMultipart(parsed.body, innerBoundary);
    const textPart = parseMimePart(defined(innerParts[0]));
    expect(textPart.contentType).toContain("text/plain");
    expect(dec.decode(textPart.body)).toContain(
      "Hello from the round-trip test",
    );
  });

  test("structured message survives assemble/parse cycle", async () => {
    const kp = await generateKeyPair();
    const provider = createEd25519Crypto(kp);
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

// ---------------------------------------------------------------------------
// Conversation attachments: assemble → extract round-trip
// ---------------------------------------------------------------------------

describe("conversation attachments round-trip", () => {
  const attachments: MessageAttachment[] = [
    {
      name: "shot.png",
      contentType: "image/png",
      data: new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    },
    {
      name: "clip.mp4",
      contentType: "video/mp4",
      data: new Uint8Array([0x00, 0x00, 0x00, 0x18, 0x66, 0x74, 0x79, 0x70]),
    },
    {
      name: "voice.mp3",
      contentType: "audio/mpeg",
      data: new Uint8Array([0xff, 0xfb, 0x90, 0x00, 0x11]),
    },
    {
      name: "report.pdf",
      contentType: "application/pdf",
      data: new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d]),
    },
  ];

  function buildConversation(
    text: string,
    atts: MessageAttachment[] | undefined,
  ): Uint8Array {
    const content = assembleSignedContent({
      kind: "conversation",
      text,
      ...(atts !== undefined ? { attachments: atts } : {}),
    });
    return assembleMessage(
      makeHeaders({ interchangeType: "conversation.message" }),
      content,
      enc.encode("FAKE-SIGNATURE"),
    );
  }

  test("assemble then extract reconstructs one attachment of each block variant", () => {
    const msg = buildConversation("see attached", attachments);
    const extracted = extractAttachments(msg);
    expect(extracted).toHaveLength(attachments.length);
    for (let i = 0; i < attachments.length; i++) {
      const orig = defined(attachments[i]);
      const got = defined(extracted[i]);
      expect(got.name).toBe(orig.name);
      expect(got.contentType).toBe(orig.contentType);
      expect(got.part).toBe(`1.${i + 2}`);
      expect(Array.from(got.data)).toEqual(Array.from(orig.data));
    }
  });

  test("no-attachments conversation round-trips with zero attachment parts", () => {
    const msg = buildConversation("just text", undefined);
    expect(extractAttachments(msg)).toHaveLength(0);
  });

  test("image-only conversation (empty text) round-trips the attachment", () => {
    const onlyImage = defined(attachments[0]);
    const msg = buildConversation("", [onlyImage]);
    const extracted = extractAttachments(msg);
    expect(extracted).toHaveLength(1);
    expect(defined(extracted[0]).contentType).toBe("image/png");
    expect(Array.from(defined(extracted[0]).data)).toEqual(
      Array.from(onlyImage.data),
    );
  });

  test("parseMailToEmail surfaces conversation attachment metadata", () => {
    const pdf = defined(attachments[3]);
    const msg = buildConversation("hi", [pdf]);
    const email = parseMailToEmail(msg, "sml_conv_att");
    expect(email.attachments).toHaveLength(1);
    expect(defined(email.attachments[0]).name).toBe("report.pdf");
    expect(defined(email.attachments[0]).type).toBe("application/pdf");
  });

  test("a text/plain document attachment is distinguished from the body text", () => {
    // The trickiest case: a text/plain attachment shares its content type
    // with the conversation body part, so the two are told apart only by
    // Content-Disposition. The body must not be read as an attachment, and
    // the attachment must not be lost.
    const doc: MessageAttachment = {
      name: "notes.txt",
      contentType: "text/plain",
      data: enc.encode("attached document contents"),
    };
    const msg = buildConversation("the conversation body", [doc]);

    const extracted = extractAttachments(msg);
    expect(extracted).toHaveLength(1);
    expect(defined(extracted[0]).name).toBe("notes.txt");
    expect(defined(extracted[0]).contentType).toBe("text/plain");
    expect(dec.decode(defined(extracted[0]).data)).toBe(
      "attached document contents",
    );

    const email = parseMailToEmail(msg, "sml_txt_doc");
    expect(email.attachments).toHaveLength(1);
    expect(defined(email.attachments[0]).name).toBe("notes.txt");
    expect(defined(email.attachments[0]).type).toBe("text/plain");
  });

  test("conversation message with attachments produces a verifiable signature", async () => {
    const kp = await generateKeyPair();
    const provider = createEd25519Crypto(kp);
    const content = assembleSignedContent({
      kind: "conversation",
      text: "signed with an image",
      attachments: [defined(attachments[0])],
    });
    const sig = await createDetachedSignatureFromProvider(content, provider);
    const msg = assembleMessage(
      makeHeaders({ interchangeType: "conversation.message" }),
      content,
      sig,
    );

    const { headers, bodyOffset } = parseHeaderSection(msg);
    const boundary = defined(
      extractBoundary(defined(headers.get("content-type"))),
    );
    const parts = parseMultipart(msg.slice(bodyOffset), boundary);
    const signedPart = defined(parts[0]);
    const sigPart = parseMimePart(defined(parts[1]));
    const valid = await verifyDetachedSignature(
      signedPart,
      sigPart.body,
      provider.getPublicKey(),
    );
    expect(valid).toBe(true);
  });

  test("an inline html sibling does not steal the PDF's IMAP path", () => {
    // A mail client that also sends text/html leaves an extra sibling
    // between BODY[1.1] and the attachment. extractAttachments skips that
    // inline part; the PDF's path must still be the sibling number
    // extractPartByPath uses (1.3), not the attachment-array index (1.2).
    const pdf = new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d]);
    const signed = [
      `Content-Type: multipart/mixed; boundary="inner"`,
      "",
      `--inner`,
      "Content-Type: text/plain; charset=utf-8",
      "Content-Transfer-Encoding: 7bit",
      "",
      "hello",
      `--inner`,
      "Content-Type: text/html; charset=utf-8",
      "Content-Disposition: inline",
      "",
      "<p>hello</p>",
      `--inner`,
      "Content-Type: application/pdf",
      "Content-Transfer-Encoding: base64",
      `Content-Disposition: attachment; filename="report.pdf"`,
      "",
      "JVBERi0=",
      `--inner--`,
      "",
    ].join("\r\n");
    const msg = assembleMessage(
      makeHeaders({ interchangeType: "conversation.message" }),
      enc.encode(signed),
      enc.encode("FAKE-SIGNATURE"),
    );

    const htmlPart = parseMimePart(extractPartByPath(msg, "1.2"));
    expect(
      defined(htmlPart.contentType.split(";")[0]).trim().toLowerCase(),
    ).toBe("text/html");
    const pdfPart = parseMimePart(extractPartByPath(msg, "1.3"));
    expect(
      defined(pdfPart.contentType.split(";")[0]).trim().toLowerCase(),
    ).toBe("application/pdf");

    const extracted = extractAttachments(msg);
    expect(extracted).toHaveLength(1);
    expect(defined(extracted[0]).name).toBe("report.pdf");
    expect(defined(extracted[0]).part).toBe("1.3");
    expect(Array.from(defined(extracted[0]).data)).toEqual(Array.from(pdf));
  });

  test("rejects an attachment name containing CRLF (header injection)", () => {
    expect(() =>
      assembleSignedContent({
        kind: "conversation",
        text: "x",
        attachments: [
          {
            name: "evil\r\nContent-Type: text/html",
            contentType: "image/png",
            data: new Uint8Array([1, 2, 3]),
          },
        ],
      }),
    ).toThrow();
  });

  test("rejects an attachment header that is not 7-bit or over 998 octets", () => {
    // These headers sit inside the signed part and have no transfer
    // encoding. An 8-bit name, or a line past 998, is one a relay can rewrite.
    const blob = new Uint8Array([1]);
    expect(() =>
      assembleSignedContent({
        kind: "conversation",
        text: "x",
        attachments: [
          { name: "résumé.pdf", contentType: "application/pdf", data: blob },
        ],
      }),
    ).toThrow(/US-ASCII/);
    expect(() =>
      assembleSignedContent({
        kind: "conversation",
        text: "x",
        attachments: [
          {
            name: "a".repeat(1100),
            contentType: "application/pdf",
            data: blob,
          },
        ],
      }),
    ).toThrow(/998/);
    expect(() =>
      assembleSignedContent({
        kind: "conversation",
        text: "x",
        attachments: [
          {
            name: "notes.txt",
            contentType: `application/${"é"}`,
            data: blob,
          },
        ],
      }),
    ).toThrow(/US-ASCII/);
  });
});
