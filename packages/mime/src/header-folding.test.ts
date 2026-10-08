// Header line length, non-ASCII header text, and quoted-printable bodies.
//
// RFC 5322 §2.1.1: a line MUST NOT exceed 998 chars. A header body is ASCII:
// a non-ASCII subject needs the RFC 2047 encoded-word form. A text part that
// is not 7-bit is quoted-printable; RFC 2045 §6.7 forbids a literal space or
// tab at the end of an encoded line because a relay may strip it.

import { describe, expect, test } from "bun:test";

import {
  assembleMessage,
  assembleSignedContent,
  buildMessageHeaders,
  decodeHeaderText,
  decodeMail,
  parseMailToEmail,
  decodePartBytes,
  extractBoundary,
  parseHeaderSection,
  parseMimePart,
  parseMultipart,
} from "./mime";
import type { MessageHeaders } from "./mime";

/** Every optional field spelled out: `MessageHeaders` requires all of them. */
const BASE: MessageHeaders = {
  from: "alpha@test.interchange",
  to: ["beta@test.interchange"],
  cc: undefined,
  date: new Date("2026-01-01T00:00:00Z"),
  messageId: "<base@test.interchange>",
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
};

const SIGNED_CONTENT = new TextEncoder().encode(
  "Content-Type: text/plain\r\n\r\nbody\r\n",
);
const SIGNATURE = new TextEncoder().encode("-----BEGIN PGP SIGNATURE-----\r\n");

function headerLines(raw: Uint8Array): string[] {
  const text = new TextDecoder().decode(raw);
  const end = text.indexOf("\r\n\r\n");
  return text.slice(0, end === -1 ? text.length : end).split("\r\n");
}

function assembled(headers: MessageHeaders): Uint8Array {
  return assembleMessage(headers, SIGNED_CONTENT, SIGNATURE);
}

/** The value a receiver reconstructs: unfold, then decode encoded-words. */
function receivedHeader(raw: Uint8Array, name: string): string | undefined {
  const { headers } = parseHeaderSection(raw);
  const value = headers.get(name.toLowerCase());
  return value === undefined ? undefined : decodeHeaderText(value);
}

describe("no header line exceeds the hard limit", () => {
  test("a long References chain is folded", () => {
    // 20 ids at 50 chars each: 1000 chars of value, past the 998 limit.
    const references = Array.from(
      { length: 20 },
      (_, i) => `<${String(i).padStart(44, "0")}@x.test>`,
    );
    const raw = assembled({ ...BASE, references });

    for (const line of headerLines(raw)) {
      expect(line.length).toBeLessThanOrEqual(998);
    }
  });

  test("folding preserves the value a receiver reconstructs", () => {
    const references = Array.from(
      { length: 20 },
      (_, i) => `<${String(i).padStart(44, "0")}@x.test>`,
    );
    const raw = assembled({ ...BASE, references });

    const parsed = buildMessageHeaders(parseHeaderSection(raw).headers);
    expect(parsed.references).toEqual(references);
  });

  test("a long address list is folded and reconstructs", () => {
    const to = Array.from(
      { length: 12 },
      (_, i) => `recipient-with-a-long-name-${String(i)}@test.interchange`,
    );
    const raw = assembled({ ...BASE, to });

    for (const line of headerLines(raw)) {
      expect(line.length).toBeLessThanOrEqual(998);
    }
    const parsed = buildMessageHeaders(parseHeaderSection(raw).headers);
    expect(parsed.to).toEqual(to);
  });

  test("a single unfoldable token is not broken apart", () => {
    // There is no legal fold point inside a msg-id; an over-long one stays whole.
    const messageId = `<${"a".repeat(200)}@test.interchange>`;
    const raw = assembled({ ...BASE, messageId });

    const parsed = buildMessageHeaders(parseHeaderSection(raw).headers);
    expect(parsed.messageId).toBe(messageId);
  });

  test("repeated spaces in a header value survive folding", () => {
    // Long enough that the fold falls inside a run of spaces; unfolding
    // replaces the break with one space, so the other space has to remain.
    const subject = `${"x".repeat(60)}  ${"y".repeat(30)}`;
    const raw = assembled({ ...BASE, subject });

    expect(headerLines(raw).some((line) => line.startsWith(" "))).toBe(true);
    expect(receivedHeader(raw, "Subject")).toBe(subject);
  });

  test("a space run that crosses the fold stays intact", () => {
    // The continuation opens with only one space; unfolding deletes the break
    // and every whitespace after it, then inserts one space. Spaces from the
    // run that do not fit before the break stay on the folded line.
    const subjects = [
      `${"a".repeat(60)}${" ".repeat(10)}b`,
      `${"x".repeat(68)}  y`,
      `${"A".repeat(70)}  hello`,
    ];
    for (const subject of subjects) {
      const raw = assembled({ ...BASE, subject });
      for (const line of headerLines(raw)) {
        expect(line.startsWith("  ")).toBe(false);
      }
      expect(receivedHeader(raw, "Subject")).toBe(subject);
    }
  });

  test("a tab-ending run does not hide an earlier fold", () => {
    // The last space before the target ends a run in a tab, so it cannot be
    // the fold; an earlier space still can.
    const subject = `hi ${"a".repeat(64)} \t${"b".repeat(930)}`;
    const raw = assembled({ ...BASE, subject });

    for (const line of headerLines(raw)) {
      expect(line.length).toBeLessThanOrEqual(998);
      expect(line.startsWith("  ")).toBe(false);
    }
    expect(headerLines(raw).some((line) => line.startsWith(" "))).toBe(true);
    expect(receivedHeader(raw, "Subject")).toBe(subject);
    expect(decodeMail(raw).rawHeaders["subject"]).toEqual([subject]);
  });

  test("a tab after a folded space run stays in the value", () => {
    // A tab that follows the fold space is leading whitespace on the
    // continuation, and unfolding deletes that whole run.
    const subjects = [
      `${"A".repeat(70)} \tX`,
      `${"a".repeat(60)}${" ".repeat(10)}\tb`,
    ];
    for (const subject of subjects) {
      const raw = assembled({ ...BASE, subject });
      expect(receivedHeader(raw, "Subject")).toBe(subject);
      expect(decodeMail(raw).rawHeaders["subject"]).toEqual([subject]);
    }
  });

  test("spaces parked at the end of a continuation survive the raw header", () => {
    const subject = `${"a".repeat(80)} ${"b".repeat(80)}  c`;
    const raw = assembled({ ...BASE, subject });

    expect(receivedHeader(raw, "Subject")).toBe(subject);
    expect(decodeMail(raw).rawHeaders["subject"]).toEqual([subject]);
  });

  test("a short header is not folded at all", () => {
    // Folding only where needed: a continuation line on a short header is
    // legal but harder to read.
    const raw = assembled({ ...BASE, subject: "hello" });
    const text = new TextDecoder().decode(raw);

    expect(text).toContain("Subject: hello\r\n");
  });
});

describe("a non-ASCII subject is encoded for the wire", () => {
  test("the wire form is ASCII and the received value is the original", () => {
    const subject = "Réunion du café — 10h";
    const raw = assembled({ ...BASE, subject });
    const text = new TextDecoder().decode(raw);
    const end = text.indexOf("\r\n\r\n");
    const headerSection = text.slice(0, end);

    // eslint-disable-next-line no-control-regex
    expect(/[^\x00-\x7F]/.test(headerSection)).toBe(false);
    expect(headerSection).toContain("=?UTF-8?B?");
    expect(receivedHeader(raw, "Subject")).toBe(subject);
    // The agent-facing parse is the one the hub delivers. The wire form
    // stays in the raw header.
    expect(parseMailToEmail(raw, "fold-subject").subject).toBe(subject);
    expect(decodeMail(raw).rawHeaders["subject"]?.[0]).toContain("=?UTF-8?B?");
  });

  test("a long non-ASCII subject round-trips across several encoded-words", () => {
    // One encoded-word caps at 75 chars, so a long subject becomes a
    // sequence of them; each must decode independently and the whitespace
    // joining them must not survive into the value.
    const subject = "Поздравляем с завершением развертывания ".repeat(4).trim();
    const raw = assembled({ ...BASE, subject });

    for (const line of headerLines(raw)) {
      expect(line.length).toBeLessThanOrEqual(998);
    }
    expect(receivedHeader(raw, "Subject")).toBe(subject);
  });

  test("a character is never split across two encoded-words", () => {
    // A multi-byte sequence straddling two words decodes to replacement
    // characters in both, so the cut has to land on a character boundary. An
    // emoji is 4 bytes, which makes a 45-byte payload boundary fall inside one
    // unless the encoder walks back off it.
    const subject = "🎉".repeat(40);
    const raw = assembled({ ...BASE, subject });
    expect(receivedHeader(raw, "Subject")).toBe(subject);
    expect(receivedHeader(raw, "Subject")).not.toContain("�");
  });

  test("an ASCII subject is left exactly as it was", () => {
    const raw = assembled({ ...BASE, subject: "Deployment complete" });

    expect(new TextDecoder().decode(raw)).toContain(
      "Subject: Deployment complete\r\n",
    );
  });
});

describe("decoding what other senders produce", () => {
  test("a Q-encoded word is decoded", () => {
    // Only B is produced here, but Q is widespread; `_` is a space and `=XX`
    // is a hex octet.
    expect(decodeHeaderText("=?UTF-8?Q?caf=C3=A9_time?=")).toBe("café time");
  });

  test("whitespace between two encoded-words is dropped", () => {
    // RFC 2047 §6.2: the separator is not content. Keeping it inserts a
    // space into the middle of a word that was split for length.
    expect(decodeHeaderText("=?UTF-8?B?w6A=?= =?UTF-8?B?w6E=?=")).toBe("àá");
  });

  test("whitespace around a word is kept", () => {
    expect(decodeHeaderText("Re: =?UTF-8?B?w6A=?= today")).toBe("Re: à today");
  });

  test("an unencoded value passes through untouched", () => {
    expect(decodeHeaderText("Plain subject")).toBe("Plain subject");
  });

  test("a charset this cannot decode keeps its encoded form", () => {
    // The encoded text is a visible fault a reader can report; replacement
    // characters would read as the sender's own text.
    const value = "=?Shift_JIS?B?gqCCooKk?=";
    expect(decodeHeaderText(value)).toBe(value);
  });

  test("a base64 word with characters outside the alphabet keeps its encoded form", () => {
    expect(decodeHeaderText("=?UTF-8?B?***?=")).toBe("=?UTF-8?B?***?=");
    expect(decodeHeaderText("=?UTF-8?B?YQ!!?=")).toBe("=?UTF-8?B?YQ!!?=");
  });

  test("a word whose bytes contradict its charset keeps its encoded form", () => {
    // `/w==` is the single byte 0xFF, which is not valid UTF-8.
    const value = "=?UTF-8?B?/w==?=";
    expect(decodeHeaderText(value)).toBe(value);
  });

  test("an ISO-8859-1 word is decoded by byte value", () => {
    expect(decodeHeaderText("=?ISO-8859-1?Q?caf=E9?=")).toBe("café");
  });

  test("a language suffix on the charset is tolerated", () => {
    // RFC 2231 §5 permits `charset*language`.
    expect(decodeHeaderText("=?UTF-8*fr?B?w6A=?=")).toBe("à");
  });
});

describe("quoted-printable text survives a whitespace-stripping relay", () => {
  test("a soft break does not close a line on a literal space or tab", () => {
    // Long enough to wrap, with a space and a tab in the run, plus one
    // non-ASCII character so the part cannot stay 7bit.
    const text = `${"alpha ".repeat(20)}beta\tgamma café`;
    const signed = assembleSignedContent({ kind: "conversation", text });
    const { headers, bodyOffset } = parseHeaderSection(signed);
    const contentType = headers.get("content-type");
    if (contentType === undefined) throw new Error("missing content-type");
    const boundary = extractBoundary(contentType);
    if (boundary === undefined) throw new Error("missing boundary");
    const first = parseMultipart(signed.slice(bodyOffset), boundary)[0];
    if (first === undefined) throw new Error("missing text part");
    const textPart = parseMimePart(first);
    expect(textPart.headers.get("content-transfer-encoding")).toBe(
      "quoted-printable",
    );

    const wire = new TextDecoder().decode(textPart.body);
    for (const line of wire.split("\r\n")) {
      expect(line.endsWith(" ") || line.endsWith("\t")).toBe(false);
      expect(/[ \t]=$/.test(line)).toBe(false);
    }

    const decoded = new TextDecoder().decode(
      decodePartBytes(textPart.body, textPart.headers),
    );
    expect(decoded).toBe(`${"alpha ".repeat(20)}beta\tgamma café`);
  });

  test("escaped whitespace does not push a line past 76 octets", () => {
    const text = `${"a".repeat(50)}${" ".repeat(25)}é${"\t".repeat(75)}ø`;
    const signed = assembleSignedContent({ kind: "conversation", text });
    const { headers, bodyOffset } = parseHeaderSection(signed);
    const contentType = headers.get("content-type");
    if (contentType === undefined) throw new Error("missing content-type");
    const boundary = extractBoundary(contentType);
    if (boundary === undefined) throw new Error("missing boundary");
    const first = parseMultipart(signed.slice(bodyOffset), boundary)[0];
    if (first === undefined) throw new Error("missing text part");
    const textPart = parseMimePart(first);

    const wire = new TextDecoder().decode(textPart.body);
    for (const line of wire.split("\r\n")) {
      expect(line.length).toBeLessThanOrEqual(76);
      expect(line.endsWith(" ") || line.endsWith("\t")).toBe(false);
    }
    expect(
      new TextDecoder().decode(
        decodePartBytes(textPart.body, textPart.headers),
      ),
    ).toBe(text);
  });
});
