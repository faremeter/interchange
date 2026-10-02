// Header line length and non-ASCII header text.
//
// Two RFC 5322 requirements this assembler did not meet. A line MUST NOT exceed
// 998 characters (section 2.1.1), and a long `References` chain reaches that on
// its own -- 20 message ids of 50 characters is 1000. And a header body is
// ASCII: a non-ASCII subject needs the RFC 2047 encoded-word form, because
// 8-bit bytes in a header are illegal without an extension no relay here
// negotiates.
//
// Both were latent rather than theoretical. Nothing in the tree writes a
// subject with an accent in it today, and threads stay short in the tests --
// which is why neither showed up until a message was put on a real wire.

import { describe, expect, test } from "bun:test";

import {
  assembleMessage,
  buildMessageHeaders,
  decodeHeaderText,
  parseHeaderSection,
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
    // 20 ids at 50 characters each: 1000 characters of value, past the 998 a
    // line may not exceed, before the header name is even counted.
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
    // The point of folding at an existing space: unfolding deletes the CRLF and
    // keeps the space, so the receiver's value is identical to the one given.
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
    // There is no legal fold point inside a msg-id. Breaking one to meet the
    // recommended 78 would corrupt it, so an over-long one stays whole.
    const messageId = `<${"a".repeat(200)}@test.interchange>`;
    const raw = assembled({ ...BASE, messageId });

    const parsed = buildMessageHeaders(parseHeaderSection(raw).headers);
    expect(parsed.messageId).toBe(messageId);
  });

  test("a short header is not folded at all", () => {
    // Folding only where it is needed: a continuation line on a short header
    // is legal but makes every message harder to read.
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
  });

  test("a long non-ASCII subject round-trips across several encoded-words", () => {
    // One encoded-word caps at 75 characters, so a long subject becomes a
    // sequence of them. Each must decode independently and the whitespace
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
    // Encoding an ASCII subject would be legal and awful: every subject in
    // every log and every mail client would read as base64.
    const raw = assembled({ ...BASE, subject: "Deployment complete" });

    expect(new TextDecoder().decode(raw)).toContain(
      "Subject: Deployment complete\r\n",
    );
  });
});

describe("decoding what other senders produce", () => {
  test("a Q-encoded word is decoded", () => {
    // Only B is produced here, but Q is widespread, so a receiver has to read
    // it. `_` is a space and `=XX` is a hex octet.
    expect(decodeHeaderText("=?UTF-8?Q?caf=C3=A9_time?=")).toBe("café time");
  });

  test("whitespace between two encoded-words is dropped", () => {
    // RFC 2047 section 6.2: the separator is not content. Keeping it inserts a
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
    // Showing the encoded text is a visible fault a reader can report.
    // Substituting replacement characters would read as the sender's own text.
    const value = "=?Shift_JIS?B?gqCCooKk?=";
    expect(decodeHeaderText(value)).toBe(value);
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
    // RFC 2231 section 5 permits `charset*language`.
    expect(decodeHeaderText("=?UTF-8*fr?B?w6A=?=")).toBe("à");
  });
});
