import { describe, test, expect } from "bun:test";
import type { MailPart, MessageAttachment } from "@intx/types/runtime";
import { isMail } from "@intx/types/runtime";
import { deriveMessageId, parseMessageIdHeader } from "@intx/types";

import {
  assembleSignedContent,
  assembleMessage,
  decodeMail,
  extractAttachments,
  parseMailToEmail,
} from "./index";
import type { MessageHeaders } from "./index";

function rawBytes(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

/**
 * Build message bytes whose body is exactly the given byte values, so a test
 * can carry a body that is not valid UTF-8.
 */
function rawWithBody(headerText: string, bodyBytes: number[]): Uint8Array {
  const head = new TextEncoder().encode(headerText);
  const out = new Uint8Array(head.length + bodyBytes.length);
  out.set(head, 0);
  out.set(Uint8Array.from(bodyBytes), head.length);
  return out;
}

function codeUnits(value: string): number[] {
  const out: number[] = [];
  for (let i = 0; i < value.length; i++) out.push(value.charCodeAt(i));
  return out;
}

/** A multipart/mixed message with one text/plain part under `encoding`. */
function multipartWithEncoding(encoding: string, body: string): Uint8Array {
  const boundary = "cte_boundary";
  return rawBytes(
    [
      "From: alice@example.com",
      "To: bob@example.com",
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

/**
 * A single-part text/plain message under `encoding`. The message's own headers
 * are the part's headers here, so this is the shape that tells whether an
 * entry point reads the declared mechanism or a reconstruction that dropped
 * it.
 */
function singlePartWithEncoding(encoding: string, body: string): Uint8Array {
  return rawBytes(
    [
      "From: alice@example.com",
      "To: bob@example.com",
      "Content-Type: text/plain",
      `Content-Transfer-Encoding: ${encoding}`,
      "",
      body,
    ].join("\r\n"),
  );
}

/**
 * A multipart/mixed message whose part 1 is the text body and whose part 2 is
 * an application/pdf attachment under `encoding`.
 */
function multipartWithAttachmentEncoding(
  encoding: string,
  body: string,
): Uint8Array {
  const boundary = "cte_att_boundary";
  return rawBytes(
    [
      "From: alice@example.com",
      "To: bob@example.com",
      `Content-Type: multipart/mixed; boundary="${boundary}"`,
      "",
      `--${boundary}`,
      "Content-Type: text/plain",
      "",
      "see attached",
      `--${boundary}`,
      "Content-Type: application/pdf",
      `Content-Transfer-Encoding: ${encoding}`,
      'Content-Disposition: attachment; filename="report.pdf"',
      "",
      body,
      `--${boundary}--`,
      "",
    ].join("\r\n"),
  );
}

/**
 * A multipart/signed message in the shape `extractAttachments` reads: an outer
 * signed envelope whose content part is a multipart/mixed carrying the text
 * body and one application/pdf attachment under `encoding`.
 */
function signedWithAttachmentEncoding(
  encoding: string,
  body: string,
): Uint8Array {
  return rawBytes(
    [
      "From: alice@example.com",
      "To: bob@example.com",
      'Content-Type: multipart/signed; protocol="application/pgp-signature"; ' +
        'micalg="pgp-sha256"; boundary="cte_outer"',
      "",
      "--cte_outer",
      'Content-Type: multipart/mixed; boundary="cte_inner"',
      "",
      "--cte_inner",
      "Content-Type: text/plain",
      "",
      "see attached",
      "--cte_inner",
      "Content-Type: application/pdf",
      `Content-Transfer-Encoding: ${encoding}`,
      'Content-Disposition: attachment; filename="report.pdf"',
      "",
      body,
      "--cte_inner--",
      "",
      "--cte_outer",
      "Content-Type: application/pgp-signature",
      "",
      "placeholder-signature",
      "--cte_outer--",
      "",
    ].join("\r\n"),
  );
}

function headers(overrides: Partial<MessageHeaders> = {}): MessageHeaders {
  return {
    from: '"Alice" <alice@example.com>',
    to: ["run@deployment.example.com"],
    cc: undefined,
    date: new Date("2026-01-02T03:04:05Z"),
    messageId: "<msg-decode-1@example.com>",
    subject: "hello there",
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
    ...overrides,
  };
}

function attachment(
  name: string,
  contentType: string,
  data: string,
): MessageAttachment {
  return { name, contentType, data: new TextEncoder().encode(data) };
}

function signedConversation(
  text: string,
  attachments: MessageAttachment[] = [],
): Uint8Array {
  const signedContent = assembleSignedContent({
    kind: "conversation",
    text,
    ...(attachments.length > 0 ? { attachments } : {}),
  });
  const signature = new TextEncoder().encode("placeholder-signature");
  return assembleMessage(headers(), signedContent, signature);
}

describe("decodeMail", () => {
  test("decodes headers (typed + raw) and the text part of a plain message", () => {
    const mail = decodeMail(signedConversation("hello world"));

    expect(mail.headers.from).toBe('"Alice" <alice@example.com>');
    expect(mail.headers.subject).toBe("hello there");
    expect(mail.headers.messageId).toBe("<msg-decode-1@example.com>");
    expect(mail.headers.interchangeType).toBe("conversation.message");

    // Full raw header map: every header present, lowercased, multi-value safe.
    expect(mail.rawHeaders["subject"]).toEqual(["hello there"]);
    expect(mail.rawHeaders["message-id"]).toEqual([
      "<msg-decode-1@example.com>",
    ]);
    expect(mail.rawHeaders["content-type"]?.[0]).toContain("multipart/signed");

    // One decoded leaf part: the text body. The PGP signature part is dropped.
    expect(mail.parts).toHaveLength(1);
    expect(mail.parts[0]?.contentType).toBe("text/plain");
    expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe(
      "hello world",
    );
  });

  test("records an absent Date and Message-ID as absent, not as empty", () => {
    // RFC 5322 defines `Date` as `date-time` and `Message-ID` as `msg-id`.
    // Neither admits an empty body, so a message carrying neither has to come
    // back with both fields absent: a defaulted empty string reads as a value
    // downstream and gets written back out as a malformed header line.
    const mail = decodeMail(
      rawBytes(
        [
          "From: alice@example.com",
          "To: bob@example.com",
          "Subject: neither header",
          "Content-Type: text/plain",
          "",
          "body",
        ].join("\r\n"),
      ),
    );

    expect(mail.headers.date).toBeUndefined();
    expect(mail.headers.messageId).toBeUndefined();
    // Absent, not present-and-undefined: the key must not survive into a
    // serialized envelope as an explicit null-ish value.
    expect("date" in mail.headers).toBe(false);
    expect("messageId" in mail.headers).toBe(false);
  });

  test("treats a blank Date and Message-ID as absent", () => {
    // A sender that writes the header and leaves it empty has named nothing,
    // which is the same treatment a blank `From` already gets.
    const mail = decodeMail(
      rawBytes(
        [
          "From: alice@example.com",
          "To: bob@example.com",
          "Date:   ",
          "Message-ID:  ",
          "Content-Type: text/plain",
          "",
          "body",
        ].join("\r\n"),
      ),
    );

    expect("date" in mail.headers).toBe(false);
    expect("messageId" in mail.headers).toBe(false);
  });

  test("treats a blank In-Reply-To as absent", () => {
    // RFC 5322 defines `In-Reply-To` as `1*msg-id` under the same clause as
    // `Message-ID`, so a header left blank names no parent. An empty value is
    // an id no message can carry, so every message whose header was blank
    // would otherwise read as a reply to one shared nonexistent parent.
    const mail = decodeMail(
      rawBytes(
        [
          "From: alice@example.com",
          "To: bob@example.com",
          "Message-ID: <blank-parent@example.com>",
          "In-Reply-To:   ",
          "Content-Type: text/plain",
          "",
          "body",
        ].join("\r\n"),
      ),
    );

    expect("inReplyTo" in mail.headers).toBe(false);
  });

  test("decodes every part of a message with attachments, no data loss", () => {
    const mail = decodeMail(
      signedConversation("see attached", [
        attachment("photo.png", "image/png", "png-bytes"),
        attachment("data.json", "application/json", '{"k":1}'),
      ]),
    );

    // Text part + 2 attachment parts; signature excluded.
    expect(mail.parts).toHaveLength(3);

    const text = mail.parts.find((p) => p.contentType === "text/plain");
    expect(new TextDecoder().decode(text?.content)).toBe("see attached");

    const png = mail.parts.find((p) => p.contentType === "image/png");
    expect(png?.filename).toBe("photo.png");
    expect(png?.disposition).toBe("attachment");
    expect(new TextDecoder().decode(png?.content)).toBe("png-bytes");

    const json = mail.parts.find((p) => p.contentType === "application/json");
    expect(json?.filename).toBe("data.json");
    expect(new TextDecoder().decode(json?.content)).toBe('{"k":1}');
  });

  test("decodes an attachments-only message with empty text", () => {
    const mail = decodeMail(
      signedConversation("", [attachment("a.mp3", "audio/mpeg", "audiobytes")]),
    );
    const audio = mail.parts.find((p) => p.contentType === "audio/mpeg");
    expect(audio?.filename).toBe("a.mp3");
    expect(new TextDecoder().decode(audio?.content)).toBe("audiobytes");
  });

  test("keeps repeated headers and unfolds continuation lines", () => {
    const mail = decodeMail(
      rawBytes(
        "From: a@b\r\n" +
          "Received: from one\r\n" +
          "Received: from two\r\n" +
          "Subject: folded\r\n subject line\r\n" +
          "Content-Type: text/plain\r\n\r\nbody",
      ),
    );
    // Every occurrence of a repeated header is preserved, in order.
    expect(mail.rawHeaders["received"]).toEqual(["from one", "from two"]);
    // A folded value is unfolded onto its header.
    expect(mail.rawHeaders["subject"]).toEqual(["folded subject line"]);
  });

  test("keeps a header named after an Object.prototype member", () => {
    // `__proto__` and `constructor` are well-formed field names (RFC 5322
    // section 3.6.8), so a peer can send them. Accumulating them into a plain
    // object reaches inherited members instead of the accumulator's own
    // properties, which either throws or discards the value.
    const mail = decodeMail(
      rawBytes(
        "From: a@b\r\n" +
          "__proto__: injected\r\n" +
          "Constructor: hostile\r\n" +
          "Content-Type: text/plain\r\n\r\nbody",
      ),
    );

    // Lossless: each value is an own data property, so it survives rather
    // than being swallowed by the inherited `__proto__` setter.
    expect(Object.hasOwn(mail.rawHeaders, "__proto__")).toBe(true);
    expect(
      Object.getOwnPropertyDescriptor(mail.rawHeaders, "__proto__")?.value,
    ).toEqual(["injected"]);
    expect(Object.hasOwn(mail.rawHeaders, "constructor")).toBe(true);
    expect(mail.rawHeaders["constructor"]).toEqual(["hostile"]);

    // Prototype-free: the map carries no inherited member, so a field name the
    // message does not carry resolves to nothing rather than to whatever
    // `Object.prototype` holds under that name -- the declared type promises
    // `string[] | undefined` for every name, and `toString` would otherwise be
    // a function. The shared accessor itself is untouched.
    expect(Object.getPrototypeOf(mail.rawHeaders)).toBeNull();
    expect(mail.rawHeaders["toString"]).toBeUndefined();
    expect(mail.rawHeaders["hasOwnProperty"]).toBeUndefined();
    expect(
      typeof Object.getOwnPropertyDescriptor(Object.prototype, "__proto__")
        ?.get,
    ).toBe("function");

    // Ordinary headers in the same message still parse.
    expect(mail.rawHeaders["from"]).toEqual(["a@b"]);
  });

  test("undoes base64 transfer-encoding on a leaf part", () => {
    const mail = decodeMail(
      rawBytes(
        "Content-Type: text/plain\r\n" +
          "Content-Transfer-Encoding: base64\r\n\r\naGVsbG8gd29ybGQ=",
      ),
    );
    expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe(
      "hello world",
    );
  });

  test("undoes quoted-printable transfer-encoding on a leaf part", () => {
    const mail = decodeMail(
      rawBytes(
        "Content-Type: text/plain\r\n" +
          "Content-Transfer-Encoding: quoted-printable\r\n\r\nhello=20world",
      ),
    );
    expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe(
      "hello world",
    );
  });

  test("does not truncate the final header when there is no body separator", () => {
    // A message that is all headers and no blank-line separator: the raw
    // header map must carry the final header value in full, not chopped.
    const mail = decodeMail(rawBytes("From: alice@example.com"));
    expect(mail.rawHeaders["from"]).toEqual(["alice@example.com"]);
    expect(mail.headers.from).toBe("alice@example.com");
  });

  test("folds a bare carriage return in both readings of the header section", () => {
    // `decodeMail` reads one header section twice -- once into the typed
    // subset, once into the raw map a workflow walks. A fold applied to only
    // one of them would leave the control character reachable through the
    // other, and the two would disagree about the same field.
    const mail = decodeMail(
      rawBytes(
        "From: alice@example.com\r\n" +
          "To: bob@example.com\r\n" +
          "Subject: hello\rBcc: attacker@evil.test\r\n" +
          "Interchange-Correlation-ID: corr-1\rBcc: attacker@evil.test\r\n" +
          "\r\nbody",
      ),
    );
    expect(mail.headers.subject).toBe("hello Bcc: attacker@evil.test");
    expect(mail.rawHeaders["subject"]).toEqual([
      "hello Bcc: attacker@evil.test",
    ]);
    expect(mail.headers.interchangeCorrelationId).toBe(
      "corr-1 Bcc: attacker@evil.test",
    );
    expect(mail.rawHeaders["bcc"]).toBeUndefined();
  });

  test("folds a bare line feed in both readings of the header section", () => {
    // RFC 5322 section 2.3 requires CR and LF to occur only together as CRLF,
    // and RFC 5321 section 4.1.1.4 refuses the lone-LF line ending by name, so
    // a bare LF terminates no field. Reading it as a terminator would resolve
    // `Bcc` from text the sender smuggled inside a field it controls, and the
    // two readings of one header section must not disagree about which fields
    // the message carries.
    const mail = decodeMail(
      rawBytes(
        "From: alice@example.com\r\n" +
          "To: bob@example.com\r\n" +
          "Subject: benign\nBcc: attacker@evil.test\r\n" +
          "\r\nbody",
      ),
    );
    expect(mail.headers.subject).toBe("benign Bcc: attacker@evil.test");
    expect(mail.rawHeaders["subject"]).toEqual([
      "benign Bcc: attacker@evil.test",
    ]);
    expect(mail.rawHeaders["bcc"]).toBeUndefined();
    // The fields either side of the smuggled one still parse in both readings.
    expect(mail.headers.to).toEqual(["bob@example.com"]);
    expect(mail.rawHeaders["from"]).toEqual(["alice@example.com"]);
  });

  test("records a blank or absent From as no originator, not an empty one", () => {
    // A defaulted "" made these two states indistinguishable and handed every
    // consumer an originator the message never carried.
    expect(
      decodeMail(rawBytes("To: bob@example.com\r\n\r\nbody")).headers.from,
    ).toBeUndefined();
    expect(
      decodeMail(rawBytes("From:   \r\nTo: bob@example.com\r\n\r\nbody"))
        .headers.from,
    ).toBeUndefined();
  });

  test("carries an unparseable From through verbatim", () => {
    // This projection is lossless. An originator that is present but not a
    // parseable address is a different judgement from one that was never
    // there, and the admission gate keys on exactly that difference.
    expect(
      decodeMail(rawBytes("From: not an address\r\n\r\nbody")).headers.from,
    ).toBe("not an address");
  });

  test("keeps every header when the header section opens with a blank line", () => {
    // The raw parse used to break on the leading empty line and return no
    // headers at all, where the typed parse skipped it and kept both fields.
    const mail = decodeMail(
      rawBytes(
        "\r\nFrom: alice@example.com\r\nTo: bob@example.com\r\n\r\nbody",
      ),
    );
    expect(mail.rawHeaders["from"]).toEqual(["alice@example.com"]);
    expect(mail.rawHeaders["to"]).toEqual(["bob@example.com"]);
    expect(mail.headers.from).toBe("alice@example.com");
    expect(mail.headers.to).toEqual(["bob@example.com"]);
  });

  test("names no id in either parser for a lone-LF message", async () => {
    // CRLF is the sole line terminator (RFC 5321 section 2.3.8, section
    // 4.1.1.4). `decodeMail` refuses these bytes, so the claim-check id
    // derived for them must not be one read out of the text it refused.
    const raw = rawBytes("Message-ID: <lf@example.com>\n\nbody");
    expect(() => decodeMail(raw)).toThrow(/must break its lines with CRLF/);
    expect(parseMessageIdHeader(raw)).toBeNull();
    expect(await deriveMessageId(raw)).toMatch(/^[0-9a-f]{64}$/);
  });

  test("resolves a leading-WSP line to no field in either parser", async () => {
    // A field begins with a printable name character (RFC 2822 section 2.2),
    // so the smuggled line continues nothing and names no field. Reading it as
    // one made the stored envelope id disagree with the dedup key.
    const raw = rawBytes(
      " Message-ID: <smuggled@evil.test>\r\n" +
        "Message-ID: <real@example.com>\r\n" +
        "\r\nbody",
    );
    const mail = decodeMail(raw);
    expect(mail.headers.messageId).toBe("<real@example.com>");
    expect(mail.rawHeaders["message-id"]).toEqual(["<real@example.com>"]);
    expect(await deriveMessageId(raw)).toBe("<real@example.com>");

    const solo = rawBytes(" Message-ID: <wsp@example.com>\r\n\r\nbody");
    const soloMail = decodeMail(solo);
    expect(soloMail.headers.messageId).toBeUndefined();
    expect(soloMail.rawHeaders["message-id"]).toBeUndefined();
    expect(parseMessageIdHeader(solo)).toBeNull();
    expect(await deriveMessageId(solo)).toMatch(/^[0-9a-f]{64}$/);
  });

  test("throws on a multipart part with no boundary rather than dropping it", () => {
    expect(() =>
      decodeMail(
        rawBytes("Content-Type: multipart/mixed\r\n\r\nlost inner content"),
      ),
    ).toThrow(/no boundary/);
  });
});

describe("Content-Transfer-Encoding normalization", () => {
  test("accepts a mechanism that carries a trailing RFC 822 comment", () => {
    // RFC 2045 section 1: comments in a MIME header field have no semantic
    // content and are ignored during processing, so this names 7bit.
    const mail = decodeMail(
      rawBytes(
        "Content-Type: text/plain\r\n" +
          "Content-Transfer-Encoding: 7bit (default)\r\n\r\nhello world",
      ),
    );
    expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe(
      "hello world",
    );
  });

  test("accepts a leading comment and a nested comment", () => {
    const mail = decodeMail(
      rawBytes(
        "Content-Type: text/plain\r\n" +
          "Content-Transfer-Encoding: (per RFC 2045 (section 6.8)) base64" +
          "\r\n\r\naGVsbG8gd29ybGQ=",
      ),
    );
    expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe(
      "hello world",
    );
  });

  test("does not let a quoted close-paren end a comment early", () => {
    // RFC 822 section 3.4.5: a backslash quotes the next character, so the
    // escaped ')' is comment text rather than the comment terminator.
    const mail = decodeMail(
      rawBytes(
        "Content-Type: text/plain\r\n" +
          "Content-Transfer-Encoding: base64 (a \\) b)\r\n\r\naGk=",
      ),
    );
    expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe("hi");
  });

  test("treats a field naming no mechanism as the 7bit default", () => {
    // RFC 2045 section 6.1 defaults an ABSENT field to 7bit. A field that is
    // present but names nothing -- empty, or nothing but a comment -- has
    // declared no mechanism either, so it takes the same default. Routing it
    // to the unrecognised arm instead treats a body the sender never claimed
    // to have encoded as opaque bytes, which surfaces to a reader as mojibake
    // rather than the characters that were sent.
    for (const encoding of ["", "   ", "(just a comment)"]) {
      const email = parseMailToEmail(
        multipartWithEncoding(encoding, "caf\u00e9"),
        "sml_cte",
      );
      expect(email.bodyValues["1"]?.value).toBe("caf\u00e9");
      expect(email.bodyValues["1"]?.isEncodingProblem).toBe(false);
    }
  });

  test("ignores a parameter tail on the mechanism", () => {
    const mail = decodeMail(
      rawBytes(
        "Content-Type: text/plain\r\n" +
          "Content-Transfer-Encoding: base64; x-note=stray\r\n\r\naGk=",
      ),
    );
    expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe("hi");
  });

  test("returns an unrecognised mechanism's body as opaque bytes", () => {
    // RFC 2045 section 6.4: an entity with an unrecognised mechanism is
    // treated as application/octet-stream. Discarding it, or running it
    // through a text decode, both destroy the bytes the treatment preserves.
    const body = [0xff, 0xfe, 0x41, 0x00, 0x80];
    const mail = decodeMail(
      rawWithBody(
        "Content-Type: text/plain\r\n" +
          "Content-Transfer-Encoding: x-uuencode\r\n\r\n",
        body,
      ),
    );
    expect(mail.parts).toHaveLength(1);
    expect(Array.from(mail.parts[0]?.content ?? new Uint8Array())).toEqual(
      body,
    );
  });

  test("still throws on a malformed body under a recognised mechanism", () => {
    // A malformed base64 body is a different condition from an unrecognised
    // mechanism, and attachment integrity depends on it surfacing.
    expect(() =>
      decodeMail(
        rawBytes(
          "Content-Type: text/plain\r\n" +
            "Content-Transfer-Encoding: base64\r\n\r\n!!! not base64 !!!",
        ),
      ),
    ).toThrow();
  });

  test("parseMailToEmail decodes base64 through an RFC 822 comment", () => {
    const email = parseMailToEmail(
      multipartWithEncoding("base64 (RFC 2045)", "aGVsbG8="),
      "sml_cte_comment",
    );
    expect(email.bodyValues["1"]?.value).toBe("hello");
    expect(email.bodyValues["1"]?.isEncodingProblem).toBe(false);
  });

  test("parseMailToEmail carries an unrecognised mechanism's bytes intact", () => {
    // The body is valid UTF-8 for a non-ASCII character, so a UTF-8 decode
    // would fold its two bytes into one code point and lose the octets that
    // octet-stream treatment exists to preserve.
    const email = parseMailToEmail(
      multipartWithEncoding("x-weird", "café"),
      "sml_cte_unknown",
    );
    expect(codeUnits(email.bodyValues["1"]?.value ?? "")).toEqual([
      0x63, 0x61, 0x66, 0xc3, 0xa9,
    ]);
    expect(email.bodyValues["1"]?.isEncodingProblem).toBe(true);
  });

  // RFC 2045 section 6.4 is one instruction with two halves: an entity whose
  // mechanism is unrecognised is not interpreted, AND it is treated as
  // application/octet-stream. The reported content type is the second half.
  // Each row pairs a mechanism with the type a part reports under it and the
  // body that mechanism yields, so a recognised mechanism is the control that
  // keeps its declared type and is decoded.
  const reportedTypeCases: {
    mechanism: string;
    textPartBody: string;
    decodedText: string;
    attachmentBody: string;
    reportedTextType: string;
    reportedAttachmentType: string;
  }[] = [
    {
      mechanism: "7bit",
      textPartBody: "hello",
      decodedText: "hello",
      attachmentBody: "pdf-bytes",
      reportedTextType: "text/plain",
      reportedAttachmentType: "application/pdf",
    },
    {
      // No comment to replace, so the value reaches the match untouched.
      mechanism: "base64",
      textPartBody: "aGVsbG8=",
      decodedText: "hello",
      attachmentBody: "cGRmLWJ5dGVz",
      reportedTextType: "text/plain",
      reportedAttachmentType: "application/pdf",
    },
    {
      mechanism: "quoted-printable",
      textPartBody: "hello",
      decodedText: "hello",
      attachmentBody: "pdf-bytes",
      reportedTextType: "text/plain",
      reportedAttachmentType: "application/pdf",
    },
    {
      mechanism: "x-uuencode",
      textPartBody: "hello",
      decodedText: "hello",
      attachmentBody: "begin 644 report.pdf",
      reportedTextType: "application/octet-stream",
      reportedAttachmentType: "application/octet-stream",
    },
    {
      // A trailing comment leaves the token before it alone, so this row
      // names x-weird and relabels for the same reason the row above does.
      mechanism: "x-weird (not a mechanism)",
      textPartBody: "hello",
      decodedText: "hello",
      attachmentBody: "weird-bytes",
      reportedTextType: "application/octet-stream",
      reportedAttachmentType: "application/octet-stream",
    },
    {
      // A run of CFWS between two lexical tokens is semantically a single
      // space (RFC 2822 section 3.2.3), so a comment separates the tokens it
      // sits between rather than joining them. This value is the two
      // tokens `ba` and `se64`; neither names a mechanism, and a single
      // mechanism is all RFC 2045 section 6.1 admits. Replacing the comment
      // with nothing instead reads it as `base64` and hands an attacker a
      // body a strict peer leaves opaque and we decode.
      mechanism: "ba(c)se64",
      textPartBody: "aGVsbG8=",
      decodedText: "aGVsbG8=",
      attachmentBody: "cGRmLWJ5dGVz",
      reportedTextType: "application/octet-stream",
      reportedAttachmentType: "application/octet-stream",
    },
    {
      // The same smuggle with the comment at a different interior offset.
      mechanism: "base(x)64",
      textPartBody: "aGVsbG8=",
      decodedText: "aGVsbG8=",
      attachmentBody: "cGRmLWJ5dGVz",
      reportedTextType: "application/octet-stream",
      reportedAttachmentType: "application/octet-stream",
    },
    {
      // One comment between every pair of characters, so no two characters
      // may be joined at all.
      mechanism: "b(1)a(2)s(3)e(4)6(5)4",
      textPartBody: "aGVsbG8=",
      decodedText: "aGVsbG8=",
      attachmentBody: "cGRmLWJ5dGVz",
      reportedTextType: "application/octet-stream",
      reportedAttachmentType: "application/octet-stream",
    },
    {
      // Separating the tokens must not cost a legal value its mechanism:
      // this one still names 7bit once the trailing comment becomes a space
      // and the value is trimmed.
      mechanism: "7bit (default)",
      textPartBody: "hello",
      decodedText: "hello",
      attachmentBody: "pdf-bytes",
      reportedTextType: "text/plain",
      reportedAttachmentType: "application/pdf",
    },
    {
      // Nothing but a comment still names no mechanism, which RFC 2045
      // section 6.1 defaults to 7bit rather than treating as unrecognised.
      mechanism: "(just a comment)",
      textPartBody: "hello",
      decodedText: "hello",
      attachmentBody: "pdf-bytes",
      reportedTextType: "text/plain",
      reportedAttachmentType: "application/pdf",
    },
  ];

  test("decodes a body part by the mechanism its value resolves to", () => {
    // The companion to the relabel below: the type says how the bytes may be
    // treated, this says which bytes the reader gets. A value that resolves
    // to no mechanism is not decoded, so its body arrives as it was sent.
    for (const c of reportedTypeCases) {
      const mail = decodeMail(
        multipartWithEncoding(c.mechanism, c.textPartBody),
      );
      expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe(
        c.decodedText,
      );

      // The JMAP projection is a second decoder over the same field, so it
      // reads the same value the same way.
      const email = parseMailToEmail(
        multipartWithEncoding(c.mechanism, c.textPartBody),
        "sml_cte_decoded_body",
      );
      expect(email.bodyValues["1"]?.value).toBe(c.decodedText);
    }
  });

  test("reads a single-part message's own transfer encoding on both paths", () => {
    // A single-part message carries its transfer encoding in the message
    // headers, and `parseMailToEmail` reaches the decoder through a part it
    // reconstructs from them. A reconstruction that carries only the content
    // type leaves the decoder with the RFC 2045 section 6.1 default, so a
    // base64 body arrives still encoded and an unrecognised mechanism's
    // octets are interpreted as text -- and the two entry points disagree on
    // the same bytes. Each row asserts the pair agrees.
    for (const c of reportedTypeCases) {
      const raw = singlePartWithEncoding(c.mechanism, c.textPartBody);

      const mail = decodeMail(raw);
      expect(mail.parts).toHaveLength(1);
      expect(mail.parts[0]?.contentType).toBe(c.reportedTextType);
      expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe(
        c.decodedText,
      );

      const email = parseMailToEmail(raw, "sml_cte_single");
      expect(email.textBody).toEqual([
        { partId: "1", type: c.reportedTextType },
      ]);
      expect(email.bodyValues["1"]?.value).toBe(c.decodedText);
      expect(email.bodyValues["1"]?.isEncodingProblem).toBe(
        c.reportedTextType === "application/octet-stream",
      );
    }
  });

  test("keeps a single-part unrecognised mechanism's octets intact", () => {
    // The octets, not just the mechanism: a UTF-8 decode folds the two bytes
    // of a non-ASCII character into one code point, which is the loss that
    // octet-stream treatment exists to prevent. `decodeMail` already returns
    // the bytes; the JMAP projection must widen the same ones.
    const raw = singlePartWithEncoding("x-weird", "café");
    const octets = [0x63, 0x61, 0x66, 0xc3, 0xa9];

    expect(Array.from(decodeMail(raw).parts[0]?.content ?? [])).toEqual(octets);

    const email = parseMailToEmail(raw, "sml_cte_single_octets");
    expect(codeUnits(email.bodyValues["1"]?.value ?? "")).toEqual(octets);
    expect(email.bodyValues["1"]?.isEncodingProblem).toBe(true);
  });

  test("reports a body part under an unrecognised mechanism as octet-stream", () => {
    // Without the relabel a consumer is told the part holds text in the
    // charset it declared while it holds bytes that are not that text, and
    // every consumer that keys on the type acts on that label.
    for (const c of reportedTypeCases) {
      const mail = decodeMail(
        multipartWithEncoding(c.mechanism, c.textPartBody),
      );
      expect(mail.parts).toHaveLength(1);
      expect(mail.parts[0]?.contentType).toBe(c.reportedTextType);

      // The JMAP projection reports the same type. The part stays listed in
      // textBody under it rather than dropping out of the list, so a consumer
      // that walks textBody to find the body still learns the part exists.
      const email = parseMailToEmail(
        multipartWithEncoding(c.mechanism, c.textPartBody),
        "sml_cte_reported_body",
      );
      expect(email.textBody).toEqual([
        { partId: "1", type: c.reportedTextType },
      ]);
      expect(email.attachments).toEqual([]);
      expect(email.bodyValues["1"]).toBeDefined();
    }
  });

  test("reports an attachment under an unrecognised mechanism as octet-stream", () => {
    for (const c of reportedTypeCases) {
      const mail = decodeMail(
        multipartWithAttachmentEncoding(c.mechanism, c.attachmentBody),
      );
      expect(mail.parts).toHaveLength(2);
      const att = mail.parts[1];
      expect(att?.contentType).toBe(c.reportedAttachmentType);
      // Relabelling the type does not cost the reader the part's identity.
      expect(att?.filename).toBe("report.pdf");
      expect(att?.disposition).toBe("attachment");
      // The body part beside it is untouched: the relabel is per part.
      expect(mail.parts[0]?.contentType).toBe("text/plain");

      const email = parseMailToEmail(
        multipartWithAttachmentEncoding(c.mechanism, c.attachmentBody),
        "sml_cte_reported_att",
      );
      expect(email.attachments[0]?.type).toBe(c.reportedAttachmentType);
      expect(email.attachments[0]?.name).toBe("report.pdf");

      // The MessageAttachment projection a conversation turn is built from.
      const extracted = extractAttachments(
        signedWithAttachmentEncoding(c.mechanism, c.attachmentBody),
      );
      expect(extracted).toHaveLength(1);
      expect(extracted[0]?.contentType).toBe(c.reportedAttachmentType);
      expect(extracted[0]?.name).toBe("report.pdf");
    }
  });

  test("keeps the encoding-problem flag beside the relabelled type", () => {
    // The flag and the relabel are complementary, not alternatives: the flag
    // says the decode did not happen, the type says what the undecoded bytes
    // may be treated as. Routing on the relabelled type would reclassify this
    // part as an attachment and the flag would never be written.
    const email = parseMailToEmail(
      multipartWithEncoding("x-weird", "café"),
      "sml_cte_flag_and_type",
    );
    expect(email.textBody[0]?.type).toBe("application/octet-stream");
    expect(email.bodyValues["1"]?.isEncodingProblem).toBe(true);
  });

  test("still walks into a multipart wrapper declaring an unrecognised mechanism", () => {
    // A wrapper's children are located from its declared type, so relabelling
    // the wrapper would make them unreachable and drop their content -- the
    // opposite of what section 6.4 protects. The relabel is leaf-only.
    const mail = decodeMail(
      rawBytes(
        [
          "From: alice@example.com",
          'Content-Type: multipart/mixed; boundary="wrap"',
          "Content-Transfer-Encoding: x-weird",
          "",
          "--wrap",
          "Content-Type: text/plain",
          "",
          "inner content",
          "--wrap--",
          "",
        ].join("\r\n"),
      ),
    );
    expect(mail.parts).toHaveLength(1);
    expect(mail.parts[0]?.contentType).toBe("text/plain");
    expect(new TextDecoder().decode(mail.parts[0]?.content)).toBe(
      "inner content",
    );
  });
});

describe("isMail", () => {
  function validMail(): Record<string, unknown> {
    const parts: MailPart[] = [
      { contentType: "text/plain", ref: "mail-part:///r/m/0-body", text: "hi" },
    ];
    return { headers: { from: "a@b", to: ["c@d"] }, rawHeaders: {}, parts };
  }

  test("accepts a minimal valid Mail shape", () => {
    expect(isMail(validMail())).toBe(true);
  });

  test("rejects rawHeaders that is not a record of string arrays", () => {
    expect(isMail({ ...validMail(), rawHeaders: { subject: ["hi"] } })).toBe(
      true,
    );
    expect(isMail({ ...validMail(), rawHeaders: { subject: "hi" } })).toBe(
      false,
    );
    expect(isMail({ ...validMail(), rawHeaders: { subject: [1] } })).toBe(
      false,
    );
    expect(isMail({ ...validMail(), rawHeaders: "subject" })).toBe(false);
    expect(isMail({ ...validMail(), rawHeaders: null })).toBe(false);
  });

  test("rejects a MessagePart-shaped part (bytes, no ref)", () => {
    // decodeMail returns MessagePart[] (content bytes); only the committed
    // MailPart[] (ref) is a Mail, so the in-memory decode is NOT a Mail.
    expect(
      isMail({
        ...validMail(),
        parts: [{ contentType: "text/plain", content: new Uint8Array() }],
      }),
    ).toBe(false);
  });

  test("rejects an undeclared key at the top level or on a part", () => {
    expect(isMail({ ...validMail(), extra: 1 })).toBe(false);
    expect(
      isMail({
        ...validMail(),
        parts: [{ contentType: "text/plain", ref: "r", nope: 1 }],
      }),
    ).toBe(false);
  });

  test("rejects a non-array parts and a bad disposition literal", () => {
    expect(isMail({ ...validMail(), parts: {} })).toBe(false);
    expect(
      isMail({
        ...validMail(),
        parts: [{ contentType: "text/plain", ref: "r", disposition: "bogus" }],
      }),
    ).toBe(false);
  });

  test("rejects headers missing the to a consumer dereferences", () => {
    expect(isMail({ ...validMail(), headers: {} })).toBe(false);
    expect(isMail({ ...validMail(), headers: { from: "a@b" } })).toBe(false);
  });

  test("accepts a Mail carrying no originator", () => {
    // Mail with no usable From is still mail. Rejecting it here routed such a
    // message down the arbitrary-step-value path, where it was stringified
    // into a text turn with no error and no log.
    expect(isMail({ ...validMail(), headers: { to: ["c@d"] } })).toBe(true);
  });
});
