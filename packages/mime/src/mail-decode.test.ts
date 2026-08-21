import { describe, test, expect } from "bun:test";
import type { MailPart, MessageAttachment } from "@intx/types/runtime";
import { isMail } from "@intx/types/runtime";

import { assembleSignedContent, assembleMessage, decodeMail } from "./index";
import type { MessageHeaders } from "./index";

function rawBytes(text: string): Uint8Array {
  return new TextEncoder().encode(text);
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

  test("throws on a multipart part with no boundary rather than dropping it", () => {
    expect(() =>
      decodeMail(
        rawBytes("Content-Type: multipart/mixed\r\n\r\nlost inner content"),
      ),
    ).toThrow(/no boundary/);
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

  test("rejects headers missing the from/to a consumer dereferences", () => {
    expect(isMail({ ...validMail(), headers: {} })).toBe(false);
    expect(isMail({ ...validMail(), headers: { from: "a@b" } })).toBe(false);
  });
});
