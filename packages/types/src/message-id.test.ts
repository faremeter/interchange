import { describe, test, expect } from "bun:test";

import {
  DELIVERY_KEY_PREFIX,
  deriveDeliveryKey,
  deriveMessageId,
  parseMessageIdHeader,
} from "./message-id";

const encoder = new TextEncoder();

describe("deriveMessageId", () => {
  test("returns the Message-ID header value verbatim when present", async () => {
    const raw = encoder.encode(
      [
        "From: a@example.com",
        "To: b@example.com",
        "Message-ID: <run-1@example.com>",
        "",
        "body",
      ].join("\r\n"),
    );
    expect(await deriveMessageId(raw)).toBe("<run-1@example.com>");
  });

  test("is case-insensitive on the header name", async () => {
    const raw = encoder.encode(
      ["message-id: <lower@example.com>", "", "body"].join("\r\n"),
    );
    expect(await deriveMessageId(raw)).toBe("<lower@example.com>");
  });

  test("names no id in a lone-LF message, taking the digest instead", async () => {
    // CRLF is the sole line terminator (RFC 5321 section 2.3.8, section
    // 4.1.1.4), so a lone LF ends no field and `@intx/mime` refuses the
    // message outright. Reading an id out of that text would give the two
    // parsers different answers about the same bytes.
    const raw = encoder.encode(
      ["Message-ID: <lf@example.com>", "", "body"].join("\n"),
    );
    expect(parseMessageIdHeader(raw)).toBeNull();
    expect(await deriveMessageId(raw)).toMatch(/^[0-9a-f]{64}$/);
  });

  test("resolves no id from a bare break inside a field body", () => {
    // The smuggled field would otherwise name the id for a message whose real
    // `Message-ID` is the one the sender wrote above it.
    const raw = encoder.encode(
      "From: a@example.com\nMessage-ID: <smuggled@evil.test>\r\n\r\nbody",
    );
    expect(parseMessageIdHeader(raw)).toBeNull();
  });

  test("names no id for an LF-only header section, whatever ends it", async () => {
    // A whole-message search for the `CRLF CRLF` separator finds the one a
    // sender left in the body and reads the header section from there, so the id
    // absorbs the fields after the first and the head of the body. This id is
    // the claim-check dedup key, a stored filename and a database join value, so
    // no body bytes may reach it.
    const bodyTerminated = encoder.encode(
      "Message-ID: <a@b>\nSubject: Hi\n\nbody\r\n\r\ntail\n",
    );
    expect(parseMessageIdHeader(bodyTerminated)).toBeNull();
    expect(await deriveMessageId(bodyTerminated)).toMatch(/^[0-9a-f]{64}$/);

    const crlfTerminated = encoder.encode(
      "Message-ID: <a@b>\nSubject: Hi\r\n\r\nbody",
    );
    expect(parseMessageIdHeader(crlfTerminated)).toBeNull();
    expect(await deriveMessageId(crlfTerminated)).toMatch(/^[0-9a-f]{64}$/);
  });

  test("names no id when an LF LF ends the header section", () => {
    // The blank line is a pair of line breaks of its own. Honouring a bare pair
    // lets a sender who controls one field body end the section early and strip
    // the `Message-ID` after it.
    const raw = encoder.encode(
      "From: a@example.com\r\nSubject: Hi\n\n" +
        "Message-ID: <stripped@example.com>\r\n\r\nbody",
    );
    expect(parseMessageIdHeader(raw)).toBeNull();
  });

  test("reads the id at the first blank line when the body carries another", () => {
    const raw = encoder.encode(
      "Message-ID: <first@example.com>\r\n\r\nbody\r\n\r\ntail\r\n",
    );
    expect(parseMessageIdHeader(raw)).toBe("<first@example.com>");
  });

  test("falls back to a sha256 hex digest with no Message-ID header", async () => {
    const raw = encoder.encode("From: a@example.com\r\n\r\nbody");
    const derived = await deriveMessageId(raw);
    // 32-byte sha256 rendered as lowercase hex.
    expect(derived).toMatch(/^[0-9a-f]{64}$/);
    // Deterministic for the same bytes.
    expect(await deriveMessageId(raw)).toBe(derived);
  });

  test("parseMessageIdHeader returns null when absent", () => {
    const raw = encoder.encode("From: a@example.com\r\n\r\nbody");
    expect(parseMessageIdHeader(raw)).toBeNull();
  });

  test("treats a blank Message-ID header as naming no id", () => {
    // RFC 2822 defines `Message-ID` as `msg-id`, which admits no empty value,
    // so a header a sender left blank names nothing.
    const raw = encoder.encode(
      ["From: a@example.com", "Message-ID:   ", "", "body"].join("\r\n"),
    );
    expect(parseMessageIdHeader(raw)).toBeNull();
  });

  test("two different messages with blank Message-ID headers derive different ids", async () => {
    // The derived id is the dedup key that makes the same bytes consume once.
    // Answering `""` for a blank header would make it one key two different
    // messages share, so the second would be discarded as a redelivery of the
    // first. Each falls back to its own digest instead.
    const first = encoder.encode(
      ["From: a@example.com", "Message-ID:", "", "first body"].join("\r\n"),
    );
    const second = encoder.encode(
      ["From: b@example.com", "Message-ID:", "", "second body"].join("\r\n"),
    );

    const firstId = await deriveMessageId(first);
    const secondId = await deriveMessageId(second);

    expect(firstId).toMatch(/^[0-9a-f]{64}$/);
    expect(secondId).toMatch(/^[0-9a-f]{64}$/);
    expect(firstId).not.toBe(secondId);
  });
});

describe("deriveDeliveryKey", () => {
  test("matches a digest computed outside the function", async () => {
    // printf 'run_a@acme.localhost\0<caf\xc3\xa9-1@example.com>' | shasum -a 256
    expect(
      await deriveDeliveryKey(
        "run_a@acme.localhost",
        "<caf\u00e9-1@example.com>",
      ),
    ).toBe(
      "mail-4cf62b5d6c6d0b19b118d1d6aa409e76f3fa347627bf67ea9d1db415d6260060",
    );
  });

  test("has the prefix and a 64-digit hex digest", async () => {
    expect(await deriveDeliveryKey("a@example.com", "<m@example.com>")).toMatch(
      new RegExp(`^${DELIVERY_KEY_PREFIX}[0-9a-f]{64}$`),
    );
  });

  test("gives two senders of one Message-ID different keys", async () => {
    const id = "<m@example.com>";
    expect(await deriveDeliveryKey("a@example.com", id)).not.toBe(
      await deriveDeliveryKey("b@example.com", id),
    );
  });

  test("gives one sender's two Message-IDs different keys", async () => {
    expect(
      await deriveDeliveryKey("a@example.com", "<m1@example.com>"),
    ).not.toBe(await deriveDeliveryKey("a@example.com", "<m2@example.com>"));
  });

  test("compares the sender byte for byte", async () => {
    expect(await deriveDeliveryKey("a@example.com", "<m@x>")).not.toBe(
      await deriveDeliveryKey("A@example.com", "<m@x>"),
    );
  });

  test("hashes a NUL in the Message-ID and rejects one in the sender", async () => {
    // printf 'a\0b\0c' | shasum -a 256
    expect(await deriveDeliveryKey("a", "b\0c")).toBe(
      "mail-8badde10c760e9b702defb4b5e225de79c515b1d2a5cfb000e140f3c6fbb5629",
    );
    await expect(deriveDeliveryKey("a\0b", "c")).rejects.toThrow(/NUL/);
  });

  test("refuses an empty sender", async () => {
    await expect(deriveDeliveryKey("", "<m@x>")).rejects.toThrow(/empty/);
  });
});
