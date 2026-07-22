import { describe, test, expect } from "bun:test";

import { deriveMessageId, parseMessageIdHeader } from "./message-id";

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
      ["message-id: <lower@example.com>", "", "body"].join("\n"),
    );
    expect(await deriveMessageId(raw)).toBe("<lower@example.com>");
  });

  test("tolerates a lone-LF header boundary", async () => {
    const raw = encoder.encode(
      ["Message-ID: <lf@example.com>", "", "body"].join("\n"),
    );
    expect(await deriveMessageId(raw)).toBe("<lf@example.com>");
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
});
