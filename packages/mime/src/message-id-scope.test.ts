import { describe, test, expect } from "bun:test";

import { isMessageId } from "./mail-builder";
import {
  generateSenderScopedMessageId,
  isDotAtomText,
  senderMessageIdScope,
} from "./mime";

describe("senderMessageIdScope", () => {
  test("names the sender's local part and domain in lower case", () => {
    expect(senderMessageIdScope("Run_A-1@Acme.Example")).toBe(
      "run_a-1.acme.example",
    );
  });

  test("gives two senders two scopes, however their parts split", () => {
    // A dotted local part is refused, so `a.b@c.test` cannot take the scope
    // `a@b.c.test` has.
    expect(senderMessageIdScope("a@b.c.test")).toBe("a.b.c.test");
    expect(() => senderMessageIdScope("a.b@c.test")).toThrow();
  });

  test("refuses an address that has no atom local part", () => {
    for (const address of [
      "",
      "no-at-sign",
      "@domain.test",
      "local@",
      '"quoted"@domain.test',
      "a@b@c.test",
    ]) {
      expect(() => senderMessageIdScope(address)).toThrow();
    }
  });
});

describe("isDotAtomText", () => {
  test("accepts atoms joined by single dots", () => {
    expect(isDotAtomText("a")).toBe(true);
    expect(isDotAtomText("a.b-c_d")).toBe(true);
    expect(isDotAtomText("0d1f-2e")).toBe(true);
  });

  test("refuses empty atoms, control characters, spaces and non-ASCII text", () => {
    for (const value of [
      "",
      ".a",
      "a.",
      "a..b",
      "a\u0000b",
      "a\u007fb",
      "a b",
      "caf\u00e9",
    ]) {
      expect(isDotAtomText(value)).toBe(false);
    }
  });
});

describe("generateSenderScopedMessageId", () => {
  test("mints distinct message identifiers in the sender's scope", () => {
    const first = generateSenderScopedMessageId("run_a@acme.example");
    const second = generateSenderScopedMessageId("run_a@acme.example");
    expect(isMessageId(first)).toBe(true);
    expect(first.endsWith("@run_a.acme.example>")).toBe(true);
    expect(first).not.toBe(second);
  });
});
