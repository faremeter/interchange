import { describe, test, expect } from "bun:test";

import { parseHeaderFields } from "./mime";

const encode = (text: string) => new TextEncoder().encode(text);

describe("parseHeaderFields", () => {
  test("keeps every value of a repeated field, in message order", () => {
    const fields = parseHeaderFields(
      encode(
        "From: a@x.test\r\nTo: b@x.test\r\nFrom: c@x.test\r\n\r\nbody From: d@x.test",
      ),
    );
    expect(fields["from"]).toEqual(["a@x.test", "c@x.test"]);
    expect(fields["to"]).toEqual(["b@x.test"]);
  });

  test("keys fields by lowercase name and unfolds continuation lines", () => {
    const fields = parseHeaderFields(
      encode("Message-ID: <a@x.test>\r\nSUBJECT: one\r\n two\r\n\r\n"),
    );
    expect(fields["message-id"]).toEqual(["<a@x.test>"]);
    expect(fields["subject"]).toEqual(["one two"]);
  });

  test("throws for a bare line break in the header section", () => {
    expect(() =>
      parseHeaderFields(encode("From: a@x.test\nTo: b@x.test\r\n\r\n")),
    ).toThrow();
  });
});
