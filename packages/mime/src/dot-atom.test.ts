import { describe, test, expect } from "bun:test";

import { isDotAtomText } from "./mime";

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
      "café",
    ]) {
      expect(isDotAtomText(value)).toBe(false);
    }
  });
});
