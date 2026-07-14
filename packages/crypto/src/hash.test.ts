import { describe, test, expect } from "bun:test";
import { hexEncode } from "@intx/types";

import { sha256 } from "./hash";

describe("sha256", () => {
  test("matches the known SHA-256 digest of a UTF-8 string", async () => {
    // NIST test vector: SHA-256("abc").
    const digest = await sha256("abc");
    expect(hexEncode(digest)).toBe(
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    );
  });

  test("returns a 32-byte digest", async () => {
    const digest = await sha256("");
    expect(digest).toBeInstanceOf(Uint8Array);
    expect(digest.length).toBe(32);
  });
});
