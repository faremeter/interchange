import { describe, test, expect } from "bun:test";

import { base64urlEncode, base64urlDecode } from "./base64url";

function makeBytes(len: number): Uint8Array {
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    bytes[i] = (i * 31 + len * 17) % 256;
  }
  return bytes;
}

// Known-answer vectors pinning exact byte->string output. Chosen to cover the
// empty input, the 1/2/3-byte padding-strip boundaries, inputs whose standard
// base64 output carries `+` and `/` (so the `-`/`_` substitution is exercised),
// and high bytes 0x80-0xFF.
const VECTORS: { bytes: number[]; b64url: string }[] = [
  { bytes: [], b64url: "" },
  { bytes: [0x66], b64url: "Zg" },
  { bytes: [0x66, 0x6f], b64url: "Zm8" },
  { bytes: [0x66, 0x6f, 0x6f], b64url: "Zm9v" },
  { bytes: [0xfb, 0xf0], b64url: "-_A" },
  { bytes: [0x3e, 0x3f, 0xbf], b64url: "Pj-_" },
  { bytes: [0xff, 0xff, 0xff], b64url: "____" },
  { bytes: [0x80, 0x81, 0xfe, 0xff], b64url: "gIH-_w" },
];

describe("base64url", () => {
  test("round-trips encode then decode across lengths 0-300", () => {
    for (let len = 0; len <= 300; len++) {
      const bytes = makeBytes(len);
      const roundTripped = base64urlDecode(base64urlEncode(bytes));
      expect(roundTripped).toEqual(bytes);
    }
  });

  test("encodes and decodes known-answer vectors", () => {
    for (const v of VECTORS) {
      const bytes = new Uint8Array(v.bytes);
      expect(base64urlEncode(bytes)).toBe(v.b64url);
      expect(base64urlDecode(v.b64url)).toEqual(bytes);
    }
  });

  test("throws on input with non-base64 characters", () => {
    expect(() => base64urlDecode("@@@@")).toThrow();
    expect(() => base64urlDecode("not valid base64!!!")).toThrow();
  });
});
