import { describe, expect, test } from "bun:test";

import { base64Encode } from "./base64";

// Hides the native encoder on one array, the way a runtime without
// Uint8Array.prototype.toBase64 presents it.
function withoutNativeEncoder(bytes: Uint8Array): Uint8Array {
  Object.defineProperty(bytes, "toBase64", { value: undefined });
  return bytes;
}

const paths = [
  { name: "native", prepare: (bytes: Uint8Array) => bytes },
  { name: "fallback", prepare: withoutNativeEncoder },
];

function bufferBase64(bytes: Uint8Array): string {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString(
    "base64",
  );
}

describe.each(paths)("base64Encode ($name)", ({ prepare }) => {
  test("encodes the RFC 4648 vectors", () => {
    const vectors: [string, string][] = [
      ["", ""],
      ["f", "Zg=="],
      ["fo", "Zm8="],
      ["foo", "Zm9v"],
      ["foob", "Zm9vYg=="],
      ["fooba", "Zm9vYmE="],
      ["foobar", "Zm9vYmFy"],
    ];
    for (const [input, expected] of vectors) {
      expect(base64Encode(prepare(new TextEncoder().encode(input)))).toBe(
        expected,
      );
    }
  });

  test("encodes every byte value with the standard alphabet", () => {
    const bytes = Uint8Array.from({ length: 256 }, (_, i) => i);
    const encoded = base64Encode(prepare(bytes));
    expect(encoded).toBe(bufferBase64(bytes));
    expect(encoded).toContain("+");
    expect(encoded).toContain("/");
  });

  test("encodes only the supplied view of a larger buffer", () => {
    const backing = Uint8Array.from({ length: 64 }, (_, i) => 255 - i);
    const view = backing.subarray(7, 18);
    expect(base64Encode(prepare(view))).toBe(bufferBase64(view));
  });

  test("encodes input spanning several fallback chunks", () => {
    const bytes = Uint8Array.from(
      { length: 24576 * 3 + 7 },
      (_, i) => (i * 31) % 256,
    );
    expect(base64Encode(prepare(bytes))).toBe(bufferBase64(bytes));
  });
});
