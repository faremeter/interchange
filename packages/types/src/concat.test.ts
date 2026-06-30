import { describe, test, expect } from "bun:test";

import { concatBytes } from "./concat";

describe("concatBytes", () => {
  test("returns an empty array for an empty list", () => {
    const out = concatBytes([]);
    expect(out).toEqual(new Uint8Array(0));
  });

  test("returns the single chunk unchanged in content", () => {
    const out = concatBytes([new Uint8Array([1, 2, 3])]);
    expect(out).toEqual(new Uint8Array([1, 2, 3]));
  });

  test("joins multiple chunks preserving order and bytes", () => {
    const out = concatBytes([
      new Uint8Array([1, 2]),
      new Uint8Array([3]),
      new Uint8Array([4, 5, 6]),
    ]);
    expect(out).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]));
  });

  test("skips empty chunks without disturbing the result", () => {
    const out = concatBytes([
      new Uint8Array([0xff]),
      new Uint8Array(0),
      new Uint8Array([0x00, 0x80]),
    ]);
    expect(out).toEqual(new Uint8Array([0xff, 0x00, 0x80]));
  });
});
