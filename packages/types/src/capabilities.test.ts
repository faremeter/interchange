import { describe, expect, test } from "bun:test";
import { type } from "arktype";

import {
  CAPABILITIES,
  Capability,
  CURATED_CAPABILITIES,
  WIRE_CAPABILITIES,
} from "./capabilities";

describe("capability vocabulary", () => {
  test("is the wire capabilities plus the curated capabilities", () => {
    expect([...CAPABILITIES]).toEqual([
      ...WIRE_CAPABILITIES,
      ...CURATED_CAPABILITIES,
    ]);
  });

  test("has 29 wire capabilities and 2 curated capabilities", () => {
    expect(WIRE_CAPABILITIES.length).toBe(29);
    expect(CURATED_CAPABILITIES.length).toBe(2);
  });

  test("contains no duplicate names", () => {
    expect(new Set(CAPABILITIES).size).toBe(CAPABILITIES.length);
  });

  test("carries the curated capabilities the matrix cannot prove", () => {
    expect([...CURATED_CAPABILITIES]).toEqual([
      "long-context",
      "prompt-caching",
    ]);
  });
});

describe("Capability validator", () => {
  test("accepts every name in the vocabulary", () => {
    for (const name of CAPABILITIES) {
      expect(Capability(name)).toBe(name);
    }
  });

  test("rejects safety-classification, which production does not support", () => {
    // safety-classification lives only in the discovery probe vocabulary; the
    // production runtime has no support for it, so the catalog must not accept
    // it. This assertion pins that boundary.
    expect(Capability("safety-classification") instanceof type.errors).toBe(
      true,
    );
    expect(
      Capability("safety-classification-streaming") instanceof type.errors,
    ).toBe(true);
  });

  test("rejects an unknown capability name", () => {
    expect(Capability("telepathy") instanceof type.errors).toBe(true);
  });
});
