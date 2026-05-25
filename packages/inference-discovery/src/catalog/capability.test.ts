import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { CAPABILITIES, Capability } from "./capability";

describe("CAPABILITIES vocabulary", () => {
  test("declares exactly 31 capabilities", () => {
    expect(CAPABILITIES.length).toBe(31);
  });

  test("contains no duplicate names", () => {
    const seen = new Set(CAPABILITIES);
    expect(seen.size).toBe(CAPABILITIES.length);
  });

  test("function-calling has no -streaming variant", () => {
    expect(CAPABILITIES.includes("function-calling")).toBe(true);
    expect(
      (CAPABILITIES as readonly string[]).includes(
        "function-calling-streaming",
      ),
    ).toBe(false);
  });
});

describe("Capability validator", () => {
  test("accepts every name in CAPABILITIES", () => {
    for (const name of CAPABILITIES) {
      const result = Capability(name);
      expect(result instanceof type.errors).toBe(false);
      expect(result).toBe(name);
    }
  });

  test("rejects an unknown capability name", () => {
    const result = Capability("not-a-capability");
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a non-string input", () => {
    const result = Capability(42);
    expect(result instanceof type.errors).toBe(true);
  });
});
