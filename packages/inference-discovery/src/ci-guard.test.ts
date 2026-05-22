import { describe, test, expect } from "bun:test";
import { assertNotCI } from "./ci-guard";

describe("assertNotCI", () => {
  test("does nothing when CI is unset", () => {
    expect(() => assertNotCI({})).not.toThrow();
  });

  test("does nothing when CI is an empty string", () => {
    expect(() => assertNotCI({ CI: "" })).not.toThrow();
  });

  test("throws when CI is 'true'", () => {
    expect(() => assertNotCI({ CI: "true" })).toThrow(/must not run in CI/);
  });

  test("throws when CI is '1'", () => {
    expect(() => assertNotCI({ CI: "1" })).toThrow(/must not run in CI/);
  });

  test("reads process.env by default", () => {
    const original = process.env.CI;
    try {
      delete process.env.CI;
      expect(() => assertNotCI()).not.toThrow();
      process.env.CI = "yes";
      expect(() => assertNotCI()).toThrow(/must not run in CI/);
    } finally {
      if (original === undefined) {
        delete process.env.CI;
      } else {
        process.env.CI = original;
      }
    }
  });
});
