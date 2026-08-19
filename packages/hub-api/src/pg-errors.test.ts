import { describe, test, expect } from "bun:test";

import { isReferencedRowViolation } from "./pg-errors";

describe("isReferencedRowViolation", () => {
  test("matches a restrict_violation (23001) nested on the cause chain", () => {
    // Mirrors how drizzle surfaces a postgres.js error: the SQLSTATE-bearing
    // driver error is the `cause` of a wrapper error.
    const wrapped = new Error("query failed", {
      cause: Object.assign(new Error("update or delete violates fk"), {
        code: "23001",
      }),
    });
    expect(isReferencedRowViolation(wrapped)).toBe(true);
  });

  test("matches a foreign_key_violation (23503) on the top-level error", () => {
    const err = Object.assign(new Error("fk violation"), { code: "23503" });
    expect(isReferencedRowViolation(err)).toBe(true);
  });

  test("does not match an unrelated postgres error code", () => {
    const err = Object.assign(new Error("unique violation"), { code: "23505" });
    expect(isReferencedRowViolation(err)).toBe(false);
  });

  test("does not match an error with no code", () => {
    expect(isReferencedRowViolation(new Error("boom"))).toBe(false);
  });

  test("does not match non-error values", () => {
    expect(isReferencedRowViolation(null)).toBe(false);
    expect(isReferencedRowViolation("23001")).toBe(false);
    expect(isReferencedRowViolation(undefined)).toBe(false);
  });

  test("stops walking a self-referential cause chain", () => {
    const a: { cause?: unknown } = {};
    a.cause = a;
    expect(isReferencedRowViolation(a)).toBe(false);
  });
});
