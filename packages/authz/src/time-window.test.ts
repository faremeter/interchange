import { describe, test, expect } from "bun:test";

import { timeWindowEvaluator } from "./time-window";
import type { ConditionContext } from "./types";

function makeCtx(date: Date): ConditionContext {
  return {
    now: date,
    resource: "tool:bash",
    action: "invoke",
    principalId: "p-1",
    tenantId: "t-1",
  };
}

// Helper: create a Date at a specific UTC time.
function utc(hour: number, minute: number): Date {
  return new Date(Date.UTC(2026, 3, 16, hour, minute, 0));
}

describe("timeWindowEvaluator", () => {
  describe("normal window (after < before)", () => {
    const window = { after: "09:00", before: "17:00", timezone: "UTC" };

    test("inside window returns true", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(12, 0)))).toBe(true);
    });

    test("at start boundary returns true", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(9, 0)))).toBe(true);
    });

    test("at end boundary returns false (exclusive)", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(17, 0)))).toBe(false);
    });

    test("before window returns false", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(8, 59)))).toBe(false);
    });

    test("after window returns false", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(17, 1)))).toBe(false);
    });
  });

  describe("cross-midnight window (after >= before)", () => {
    const window = { after: "22:00", before: "06:00", timezone: "UTC" };

    test("late night returns true", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(23, 0)))).toBe(true);
    });

    test("early morning returns true", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(3, 0)))).toBe(true);
    });

    test("at start boundary returns true", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(22, 0)))).toBe(true);
    });

    test("at end boundary returns false (exclusive)", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(6, 0)))).toBe(false);
    });

    test("daytime returns false", () => {
      expect(timeWindowEvaluator(window, makeCtx(utc(12, 0)))).toBe(false);
    });
  });

  describe("midnight edge cases", () => {
    test("midnight in a cross-midnight window returns true", () => {
      const window = { after: "22:00", before: "06:00", timezone: "UTC" };
      expect(timeWindowEvaluator(window, makeCtx(utc(0, 0)))).toBe(true);
    });

    test("00:01 in a cross-midnight window returns true", () => {
      const window = { after: "22:00", before: "06:00", timezone: "UTC" };
      expect(timeWindowEvaluator(window, makeCtx(utc(0, 1)))).toBe(true);
    });

    test("midnight in a normal window starting at 00:00", () => {
      const window = { after: "00:00", before: "08:00", timezone: "UTC" };
      expect(timeWindowEvaluator(window, makeCtx(utc(0, 0)))).toBe(true);
    });
  });

  describe("timezone conversion", () => {
    test("converts to specified timezone", () => {
      // 20:00 UTC = 13:00 America/Los_Angeles (PDT, UTC-7)
      const window = {
        after: "12:00",
        before: "14:00",
        timezone: "America/Los_Angeles",
      };
      expect(timeWindowEvaluator(window, makeCtx(utc(20, 0)))).toBe(true);
    });

    test("outside window in specified timezone", () => {
      // 10:00 UTC = 03:00 America/Los_Angeles (PDT)
      const window = {
        after: "09:00",
        before: "17:00",
        timezone: "America/Los_Angeles",
      };
      expect(timeWindowEvaluator(window, makeCtx(utc(10, 0)))).toBe(false);
    });
  });

  describe("validation", () => {
    const ctx = makeCtx(utc(12, 0));

    test("throws on non-object value", () => {
      expect(() => timeWindowEvaluator("not an object", ctx)).toThrow(
        "expected an object",
      );
    });

    test("throws on null value", () => {
      expect(() => timeWindowEvaluator(null, ctx)).toThrow(
        "expected an object",
      );
    });

    test("throws on missing after", () => {
      expect(() =>
        timeWindowEvaluator({ before: "17:00", timezone: "UTC" }, ctx),
      ).toThrow('"after" must be a string');
    });

    test("throws on missing before", () => {
      expect(() =>
        timeWindowEvaluator({ after: "09:00", timezone: "UTC" }, ctx),
      ).toThrow('"before" must be a string');
    });

    test("throws on missing timezone", () => {
      expect(() =>
        timeWindowEvaluator({ after: "09:00", before: "17:00" }, ctx),
      ).toThrow('"timezone" is required');
    });

    test("throws on malformed time string", () => {
      expect(() =>
        timeWindowEvaluator(
          { after: "9:00", before: "17:00", timezone: "UTC" },
          ctx,
        ),
      ).toThrow("must be HH:MM");
    });

    test("throws on out-of-range hour", () => {
      expect(() =>
        timeWindowEvaluator(
          { after: "25:00", before: "17:00", timezone: "UTC" },
          ctx,
        ),
      ).toThrow("out of range");
    });

    test("throws on out-of-range minute", () => {
      expect(() =>
        timeWindowEvaluator(
          { after: "09:60", before: "17:00", timezone: "UTC" },
          ctx,
        ),
      ).toThrow("out of range");
    });

    test("throws when after equals before", () => {
      expect(() =>
        timeWindowEvaluator(
          { after: "12:00", before: "12:00", timezone: "UTC" },
          ctx,
        ),
      ).toThrow("zero-duration window");
    });

    test("throws on invalid timezone", () => {
      expect(() =>
        timeWindowEvaluator(
          { after: "09:00", before: "17:00", timezone: "Mars/Olympus" },
          ctx,
        ),
      ).toThrow();
    });
  });
});
