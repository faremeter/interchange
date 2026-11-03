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
    consumer: "",
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
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(12, 0)))).toBe(
        true,
      );
    });

    test("at start boundary returns true", () => {
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(9, 0)))).toBe(
        true,
      );
    });

    test("at end boundary returns false (exclusive)", () => {
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(17, 0)))).toBe(
        false,
      );
    });

    test("before window returns false", () => {
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(8, 59)))).toBe(
        false,
      );
    });

    test("after window returns false", () => {
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(17, 1)))).toBe(
        false,
      );
    });
  });

  describe("cross-midnight window (after >= before)", () => {
    const window = { after: "22:00", before: "06:00", timezone: "UTC" };

    test("late night returns true", () => {
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(23, 0)))).toBe(
        true,
      );
    });

    test("early morning returns true", () => {
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(3, 0)))).toBe(
        true,
      );
    });

    test("at start boundary returns true", () => {
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(22, 0)))).toBe(
        true,
      );
    });

    test("at end boundary returns false (exclusive)", () => {
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(6, 0)))).toBe(
        false,
      );
    });

    test("daytime returns false", () => {
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(12, 0)))).toBe(
        false,
      );
    });
  });

  describe("midnight edge cases", () => {
    test("midnight in a cross-midnight window returns true", () => {
      const window = { after: "22:00", before: "06:00", timezone: "UTC" };
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(0, 0)))).toBe(
        true,
      );
    });

    test("00:01 in a cross-midnight window returns true", () => {
      const window = { after: "22:00", before: "06:00", timezone: "UTC" };
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(0, 1)))).toBe(
        true,
      );
    });

    test("midnight in a normal window starting at 00:00", () => {
      const window = { after: "00:00", before: "08:00", timezone: "UTC" };
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(0, 0)))).toBe(
        true,
      );
    });
  });

  describe("timezone conversion", () => {
    test("evaluates against a UTC offset", () => {
      // 00:30 UTC = 05:30 at +05:00
      const window = { after: "05:00", before: "06:00", timezone: "+05:00" };
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(0, 30)))).toBe(
        true,
      );
    });

    test("converts to specified timezone", () => {
      // 20:00 UTC = 13:00 America/Los_Angeles (PDT, UTC-7)
      const window = {
        after: "12:00",
        before: "14:00",
        timezone: "America/Los_Angeles",
      };
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(20, 0)))).toBe(
        true,
      );
    });

    test("outside window in specified timezone", () => {
      // 10:00 UTC = 03:00 America/Los_Angeles (PDT)
      const window = {
        after: "09:00",
        before: "17:00",
        timezone: "America/Los_Angeles",
      };
      expect(timeWindowEvaluator.evaluate(window, makeCtx(utc(10, 0)))).toBe(
        false,
      );
    });
  });

  describe("validation", () => {
    const ctx = makeCtx(utc(12, 0));
    const valid = { after: "09:00", before: "17:00", timezone: "UTC" };

    const invalid: [string, unknown, string][] = [
      ["a non-object value", "not an object", "must be an object"],
      ["a null value", null, "must be an object"],
      [
        "a missing after",
        { before: "17:00", timezone: "UTC" },
        "after must be a string",
      ],
      [
        "a missing before",
        { after: "09:00", timezone: "UTC" },
        "before must be a string",
      ],
      [
        "a missing timezone",
        { after: "09:00", before: "17:00" },
        "timezone must be a string",
      ],
      [
        "the wrong key names",
        { start: "09:00", end: "17:00", timezone: "UTC" },
        "start must be removed",
      ],
      ["an undeclared key", { ...valid, extra: 1 }, "extra must be removed"],
      [
        "a malformed time string",
        { ...valid, after: "9:00" },
        'after must be an HH:MM time from 00:00 to 23:59 (was "9:00")',
      ],
      [
        "an out-of-range hour",
        { ...valid, after: "25:00" },
        "after must be an HH:MM time",
      ],
      [
        "an out-of-range minute",
        { ...valid, before: "09:60" },
        "before must be an HH:MM time",
      ],
      [
        "equal bounds",
        { ...valid, after: "12:00", before: "12:00" },
        'must be a window whose "after" and "before" differ',
      ],
      [
        "an unknown timezone",
        { ...valid, timezone: "Mars/Olympus" },
        "timezone must be a time zone name or UTC offset",
      ],
    ];

    test("accepts a valid window", () => {
      expect(timeWindowEvaluator.validate(valid)).toBeNull();
    });

    test("accepts a UTC offset and a zone name in any letter case", () => {
      for (const timezone of [
        "+05:30",
        "-0800",
        "utc",
        "america/los_angeles",
      ]) {
        expect(timeWindowEvaluator.validate({ ...valid, timezone })).toBeNull();
      }
    });

    for (const [label, value, message] of invalid) {
      test(`validate rejects ${label}`, () => {
        const problem = timeWindowEvaluator.validate(value);
        expect(problem).toStartWith("time_window: ");
        expect(problem).toContain(message);
      });

      test(`evaluate throws on ${label}`, () => {
        expect(() => timeWindowEvaluator.evaluate(value, ctx)).toThrow(message);
      });
    }

    test("reports every problem on one line", () => {
      const problem = timeWindowEvaluator.validate({ start: "09:00" });
      expect(problem).not.toContain("\n");
      expect(problem).toContain("after must be a string");
      expect(problem).toContain("timezone must be a string");
      expect(problem).toContain("start must be removed");
    });
  });
});
