// Time-window condition evaluator.
//
// Restricts a grant to a specific time-of-day window. The grant's
// conditions object should contain:
//
//   { time_window: { after: "09:00", before: "17:00", timezone: "America/Los_Angeles" } }
//
// The evaluator returns true if ctx.now falls within [after, before).
// Cross-midnight windows (after > before) are supported:
//   { after: "22:00", before: "06:00" } → active from 10pm to 6am.
//
// Timezone is required and must be a zone `Intl` accepts: an IANA name
// such as "America/Los_Angeles", or a UTC offset. Time strings must be
// HH:MM in 24-hour format, "after" and "before" must differ, and no other
// key is allowed. Each of these is an error.

import { type } from "arktype";

import { createConditionEvaluator } from "./condition-evaluator";

// Throws a RangeError for a zone that `Intl` does not know. Built per call
// rather than cached: `Intl` accepts any letter case and UTC offsets, so a
// cache keyed by the authored string would grow with every distinct input.
function formatterFor(timezone: string): Intl.DateTimeFormat {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

const CLOCK_TIME = /^([01]\d|2[0-3]):([0-5]\d)$/;

const ClockTime = type("string")
  .narrow(
    (s, ctx) =>
      CLOCK_TIME.test(s) || ctx.mustBe("an HH:MM time from 00:00 to 23:59"),
  )
  .pipe((s) => parseInt(s.slice(0, 2), 10) * 60 + parseInt(s.slice(3), 10));

const TimeZone = type("string").narrow((timezone, ctx) => {
  try {
    formatterFor(timezone);
    return true;
  } catch {
    return ctx.mustBe("a time zone name or UTC offset");
  }
});

const TimeWindow = type({
  "+": "reject",
  after: ClockTime,
  before: ClockTime,
  timezone: TimeZone,
}).narrow(
  (window, ctx) =>
    window.after !== window.before ||
    // The narrowed value holds minutes, not the authored HH:MM strings, so
    // an "actual" here would mislead.
    ctx.reject({
      expected: 'a window whose "after" and "before" differ',
      actual: "",
    }),
);

function minutesInZone(now: Date, timezone: string): number {
  let hour = 0;
  let minute = 0;
  for (const part of formatterFor(timezone).formatToParts(now)) {
    if (part.type === "hour") hour = parseInt(part.value, 10);
    if (part.type === "minute") minute = parseInt(part.value, 10);
  }
  return hour * 60 + minute;
}

/**
 * Condition evaluator for time-of-day windows.
 *
 * Register as `time_window` in the condition registry:
 *   `{ time_window: timeWindowEvaluator }`
 */
export const timeWindowEvaluator = createConditionEvaluator(
  "time_window",
  TimeWindow,
  ({ after, before, timezone }, ctx) => {
    const current = minutesInZone(ctx.now, timezone);
    if (after < before) {
      // Normal window: [after, before)
      return current >= after && current < before;
    }
    // Cross-midnight window: >= after OR < before
    return current >= after || current < before;
  },
);
