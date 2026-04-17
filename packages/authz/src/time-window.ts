// Time-window condition evaluator.
//
// Restricts a grant to a specific time-of-day window. The grant's
// conditions object should contain:
//
//   { time_window: { after: "09:00", before: "17:00", timezone: "America/Los_Angeles" } }
//
// The evaluator returns true if ctx.now falls within [after, before).
// Cross-midnight windows (after >= before) are supported:
//   { after: "22:00", before: "06:00" } → active from 10pm to 6am.
//
// Timezone is required — omitting it is an error. Time strings must
// be HH:MM in 24-hour format.

import type { ConditionEvaluator } from "./types";

type TimeWindowValue = {
  after: string;
  before: string;
  timezone: string;
};

function parseHHMM(s: string, field: string): { hour: number; minute: number } {
  const match = /^(\d{2}):(\d{2})$/.exec(s);
  if (match === null) {
    throw new Error(
      `time_window: "${field}" must be HH:MM in 24-hour format, got "${s}"`,
    );
  }
  const hour = parseInt(match[1] as string, 10);
  const minute = parseInt(match[2] as string, 10);
  if (hour > 23 || minute > 59) {
    throw new Error(`time_window: "${field}" is out of range, got "${s}"`);
  }
  return { hour, minute };
}

function validateShape(value: unknown): TimeWindowValue {
  if (typeof value !== "object" || value === null) {
    throw new Error(
      `time_window: expected an object with { after, before, timezone }, got ${typeof value}`,
    );
  }

  const obj = value as Record<string, unknown>;

  if (typeof obj.after !== "string") {
    throw new Error(
      `time_window: "after" must be a string, got ${typeof obj.after}`,
    );
  }
  if (typeof obj.before !== "string") {
    throw new Error(
      `time_window: "before" must be a string, got ${typeof obj.before}`,
    );
  }
  if (typeof obj.timezone !== "string") {
    throw new Error(
      `time_window: "timezone" is required and must be a string, got ${typeof obj.timezone}`,
    );
  }

  return { after: obj.after, before: obj.before, timezone: obj.timezone };
}

function toMinutes(h: number, m: number): number {
  return h * 60 + m;
}

function getCurrentTimeInZone(
  now: Date,
  timezone: string,
): { hour: number; minute: number } {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(now);

  let hour = 0;
  let minute = 0;
  for (const part of parts) {
    if (part.type === "hour") hour = parseInt(part.value, 10);
    if (part.type === "minute") minute = parseInt(part.value, 10);
  }
  return { hour, minute };
}

/**
 * Condition evaluator for time-of-day windows.
 *
 * Register as `time_window` in the condition registry:
 *   `{ time_window: timeWindowEvaluator }`
 */
export const timeWindowEvaluator: ConditionEvaluator = (
  value: unknown,
  ctx,
): boolean => {
  const tw = validateShape(value);
  const after = parseHHMM(tw.after, "after");
  const before = parseHHMM(tw.before, "before");
  const current = getCurrentTimeInZone(ctx.now, tw.timezone);

  const afterMins = toMinutes(after.hour, after.minute);
  const beforeMins = toMinutes(before.hour, before.minute);
  const currentMins = toMinutes(current.hour, current.minute);

  if (afterMins === beforeMins) {
    throw new Error(
      `time_window: "after" and "before" are equal ("${tw.after}"), which produces a zero-duration window`,
    );
  }

  if (afterMins < beforeMins) {
    // Normal window: [after, before)
    return currentMins >= afterMins && currentMins < beforeMins;
  }
  // Cross-midnight window: >= after OR < before
  return currentMins >= afterMins || currentMins < beforeMins;
};
