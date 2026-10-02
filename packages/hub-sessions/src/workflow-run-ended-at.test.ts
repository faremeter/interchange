import { expect, test } from "bun:test";

import { getWorkflowRunEndedAt } from "./workflow-run-ended-at";

test.each([
  [{ at: "2026-01-01T12:00:00.000Z" }, "2026-01-01T12:00:00.000Z"],
  [{ at: "2026-01-01T10:00:00.000Z" }, "2026-01-01T11:00:00.000Z"],
  [{ at: "2026-01-01T13:00:00.000Z" }, "2026-01-01T12:10:00.000Z"],
  [{}, "2026-01-01T12:10:00.000Z"],
  [{ at: "invalid" }, "2026-01-01T12:10:00.000Z"],
  [{ at: 42 }, "2026-01-01T12:10:00.000Z"],
])("terminal event %j uses bounded end time %s", (body, expected) => {
  expect(
    getWorkflowRunEndedAt(
      body,
      new Date("2026-01-01T11:00:00.000Z"),
      new Date("2026-01-01T12:10:00.000Z"),
    ).toISOString(),
  ).toBe(expected);
});
