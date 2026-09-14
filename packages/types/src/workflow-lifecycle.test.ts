import { describe, expect, test } from "bun:test";
import { type } from "arktype";

import {
  WorkflowLifecyclePolicy,
  clampWorkflowLifecyclePolicy,
  lifecycleDeadline,
  resolveWorkflowLifecyclePolicy,
} from "./workflow-lifecycle";

describe("workflow lifecycle policy", () => {
  test("bounds every duration unit equally and keeps the maximum deadline representable", () => {
    const start = new Date("2026-09-21T00:00:00Z");
    const expected = new Date(start.getTime() + 36_500 * 86_400_000);
    for (const duration of ["36500d", "876000h", "52560000m", "3153600000s"]) {
      expect(
        WorkflowLifecyclePolicy({
          maxLifetime: duration,
          capacityRetention: { completed: duration },
        }),
      ).not.toBeInstanceOf(type.errors);
      expect(lifecycleDeadline(start, duration)).toEqual(expected);
    }
    for (const duration of [
      "36501d",
      "876001h",
      "52560001m",
      "3153600001s",
      "100000000d",
    ]) {
      expect(WorkflowLifecyclePolicy({ maxLifetime: duration })).toBeInstanceOf(
        type.errors,
      );
      for (const status of ["completed", "failed", "cancelled"] as const) {
        expect(
          WorkflowLifecyclePolicy({
            capacityRetention: { [status]: duration },
          }),
        ).toBeInstanceOf(type.errors);
      }
    }
  });

  test("accepts immediate release but rejects a zero lifetime and malformed durations", () => {
    expect(
      WorkflowLifecyclePolicy({ capacityRetention: { completed: "0s" } }),
    ).toEqual({ capacityRetention: { completed: "0s" } });
    for (const duration of [
      "0s",
      "-1h",
      "1.5h",
      "1hour",
      "1e3s",
      "999999999999999999d",
    ]) {
      expect(WorkflowLifecyclePolicy({ maxLifetime: duration })).toBeInstanceOf(
        type.errors,
      );
    }
    expect(
      WorkflowLifecyclePolicy({ capacityRetention: { typo: "1h" } }),
    ).toBeInstanceOf(type.errors);
  });

  test("inherits each field and permits shorter equivalent-unit limits", () => {
    expect(
      resolveWorkflowLifecyclePolicy([
        {
          maxLifetime: "1d",
          capacityRetention: { completed: "0s", failed: "1h" },
        },
        { maxLifetime: "24h" },
        { maxLifetime: "2h", capacityRetention: { failed: "15m" } },
      ]),
    ).toEqual({
      ok: true,
      policy: {
        maxLifetime: "2h",
        capacityRetention: { completed: "0s", failed: "15m" },
      },
    });
  });

  test("omission cannot remove an ancestor limit and a descendant cannot extend it", () => {
    expect(
      resolveWorkflowLifecyclePolicy([
        { maxLifetime: "1h" },
        {},
        { maxLifetime: "2h" },
      ]),
    ).toEqual({
      ok: false,
      field: "maxLifetime",
      requested: "2h",
      limit: "1h",
    });
    expect(
      resolveWorkflowLifecyclePolicy([
        { capacityRetention: { completed: "0s" } },
        { capacityRetention: { completed: "1m" } },
      ]),
    ).toEqual({
      ok: false,
      field: "capacityRetention.completed",
      requested: "1m",
      limit: "0s",
    });
    expect(resolveWorkflowLifecyclePolicy([{}, {}])).toEqual({
      ok: true,
      policy: {},
    });
  });

  test("clamping caps each value at its inherited limit and keeps valid shorter ones", () => {
    expect(
      clampWorkflowLifecyclePolicy([
        { maxLifetime: "6h", capacityRetention: { completed: "0s" } },
        {
          maxLifetime: "12h",
          capacityRetention: { completed: "1h", failed: "30m" },
        },
        { maxLifetime: "2h", capacityRetention: { failed: "2h" } },
      ]),
    ).toEqual({
      maxLifetime: "2h",
      capacityRetention: { completed: "0s", failed: "30m" },
    });
  });

  test("validation reports the first conflict even when later levels also exceed", () => {
    expect(
      resolveWorkflowLifecyclePolicy([
        { maxLifetime: "1h" },
        { capacityRetention: { failed: "1h" } },
        { maxLifetime: "3h", capacityRetention: { failed: "2h" } },
      ]),
    ).toEqual({
      ok: false,
      field: "maxLifetime",
      requested: "3h",
      limit: "1h",
    });
  });

  test("computes deadlines from the supplied durable timestamp", () => {
    expect(
      lifecycleDeadline(new Date("2026-01-01T00:00:00Z"), "2h").toISOString(),
    ).toBe("2026-01-01T02:00:00.000Z");
  });
});
