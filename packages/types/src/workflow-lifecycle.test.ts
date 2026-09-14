import { describe, expect, test } from "bun:test";
import { type } from "arktype";

import {
  WorkflowLifecyclePolicy,
  lifecycleDeadline,
  resolveWorkflowLifecyclePolicy,
} from "./workflow-lifecycle";

describe("workflow lifecycle policy", () => {
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

  test("computes deadlines from the supplied durable timestamp", () => {
    expect(
      lifecycleDeadline(new Date("2026-01-01T00:00:00Z"), "2h").toISOString(),
    ).toBe("2026-01-01T02:00:00.000Z");
  });
});
