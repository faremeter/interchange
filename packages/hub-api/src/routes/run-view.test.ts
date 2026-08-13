import { describe, test, expect } from "bun:test";

import { workflowRun } from "@intx/db/schema";
import type { RoutableRecord } from "@intx/hub-sessions";

import { formatRunView, mapRunStatusToViewStatus } from "./run-view";

function makeRecord(overrides: Partial<RoutableRecord> = {}): RoutableRecord {
  return {
    id: "run_1",
    tenantId: "tnt_1",
    address: "run_1@tenant.example",
    publicKey: null,
    status: "running",
    createdAt: new Date("2026-01-01T00:00:00Z"),
    updatedAt: new Date("2026-01-02T00:00:00Z"),
    endedAt: null,
    definitionId: "wfd_1",
    principalId: "prn_1",
    kernelId: null,
    sidecarId: null,
    ...overrides,
  };
}

describe("mapRunStatusToViewStatus", () => {
  test("maps every run status onto the run-view vocabulary", () => {
    expect(mapRunStatusToViewStatus("deployed")).toBe("deployed");
    expect(mapRunStatusToViewStatus("running")).toBe("running");
    expect(mapRunStatusToViewStatus("completed")).toBe("stopped");
    expect(mapRunStatusToViewStatus("cancelled")).toBe("stopped");
    expect(mapRunStatusToViewStatus("failed")).toBe("error");
  });

  test("throws on an unmapped status rather than leak it", () => {
    expect(() => mapRunStatusToViewStatus("bogus")).toThrow(
      /unmapped workflow_run status/,
    );
  });

  test("maps every workflow_run status the schema defines", () => {
    // The status-filter inverse is derived by bucketing this enum through the
    // map, so a new run status the map does not handle must fail here rather
    // than throw obscurely when that inverse is built.
    for (const status of workflowRun.status.enumValues) {
      expect(() => mapRunStatusToViewStatus(status)).not.toThrow();
    }
  });
});

describe("formatRunView", () => {
  test("shapes a running run into the run view", () => {
    const view = formatRunView(makeRecord(), "My Agent");
    expect(view).toMatchObject({
      id: "run_1",
      definitionId: "wfd_1",
      definitionName: "My Agent",
      tenantId: "tnt_1",
      address: "run_1@tenant.example",
      status: "running",
      publicKey: null,
      kernelId: null,
      sidecarId: null,
      endedAt: null,
    });
    // updatedAt is whatever the reader normalized (endedAt ?? createdAt).
    expect(view.updatedAt).toBe("2026-01-02T00:00:00.000Z");
    expect(view.createdAt).toBe("2026-01-01T00:00:00.000Z");
    expect("runtimeStatus" in view).toBe(false);
  });

  test("renders a deployed run as live", () => {
    const view = formatRunView(makeRecord({ status: "deployed" }), "My Agent");
    expect(view.status).toBe("deployed");
  });

  test("maps a terminal run's status and formats its endedAt", () => {
    const endedAt = new Date("2026-01-03T00:00:00Z");
    const failed = formatRunView(
      makeRecord({ status: "failed", endedAt }),
      "My Agent",
    );
    expect(failed.status).toBe("error");
    expect(failed.endedAt).toBe("2026-01-03T00:00:00.000Z");

    expect(formatRunView(makeRecord({ status: "completed" }), "A").status).toBe(
      "stopped",
    );
    expect(formatRunView(makeRecord({ status: "cancelled" }), "A").status).toBe(
      "stopped",
    );
  });

  test("includes runtimeStatus only when supplied", () => {
    const view = formatRunView(makeRecord(), "A", "idle");
    expect(view).toMatchObject({ runtimeStatus: "idle" });
  });
});
