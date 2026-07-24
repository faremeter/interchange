import { describe, test, expect } from "bun:test";

import type { RoutableRecord } from "@intx/hub-sessions";

import {
  formatInstanceView,
  mapRunStatusToInstanceStatus,
} from "./instance-view";

function makeRecord(overrides: Partial<RoutableRecord> = {}): RoutableRecord {
  return {
    kind: "run",
    id: "ins_1",
    tenantId: "tnt_1",
    address: "ins_1@tenant.example",
    publicKey: null,
    status: "running",
    createdAt: new Date("2026-01-01T00:00:00Z"),
    updatedAt: new Date("2026-01-02T00:00:00Z"),
    endedAt: null,
    agentId: "agt_1",
    principalId: "prn_1",
    sessionId: "ses_1",
    kernelId: null,
    sidecarId: null,
    ...overrides,
  };
}

describe("mapRunStatusToInstanceStatus", () => {
  test("maps every run status onto the instance vocabulary", () => {
    expect(mapRunStatusToInstanceStatus("running")).toBe("running");
    expect(mapRunStatusToInstanceStatus("completed")).toBe("stopped");
    expect(mapRunStatusToInstanceStatus("cancelled")).toBe("stopped");
    expect(mapRunStatusToInstanceStatus("failed")).toBe("error");
  });

  test("throws on an unmapped status rather than leak it", () => {
    expect(() => mapRunStatusToInstanceStatus("bogus")).toThrow(
      /unmapped workflow_run status/,
    );
  });
});

describe("formatInstanceView", () => {
  test("shapes a running folded run as a running instance", () => {
    const view = formatInstanceView(makeRecord(), "My Agent");
    expect(view).toMatchObject({
      id: "ins_1",
      agentId: "agt_1",
      agentName: "My Agent",
      tenantId: "tnt_1",
      address: "ins_1@tenant.example",
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

  test("maps a terminal run's status and formats its endedAt", () => {
    const endedAt = new Date("2026-01-03T00:00:00Z");
    const failed = formatInstanceView(
      makeRecord({ status: "failed", endedAt }),
      "My Agent",
    );
    expect(failed.status).toBe("error");
    expect(failed.endedAt).toBe("2026-01-03T00:00:00.000Z");

    expect(
      formatInstanceView(makeRecord({ status: "completed" }), "A").status,
    ).toBe("stopped");
    expect(
      formatInstanceView(makeRecord({ status: "cancelled" }), "A").status,
    ).toBe("stopped");
  });

  test("passes a legacy instance status through unchanged", () => {
    const view = formatInstanceView(
      makeRecord({ kind: "instance", status: "deployed" }),
      "A",
    );
    expect(view.status).toBe("deployed");
  });

  test("throws on an unknown instance status rather than leak it", () => {
    expect(() =>
      formatInstanceView(
        makeRecord({ kind: "instance", status: "bogus" }),
        "A",
      ),
    ).toThrow(/unknown agent_instance status/);
  });

  test("includes runtimeStatus only when supplied", () => {
    const view = formatInstanceView(makeRecord(), "A", "idle");
    expect(view).toMatchObject({ runtimeStatus: "idle" });
  });
});
