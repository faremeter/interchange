import { describe, test, expect } from "bun:test";

import { deriveWorkflowRunId } from "./workflow-run-id";

describe("deriveWorkflowRunId", () => {
  test("returns the local part before the @", () => {
    expect(deriveWorkflowRunId("run_abc123@tenant.example")).toBe("run_abc123");
  });

  test("throws when there is no @", () => {
    expect(() => deriveWorkflowRunId("run_abc123")).toThrow(
      "Invalid run address",
    );
  });

  test("throws when the domain is empty", () => {
    expect(() => deriveWorkflowRunId("run_abc123@")).toThrow(
      "Invalid run address",
    );
  });

  test("throws when the local part lacks the run_ prefix", () => {
    expect(() => deriveWorkflowRunId("ins_abc123@tenant.example")).toThrow(
      "Invalid run address",
    );
  });

  test("splits on the first @ when the address carries several", () => {
    expect(deriveWorkflowRunId("run_abc123@foo@tenant.example")).toBe(
      "run_abc123",
    );
  });
});
