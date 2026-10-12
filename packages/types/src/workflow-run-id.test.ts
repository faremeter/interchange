import { describe, test, expect } from "bun:test";

import { deriveWorkflowRunId } from "./workflow-run-id";

describe("deriveWorkflowRunId", () => {
  test("returns the local part before the @", () => {
    expect(deriveWorkflowRunId("run_abc123@tenant.example")).toBe("run_abc123");
  });

  test("throws when there is no @", () => {
    expect(() => deriveWorkflowRunId("run_abc123")).toThrow(
      'Invalid run address: "run_abc123"',
    );
  });

  test("throws when the domain is empty", () => {
    expect(() => deriveWorkflowRunId("run_abc123@")).toThrow(
      'Invalid run address: "run_abc123@"',
    );
  });

  test("throws when the local part lacks the run_ prefix", () => {
    expect(() => deriveWorkflowRunId("usr_abc123@tenant.example")).toThrow(
      'Invalid run address: "usr_abc123@tenant.example"',
    );
  });

  test("splits on the first @ when the address carries several", () => {
    expect(deriveWorkflowRunId("run_abc123@foo@tenant.example")).toBe(
      "run_abc123",
    );
  });

  test("throws when the local part is empty", () => {
    expect(() => deriveWorkflowRunId("@tenant.example")).toThrow(
      'Invalid run address: "@tenant.example"',
    );
  });

  test("throws on empty string", () => {
    expect(() => deriveWorkflowRunId("")).toThrow('Invalid run address: ""');
  });

  test("includes the invalid address in the error message", () => {
    expect(() => deriveWorkflowRunId("not-an-address")).toThrow(
      'Invalid run address: "not-an-address"',
    );
  });
});
