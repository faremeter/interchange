import { describe, test, expect } from "bun:test";

import { deriveWorkflowRunId } from "./workflow-run-id";

describe("deriveWorkflowRunId", () => {
  test("returns the local part before the @", () => {
    expect(deriveWorkflowRunId("ins_abc123@tenant.example")).toBe("ins_abc123");
  });

  test("returns the whole input when there is no @", () => {
    expect(deriveWorkflowRunId("ins_abc123")).toBe("ins_abc123");
  });

  test("returns the local part when the address ends with @", () => {
    expect(deriveWorkflowRunId("ins_abc123@")).toBe("ins_abc123");
  });

  test("splits on the last @ when the address carries several", () => {
    expect(deriveWorkflowRunId("ins_abc123@foo@tenant.example")).toBe(
      "ins_abc123@foo",
    );
  });
});
