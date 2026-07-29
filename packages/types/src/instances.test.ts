import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { CreateWorkflowRun } from "./instances";

// ---------------------------------------------------------------------------
// CreateWorkflowRun
// ---------------------------------------------------------------------------

describe("CreateWorkflowRun", () => {
  test("accepts invokerGrants array", () => {
    const result = CreateWorkflowRun({
      definitionId: "wfd_1",
      invokerGrants: [{ resource: "wallet:wal_1", action: "spend" }],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts invokerGrants with effect", () => {
    const result = CreateWorkflowRun({
      definitionId: "wfd_1",
      invokerGrants: [
        { resource: "tool:bash", action: "invoke", effect: "allow" },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts absent invokerGrants (optional)", () => {
    const result = CreateWorkflowRun({ definitionId: "wfd_1" });
    expect(result instanceof type.errors).toBe(false);
  });
});
