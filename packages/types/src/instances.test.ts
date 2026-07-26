import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { CreateAgentInstance } from "./instances";

// ---------------------------------------------------------------------------
// CreateAgentInstance
// ---------------------------------------------------------------------------

describe("CreateAgentInstance", () => {
  test("accepts invokerGrants array", () => {
    const result = CreateAgentInstance({
      agentId: "agt_1",
      invokerGrants: [{ resource: "wallet:wal_1", action: "spend" }],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts invokerGrants with effect", () => {
    const result = CreateAgentInstance({
      agentId: "agt_1",
      invokerGrants: [
        { resource: "tool:bash", action: "invoke", effect: "allow" },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts absent invokerGrants (optional)", () => {
    const result = CreateAgentInstance({ agentId: "agt_1" });
    expect(result instanceof type.errors).toBe(false);
  });
});
