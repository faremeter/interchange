import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { CreateAgent, UpdateAgent, AgentResponse } from "./agents";

// ---------------------------------------------------------------------------
// 1. CreateAgent
// ---------------------------------------------------------------------------

describe("CreateAgent", () => {
  test("accepts grantRequirements array", () => {
    const result = CreateAgent({
      name: "My Agent",
      grantRequirements: [
        { resource: "tool:bash", action: "invoke", source: "creator" },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts roleIds array", () => {
    const result = CreateAgent({
      name: "My Agent",
      roleIds: ["role_1", "role_2"],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts absent grantRequirements and roleIds (optional)", () => {
    const result = CreateAgent({ name: "My Agent" });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts modelRequirements", () => {
    const result = CreateAgent({
      name: "My Agent",
      modelRequirements: [
        {
          model: "opus",
          capabilities: ["function-calling-multi-turn"],
          providers: { mode: "prefer", order: ["anthropic"] },
        },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects modelRequirements with a duplicate model", () => {
    // The uniqueness narrow on ModelRequirements must fire through the
    // definition boundary, not just on the bare validator.
    const result = CreateAgent({
      name: "My Agent",
      modelRequirements: [
        { model: "opus", capabilities: ["vision-input"] },
        { model: "opus", capabilities: ["function-calling-multi-turn"] },
      ],
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 2. UpdateAgent
// ---------------------------------------------------------------------------

describe("UpdateAgent", () => {
  test("accepts grantRequirements array", () => {
    const result = UpdateAgent({
      grantRequirements: [
        { resource: "wallet:*", action: "spend", source: "invoker" },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts roleIds array", () => {
    const result = UpdateAgent({
      roleIds: ["role_1"],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts empty update (all fields optional)", () => {
    const result = UpdateAgent({});
    expect(result instanceof type.errors).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 3. AgentResponse
// ---------------------------------------------------------------------------

describe("AgentResponse", () => {
  const validResponse = {
    id: "agt_1",
    tenantId: "tnt_1",
    creatorPrincipalId: "prn_1",
    name: "Agent",
    currentVersion: "1",
    status: "deployed" as const,
    toolPackages: [],
    createdAt: "2024-01-01T00:00:00Z",
    updatedAt: "2024-01-01T00:00:00Z",
  };

  test("accepts a valid response with creatorPrincipalId as a string", () => {
    const result = AgentResponse(validResponse);
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a response with creatorPrincipalId as null", () => {
    const result = AgentResponse({
      ...validResponse,
      creatorPrincipalId: null,
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a response with creatorPrincipalId absent", () => {
    const { creatorPrincipalId: _omitted, ...withoutCreator } = validResponse;
    const result = AgentResponse(withoutCreator);
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a response with toolPackages absent", () => {
    const { toolPackages: _omitted, ...withoutToolPackages } = validResponse;
    const result = AgentResponse(withoutToolPackages);
    expect(result instanceof type.errors).toBe(true);
  });

  test("accepts grantRequirements on the response", () => {
    const result = AgentResponse({
      ...validResponse,
      grantRequirements: [
        { resource: "tool:bash", action: "invoke", source: "creator" },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts roles on the response", () => {
    const result = AgentResponse({
      ...validResponse,
      roles: [{ id: "role_1", name: "researcher" }],
    });
    expect(result instanceof type.errors).toBe(false);
  });
});
