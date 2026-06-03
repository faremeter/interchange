import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  credentialRequirementSources,
  grantRequirementSources,
  CredentialRequirement,
  GrantRequirement,
  CreateAgent,
  UpdateAgent,
  AgentResponse,
  CreateAgentInstance,
} from "./agents";

// ---------------------------------------------------------------------------
// 1. Source enum separation
// ---------------------------------------------------------------------------

describe("source enums", () => {
  test("credentialRequirementSources includes tenant, creator, invoker", () => {
    expect([...credentialRequirementSources]).toEqual([
      "tenant",
      "creator",
      "invoker",
    ]);
  });

  test("grantRequirementSources includes only creator and invoker", () => {
    expect([...grantRequirementSources]).toEqual(["creator", "invoker"]);
  });
});

// ---------------------------------------------------------------------------
// 2. CredentialRequirement accepts tenant source
// ---------------------------------------------------------------------------

describe("CredentialRequirement validator", () => {
  test("accepts tenant source", () => {
    const result = CredentialRequirement({
      providerName: "Anthropic",
      source: "tenant",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts creator source", () => {
    const result = CredentialRequirement({
      providerName: "Anthropic",
      source: "creator",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts invoker source", () => {
    const result = CredentialRequirement({
      providerName: "Anthropic",
      source: "invoker",
    });
    expect(result instanceof type.errors).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 3. GrantRequirement validation
// ---------------------------------------------------------------------------

describe("GrantRequirement validator", () => {
  test("accepts a minimal valid requirement", () => {
    const result = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
      source: "creator",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts creator and invoker sources", () => {
    for (const source of ["creator", "invoker"] as const) {
      const result = GrantRequirement({
        resource: "wallet:*",
        action: "spend",
        source,
      });
      expect(result instanceof type.errors).toBe(false);
    }
  });

  test("rejects tenant source", () => {
    const result = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
      source: "tenant",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("accepts a fully populated requirement", () => {
    const result = GrantRequirement({
      resource: "credential:crd_stripe",
      action: "use",
      effect: "allow",
      source: "creator",
      conditions: { max_spend_per_day: 100, currency: "USD" },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts null conditions", () => {
    const result = GrantRequirement({
      resource: "tool:*",
      action: "invoke",
      source: "invoker",
      conditions: null,
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an unknown source", () => {
    const result = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
      source: "system",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an unknown effect", () => {
    const result = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
      source: "creator",
      effect: "maybe",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects missing resource", () => {
    const result = GrantRequirement({
      action: "invoke",
      source: "creator",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects missing action", () => {
    const result = GrantRequirement({
      resource: "tool:bash",
      source: "creator",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects missing source", () => {
    const result = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("effect is optional", () => {
    const withoutEffect = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
      source: "creator",
    });
    const withEffect = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
      source: "creator",
      effect: "ask",
    });
    expect(withoutEffect instanceof type.errors).toBe(false);
    expect(withEffect instanceof type.errors).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 4. CreateAgent
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
});

// ---------------------------------------------------------------------------
// 5. UpdateAgent
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
// 6. AgentResponse
// ---------------------------------------------------------------------------

describe("AgentResponse", () => {
  const validResponse = {
    id: "agt_1",
    tenantId: "tnt_1",
    creatorPrincipalId: "prn_1",
    name: "Agent",
    currentVersion: "1",
    status: "deployed" as const,
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

// ---------------------------------------------------------------------------
// 7. CreateAgentInstance
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
