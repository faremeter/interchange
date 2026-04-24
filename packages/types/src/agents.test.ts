import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  delegationSources,
  credentialRequirementSources,
  GrantRequirement,
  CreateAgent,
  UpdateAgent,
  AgentResponse,
  CreateAgentInstance,
} from "./agents";

// ---------------------------------------------------------------------------
// 1. Alias correctness
// ---------------------------------------------------------------------------

describe("credentialRequirementSources alias", () => {
  test("credentialRequirementSources is the same reference as delegationSources", () => {
    expect(credentialRequirementSources).toBe(delegationSources);
  });

  test("both arrays contain exactly the three expected sources", () => {
    expect([...delegationSources]).toEqual(["tenant", "creator", "invoker"]);
    expect([...credentialRequirementSources]).toEqual([
      "tenant",
      "creator",
      "invoker",
    ]);
  });
});

// ---------------------------------------------------------------------------
// 2. GrantRequirement validation
// ---------------------------------------------------------------------------

describe("GrantRequirement validator", () => {
  test("accepts a minimal valid requirement (no effect, no conditions)", () => {
    const result = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
      source: "tenant",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts all three valid sources", () => {
    for (const source of ["tenant", "creator", "invoker"] as const) {
      const result = GrantRequirement({
        resource: "wallet:*",
        action: "spend",
        source,
      });
      expect(result instanceof type.errors).toBe(false);
    }
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
      source: "tenant",
      effect: "maybe",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects missing resource", () => {
    const result = GrantRequirement({
      action: "invoke",
      source: "tenant",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects missing action", () => {
    const result = GrantRequirement({
      resource: "tool:bash",
      source: "tenant",
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

  test("effect is optional — absent and present are both valid", () => {
    const withoutEffect = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
      source: "tenant",
    });
    const withEffect = GrantRequirement({
      resource: "tool:bash",
      action: "invoke",
      source: "tenant",
      effect: "ask",
    });
    expect(withoutEffect instanceof type.errors).toBe(false);
    expect(withEffect instanceof type.errors).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 3. CreateAgent — grantRequirements replaces initialGrants
// ---------------------------------------------------------------------------

describe("CreateAgent", () => {
  test("accepts grantRequirements array", () => {
    const result = CreateAgent({
      name: "My Agent",
      grantRequirements: [
        { resource: "tool:bash", action: "invoke", source: "tenant" },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts absent grantRequirements (optional)", () => {
    const result = CreateAgent({ name: "My Agent" });
    expect(result instanceof type.errors).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 4. UpdateAgent — grantRequirements added
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

  test("accepts empty update (all fields optional)", () => {
    const result = UpdateAgent({});
    expect(result instanceof type.errors).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 5. AgentResponse — creatorPrincipalId replaces principalId
// ---------------------------------------------------------------------------

describe("AgentResponse", () => {
  const validResponse = {
    id: "agt_1",
    tenantId: "tnt_1",
    name: "Agent",
    currentVersion: "1",
    status: "deployed" as const,
    createdAt: "2024-01-01T00:00:00Z",
    updatedAt: "2024-01-01T00:00:00Z",
  };

  test("accepts a valid response with creatorPrincipalId as a string", () => {
    const result = AgentResponse({
      ...validResponse,
      creatorPrincipalId: "prn_1",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a valid response with creatorPrincipalId as null", () => {
    const result = AgentResponse({
      ...validResponse,
      creatorPrincipalId: null,
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a valid response with absent creatorPrincipalId (optional)", () => {
    const result = AgentResponse(validResponse);
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts grantRequirements on the response", () => {
    const result = AgentResponse({
      ...validResponse,
      grantRequirements: [
        { resource: "tool:bash", action: "invoke", source: "tenant" },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// 6. CreateAgentInstance — invokerGrants
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
