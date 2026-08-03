import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  CreateGrant,
  grantRequirementSources,
  GrantRequirement,
} from "./grants";

// ---------------------------------------------------------------------------
// 1. Source enum
// ---------------------------------------------------------------------------

describe("source enums", () => {
  test("grantRequirementSources includes only creator and invoker", () => {
    expect([...grantRequirementSources]).toEqual(["creator", "invoker"]);
  });
});

// ---------------------------------------------------------------------------
// 2. GrantRequirement validation
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
// 3. CreateGrant: exactly one target (matches the grant_target_exactly_one DB
//    CHECK, so a malformed request is a 400 rather than a database 500)
// ---------------------------------------------------------------------------

describe("CreateGrant target validation", () => {
  const base = {
    resource: "credential:crd_x",
    action: "use",
    effect: "allow",
    origin: "system",
  };

  test("accepts a role-targeted grant", () => {
    expect(
      CreateGrant({ ...base, roleId: "rol_x" }) instanceof type.errors,
    ).toBe(false);
  });

  test("accepts a principal-targeted grant", () => {
    expect(
      CreateGrant({ ...base, principalId: "prn_x" }) instanceof type.errors,
    ).toBe(false);
  });

  test("rejects a grant with neither target", () => {
    expect(CreateGrant({ ...base }) instanceof type.errors).toBe(true);
  });

  test("rejects a grant with both targets", () => {
    expect(
      CreateGrant({ ...base, roleId: "rol_x", principalId: "prn_x" }) instanceof
        type.errors,
    ).toBe(true);
  });

  test("treats an explicit null target as absent", () => {
    expect(
      CreateGrant({ ...base, roleId: "rol_x", principalId: null }) instanceof
        type.errors,
    ).toBe(false);
    expect(
      CreateGrant({ ...base, roleId: null, principalId: null }) instanceof
        type.errors,
    ).toBe(true);
  });
});
