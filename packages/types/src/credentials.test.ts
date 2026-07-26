import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  credentialRequirementSources,
  CredentialRequirement,
} from "./credentials";

// ---------------------------------------------------------------------------
// 1. Source enum
// ---------------------------------------------------------------------------

describe("source enums", () => {
  test("credentialRequirementSources includes tenant, creator, invoker", () => {
    expect([...credentialRequirementSources]).toEqual([
      "tenant",
      "creator",
      "invoker",
    ]);
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
