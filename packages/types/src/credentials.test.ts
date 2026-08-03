import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  credentialRequirementSources,
  CredentialBinding,
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

// ---------------------------------------------------------------------------
// 3. CredentialBinding: locator and authority are separate axes
// ---------------------------------------------------------------------------

describe("CredentialBinding validator", () => {
  const wellFormed = {
    package: "@intx/tools-github",
    handle: "gh",
    provider: "github",
    locator: "tenant",
  };

  test("accepts a well-formed binding", () => {
    expect(CredentialBinding(wellFormed) instanceof type.errors).toBe(false);
  });

  test("accepts an optional name", () => {
    expect(
      CredentialBinding({
        ...wellFormed,
        name: "deploy-key",
      }) instanceof type.errors,
    ).toBe(false);
  });

  test("rejects an unknown locator (only tenant is supported today)", () => {
    // creator/invoker-brought locators are future work; they must fail closed.
    expect(
      CredentialBinding({ ...wellFormed, locator: "creator" }) instanceof
        type.errors,
    ).toBe(true);
  });

  test("rejects a malformed handle", () => {
    expect(
      CredentialBinding({ ...wellFormed, handle: "Bad Handle!" }) instanceof
        type.errors,
    ).toBe(true);
  });

  test("rejects a missing required field", () => {
    const { provider: _provider, ...missingProvider } = wellFormed;
    expect(CredentialBinding(missingProvider) instanceof type.errors).toBe(
      true,
    );
  });
});
