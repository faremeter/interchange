import { describe, expect, test } from "bun:test";
import { type } from "arktype";

import {
  ModelOfferingResponse,
  ModelProviderPlugin,
  modelProviderPlugins,
  ModelProviderResponse,
  ModelRequirement,
  ModelRequirements,
  PricingRowResponse,
} from "./catalog";

describe("ModelProviderPlugin enum", () => {
  test("accepts each adapter key", () => {
    for (const plugin of modelProviderPlugins) {
      expect(ModelProviderPlugin(plugin)).toBe(plugin);
    }
  });

  test("rejects an unknown plugin", () => {
    expect(ModelProviderPlugin("cohere") instanceof type.errors).toBe(true);
  });
});

describe("ModelOfferingResponse", () => {
  const base = {
    id: "mof_1",
    tenantId: "ten_1",
    modelId: "mdl_1",
    providerId: "mpv_1",
    priority: 0,
    deploymentTags: [],
    capabilities: ["vision-input", "function-calling-multi-turn"],
    quirks: null,
    disabled: false,
    createdAt: "2026-06-18T00:00:00Z",
    updatedAt: "2026-06-18T00:00:00Z",
  };

  test("accepts curated capabilities", () => {
    expect(ModelOfferingResponse(base) instanceof type.errors).toBe(false);
  });

  test("rejects a non-curated capability", () => {
    const bad = { ...base, capabilities: ["telepathy"] };
    expect(ModelOfferingResponse(bad) instanceof type.errors).toBe(true);
  });

  test("accepts a populated quirks bag", () => {
    const withQuirks = {
      ...base,
      quirks: { forceAssistantReasoningContent: true },
    };
    expect(ModelOfferingResponse(withQuirks) instanceof type.errors).toBe(
      false,
    );
  });
});

describe("ModelProviderResponse", () => {
  const base = {
    id: "mpv_1",
    tenantId: "ten_1",
    name: "Anthropic direct",
    plugin: "anthropic",
    baseURL: "https://api.anthropic.com",
    disabled: false,
    createdAt: "2026-06-18T00:00:00Z",
    updatedAt: "2026-06-18T00:00:00Z",
  };

  test("accepts a credential-backed provider", () => {
    const row = { ...base, credentialId: "cred_1" };
    expect(ModelProviderResponse(row) instanceof type.errors).toBe(false);
  });

  test("accepts a wallet-backed provider", () => {
    const row = { ...base, walletId: "wal_1" };
    expect(ModelProviderResponse(row) instanceof type.errors).toBe(false);
  });
});

describe("ModelRequirement", () => {
  test("accepts a bare model name", () => {
    expect(ModelRequirement({ model: "opus" }) instanceof type.errors).toBe(
      false,
    );
  });

  test("accepts a capability filter and a provider preference", () => {
    const req = {
      model: "opus",
      capabilities: ["vision-input", "function-calling-multi-turn"],
      providers: { mode: "pin", order: ["anthropic"] },
    };
    expect(ModelRequirement(req) instanceof type.errors).toBe(false);
  });

  test("rejects a non-curated capability", () => {
    const req = { model: "opus", capabilities: ["telepathy"] };
    expect(ModelRequirement(req) instanceof type.errors).toBe(true);
  });

  test("rejects an unknown preference mode", () => {
    const req = { model: "opus", providers: { mode: "force", order: [] } };
    expect(ModelRequirement(req) instanceof type.errors).toBe(true);
  });

  test("requires the model name", () => {
    expect(ModelRequirement({}) instanceof type.errors).toBe(true);
  });
});

describe("ModelRequirements", () => {
  test("accepts distinct model names", () => {
    const reqs = [{ model: "opus" }, { model: "sonnet" }];
    expect(ModelRequirements(reqs) instanceof type.errors).toBe(false);
  });

  test("rejects two requirements for the same model", () => {
    const reqs = [
      { model: "opus", capabilities: ["vision-input"] },
      { model: "opus", capabilities: ["function-calling-multi-turn"] },
    ];
    expect(ModelRequirements(reqs) instanceof type.errors).toBe(true);
  });

  test("accepts an empty array", () => {
    expect(ModelRequirements([]) instanceof type.errors).toBe(false);
  });
});

describe("PricingRowResponse", () => {
  const base = {
    id: "prc_1",
    tenantId: "ten_1",
    offeringId: "mof_1",
    currency: "USD",
    effectiveFrom: "2026-06-18T00:00:00Z",
    createdAt: "2026-06-18T00:00:00Z",
  };

  test("accepts a row with a subset of fee axes", () => {
    const row = { ...base, inputTokenPrice: "0.000003", perImageFee: null };
    expect(PricingRowResponse(row) instanceof type.errors).toBe(false);
  });

  test("requires effectiveFrom", () => {
    const { effectiveFrom: _omit, ...without } = base;
    expect(PricingRowResponse(without) instanceof type.errors).toBe(true);
  });
});
