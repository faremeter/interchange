// Tests for the shared env-driven provider resolver used by the
// agent-* examples. The resolver is small but every example depends on
// it for both production (env-driven) and test (override-driven)
// paths, so the three exits (override wins, env wins, neither) all
// need to be locked down by tests.

import { describe, expect, test } from "bun:test";

import {
  DEFAULT_ANTHROPIC_BASE_URL,
  DEFAULT_ANTHROPIC_MODEL,
  resolveProvider,
} from "@interchange/example-agent-common";
import type { ProviderConfig } from "@interchange/types/runtime";

const STUB_PROVIDER: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-override",
  model: "claude-3-5-sonnet",
};

describe("resolveProvider", () => {
  test("returns the override unchanged when providerOverride is supplied", () => {
    const r = resolveProvider({
      env: {},
      providerOverride: STUB_PROVIDER,
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(true);
    if (!r.ok) throw new Error("unreachable");
    expect(r.provider).toBe(STUB_PROVIDER);
    expect(r.model).toBe(STUB_PROVIDER.model ?? "");
  });

  test("rejects a providerOverride with no model set", () => {
    const r = resolveProvider({
      env: {},
      providerOverride: {
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "sk-test",
      },
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.help).toContain("model");
  });

  test("builds an Anthropic provider from ANTHROPIC_API_KEY", () => {
    const r = resolveProvider({
      env: { ANTHROPIC_API_KEY: "sk-real" },
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(true);
    if (!r.ok) throw new Error("unreachable");
    expect(r.provider).toEqual({
      provider: "anthropic",
      baseURL: DEFAULT_ANTHROPIC_BASE_URL,
      apiKey: "sk-real",
      model: DEFAULT_ANTHROPIC_MODEL,
    });
    expect(r.model).toBe(DEFAULT_ANTHROPIC_MODEL);
  });

  test("honors an explicit model override when env is used", () => {
    const r = resolveProvider({
      env: { ANTHROPIC_API_KEY: "sk-real" },
      model: "claude-3-5-haiku",
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(true);
    if (!r.ok) throw new Error("unreachable");
    expect(r.provider.model).toBe("claude-3-5-haiku");
  });

  test("returns help text when neither override nor env is present", () => {
    const r = resolveProvider({
      env: {},
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.help).toContain("agent-quickstart");
    expect(r.help).toContain("ANTHROPIC_API_KEY");
  });

  test("treats an empty ANTHROPIC_API_KEY as missing", () => {
    const r = resolveProvider({
      env: { ANTHROPIC_API_KEY: "" },
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(false);
  });
});
