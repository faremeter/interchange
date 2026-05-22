// Tests for the shared env-driven source resolver used by the
// agent-* examples. The resolver is small but every example depends on
// it for both production (env-driven) and test (override-driven)
// paths, so the three exits (override wins, env wins, neither) all
// need to be locked down by tests.

import { describe, expect, test } from "bun:test";

import {
  DEFAULT_ANTHROPIC_BASE_URL,
  DEFAULT_ANTHROPIC_MODEL,
  resolveSource,
} from "@intx/example-agent-common";
import type { InferenceSource } from "@intx/types/runtime";

const STUB_SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-override",
  model: "claude-3-5-sonnet",
};

describe("resolveSource", () => {
  test("returns the override unchanged when sourceOverride is supplied", () => {
    const r = resolveSource({
      env: {},
      sourceOverride: STUB_SOURCE,
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(true);
    if (!r.ok) throw new Error("unreachable");
    expect(r.source).toBe(STUB_SOURCE);
  });

  test("builds an Anthropic source from ANTHROPIC_API_KEY", () => {
    const r = resolveSource({
      env: { ANTHROPIC_API_KEY: "sk-real" },
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(true);
    if (!r.ok) throw new Error("unreachable");
    expect(r.source).toEqual({
      id: `anthropic:${DEFAULT_ANTHROPIC_MODEL}`,
      provider: "anthropic",
      baseURL: DEFAULT_ANTHROPIC_BASE_URL,
      apiKey: "sk-real",
      model: DEFAULT_ANTHROPIC_MODEL,
    });
  });

  test("honors an explicit model override when env is used", () => {
    const r = resolveSource({
      env: { ANTHROPIC_API_KEY: "sk-real" },
      model: "claude-3-5-haiku",
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(true);
    if (!r.ok) throw new Error("unreachable");
    expect(r.source.model).toBe("claude-3-5-haiku");
    expect(r.source.id).toBe("anthropic:claude-3-5-haiku");
  });

  test("returns help text when neither override nor env is present", () => {
    const r = resolveSource({
      env: {},
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(false);
    if (r.ok) throw new Error("unreachable");
    expect(r.help).toContain("agent-quickstart");
    expect(r.help).toContain("ANTHROPIC_API_KEY");
  });

  test("treats an empty ANTHROPIC_API_KEY as missing", () => {
    const r = resolveSource({
      env: { ANTHROPIC_API_KEY: "" },
      exampleName: "agent-quickstart",
    });
    expect(r.ok).toBe(false);
  });
});
