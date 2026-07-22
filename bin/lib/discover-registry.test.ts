import { describe, expect, test } from "bun:test";

import {
  PLUGIN_REGISTRY,
  findPlugin,
  type RegisteredPlugin,
} from "./discover-registry";

function requireEntry(name: string): RegisteredPlugin {
  const entry = findPlugin(name);
  if (entry === undefined) {
    throw new Error(`PLUGIN_REGISTRY has no entry for '${name}'`);
  }
  return entry;
}

const REQUIRED_ENV_BY_PROVIDER: Record<string, readonly string[]> = {
  anthropic: ["ANTHROPIC_API_KEY"],
  "google-genai": ["GOOGLE_API_KEY"],
  "opencode-zen": ["OPENCODE_API_KEY", "OPENCODE_BASE_URL"],
  openai: ["OPENAI_API_KEY"],
};

describe("PLUGIN_REGISTRY provider contract", () => {
  test("advertises exactly the expected providers in order", () => {
    const names = PLUGIN_REGISTRY.map((entry) => entry.name);
    expect(names).toEqual([
      "anthropic",
      "google-genai",
      "opencode-zen",
      "openai",
    ]);
  });

  for (const [provider, requiredEnv] of Object.entries(
    REQUIRED_ENV_BY_PROVIDER,
  )) {
    test(`${provider} advertises requiredEnv ${requiredEnv.join(", ")}`, () => {
      expect(requireEntry(provider).requiredEnv).toEqual(requiredEnv);
    });
  }
});

describe("findPlugin", () => {
  test("returns the matching entry for a known provider", () => {
    expect(findPlugin("openai")?.name).toBe("openai");
  });

  test("returns undefined for an unknown provider", () => {
    expect(findPlugin("gemini")).toBeUndefined();
  });
});

describe("create factory env wiring", () => {
  test("anthropic builds a claude-sonnet-5 plug-in from ANTHROPIC_API_KEY", () => {
    const plugin = requireEntry("anthropic").create({
      ANTHROPIC_API_KEY: "test-key",
    });
    expect(plugin.name).toBe("anthropic");
    expect(plugin.models).toContain("claude-sonnet-5");
  });

  test("anthropic raises when ANTHROPIC_API_KEY is absent", () => {
    expect(() => requireEntry("anthropic").create({})).toThrow(
      "ANTHROPIC_API_KEY",
    );
  });

  test("google-genai builds a gemini-2.5-pro plug-in from GOOGLE_API_KEY", () => {
    const plugin = requireEntry("google-genai").create({
      GOOGLE_API_KEY: "test-key",
    });
    expect(plugin.name).toBe("google-genai");
    expect(plugin.models).toContain("gemini-2.5-pro");
  });

  test("google-genai raises when GOOGLE_API_KEY is absent", () => {
    expect(() => requireEntry("google-genai").create({})).toThrow(
      "GOOGLE_API_KEY",
    );
  });

  test("openai builds a gpt-5.5 plug-in from OPENAI_API_KEY", () => {
    const plugin = requireEntry("openai").create({
      OPENAI_API_KEY: "test-key",
    });
    expect(plugin.name).toBe("openai");
    expect(plugin.models).toContain("gpt-5.5");
  });

  test("openai raises when OPENAI_API_KEY is absent", () => {
    expect(() => requireEntry("openai").create({})).toThrow("OPENAI_API_KEY");
  });

  test("opencode-zen builds a plug-in from OPENCODE_API_KEY and OPENCODE_BASE_URL", () => {
    const plugin = requireEntry("opencode-zen").create({
      OPENCODE_API_KEY: "test-key",
      OPENCODE_BASE_URL: "https://example.test/v1",
    });
    expect(plugin.name).toBe("opencode-zen");
    expect(plugin.models).toContain("kimi-k3");
  });

  test("opencode-zen raises when OPENCODE_BASE_URL is absent", () => {
    expect(() =>
      requireEntry("opencode-zen").create({ OPENCODE_API_KEY: "test-key" }),
    ).toThrow("OPENCODE_API_KEY or OPENCODE_BASE_URL");
  });

  test("opencode-zen does not accept the OpenAI credential", () => {
    // The OpenAI key must not satisfy the opencode-zen factory: it reads only
    // OPENCODE_API_KEY / OPENCODE_BASE_URL, so a mis-wire that read
    // OPENAI_API_KEY instead would be caught here.
    expect(() =>
      requireEntry("opencode-zen").create({ OPENAI_API_KEY: "test-key" }),
    ).toThrow("OPENCODE_API_KEY or OPENCODE_BASE_URL");
  });
});
