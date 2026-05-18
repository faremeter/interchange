import { describe, test, expect } from "bun:test";

import type { ProviderConfig } from "@interchange/types/runtime";

import {
  createProviderRegistry,
  InvalidProviderConfigError,
  ProviderNotFoundError,
} from "./provider";

const P_ANTHROPIC: ProviderConfig = {
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-anthropic-1",
  model: "claude-3-5-sonnet",
};

const P_OPENAI: ProviderConfig = {
  provider: "openai",
  baseURL: "https://api.openai.com",
  apiKey: "sk-openai-1",
  model: "gpt-4o",
};

/**
 * Test helper: produce an intentionally-invalid `ProviderConfig` value so
 * we can verify that runtime validation rejects it. The whole point is to
 * exercise the arktype check, which requires bypassing the static type.
 */
function invalidConfig(value: unknown): ProviderConfig {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional invalid input for arktype validation test
  return value as ProviderConfig;
}

describe("createProviderRegistry", () => {
  test("selects the provider whose model matches defaultModel", () => {
    const reg = createProviderRegistry({
      providers: [P_ANTHROPIC, P_OPENAI],
      defaultModel: "gpt-4o",
    });
    expect(reg.active.provider).toBe("openai");
    expect(reg.active.model).toBe("gpt-4o");
    expect(reg.active.apiKey).toBe("sk-openai-1");
  });

  test("rejects an empty providers[] array", () => {
    expect(() =>
      createProviderRegistry({ providers: [], defaultModel: "anything" }),
    ).toThrow(InvalidProviderConfigError);
  });

  test("rejects providers that fail ProviderConfig arktype validation", () => {
    expect(() =>
      createProviderRegistry({
        providers: [invalidConfig({ provider: "x", apiKey: "k" })],
        defaultModel: "anything",
      }),
    ).toThrow(InvalidProviderConfigError);
  });

  test("rejects a provider entry missing model", () => {
    const noModel: ProviderConfig = {
      provider: "anthropic",
      baseURL: "u",
      apiKey: "k",
    };
    expect(() =>
      createProviderRegistry({
        providers: [noModel],
        defaultModel: "anything",
      }),
    ).toThrow(InvalidProviderConfigError);
  });

  test("throws ProviderNotFoundError when defaultModel matches no provider", () => {
    expect(() =>
      createProviderRegistry({
        providers: [P_ANTHROPIC],
        defaultModel: "gpt-4o",
      }),
    ).toThrow(ProviderNotFoundError);
  });

  test("active is a mutable holder; setProvider mutates fields in place", () => {
    const reg = createProviderRegistry({
      providers: [P_ANTHROPIC],
      defaultModel: "claude-3-5-sonnet",
    });
    const reference = reg.active;

    reg.setProvider({
      provider: "anthropic",
      baseURL: "https://proxy.example.com",
      apiKey: "sk-new",
      model: "claude-3-5-haiku",
    });

    expect(reg.active).toBe(reference);
    expect(reg.active.provider).toBe("anthropic");
    expect(reg.active.baseURL).toBe("https://proxy.example.com");
    expect(reg.active.apiKey).toBe("sk-new");
    expect(reg.active.model).toBe("claude-3-5-haiku");
  });

  test("setProvider deletes model when the new config has none", () => {
    const reg = createProviderRegistry({
      providers: [P_ANTHROPIC],
      defaultModel: "claude-3-5-sonnet",
    });

    reg.setProvider({
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "sk-anthropic-1",
    });

    expect(reg.active.model).toBeUndefined();
  });

  test("setProvider throws InvalidProviderConfigError on invalid input", () => {
    const reg = createProviderRegistry({
      providers: [P_ANTHROPIC],
      defaultModel: "claude-3-5-sonnet",
    });

    expect(() => reg.setProvider(invalidConfig({ provider: "x" }))).toThrow(
      InvalidProviderConfigError,
    );
  });

  test("does not mutate the caller's providers[] entries", () => {
    const inputs: ProviderConfig[] = [{ ...P_ANTHROPIC }];
    const reg = createProviderRegistry({
      providers: inputs,
      defaultModel: "claude-3-5-sonnet",
    });

    reg.setProvider({
      provider: "anthropic",
      baseURL: "https://other.example.com",
      apiKey: "sk-other",
      model: "claude-3-5-sonnet",
    });

    expect(inputs[0]?.apiKey).toBe("sk-anthropic-1");
    expect(inputs[0]?.baseURL).toBe("https://api.anthropic.com");
  });
});
