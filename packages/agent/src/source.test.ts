import { describe, test, expect } from "bun:test";

import type { InferenceSource } from "@intx/types/runtime";

import {
  createSourceRegistry,
  InvalidInferenceSourceError,
  SourceNotFoundError,
} from "./source";

const S_ANTHROPIC: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-anthropic-1",
  model: "claude-3-5-sonnet",
};

const S_OPENAI: InferenceSource = {
  id: "openai:gpt-4o",
  provider: "openai",
  baseURL: "https://api.openai.com",
  apiKey: "sk-openai-1",
  model: "gpt-4o",
};

/**
 * Test helper: produce an intentionally-invalid `InferenceSource` value so
 * we can verify that runtime validation rejects it. The whole point is to
 * exercise the arktype check, which requires bypassing the static type.
 */
function invalidSource(value: unknown): InferenceSource {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional invalid input for arktype validation test
  return value as InferenceSource;
}

describe("createSourceRegistry", () => {
  test("selects the source whose id matches defaultSource", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC, S_OPENAI],
      defaultSource: "openai:gpt-4o",
    });
    expect(reg.active.provider).toBe("openai");
    expect(reg.active.model).toBe("gpt-4o");
    expect(reg.active.apiKey).toBe("sk-openai-1");
  });

  test("rejects an empty sources[] array", () => {
    expect(() =>
      createSourceRegistry({ sources: [], defaultSource: "anything" }),
    ).toThrow(InvalidInferenceSourceError);
  });

  test("rejects sources that fail InferenceSource arktype validation", () => {
    expect(() =>
      createSourceRegistry({
        sources: [invalidSource({ id: "x", provider: "x", apiKey: "k" })],
        defaultSource: "anything",
      }),
    ).toThrow(InvalidInferenceSourceError);
  });

  test("rejects a source entry missing model", () => {
    const noModel = invalidSource({
      id: "anthropic:none",
      provider: "anthropic",
      baseURL: "u",
      apiKey: "k",
    });
    expect(() =>
      createSourceRegistry({
        sources: [noModel],
        defaultSource: "anthropic:none",
      }),
    ).toThrow(InvalidInferenceSourceError);
  });

  test("rejects sources[] with duplicate ids", () => {
    expect(() =>
      createSourceRegistry({
        sources: [S_ANTHROPIC, { ...S_ANTHROPIC, apiKey: "sk-other" }],
        defaultSource: S_ANTHROPIC.id,
      }),
    ).toThrow(InvalidInferenceSourceError);
  });

  test("throws SourceNotFoundError when defaultSource matches no source", () => {
    expect(() =>
      createSourceRegistry({
        sources: [S_ANTHROPIC],
        defaultSource: "openai:gpt-4o",
      }),
    ).toThrow(SourceNotFoundError);
  });

  test("active is a mutable holder; setSource mutates fields in place", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC],
      defaultSource: "anthropic:claude-3-5-sonnet",
    });
    const reference = reg.active;

    reg.setSource({
      id: "anthropic:claude-3-5-haiku",
      provider: "anthropic",
      baseURL: "https://proxy.example.com",
      apiKey: "sk-new",
      model: "claude-3-5-haiku",
    });

    expect(reg.active).toBe(reference);
    expect(reg.active.id).toBe("anthropic:claude-3-5-haiku");
    expect(reg.active.provider).toBe("anthropic");
    expect(reg.active.baseURL).toBe("https://proxy.example.com");
    expect(reg.active.apiKey).toBe("sk-new");
    expect(reg.active.model).toBe("claude-3-5-haiku");
  });

  test("setSource overwrites defaults, capabilities, and quirks, including deletion", () => {
    const reg = createSourceRegistry({
      sources: [
        {
          ...S_ANTHROPIC,
          defaults: { maxTokens: 1024 },
          capabilities: ["text"],
          quirks: { forceAssistantReasoningContent: true },
        },
      ],
      defaultSource: S_ANTHROPIC.id,
    });

    reg.setSource({
      ...S_ANTHROPIC,
      defaults: { maxTokens: 4096 },
      capabilities: ["text", "vision"],
      quirks: { reasoningFieldNames: ["reasoning"] },
    });
    expect(reg.active.defaults).toEqual({ maxTokens: 4096 });
    expect(reg.active.capabilities).toEqual(["text", "vision"]);
    expect(reg.active.quirks).toEqual({ reasoningFieldNames: ["reasoning"] });

    reg.setSource(S_ANTHROPIC);
    expect(reg.active.defaults).toBeUndefined();
    expect(reg.active.capabilities).toBeUndefined();
    expect(reg.active.quirks).toBeUndefined();
  });

  test("setSource throws InvalidInferenceSourceError on invalid input", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC],
      defaultSource: S_ANTHROPIC.id,
    });

    expect(() => reg.setSource(invalidSource({ provider: "x" }))).toThrow(
      InvalidInferenceSourceError,
    );
  });

  test("does not mutate the caller's sources[] entries", () => {
    const inputs: InferenceSource[] = [{ ...S_ANTHROPIC }];
    const reg = createSourceRegistry({
      sources: inputs,
      defaultSource: S_ANTHROPIC.id,
    });

    reg.setSource({
      ...S_ANTHROPIC,
      baseURL: "https://other.example.com",
      apiKey: "sk-other",
    });

    expect(inputs[0]?.apiKey).toBe("sk-anthropic-1");
    expect(inputs[0]?.baseURL).toBe("https://api.anthropic.com");
  });

  test("setSources replaces the list and activates the new default in place", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC],
      defaultSource: S_ANTHROPIC.id,
    });
    const reference = reg.active;

    reg.setSources([S_ANTHROPIC, S_OPENAI], "openai:gpt-4o");

    expect(reg.active).toBe(reference);
    expect(reg.active.id).toBe("openai:gpt-4o");
    expect(reg.active.provider).toBe("openai");
    expect(reg.active.apiKey).toBe("sk-openai-1");
  });

  test("setSources throws when the new default matches no source", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC],
      defaultSource: S_ANTHROPIC.id,
    });
    expect(() => reg.setSources([S_OPENAI], "anthropic:none")).toThrow(
      SourceNotFoundError,
    );
  });

  test("setSources rejects an empty list", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC],
      defaultSource: S_ANTHROPIC.id,
    });
    expect(() => reg.setSources([], "anything")).toThrow(
      InvalidInferenceSourceError,
    );
  });

  test("failOverToNextSource advances through the list and reports exhaustion", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC, S_OPENAI],
      defaultSource: S_ANTHROPIC.id,
    });
    const reference = reg.active;
    expect(reg.active.id).toBe(S_ANTHROPIC.id);

    expect(reg.failOverToNextSource()).toBe(true);
    expect(reg.active).toBe(reference); // mutated in place
    expect(reg.active.id).toBe(S_OPENAI.id);
    expect(reg.active.apiKey).toBe(S_OPENAI.apiKey);

    // Already at the last source: no further failover target.
    expect(reg.failOverToNextSource()).toBe(false);
    expect(reg.active.id).toBe(S_OPENAI.id);
  });

  test("failOverToNextSource starts from the default, not the list head", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC, S_OPENAI],
      defaultSource: S_OPENAI.id,
    });
    // Default is the last entry, so there is nothing to fail over to.
    expect(reg.failOverToNextSource()).toBe(false);
    expect(reg.active.id).toBe(S_OPENAI.id);
  });

  test("resetToPreferredSource returns to the default after a failover", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC, S_OPENAI],
      defaultSource: S_ANTHROPIC.id,
    });
    const reference = reg.active;
    reg.failOverToNextSource();
    expect(reg.active.id).toBe(S_OPENAI.id);

    reg.resetToPreferredSource();
    expect(reg.active).toBe(reference);
    expect(reg.active.id).toBe(S_ANTHROPIC.id);
  });

  test("setSources repositions the default that resetToPreferredSource restores", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC],
      defaultSource: S_ANTHROPIC.id,
    });
    reg.setSources([S_ANTHROPIC, S_OPENAI], S_OPENAI.id);
    expect(reg.active.id).toBe(S_OPENAI.id);

    reg.resetToPreferredSource();
    expect(reg.active.id).toBe(S_OPENAI.id);
    expect(reg.failOverToNextSource()).toBe(false);
  });

  test("a setSource hot-swap survives resetToPreferredSource after a failover", () => {
    const reg = createSourceRegistry({
      sources: [S_ANTHROPIC, S_OPENAI],
      defaultSource: S_ANTHROPIC.id,
    });
    // Fail over off the default, then hot-swap to an off-list source.
    reg.failOverToNextSource();
    expect(reg.active.id).toBe(S_OPENAI.id);
    reg.setSource({
      id: "anthropic:claude-3-5-haiku",
      provider: "anthropic",
      baseURL: "https://proxy.example.com",
      apiKey: "sk-hot",
      model: "claude-3-5-haiku",
    });

    // The per-cycle reset must not discard the deliberate hot-swap.
    reg.resetToPreferredSource();
    expect(reg.active.id).toBe("anthropic:claude-3-5-haiku");
    expect(reg.active.apiKey).toBe("sk-hot");
  });
});
