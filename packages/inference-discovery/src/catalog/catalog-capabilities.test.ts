import { describe, expect, test } from "bun:test";

import { catalogCapabilitiesFor } from "./catalog-capabilities";
import type { SupportEntry } from "./support-matrix";

const PROVIDER = "test-provider";
const MODEL = "test-model";

function entry(
  capability: SupportEntry["capability"],
  outcome: SupportEntry["outcome"],
): SupportEntry {
  return { provider: PROVIDER, model: MODEL, capability, outcome };
}

describe("catalogCapabilitiesFor over synthetic rows", () => {
  test("a proven streaming variant lights up its base and itself", () => {
    const result = catalogCapabilitiesFor(PROVIDER, MODEL, [
      entry("vision-input-streaming", "captured"),
    ]);
    expect(result).toEqual(["vision-input", "vision-input-streaming"]);
  });

  test("a proven base lights up only the base", () => {
    const result = catalogCapabilitiesFor(PROVIDER, MODEL, [
      entry("vision-input", "captured"),
    ]);
    expect(result).toEqual(["vision-input"]);
  });

  test("misled rows contribute", () => {
    const result = catalogCapabilitiesFor(PROVIDER, MODEL, [
      entry("redacted-thinking", "misled"),
    ]);
    expect(result).toEqual(["redacted-thinking"]);
  });

  test("refused, http-error, and unsupported rows do not contribute", () => {
    const result = catalogCapabilitiesFor(PROVIDER, MODEL, [
      entry("vision-input", "refused"),
      entry("audio-input", "http-error"),
      entry("video-input", "unsupported"),
    ]);
    expect(result).toEqual([]);
  });

  test("anti-evidence counts against its own variant only", () => {
    const result = catalogCapabilitiesFor(PROVIDER, MODEL, [
      entry("vision-input", "captured"),
      entry("vision-input-streaming", "refused"),
    ]);
    expect(result).toEqual(["vision-input"]);
  });

  test("a proven discovery-only streaming probe strips then drops", () => {
    // safety-classification-streaming strips to safety-classification; neither
    // variant is in the production wire vocabulary, so the result is empty.
    const result = catalogCapabilitiesFor(PROVIDER, MODEL, [
      entry("safety-classification-streaming", "captured"),
    ]);
    expect(result).toEqual([]);
  });

  test("an unknown (provider, model) yields nothing", () => {
    const result = catalogCapabilitiesFor("nobody", "nothing", [
      entry("vision-input", "captured"),
    ]);
    expect(result).toEqual([]);
  });

  test("rows for a different (provider, model) are ignored", () => {
    const result = catalogCapabilitiesFor(PROVIDER, MODEL, [
      entry("vision-input", "captured"),
      {
        provider: "other",
        model: MODEL,
        capability: "audio-input",
        outcome: "captured",
      },
    ]);
    expect(result).toEqual(["vision-input"]);
  });
});

describe("catalogCapabilitiesFor over the production support matrix", () => {
  test("projects gemini-2.5-flash's captured flows and drops safety-classification", () => {
    const result = catalogCapabilitiesFor("google-genai", "gemini-2.5-flash");
    expect(result).toContain("vision-input");
    expect(result).toContain("audio-input");
    expect(result).toContain("function-calling-multi-turn");
    expect(result).not.toContain("safety-classification");
    expect(result).not.toContain("safety-classification-streaming");
  });

  test("keeps a model's captured flows and drops its non-fixture-bearing ones", () => {
    // deepseek-v4-pro's reasoning-content flows are captured, but its
    // structured-output and vision-input probes are http-error.
    const result = catalogCapabilitiesFor("opencode-zen", "deepseek-v4-pro");
    expect(result).toContain("reasoning-content");
    expect(result).toContain("reasoning-content-streaming");
    expect(result).not.toContain("structured-output");
    expect(result).not.toContain("vision-input");
  });

  test("projects gpt-5.5's captured flows and omits reasoning-content", () => {
    // gpt-5.5 on api.openai.com Chat Completions returns no reasoning field,
    // so reasoning-content is unsupported (not misled) and never projected —
    // the highest-value assertion here, since a naive read would assume a
    // frontier model exposes its reasoning trace.
    const result = catalogCapabilitiesFor("openai", "gpt-5.5");
    expect(result).toContain("function-calling");
    expect(result).toContain("vision-input");
    expect(result).toContain("structured-output-streaming");
    expect(result).not.toContain("reasoning-content");
    expect(result).toHaveLength(7);
  });

  test("projects claude-sonnet-5's thinking and code-execution flows", () => {
    const result = catalogCapabilitiesFor("anthropic", "claude-sonnet-5");
    expect(result).toContain("reasoning-content");
    expect(result).toContain("code-execution");
    expect(result).toContain("redacted-thinking");
    expect(result).toHaveLength(21);
  });

  test("projects gemini-2.5-pro's audio, video, and code-execution flows", () => {
    const result = catalogCapabilitiesFor("google-genai", "gemini-2.5-pro");
    expect(result).toContain("audio-input");
    expect(result).toContain("video-input");
    expect(result).toContain("code-execution");
    expect(result).toHaveLength(22);
  });

  test("projects kimi-k3's reasoning, vision, and structured-output flows", () => {
    const result = catalogCapabilitiesFor("opencode-zen", "kimi-k3");
    expect(result).toContain("reasoning-content");
    expect(result).toContain("vision-input");
    expect(result).toContain("structured-output");
    expect(result).toHaveLength(9);
  });

  test("projects kimi-k2.7-code's reasoning, vision, and structured-output flows", () => {
    const result = catalogCapabilitiesFor("opencode-zen", "kimi-k2.7-code");
    expect(result).toContain("reasoning-content");
    expect(result).toContain("vision-input");
    expect(result).toContain("structured-output");
    expect(result).toHaveLength(9);
  });
});
