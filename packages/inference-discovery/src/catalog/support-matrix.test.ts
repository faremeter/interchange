import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  SUPPORT_MATRIX,
  SupportEntry,
  STRUCTURED_OUTPUT_CAPABILITIES,
  getFixtureDir,
} from "./support-matrix";

describe("SUPPORT_MATRIX validation", () => {
  test("every entry parses as a SupportEntry", () => {
    for (const entry of SUPPORT_MATRIX) {
      const result = SupportEntry(entry);
      expect(result instanceof type.errors).toBe(false);
    }
  });

  test("contains at least 22 google-genai captured entries", () => {
    const count = SUPPORT_MATRIX.filter(
      (entry) =>
        entry.provider === "google-genai" && entry.outcome === "captured",
    ).length;
    expect(count).toBeGreaterThanOrEqual(22);
  });

  test("contains at least 33 opencode-zen captured entries", () => {
    const count = SUPPORT_MATRIX.filter(
      (entry) =>
        entry.provider === "opencode-zen" && entry.outcome === "captured",
    ).length;
    expect(count).toBeGreaterThanOrEqual(33);
  });

  test("contains at least one non-captured opencode-zen entry with notes", () => {
    const nonCaptured = SUPPORT_MATRIX.filter(
      (entry) =>
        entry.provider === "opencode-zen" && entry.outcome !== "captured",
    );
    expect(nonCaptured.length).toBeGreaterThanOrEqual(1);
    for (const entry of nonCaptured) {
      expect(typeof entry.notes).toBe("string");
      expect((entry.notes ?? "").length).toBeGreaterThan(0);
    }
  });

  test("no duplicate (provider, model, capability) triples", () => {
    const seen = new Set<string>();
    for (const entry of SUPPORT_MATRIX) {
      const key = `${entry.provider}|${entry.model}|${entry.capability}`;
      expect(seen.has(key)).toBe(false);
      seen.add(key);
    }
  });
});

describe("STRUCTURED_OUTPUT_CAPABILITIES", () => {
  // Pins the shared constant's exact membership so that adding a member fails
  // here loudly rather than silently fanning an extra row across every model
  // that references it.
  test("is exactly the two structured-output capabilities", () => {
    expect(STRUCTURED_OUTPUT_CAPABILITIES).toEqual([
      "structured-output",
      "structured-output-streaming",
    ]);
  });
});

describe("getFixtureDir", () => {
  test("composes the anthropic package wire path for a captured entry", () => {
    const entry = SUPPORT_MATRIX.find(
      (e) => e.provider === "anthropic" && e.outcome === "captured",
    );
    expect(entry).toBeDefined();
    if (entry === undefined) return;
    expect(getFixtureDir(entry)).toBe(
      `packages/inference-discovery-anthropic/wire/anthropic/${entry.model}/${entry.capability}`,
    );
  });

  test("composes the openai package wire path for a captured opencode-zen entry", () => {
    const entry = SUPPORT_MATRIX.find(
      (e) => e.provider === "opencode-zen" && e.outcome === "captured",
    );
    expect(entry).toBeDefined();
    if (entry === undefined) return;
    expect(getFixtureDir(entry)).toBe(
      `packages/inference-discovery-openai/wire/opencode-zen/${entry.model}/${entry.capability}`,
    );
  });

  test("returns a fixture path for a misled entry", () => {
    const entry = SUPPORT_MATRIX.find(
      (e) => e.provider === "anthropic" && e.outcome === "misled",
    );
    expect(entry).toBeDefined();
    if (entry === undefined) return;
    expect(getFixtureDir(entry)).toBe(
      `packages/inference-discovery-anthropic/wire/anthropic/${entry.model}/${entry.capability}`,
    );
  });

  test("returns null for an entry without a fixture", () => {
    const noFixture = SUPPORT_MATRIX.find(
      (e) => e.outcome !== "captured" && e.outcome !== "misled",
    );
    expect(noFixture).toBeDefined();
    if (noFixture === undefined) return;
    expect(getFixtureDir(noFixture)).toBeNull();
  });

  test("throws for a fixture-bearing entry whose provider has no wire root", () => {
    const entry: SupportEntry = {
      provider: "made-up-provider",
      model: "some-model",
      capability: "plain-text",
      outcome: "captured",
    };
    expect(() => getFixtureDir(entry)).toThrow(/no fixture root/);
  });
});
