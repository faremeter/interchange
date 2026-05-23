import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { SUPPORT_MATRIX, SupportEntry, getFixtureDir } from "./support-matrix";

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

describe("getFixtureDir", () => {
  test("returns a wire-relative path for a captured entry", () => {
    const captured = SUPPORT_MATRIX.find((e) => e.outcome === "captured");
    expect(captured).toBeDefined();
    if (captured === undefined) return;
    const dir = getFixtureDir(captured);
    expect(dir).toBe(
      `packages/inference-testing/wire/${captured.provider}/${captured.model}/${captured.capability}`,
    );
  });

  test("returns a wire-relative path for a misled entry", () => {
    const misled = SUPPORT_MATRIX.find((e) => e.outcome === "misled");
    if (misled === undefined) return;
    const dir = getFixtureDir(misled);
    expect(dir).toBe(
      `packages/inference-testing/wire/${misled.provider}/${misled.model}/${misled.capability}`,
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
});
