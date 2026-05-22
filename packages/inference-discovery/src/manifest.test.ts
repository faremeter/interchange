import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { FixtureManifest } from "./catalog";
import { buildManifest } from "./manifest";

describe("buildManifest", () => {
  test("produces a record accepted by FixtureManifest", () => {
    const manifest = buildManifest({
      provider: "google-genai",
      model: "gemini-2.5-flash",
      capability: "plain-text",
      now: () => new Date("2026-05-22T00:00:00Z"),
    });
    const validated = FixtureManifest(manifest);
    expect(validated instanceof type.errors).toBe(false);
    expect(manifest.provider).toBe("google-genai");
    expect(manifest.model).toBe("gemini-2.5-flash");
    expect(manifest.capability).toBe("plain-text");
    expect(manifest.capturedAt).toBe("2026-05-22T00:00:00.000Z");
    expect(manifest.schemaVersion).toBe("1");
    expect(manifest.observedModelVersion).toBeUndefined();
  });

  test("omits observedModelVersion when not provided", () => {
    const manifest = buildManifest({
      provider: "p",
      model: "m",
      capability: "plain-text",
      now: () => new Date("2026-05-22T00:00:00Z"),
    });
    expect("observedModelVersion" in manifest).toBe(false);
  });

  test("includes observedModelVersion when provided", () => {
    const manifest = buildManifest({
      provider: "p",
      model: "m",
      capability: "plain-text",
      now: () => new Date("2026-05-22T00:00:00Z"),
      observedModelVersion: "abc123",
    });
    expect(manifest.observedModelVersion).toBe("abc123");
  });

  test("uses real Date by default", () => {
    const before = new Date().toISOString();
    const manifest = buildManifest({
      provider: "p",
      model: "m",
      capability: "plain-text",
    });
    const after = new Date().toISOString();
    expect(manifest.capturedAt >= before).toBe(true);
    expect(manifest.capturedAt <= after).toBe(true);
  });
});
