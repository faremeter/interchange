import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { FixtureManifest } from "./manifest";

describe("FixtureManifest validator", () => {
  test("accepts a well-formed manifest", () => {
    const result = FixtureManifest({
      provider: "google-genai",
      model: "gemini-2.5-flash",
      capability: "plain-text",
      capturedAt: "2026-05-20T15:00:00Z",
      schemaVersion: "1",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an explicit null observedModelVersion", () => {
    const result = FixtureManifest({
      provider: "opencode-zen",
      model: "kimi-k2.6",
      capability: "reasoning-content",
      capturedAt: "2026-05-20T15:00:00Z",
      observedModelVersion: null,
      schemaVersion: "1",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a populated observedModelVersion string", () => {
    const result = FixtureManifest({
      provider: "opencode-zen",
      model: "kimi-k2.6",
      capability: "plain-text",
      capturedAt: "2026-05-20T15:00:00Z",
      observedModelVersion: "moonshotai/kimi-k2.6-20260420",
      schemaVersion: "1",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a manifest with an unknown capability", () => {
    const result = FixtureManifest({
      provider: "google-genai",
      model: "gemini-2.5-flash",
      capability: "not-a-real-capability",
      capturedAt: "2026-05-20T15:00:00Z",
      schemaVersion: "1",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a manifest with the wrong schemaVersion literal", () => {
    const result = FixtureManifest({
      provider: "google-genai",
      model: "gemini-2.5-flash",
      capability: "plain-text",
      capturedAt: "2026-05-20T15:00:00Z",
      schemaVersion: "2",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a manifest missing required fields", () => {
    const result = FixtureManifest({
      provider: "google-genai",
      capability: "plain-text",
      capturedAt: "2026-05-20T15:00:00Z",
      schemaVersion: "1",
    });
    expect(result instanceof type.errors).toBe(true);
  });
});
