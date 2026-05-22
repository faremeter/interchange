import { describe, test, expect } from "bun:test";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { type } from "arktype";
import {
  CAPABILITIES,
  SUPPORT_MATRIX,
  FixtureManifest,
  getFixtureDir,
} from "@intx/inference-discovery/catalog";

const repoRoot = join(import.meta.dir, "..", "..", "..");

function hasCapturedRequest(dir: string): boolean {
  if (existsSync(join(dir, "request.json"))) return true;
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory() && hasCapturedRequest(join(dir, entry.name))) {
      return true;
    }
  }
  return false;
}

describe("inference discovery catalog contract", () => {
  test("vocabulary has exactly 25 capabilities", () => {
    expect(CAPABILITIES.length).toBe(25);
  });

  test("no duplicate capability names", () => {
    expect(new Set(CAPABILITIES).size).toBe(CAPABILITIES.length);
  });

  test("every captured entry resolves on disk and its manifest validates", () => {
    const captured = SUPPORT_MATRIX.filter((e) => e.outcome === "captured");
    expect(captured.length).toBeGreaterThan(0);

    for (const entry of captured) {
      const rel = getFixtureDir(entry);
      expect(rel).not.toBeNull();
      if (rel === null) continue;

      const abs = join(repoRoot, rel);
      expect(statSync(abs).isDirectory()).toBe(true);
      expect(hasCapturedRequest(abs)).toBe(true);
      expect(existsSync(join(abs, "manifest.json"))).toBe(true);

      const raw: unknown = JSON.parse(
        readFileSync(join(abs, "manifest.json"), "utf8"),
      );
      const parsed = FixtureManifest(raw);
      expect(parsed instanceof type.errors).toBe(false);
      expect(parsed).toMatchObject({
        provider: entry.provider,
        model: entry.model,
        capability: entry.capability,
      });
    }
  });

  test("getFixtureDir returns null for non-captured entries", () => {
    const nonCaptured = SUPPORT_MATRIX.filter((e) => e.outcome !== "captured");
    for (const entry of nonCaptured) {
      expect(getFixtureDir(entry)).toBeNull();
    }
  });

  test("captured count is at least 55", () => {
    expect(
      SUPPORT_MATRIX.filter((e) => e.outcome === "captured").length,
    ).toBeGreaterThanOrEqual(55);
  });
});
