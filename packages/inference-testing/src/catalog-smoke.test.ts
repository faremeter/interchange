import { describe, test, expect } from "bun:test";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { type } from "arktype";
import {
  CAPABILITIES,
  SUPPORT_MATRIX,
  CaptureManifest,
  getSessionDir,
} from "@intx/inference-discovery/catalog";

const repoRoot = join(import.meta.dir, "..", "..", "..");

function hasCapturedRequest(dir: string): boolean {
  if (existsSync(join(dir, "request.json"))) return true;
  if (existsSync(join(dir, "request.bin"))) return true;
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory() && hasCapturedRequest(join(dir, entry.name))) {
      return true;
    }
  }
  return false;
}

describe("inference discovery catalog contract", () => {
  test("vocabulary has exactly 32 capabilities", () => {
    expect(CAPABILITIES.length).toBe(32);
  });

  test("no duplicate capability names", () => {
    expect(new Set(CAPABILITIES).size).toBe(CAPABILITIES.length);
  });

  test("every fixture-bearing entry resolves a session whose manifest is authoritative", () => {
    const fixtureBearing = SUPPORT_MATRIX.filter(
      (e) => e.outcome === "captured" || e.outcome === "misled",
    );
    expect(fixtureBearing.length).toBeGreaterThan(0);

    for (const entry of fixtureBearing) {
      const rel = getSessionDir(entry);
      expect(rel).not.toBeNull();
      if (rel === null) continue;

      const abs = join(repoRoot, rel);
      expect(statSync(abs).isDirectory()).toBe(true);
      expect(existsSync(join(abs, "session.json"))).toBe(true);
      expect(hasCapturedRequest(abs)).toBe(true);

      const raw: unknown = JSON.parse(
        readFileSync(join(abs, "session.json"), "utf8"),
      );
      const parsed = CaptureManifest(raw);
      expect(parsed instanceof type.errors).toBe(false);
      if (parsed instanceof type.errors) continue;

      // The session manifest is authoritative for the capability and model
      // the directory holds; cross-check it against the matrix cell. The
      // brand provider is not in the manifest (source.provider is the
      // adapter name), so it stays cross-checked by the resolved path.
      expect(parsed.capability).toBe(entry.capability);
      expect(parsed.source.model).toBe(entry.model);
    }
  });

  test("getSessionDir returns null for entries without fixtures", () => {
    const noFixture = SUPPORT_MATRIX.filter(
      (e) => e.outcome !== "captured" && e.outcome !== "misled",
    );
    for (const entry of noFixture) {
      expect(getSessionDir(entry)).toBeNull();
    }
  });

  test("captured count is at least 55", () => {
    expect(
      SUPPORT_MATRIX.filter((e) => e.outcome === "captured").length,
    ).toBeGreaterThanOrEqual(55);
  });
});
