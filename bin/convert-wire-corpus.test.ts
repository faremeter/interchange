import { describe, test, expect, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import * as path from "node:path";

import { loadCaptureManifest } from "@intx/inference-discovery/catalog";

import { convertCorpus } from "./convert-wire-corpus";

const REPO_ROOT = path.resolve(import.meta.dirname, "..");
const ANTHROPIC_SESSION_ROOT =
  "packages/inference-discovery-anthropic/sessions/anthropic";

let tmpDirs: string[] = [];

afterEach(async () => {
  for (const dir of tmpDirs) {
    await fs.rm(dir, { recursive: true, force: true });
  }
  tmpDirs = [];
});

async function makeTmpDir(): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "wire-corpus-"));
  tmpDirs.push(dir);
  return dir;
}

// Walk to the first directory that holds a session.json.
async function firstSessionDir(root: string): Promise<string> {
  async function walk(dir: string): Promise<string | null> {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    if (entries.some((e) => e.isFile() && e.name === "session.json")) {
      return dir;
    }
    for (const entry of entries) {
      if (entry.isDirectory()) {
        const found = await walk(path.join(dir, entry.name));
        if (found !== null) return found;
      }
    }
    return null;
  }
  const found = await walk(root);
  if (found === null) {
    throw new Error(`no session.json found under ${root}`);
  }
  return found;
}

describe("convertCorpus", () => {
  test("converts every fixture-bearing Anthropic cell into a valid v2 session", async () => {
    const outputRoot = await makeTmpDir();
    const count = await convertCorpus({
      provider: "anthropic",
      repoRoot: REPO_ROOT,
      outputRoot,
    });
    expect(count).toBeGreaterThan(0);

    const sessionDir = await firstSessionDir(
      path.join(outputRoot, ANTHROPIC_SESSION_ROOT),
    );
    const manifest = await loadCaptureManifest(sessionDir);
    expect(manifest.schemaVersion).toBe("2");
    expect(manifest.origin).toBe("live");
    expect(manifest.source.provider).toBe("anthropic");
    expect(manifest.source.baseURL).toBe("https://api.anthropic.com");
  });

  test("clears a stale session before regenerating so orphans do not survive", async () => {
    const outputRoot = await makeTmpDir();
    const staleCell = path.join(
      outputRoot,
      ANTHROPIC_SESSION_ROOT,
      "model-that-left-the-matrix",
    );
    await fs.mkdir(staleCell, { recursive: true });
    await fs.writeFile(path.join(staleCell, "session.json"), "{}");

    await convertCorpus({
      provider: "anthropic",
      repoRoot: REPO_ROOT,
      outputRoot,
    });

    await expect(fs.access(staleCell)).rejects.toThrow();
  });

  test("throws for a provider with no fixture-bearing cells", async () => {
    const outputRoot = await makeTmpDir();
    await expect(
      convertCorpus({
        provider: "no-such-provider",
        repoRoot: REPO_ROOT,
        outputRoot,
      }),
    ).rejects.toThrow(/no fixture-bearing cells/);
  });
});
