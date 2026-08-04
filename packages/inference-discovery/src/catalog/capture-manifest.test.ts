import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { type } from "arktype";

import {
  CaptureManifest,
  loadCaptureManifest,
  writeCaptureManifest,
} from "./capture-manifest";

async function makeTmpDir(): Promise<string> {
  return await fs.mkdtemp(path.join(os.tmpdir(), "capture-manifest-test-"));
}

describe("CaptureManifest", () => {
  test("validates a well-formed manifest", () => {
    const result = CaptureManifest({
      schemaVersion: "1",
      source: {
        provider: "anthropic",
        model: "claude-test",
        baseURL: "https://api.anthropic.com",
      },
      capturedAt: "2026-05-25T12:00:00Z",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a manifest with an unknown schema version", () => {
    const result = CaptureManifest({
      schemaVersion: "2",
      source: {
        provider: "anthropic",
        model: "claude-test",
        baseURL: "https://api.anthropic.com",
      },
      capturedAt: "2026-05-25T12:00:00Z",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a manifest missing the source field", () => {
    const result = CaptureManifest({
      schemaVersion: "1",
      capturedAt: "2026-05-25T12:00:00Z",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a manifest with a non-string baseURL", () => {
    const result = CaptureManifest({
      schemaVersion: "1",
      source: { provider: "anthropic", model: "claude-test", baseURL: 42 },
      capturedAt: "2026-05-25T12:00:00Z",
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("writeCaptureManifest / loadCaptureManifest round-trip", () => {
  test("writes and reads back an identical manifest", async () => {
    const dir = await makeTmpDir();
    try {
      const manifest: CaptureManifest = {
        schemaVersion: "1",
        source: {
          provider: "anthropic",
          model: "claude-test",
          baseURL: "https://api.anthropic.com",
        },
        capturedAt: "2026-05-25T12:00:00Z",
      };
      await writeCaptureManifest(dir, manifest);
      const loaded = await loadCaptureManifest(dir);
      expect(loaded).toEqual(manifest);
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("creates the capture directory if it does not exist", async () => {
    const parent = await makeTmpDir();
    try {
      const dir = path.join(parent, "nested", "capture");
      await writeCaptureManifest(dir, {
        schemaVersion: "1",
        source: { provider: "p", model: "m", baseURL: "https://example" },
        capturedAt: "2026-05-25T12:00:00Z",
      });
      const loaded = await loadCaptureManifest(dir);
      expect(loaded.source.provider).toBe("p");
    } finally {
      await fs.rm(parent, { recursive: true, force: true });
    }
  });

  test("loadCaptureManifest rejects an unknown schema version", async () => {
    const dir = await makeTmpDir();
    try {
      await fs.writeFile(
        path.join(dir, "session.json"),
        JSON.stringify({
          schemaVersion: "2",
          source: { provider: "p", model: "m", baseURL: "https://example" },
          capturedAt: "2026-05-25T12:00:00Z",
        }),
      );
      await expect(loadCaptureManifest(dir)).rejects.toThrow(
        /Invalid capture manifest/,
      );
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("loadCaptureManifest rejects malformed JSON", async () => {
    const dir = await makeTmpDir();
    try {
      await fs.writeFile(path.join(dir, "session.json"), "{not json");
      await expect(loadCaptureManifest(dir)).rejects.toThrow();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("loadCaptureManifest rejects when session.json does not exist", async () => {
    const dir = await makeTmpDir();
    try {
      await expect(loadCaptureManifest(dir)).rejects.toThrow();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("writeCaptureManifest refuses to write an invalid manifest", async () => {
    const dir = await makeTmpDir();
    try {
      await expect(
        writeCaptureManifest(dir, {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- exercising rejection of bad input
          schemaVersion: "99" as "1",
          source: { provider: "p", model: "m", baseURL: "https://example" },
          capturedAt: "2026-05-25T12:00:00Z",
        }),
      ).rejects.toThrow(/Refusing to write invalid capture manifest/);
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });
});
