import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { type } from "arktype";

import {
  SessionManifest,
  loadSessionManifest,
  writeSessionManifest,
} from "./session-manifest";

async function makeTmpDir(): Promise<string> {
  return await fs.mkdtemp(path.join(os.tmpdir(), "session-manifest-test-"));
}

describe("SessionManifest", () => {
  test("validates a well-formed manifest", () => {
    const result = SessionManifest({
      sessionSchemaVersion: "1",
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
    const result = SessionManifest({
      sessionSchemaVersion: "2",
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
    const result = SessionManifest({
      sessionSchemaVersion: "1",
      capturedAt: "2026-05-25T12:00:00Z",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a manifest with a non-string baseURL", () => {
    const result = SessionManifest({
      sessionSchemaVersion: "1",
      source: { provider: "anthropic", model: "claude-test", baseURL: 42 },
      capturedAt: "2026-05-25T12:00:00Z",
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("writeSessionManifest / loadSessionManifest round-trip", () => {
  test("writes and reads back an identical manifest", async () => {
    const dir = await makeTmpDir();
    try {
      const manifest: SessionManifest = {
        sessionSchemaVersion: "1",
        source: {
          provider: "anthropic",
          model: "claude-test",
          baseURL: "https://api.anthropic.com",
        },
        capturedAt: "2026-05-25T12:00:00Z",
      };
      await writeSessionManifest(dir, manifest);
      const loaded = await loadSessionManifest(dir);
      expect(loaded).toEqual(manifest);
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("creates the session directory if it does not exist", async () => {
    const parent = await makeTmpDir();
    try {
      const dir = path.join(parent, "nested", "session");
      await writeSessionManifest(dir, {
        sessionSchemaVersion: "1",
        source: { provider: "p", model: "m", baseURL: "https://example" },
        capturedAt: "2026-05-25T12:00:00Z",
      });
      const loaded = await loadSessionManifest(dir);
      expect(loaded.source.provider).toBe("p");
    } finally {
      await fs.rm(parent, { recursive: true, force: true });
    }
  });

  test("loadSessionManifest rejects an unknown schema version", async () => {
    const dir = await makeTmpDir();
    try {
      await fs.writeFile(
        path.join(dir, "session.json"),
        JSON.stringify({
          sessionSchemaVersion: "2",
          source: { provider: "p", model: "m", baseURL: "https://example" },
          capturedAt: "2026-05-25T12:00:00Z",
        }),
      );
      await expect(loadSessionManifest(dir)).rejects.toThrow(
        /Invalid session manifest/,
      );
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("loadSessionManifest rejects malformed JSON", async () => {
    const dir = await makeTmpDir();
    try {
      await fs.writeFile(path.join(dir, "session.json"), "{not json");
      await expect(loadSessionManifest(dir)).rejects.toThrow();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("loadSessionManifest rejects when session.json does not exist", async () => {
    const dir = await makeTmpDir();
    try {
      await expect(loadSessionManifest(dir)).rejects.toThrow();
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("writeSessionManifest refuses to write an invalid manifest", async () => {
    const dir = await makeTmpDir();
    try {
      await expect(
        writeSessionManifest(dir, {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- exercising rejection of bad input
          sessionSchemaVersion: "99" as "1",
          source: { provider: "p", model: "m", baseURL: "https://example" },
          capturedAt: "2026-05-25T12:00:00Z",
        }),
      ).rejects.toThrow(/Refusing to write invalid session manifest/);
    } finally {
      await fs.rm(dir, { recursive: true, force: true });
    }
  });
});
