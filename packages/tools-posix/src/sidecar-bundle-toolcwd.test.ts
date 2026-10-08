// Guard: posix tools must scope to `env.toolCwd`, not `env.workdir`.
// Other tests keep both keys equal, so this one forces them apart.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm, readFile, access } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createDefaultDirectorRegistry } from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type { InferenceSource } from "@intx/types/runtime";

import { posix, type PosixToolEnv } from "./sidecar-bundle";

const SOURCE: InferenceSource = {
  id: "anthropic:mock-model",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-test",
  model: "mock-model",
};

function neverAbort(): AbortSignal {
  return new AbortController().signal;
}

let toolDir: string;
let workDir: string;
let env: PosixToolEnv;

beforeAll(async () => {
  toolDir = await mkdtemp(join(tmpdir(), "tools-posix-toolcwd-"));
  workDir = await mkdtemp(join(tmpdir(), "tools-posix-workdir-"));
  const storage = await createIsogitStore(workDir);
  env = {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    storage,
    workdir: workDir,
    toolCwd: toolDir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
  };
});

afterAll(async () => {
  await Promise.all([
    toolDir !== undefined
      ? rm(toolDir, { recursive: true, force: true })
      : undefined,
    workDir !== undefined
      ? rm(workDir, { recursive: true, force: true })
      : undefined,
  ]);
});

describe("posix sidecar-bundle working directory", () => {
  test("resolves a relative write against toolCwd, not workdir", async () => {
    const bundle = posix(env);
    try {
      const result = await bundle.run(
        {
          id: "w1",
          name: "write_file",
          arguments: { path: "sentinel.txt", content: "hello" },
        },
        neverAbort(),
      );
      expect(result.isError).toBeFalsy();

      const written = await readFile(join(toolDir, "sentinel.txt"), "utf8");
      expect(written).toBe("hello");

      await expect(access(join(workDir, "sentinel.txt"))).rejects.toThrow();
    } finally {
      if (bundle.dispose !== undefined) {
        await bundle.dispose();
      }
    }
  });
});
