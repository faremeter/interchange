// Behavior guard: the LSP sidecar bundle must scope its working directory
// to `env.toolCwd` (the tool filesystem scope), not `env.workdir` (the lock
// boundary). Every other test keeps the two equal, so a regression that
// reads `env.workdir` again would pass the suite silently. This forces the
// two apart and asserts the constructed plugin is rooted at `toolCwd`.
//
// `LSPPlugin.cwd` is surfaced straight from the manager's `ctx.cwd` — the
// same directory `createLSPManager` hands every server as
// `serverCtx.directory` and gates file operations against (proven in
// lsp.test.ts). So this assertion tracks the value that actually drives the
// LSP, not a copy that could drift from it.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createDefaultDirectorRegistry } from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type { PosixToolEnv } from "@intx/tools-posix/sidecar-bundle";
import type { InferenceSource } from "@intx/types/runtime";

import { lsp } from "./sidecar-bundle";

const SOURCE: InferenceSource = {
  id: "anthropic:mock-model",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test",
  model: "mock-model",
};

let toolDir: string;
let workDir: string;
let env: PosixToolEnv;

beforeAll(async () => {
  toolDir = await mkdtemp(join(tmpdir(), "tools-lsp-toolcwd-"));
  workDir = await mkdtemp(join(tmpdir(), "tools-lsp-workdir-"));
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

describe("lsp sidecar-bundle working directory", () => {
  test("roots the LSP at env.toolCwd, not workdir", async () => {
    const plugin = lsp(env);
    try {
      expect(plugin.cwd).toBe(toolDir);
    } finally {
      if (plugin.dispose !== undefined) {
        await plugin.dispose();
      }
    }
  });
});
