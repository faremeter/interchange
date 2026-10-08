// Drift guard: the static `posix.definitions` declaration must match the
// names the instantiated bundle emits, because the deploy-time capability
// walk reads the declaration without invoking the factory. Plugin tools
// are out of scope: they arrive at runtime via `env.plugins`.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createDefaultDirectorRegistry } from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type { InferenceSource } from "@intx/types/runtime";

import { posix, type PosixToolEnv } from "./sidecar-bundle";
import { TOOL_NAMES } from "./registry";

const SOURCE: InferenceSource = {
  id: "anthropic:mock-model",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-test",
  model: "mock-model",
};

let tmpDir: string;
let env: PosixToolEnv;

beforeAll(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "tools-posix-sidecar-bundle-test-"));
  const storage = await createIsogitStore(tmpDir);
  env = {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    storage,
    workdir: tmpDir,
    toolCwd: tmpDir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
  };
});

afterAll(async () => {
  if (tmpDir !== undefined) {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

describe("posix sidecar-bundle static declaration", () => {
  test("declared definition names match the instantiated bundle's names", () => {
    const bundle = posix(env);
    const declared = new Set(posix.definitions.map((d) => d.name));
    const emitted = new Set(bundle.definitions.map((d) => d.name));
    expect(emitted).toEqual(declared);
  });

  test("every posix tool is gated behind per-invocation approval", () => {
    // Normalize an absent marker to "allow": `toEqual` drops undefined
    // values, so a raw `d.approval` would hide an ungated tool from the
    // comparison instead of failing it.
    const partition = Object.fromEntries(
      posix.definitions.map((d) => [d.name, d.approval ?? "allow"]),
    );
    expect(partition).toEqual({
      [TOOL_NAMES.READ_FILE]: "ask",
      [TOOL_NAMES.WRITE_FILE]: "ask",
      [TOOL_NAMES.EDIT_FILE]: "ask",
      [TOOL_NAMES.RUN_SHELL]: "ask",
      [TOOL_NAMES.SEARCH_FILES]: "ask",
      [TOOL_NAMES.GREP]: "ask",
    });
  });
});
