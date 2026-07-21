// Drift guard: the static `posix.definitions` declaration must match the
// tool names the factory's bundle actually emits when instantiated. The
// deploy-time capability walk reads the static declaration WITHOUT
// invoking the factory, so the two must not diverge.
//
// The factory is instantiated with a minimal real env and NO plugins.
// Plugin/env-injected tools (e.g. LSP) are intentionally out of scope
// for the static declaration: they are contributed at runtime via
// `env.plugins`, which the walk never sees, so they cannot appear in a
// declaration read before instantiation.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createDefaultDirectorRegistry, type BaseEnv } from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit";
import type { InferenceSource } from "@intx/types/runtime";

import { posix } from "./sidecar-bundle";
import { TOOL_NAMES } from "./registry";

const SOURCE: InferenceSource = {
  id: "anthropic:mock-model",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test",
  model: "mock-model",
};

let tmpDir: string;
let env: BaseEnv;

beforeAll(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "tools-posix-sidecar-bundle-test-"));
  const storage = await createIsogitStore(tmpDir);
  env = {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    storage,
    workdir: tmpDir,
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
    // Pin the exact name -> approval partition the capability walk will
    // consume. Normalize an absent marker to "allow" so an ungated tool
    // stays a visible key: `toEqual` drops keys whose value is
    // `undefined`, so a raw `d.approval` would let a future ungated tool
    // vanish from the comparison and pass silently. With the normalized
    // value, a newly-added ungated tool surfaces as an extra "allow" key
    // and fails this comparison.
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
