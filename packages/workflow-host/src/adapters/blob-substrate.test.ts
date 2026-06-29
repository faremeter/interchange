import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { createRepoStore } from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
  RepoId,
  ValidatePushResult,
} from "@intx/hub-sessions";

import { createWorkflowRunBlobSubstrate } from "./blob-substrate";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

const REF = "refs/heads/main";
const allowAll: AuthorizeFn = () => ({ allowed: true });

// The blob substrate writes under `runs/<runId>/blobs/<sha256>`, which
// the workflow-run kind handler's strict `runs/<runId>/` schema
// rejects (only `events/` is permitted alongside event blobs). The
// adapter tests target the adapter's behavior, not the kind handler's
// schema, so a permissive `agent-state`-shaped handler stands in for
// the production wiring -- the same pattern the sibling repo-store
// adapter test uses for its permissive smoke case.
const permissiveHandler: KindHandler = {
  kind: "agent-state",
  directoryPrefix: "blob-substrate-test",
  validatePush(): ValidatePushResult {
    return { ok: true };
  },
  onRefUpdated() {
    /* no-op */
  },
};

const TEST_PRINCIPAL: Principal = { kind: "test" };

async function makeAdapter(runId: string, deploymentId: string) {
  const dataDir = await makeTempDir("blob-substrate-adapter-");
  const repoId: RepoId = { kind: "agent-state", id: deploymentId };
  const substrate = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "agent-state": permissiveHandler },
    authorize: allowAll,
  });
  const adapter = createWorkflowRunBlobSubstrate({
    substrate,
    repoId,
    principal: TEST_PRINCIPAL,
    runId,
    ref: REF,
  });
  return { adapter, substrate, repoId };
}

describe("workflow-host BlobSubstrate adapter — inline path", () => {
  test("recordOutput inlines values whose JSON-stringified length fits the threshold", async () => {
    const { adapter } = await makeAdapter("run-inline", "deployment-inline");
    const value = { hello: "world", n: 42 };
    const { ref } = await adapter.recordOutput("step-a", 1, value);
    expect(ref.startsWith("inline:")).toBe(true);
    // The inline ref's body is the verbatim JSON payload, matching
    // the in-memory adapter's shape. This is the contract resolveRef
    // round-trips against.
    expect(ref.slice("inline:".length)).toBe(JSON.stringify(value));
  });

  test("ephemeral is false for the production adapter", async () => {
    const { adapter } = await makeAdapter(
      "run-ephemeral",
      "deployment-ephemeral",
    );
    expect(adapter.ephemeral).toBe(false);
  });

  test("recordOutput throws when value cannot be JSON-stringified", async () => {
    const { adapter } = await makeAdapter(
      "run-unserializable",
      "deployment-unserializable",
    );
    await expect(adapter.recordOutput("step-a", 1, undefined)).rejects.toThrow(
      /cannot serialize/,
    );
  });

  test("resolveRef round-trips an inline ref to the original value", async () => {
    const { adapter } = await makeAdapter(
      "run-inline-rt",
      "deployment-inline-rt",
    );
    const original = { a: 1, nested: { b: [2, 3, "x"] } };
    const { ref } = await adapter.recordOutput("step-rt", 1, original);
    const restored = await adapter.resolveRef(ref);
    expect(restored).toEqual(original);
  });
});

describe("workflow-host BlobSubstrate adapter — blob path", () => {
  test("recordOutput spills to a blob ref when JSON exceeds the threshold", async () => {
    const { adapter, substrate, repoId } = await makeAdapter(
      "run-spill",
      "deployment-spill",
    );
    // Construct a value whose JSON representation comfortably exceeds
    // 1 MiB so the adapter takes the blob path. A 1.5 MiB payload of
    // a repeating filler keeps the test fast while crossing the
    // threshold by a healthy margin.
    const filler = "x".repeat(1_500_000);
    const value = { big: filler };
    const { ref } = await adapter.recordOutput("step-big", 1, value);
    expect(ref.startsWith("blob:")).toBe(true);
    const key = ref.slice("blob:".length);
    // The key is a hex-encoded sha256 (64 chars).
    expect(key).toMatch(/^[0-9a-f]{64}$/);
    // The bytes land at runs/<runId>/blobs/<key> on disk.
    const blobPath = path.join(
      substrate.getRepoDir(repoId),
      "runs",
      "run-spill",
      "blobs",
      key,
    );
    expect(fs.existsSync(blobPath)).toBe(true);
    const raw = fs.readFileSync(blobPath, "utf8");
    expect(JSON.parse(raw)).toEqual(value);
  });

  test("resolveRef round-trips a blob ref by reading from the repo directory", async () => {
    const { adapter } = await makeAdapter("run-blob-rt", "deployment-blob-rt");
    const filler = "y".repeat(1_500_000);
    const original = { payload: filler, meta: { kind: "big" } };
    const { ref } = await adapter.recordOutput("step-rt-big", 1, original);
    expect(ref.startsWith("blob:")).toBe(true);
    const restored = await adapter.resolveRef(ref);
    expect(restored).toEqual(original);
  });

  test("resolveRef rejects an unrecognized ref shape", async () => {
    const { adapter } = await makeAdapter("run-bad-ref", "deployment-bad-ref");
    await expect(adapter.resolveRef("garbage:nothing")).rejects.toThrow(
      /unrecognized ref/,
    );
  });

  test("resolveRef surfaces a missing-blob error when the key is unknown", async () => {
    const { adapter } = await makeAdapter(
      "run-missing-blob",
      "deployment-missing-blob",
    );
    await expect(adapter.resolveRef("blob:deadbeef")).rejects.toThrow(
      /not found on disk/,
    );
  });
});
