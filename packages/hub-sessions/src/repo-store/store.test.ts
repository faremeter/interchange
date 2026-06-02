import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto-node";
import type { KeyPair } from "@intx/types/runtime";
import { createRepoStore } from "./store";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
  RepoAction,
  RepoId,
  ValidatePushResult,
} from "./types";

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
    await fs.promises.rm(d, { recursive: true, force: true }).catch((_e) => {
      /* best effort cleanup */
    });
  }
});

type RefUpdateRecord = {
  repoId: RepoId;
  ref: string;
  oldSha: string | null;
  newSha: string;
};

type TestHandler = KindHandler & {
  onRefUpdatedCalls: RefUpdateRecord[];
};

function createTestHandler(opts?: {
  allowTopLevelPaths?: (topLevelTreePaths: string[]) => boolean;
}): TestHandler {
  const onRefUpdatedCalls: RefUpdateRecord[] = [];
  const allowFn = opts?.allowTopLevelPaths;
  return {
    kind: "agent-state",
    directoryPrefix: "repos-under-test",
    validatePush({ topLevelTreePaths }): ValidatePushResult {
      // Ignore the readBlob argument: this fixture only ever needs path-level
      // checks. Real handlers that need blob contents (e.g. skillKindHandler)
      // exercise readBlob in their own dedicated tests.
      if (allowFn === undefined) {
        return { ok: true };
      }
      if (allowFn(topLevelTreePaths)) {
        return { ok: true };
      }
      return { ok: false, reason: "stub rejected push" };
    },
    onRefUpdated(args) {
      onRefUpdatedCalls.push(args);
    },
    onRefUpdatedCalls,
  };
}

const allowAll: AuthorizeFn = () => ({ allowed: true });

const principal: Principal = { kind: "test" };

const repoId: RepoId = { kind: "agent-state", id: "subject" };

const REF = "refs/heads/test";

async function readTreePaths(dir: string, treeOid: string): Promise<string[]> {
  const { tree } = await git.readTree({ fs, dir, oid: treeOid });
  return tree.map((e) => e.path);
}

describe("RepoStore", () => {
  test("initRepo is idempotent", async () => {
    const dataDir = await makeTempDir("repo-store-init-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.initRepo(repoId);
    await store.initRepo(repoId);

    const gitDir = path.join(
      dataDir,
      handler.directoryPrefix,
      repoId.id,
      ".git",
    );
    const stat = await fs.promises.stat(gitDir);
    expect(stat.isDirectory()).toBe(true);
  });

  test("writeTree writes files, signs the commit, advances the ref, calls onRefUpdated", async () => {
    const dataDir = await makeTempDir("repo-store-write-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const { commitSha } = await store.writeTree(principal, repoId, REF, {
      files: {
        "deploy/prompt.md": "hello",
        "workspace/example.txt": "example",
      },
      message: "initial",
    });

    expect(commitSha).toMatch(/^[0-9a-f]{40}$/);

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const resolved = await git.resolveRef({ fs, dir, ref: REF });
    expect(resolved).toBe(commitSha);

    const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
    expect(commit.gpgsig).toBeDefined();
    expect(commit.gpgsig).toContain("-----BEGIN SSH SIGNATURE-----");

    expect(handler.onRefUpdatedCalls).toHaveLength(1);
    const call = handler.onRefUpdatedCalls[0];
    if (!call) throw new Error("unreachable");
    expect(call.ref).toBe(REF);
    expect(call.oldSha).toBeNull();
    expect(call.newSha).toBe(commitSha);
    expect(call.repoId).toEqual(repoId);
  });

  test("writeTree on an existing ref advances from that ref", async () => {
    const dataDir = await makeTempDir("repo-store-advance-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const first = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "v1" },
      message: "first",
    });
    const second = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "v2" },
      message: "second",
    });

    expect(second.commitSha).not.toBe(first.commitSha);

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const { commit } = await git.readCommit({
      fs,
      dir,
      oid: second.commitSha,
    });
    expect(commit.parent).toContain(first.commitSha);

    expect(handler.onRefUpdatedCalls).toHaveLength(2);
    const secondCall = handler.onRefUpdatedCalls[1];
    if (!secondCall) throw new Error("unreachable");
    expect(secondCall.oldSha).toBe(first.commitSha);
    expect(secondCall.newSha).toBe(second.commitSha);
  });

  test("writeTree with clearPrefix clears stale tracked files under that prefix", async () => {
    const dataDir = await makeTempDir("repo-store-clear-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "A", "deploy/b.md": "B" },
      clearPrefix: "deploy/",
      message: "first",
    });
    const second = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "A2" },
      clearPrefix: "deploy/",
      message: "second",
    });

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const { commit } = await git.readCommit({
      fs,
      dir,
      oid: second.commitSha,
    });
    const rootEntries = await readTreePaths(dir, commit.tree);
    expect(rootEntries).toContain("deploy");

    const { tree: rootTree } = await git.readTree({
      fs,
      dir,
      oid: commit.tree,
    });
    const deployEntry = rootTree.find((e) => e.path === "deploy");
    if (!deployEntry) throw new Error("deploy subtree missing");
    const deployPaths = await readTreePaths(dir, deployEntry.oid);
    expect(deployPaths).toContain("a.md");
    expect(deployPaths).not.toContain("b.md");
  });

  test("writeTree without clearPrefix is purely additive", async () => {
    const dataDir = await makeTempDir("repo-store-additive-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "x/a": "1" },
      message: "first",
    });
    const second = await store.writeTree(principal, repoId, REF, {
      files: { "x/b": "2" },
      message: "second",
    });

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const { commit } = await git.readCommit({
      fs,
      dir,
      oid: second.commitSha,
    });
    const { tree: rootTree } = await git.readTree({
      fs,
      dir,
      oid: commit.tree,
    });
    const xEntry = rootTree.find((e) => e.path === "x");
    if (!xEntry) throw new Error("x subtree missing");
    const xPaths = await readTreePaths(dir, xEntry.oid);
    expect(xPaths).toContain("a");
    expect(xPaths).toContain("b");
  });

  test("receivePack accepts a valid pack and rejects one failing validatePush", async () => {
    const sourceDataDir = await makeTempDir("repo-store-pack-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "from-source" },
      message: "source content",
    });
    const { pack, commitSha } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const acceptDir = await makeTempDir("repo-store-pack-accept-");
    const acceptHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const acceptStore = createRepoStore({
      dataDir: acceptDir,
      signingKey,
      handlers: { "agent-state": acceptHandler },
      authorize: allowAll,
    });
    await acceptStore.receivePack(
      principal,
      repoId,
      REF,
      pack,
      commitSha,
      null,
    );
    expect(await acceptStore.resolveRef(principal, repoId, REF)).toBe(
      commitSha,
    );
    expect(acceptHandler.onRefUpdatedCalls).toHaveLength(1);

    const rejectDir = await makeTempDir("repo-store-pack-reject-");
    const rejectHandler = createTestHandler({
      allowTopLevelPaths: () => false,
    });
    const rejectStore = createRepoStore({
      dataDir: rejectDir,
      signingKey,
      handlers: { "agent-state": rejectHandler },
      authorize: allowAll,
    });
    await expect(
      rejectStore.receivePack(principal, repoId, REF, pack, commitSha, null),
    ).rejects.toThrow(/^path_violation/);
    expect(await rejectStore.resolveRef(principal, repoId, REF)).toBeNull();
  });

  test("createPack and receivePack round-trip", async () => {
    const sourceDir = await makeTempDir("repo-store-rt-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/payload.txt": "round-trip body" },
      message: "rt",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-rt-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });

    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      pack,
      commitSha,
      null,
    );
    const resolved = await targetStore.resolveRef(principal, repoId, REF);
    expect(resolved).toBe(commitSha);
  });

  test("resolveRef returns null for a missing ref", async () => {
    const dataDir = await makeTempDir("repo-store-resolve-missing-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });
    await store.initRepo(repoId);
    const resolved = await store.resolveRef(
      principal,
      repoId,
      "refs/heads/nonexistent",
    );
    expect(resolved).toBeNull();
  });

  test("authorize gates each authorize-gated operation", async () => {
    const sourceDir = await makeTempDir("repo-store-authz-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha: existingSha, ...rest } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      {
        files: { "deploy/a.md": "pack-source" },
        message: "source",
      },
    );
    void rest;
    const { pack: validPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const denyDir = await makeTempDir("repo-store-authz-deny-");
    let denyCallCount = 0;
    const denyAuthorize: AuthorizeFn = () => {
      denyCallCount += 1;
      return { allowed: false, reason: "denied" };
    };
    const denyHandler = createTestHandler({ allowTopLevelPaths: () => true });
    const denyStore = createRepoStore({
      dataDir: denyDir,
      signingKey,
      handlers: { "agent-state": denyHandler },
      authorize: denyAuthorize,
    });

    await denyStore.initRepo(repoId);
    expect(denyCallCount).toBe(0);

    await expect(
      denyStore.writeTree(principal, repoId, REF, {
        files: { a: "1" },
        message: "x",
      }),
    ).rejects.toThrow(/^authorize_denied:.*denied/);
    await expect(
      denyStore.receivePack(
        principal,
        repoId,
        REF,
        validPack,
        existingSha,
        null,
      ),
    ).rejects.toThrow(/^authorize_denied:.*denied/);
    await expect(denyStore.createPack(principal, repoId, REF)).rejects.toThrow(
      /^authorize_denied:.*denied/,
    );
    await expect(denyStore.resolveRef(principal, repoId, REF)).rejects.toThrow(
      /^authorize_denied:.*denied/,
    );

    const partialDir = await makeTempDir("repo-store-authz-partial-");
    let partialCallCount = 0;
    const allowOnlyResolve: AuthorizeFn = (
      _p,
      _r,
      _ref,
      action: RepoAction,
    ) => {
      partialCallCount += 1;
      if (action === "resolveRef") return { allowed: true };
      return { allowed: false, reason: "denied" };
    };
    const partialHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const partialStore = createRepoStore({
      dataDir: partialDir,
      signingKey,
      handlers: { "agent-state": partialHandler },
      authorize: allowOnlyResolve,
    });

    await partialStore.initRepo(repoId);
    expect(partialCallCount).toBe(0);

    const resolvedMissing = await partialStore.resolveRef(
      principal,
      repoId,
      REF,
    );
    expect(resolvedMissing).toBeNull();
    expect(partialCallCount).toBe(1);

    await expect(
      partialStore.writeTree(principal, repoId, REF, {
        files: { a: "1" },
        message: "x",
      }),
    ).rejects.toThrow(/^authorize_denied/);
    await expect(
      partialStore.receivePack(
        principal,
        repoId,
        REF,
        validPack,
        existingSha,
        null,
      ),
    ).rejects.toThrow(/^authorize_denied/);
    await expect(
      partialStore.createPack(principal, repoId, REF),
    ).rejects.toThrow(/^authorize_denied/);
  });

  test("onRefUpdated is not called on a failed receivePack", async () => {
    const sourceDir = await makeTempDir("repo-store-fail-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "pack-source" },
      message: "source",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-fail-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => false,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });

    await expect(
      targetStore.receivePack(principal, repoId, REF, pack, commitSha, null),
    ).rejects.toThrow(/^path_violation/);

    expect(targetHandler.onRefUpdatedCalls).toHaveLength(0);
    const resolved = await targetStore.resolveRef(principal, repoId, REF);
    expect(resolved).toBeNull();
  });

  test("writeTree rejects when the handler's validatePush rejects", async () => {
    const dataDir = await makeTempDir("repo-store-writetree-reject-");
    const handler = createTestHandler({
      allowTopLevelPaths: () => false,
    });
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await expect(
      store.writeTree(principal, repoId, REF, {
        files: { "deploy/a.md": "rejected" },
        message: "should be rejected",
      }),
    ).rejects.toThrow(/^path_violation/);

    expect(handler.onRefUpdatedCalls).toHaveLength(0);
    const resolved = await store.resolveRef(principal, repoId, REF);
    expect(resolved).toBeNull();
  });

  test("writeTree passes the readBlob callback that resolves declared file contents", async () => {
    const dataDir = await makeTempDir("repo-store-writetree-readblob-");
    const captured: { paths: string[] | null; blob: Uint8Array | null } = {
      paths: null,
      blob: null,
    };
    const handler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      async validatePush({ topLevelTreePaths, readBlob }) {
        captured.paths = topLevelTreePaths;
        captured.blob = await readBlob("deploy/a.md");
        return { ok: true };
      },
      onRefUpdated() {
        /* no-op */
      },
      onRefUpdatedCalls: [],
    };
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "blob-body", "top.txt": "top" },
      message: "with readBlob",
    });

    expect(captured.paths?.sort()).toEqual(["deploy", "top.txt"]);
    expect(captured.blob).not.toBeNull();
    expect(new TextDecoder().decode(captured.blob ?? new Uint8Array())).toBe(
      "blob-body",
    );
  });

  test("validatePush runs on receivePack regardless of authorize verdict", async () => {
    const sourceDir = await makeTempDir("repo-store-bypass-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "pack-source" },
      message: "source",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-bypass-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => false,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });

    await expect(
      targetStore.receivePack(principal, repoId, REF, pack, commitSha, null),
    ).rejects.toThrow(/^path_violation/);
  });

  test("receivePack accepts a fresh ref when expectedOldSha is null", async () => {
    const sourceDir = await makeTempDir("repo-store-cas-fresh-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "v1" },
      message: "v1",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-cas-fresh-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });

    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      pack,
      commitSha,
      null,
    );

    expect(await targetStore.resolveRef(principal, repoId, REF)).toBe(
      commitSha,
    );
    expect(targetHandler.onRefUpdatedCalls).toHaveLength(1);
    const call = targetHandler.onRefUpdatedCalls[0];
    if (!call) throw new Error("unreachable");
    expect(call.oldSha).toBeNull();
    expect(call.newSha).toBe(commitSha);
  });

  test("receivePack rejects when expectedOldSha is stale", async () => {
    const sourceDir = await makeTempDir("repo-store-cas-stale-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha: firstSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v1" }, message: "v1" },
    );
    const { pack: firstPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );
    const { commitSha: secondSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v2" }, message: "v2" },
    );
    const { pack: secondPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const targetDir = await makeTempDir("repo-store-cas-stale-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });
    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      firstPack,
      firstSha,
      null,
    );

    await expect(
      targetStore.receivePack(
        principal,
        repoId,
        REF,
        secondPack,
        secondSha,
        null,
      ),
    ).rejects.toThrow(/^non_fast_forward:/);

    expect(await targetStore.resolveRef(principal, repoId, REF)).toBe(firstSha);
    expect(targetHandler.onRefUpdatedCalls).toHaveLength(1);
  });

  test("receivePack feeds the substrate-returned oldSha into onRefUpdated", async () => {
    const sourceDir = await makeTempDir("repo-store-oldsha-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha: firstSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v1" }, message: "v1" },
    );
    const { pack: firstPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );
    const { commitSha: secondSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v2" }, message: "v2" },
    );
    const { pack: secondPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const targetDir = await makeTempDir("repo-store-oldsha-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });
    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      firstPack,
      firstSha,
      null,
    );
    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      secondPack,
      secondSha,
      firstSha,
    );

    expect(targetHandler.onRefUpdatedCalls).toHaveLength(2);
    const second = targetHandler.onRefUpdatedCalls[1];
    if (!second) throw new Error("unreachable");
    expect(second.oldSha).toBe(firstSha);
    expect(second.newSha).toBe(secondSha);
  });

  test("concurrent receivePack against the same repo serializes", async () => {
    const sourceDir = await makeTempDir("repo-store-serial-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha: firstSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v1" }, message: "v1" },
    );
    const { pack: firstPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );
    const { commitSha: secondSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v2" }, message: "v2" },
    );
    const { pack: secondPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const targetDir = await makeTempDir("repo-store-serial-target-");
    const events: string[] = [];
    const slowHandler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      async validatePush() {
        events.push("validate");
        await new Promise((resolve) => setTimeout(resolve, 50));
        return { ok: true };
      },
      onRefUpdated({ newSha }) {
        events.push(`update:${newSha.substring(0, 7)}`);
      },
      onRefUpdatedCalls: [],
    };
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": slowHandler },
      authorize: allowAll,
    });

    const firstP = targetStore.receivePack(
      principal,
      repoId,
      REF,
      firstPack,
      firstSha,
      null,
    );
    const secondP = targetStore.receivePack(
      principal,
      repoId,
      REF,
      secondPack,
      secondSha,
      firstSha,
    );

    await Promise.all([firstP, secondP]);

    expect(events).toEqual([
      "validate",
      `update:${firstSha.substring(0, 7)}`,
      "validate",
      `update:${secondSha.substring(0, 7)}`,
    ]);
    expect(await targetStore.resolveRef(principal, repoId, REF)).toBe(
      secondSha,
    );
  });

  test("concurrent receivePack against distinct repos runs in parallel", async () => {
    const sourceDir = await makeTempDir("repo-store-parallel-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const repoA: RepoId = { kind: "agent-state", id: "alpha" };
    const repoB: RepoId = { kind: "agent-state", id: "beta" };
    const { commitSha: shaA } = await sourceStore.writeTree(
      principal,
      repoA,
      REF,
      { files: { "deploy/a.md": "a" }, message: "a" },
    );
    const { pack: packA } = await sourceStore.createPack(principal, repoA, REF);
    const { commitSha: shaB } = await sourceStore.writeTree(
      principal,
      repoB,
      REF,
      { files: { "deploy/b.md": "b" }, message: "b" },
    );
    const { pack: packB } = await sourceStore.createPack(principal, repoB, REF);

    const targetDir = await makeTempDir("repo-store-parallel-target-");
    const enters: number[] = [];
    let activeConcurrent = 0;
    let observedMaxConcurrent = 0;
    const trackingHandler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      async validatePush() {
        activeConcurrent += 1;
        observedMaxConcurrent = Math.max(
          observedMaxConcurrent,
          activeConcurrent,
        );
        enters.push(Date.now());
        await new Promise((resolve) => setTimeout(resolve, 100));
        activeConcurrent -= 1;
        return { ok: true };
      },
      onRefUpdated() {
        /* no-op */
      },
      onRefUpdatedCalls: [],
    };
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": trackingHandler },
      authorize: allowAll,
    });

    const start = Date.now();
    await Promise.all([
      targetStore.receivePack(principal, repoA, REF, packA, shaA, null),
      targetStore.receivePack(principal, repoB, REF, packB, shaB, null),
    ]);
    const elapsed = Date.now() - start;

    expect(observedMaxConcurrent).toBe(2);
    // Serialized work would take ~200ms; parallel work completes well
    // under 180ms. The bound is generous to absorb scheduler jitter on
    // contended CI runners.
    expect(elapsed).toBeLessThan(180);
  });

  test("getRepoDir returns dataDir/<directoryPrefix>/<id>", async () => {
    const dataDir = await makeTempDir("repo-store-dir-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const expected = path.join(dataDir, handler.directoryPrefix, repoId.id);
    expect(store.getRepoDir(repoId)).toBe(expected);
  });

  test("getRepoDir rejects an unsafe repo id without touching the filesystem", () => {
    const dataDir = path.join(os.tmpdir(), "repo-store-dir-unsafe-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    expect(() =>
      store.getRepoDir({ kind: "agent-state", id: "../escape" }),
    ).toThrow(/^repo_id_invalid/);
  });

  test("listRefs returns the genesis branch on a freshly-initialised repo", async () => {
    const dataDir = await makeTempDir("repo-store-list-genesis-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.initRepo(repoId);
    const refs = await store.listRefs(principal, repoId);
    expect(refs.length).toBeGreaterThanOrEqual(1);

    const main = refs.find((r) => r.name === "refs/heads/main");
    expect(main).toBeDefined();
    expect(main?.sha).toMatch(/^[0-9a-f]{40}$/);

    const dir = store.getRepoDir(repoId);
    const tipFromGit = await git.resolveRef({
      fs,
      dir,
      ref: "refs/heads/main",
    });
    expect(main?.sha).toBe(tipFromGit);
  });

  test("listRefs returns names sorted lexicographically including tags", async () => {
    const dataDir = await makeTempDir("repo-store-list-sort-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, "refs/heads/main", {
      files: { "a.md": "one" },
      message: "first",
    });
    await store.writeTree(principal, repoId, "refs/heads/zzz-branch", {
      files: { "b.md": "two" },
      message: "second",
    });

    const dir = store.getRepoDir(repoId);
    const mainSha = await git.resolveRef({
      fs,
      dir,
      ref: "refs/heads/main",
    });
    await git.writeRef({
      fs,
      dir,
      ref: "refs/tags/v1",
      value: mainSha,
      force: true,
    });

    const refs = await store.listRefs(principal, repoId);
    const names = refs.map((r) => r.name);
    const sorted = [...names].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    expect(names).toEqual(sorted);
    expect(names).toContain("refs/heads/main");
    expect(names).toContain("refs/heads/zzz-branch");
    expect(names).toContain("refs/tags/v1");
  });

  test("listRefs returns the empty list before initRepo is called", async () => {
    const dataDir = await makeTempDir("repo-store-list-empty-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const refs = await store.listRefs(principal, repoId);
    expect(refs).toEqual([]);
  });

  test("listRefs is gated under the resolveRef authorize action", async () => {
    const dataDir = await makeTempDir("repo-store-list-deny-");
    const seenActions: RepoAction[] = [];
    const denyAuthorize: AuthorizeFn = (_p, _r, _ref, action) => {
      seenActions.push(action);
      return { allowed: false, reason: "denied" };
    };
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: denyAuthorize,
    });

    await expect(store.listRefs(principal, repoId)).rejects.toThrow(
      /^authorize_denied/,
    );
    expect(seenActions).toEqual(["resolveRef"]);
  });

  test("initRepo forwards a per-call gitignore override into the genesis tree", async () => {
    const dataDir = await makeTempDir("repo-store-init-gitignore-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const customBody = ".DS_Store\nnode_modules/\nkeys/\n";
    await store.initRepo(repoId, { gitignore: customBody });

    const dir = store.getRepoDir(repoId);
    const onDisk = await fs.promises.readFile(
      path.join(dir, ".gitignore"),
      "utf-8",
    );
    expect(onDisk).toBe(customBody);
  });

  test("withRepoLock releases on substrate exception and the map drains", async () => {
    const sourceDir = await makeTempDir("repo-store-release-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "v1" },
      message: "v1",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-release-target-");
    let rejectOnce = true;
    const flakyHandler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      validatePush() {
        if (rejectOnce) {
          rejectOnce = false;
          return { ok: false, reason: "first attempt rejected" };
        }
        return { ok: true };
      },
      onRefUpdated() {
        /* no-op */
      },
      onRefUpdatedCalls: [],
    };
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": flakyHandler },
      authorize: allowAll,
    });

    await expect(
      targetStore.receivePack(principal, repoId, REF, pack, commitSha, null),
    ).rejects.toThrow(/^path_violation/);

    // The second call would deadlock if the lock were never released.
    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      pack,
      commitSha,
      null,
    );
    expect(await targetStore.resolveRef(principal, repoId, REF)).toBe(
      commitSha,
    );
  });
});
