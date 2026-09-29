import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import git from "isomorphic-git";

import { createSessionManager } from "./session-manager";
import type { AgentRepoStore } from "./agent-repo-store";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fsp.mkdtemp(path.join(os.tmpdir(), "session-manager-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

function makeStubRepoStore(opts: {
  dataDir: string;
  applyDeployPack: AgentRepoStore["applyDeployPack"];
  remove: AgentRepoStore["remove"];
}): AgentRepoStore {
  return {
    getAgentDir: (address) => path.join(opts.dataDir, address),
    initRepo: () => {
      throw new Error("initRepo is not exercised by this test");
    },
    applyDeployPack: opts.applyDeployPack,
    remove: opts.remove,
  };
}

function applyEmptyDeployPack(
  manager: ReturnType<typeof createSessionManager>,
): Promise<void> {
  return manager.applyDeployPack(
    "agent@local",
    new Uint8Array(),
    "refs/heads/main",
    "0".repeat(40),
    "transfer-1",
  );
}

function makeManagerWithRepoStore(
  repoStore: AgentRepoStore,
): ReturnType<typeof createSessionManager> {
  return createSessionManager({ repoStore });
}

async function buildAssetPack(
  files: Record<string, string>,
): Promise<{ pack: Uint8Array; commitSha: string }> {
  const sourceDir = await tempDir();
  await git.init({ fs, dir: sourceDir, defaultBranch: "main" });

  for (const [rel, content] of Object.entries(files)) {
    const abs = path.join(sourceDir, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await fsp.writeFile(abs, content);
    await git.add({ fs, dir: sourceDir, filepath: rel });
  }

  const commitSha = await git.commit({
    fs,
    dir: sourceDir,
    message: "asset",
    author: { name: "test", email: "test@test.dev" },
  });

  const oids = new Set<string>([commitSha]);
  const { commit } = await git.readCommit({
    fs,
    dir: sourceDir,
    oid: commitSha,
  });
  oids.add(commit.tree);
  async function walkTree(treeOid: string): Promise<void> {
    const { tree } = await git.readTree({ fs, dir: sourceDir, oid: treeOid });
    for (const entry of tree) {
      oids.add(entry.oid);
      if (entry.type === "tree") await walkTree(entry.oid);
    }
  }
  await walkTree(commit.tree);

  const result = await git.packObjects({
    fs,
    dir: sourceDir,
    oids: [...oids],
    write: false,
  });
  if (result.packfile === undefined) {
    throw new Error("packObjects produced no packfile");
  }
  return { pack: result.packfile, commitSha };
}

describe("SessionManager.applyAssetPack", () => {
  test("materializes the pack under <agentDir>/workspace/<mountPath>/", async () => {
    const dataDir = await tempDir();
    const { pack, commitSha } = await buildAssetPack({
      "greet/SKILL.md": "---\nname: greet\n---\nbody\n",
    });

    // The wrapper's only logic over `applyAssetPackFn` is the workspace-root
    // composition: `<agentDir>/workspace`. Prove the pack lands there.
    const repoStore = makeStubRepoStore({
      dataDir,
      applyDeployPack: () =>
        Promise.reject(new Error("applyDeployPack not exercised by this test")),
      remove: () =>
        Promise.reject(new Error("remove not exercised by this test")),
    });
    const manager = makeManagerWithRepoStore(repoStore);

    await manager.applyAssetPack(
      "agent@local",
      "skills/example/",
      pack,
      "refs/heads/main",
      commitSha,
    );

    const materialized = path.join(
      dataDir,
      "agent@local",
      "workspace",
      "skills/example",
      "greet/SKILL.md",
    );
    expect(fs.existsSync(materialized)).toBe(true);
  });
});

describe("SessionManager repo-operation serialization", () => {
  test("deleteAgentDir removes the directory only after an in-flight deploy-pack apply completes", async () => {
    const dataDir = await tempDir();
    const events: string[] = [];

    let releaseApply!: () => void;
    const applyGate = new Promise<void>((resolve) => {
      releaseApply = resolve;
    });

    const repoStore = makeStubRepoStore({
      dataDir,
      async applyDeployPack() {
        events.push("applyDeployPack:start");
        await applyGate;
        events.push("applyDeployPack:end");
      },
      async remove() {
        events.push("remove");
      },
    });

    const manager = makeManagerWithRepoStore(repoStore);

    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => rejections.push(reason);
    process.on("unhandledRejection", onRejection);
    try {
      const applied = applyEmptyDeployPack(manager);
      const deletion = manager.deleteAgentDir("agent@local");

      // Let the apply enter its gate and the deletion reach its drain await.
      // With the gate still closed, the removal must not run.
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      expect(events).toEqual(["applyDeployPack:start"]);

      releaseApply();
      await Promise.all([applied, deletion]);

      expect(events).toEqual([
        "applyDeployPack:start",
        "applyDeployPack:end",
        "remove",
      ]);
    } finally {
      process.off("unhandledRejection", onRejection);
    }

    // A pending unhandled rejection surfaces on the next macrotask.
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(rejections).toEqual([]);
  });

  test("a rejecting repo operation propagates to its caller without poisoning the chain", async () => {
    const dataDir = await tempDir();
    let calls = 0;

    const repoStore = makeStubRepoStore({
      dataDir,
      async applyDeployPack() {
        calls += 1;
        if (calls === 1) {
          throw new Error("deploy pack boom");
        }
      },
      async remove() {
        /* unused in this test */
      },
    });

    const manager = makeManagerWithRepoStore(repoStore);

    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => rejections.push(reason);
    process.on("unhandledRejection", onRejection);
    try {
      // The failing op's rejection reaches its own caller.
      await expect(applyEmptyDeployPack(manager)).rejects.toThrow(
        "deploy pack boom",
      );

      // The chain is not poisoned: the next op runs and resolves normally.
      await applyEmptyDeployPack(manager);
      expect(calls).toBe(2);
    } finally {
      process.off("unhandledRejection", onRejection);
    }

    // The rejection-swallowing tail must not surface as an unhandled
    // rejection on the next macrotask.
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(rejections).toEqual([]);
  });
});
