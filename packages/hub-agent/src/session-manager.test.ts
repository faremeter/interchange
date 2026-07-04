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
  createStatePack: AgentRepoStore["createStatePack"];
  remove: AgentRepoStore["remove"];
}): AgentRepoStore {
  const unused = (name: string) => (): never => {
    throw new Error(`${name} is not exercised by this test`);
  };
  return {
    getAgentDir: (address) => path.join(opts.dataDir, address),
    initRepo: unused("initRepo"),
    applyDeployPack: unused("applyDeployPack"),
    createStatePack: opts.createStatePack,
    getDeployRef: unused("getDeployRef"),
    remove: opts.remove,
    persistConfig: unused("persistConfig"),
    persistPairing: unused("persistPairing"),
    scanConfigs: unused("scanConfigs"),
  };
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
      createStatePack: () =>
        Promise.reject(new Error("createStatePack not exercised by this test")),
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
  test("deleteAgentDir removes the directory only after an in-flight state-pack read completes", async () => {
    const dataDir = await tempDir();
    const events: string[] = [];

    let releaseStatePack!: () => void;
    const statePackGate = new Promise<void>((resolve) => {
      releaseStatePack = resolve;
    });

    const repoStore = makeStubRepoStore({
      dataDir,
      async createStatePack() {
        events.push("createStatePack:start");
        await statePackGate;
        events.push("createStatePack:end");
        return {
          pack: new Uint8Array(),
          commitSha: "0".repeat(40),
          ref: "refs/heads/main",
        };
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
      const statePack = manager.createStatePack("agent@local");
      const deletion = manager.deleteAgentDir("agent@local");

      // Let the state-pack read enter its gate and the deletion reach its
      // drain await. With the gate still closed, the removal must not run.
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      expect(events).toEqual(["createStatePack:start"]);

      releaseStatePack();
      await Promise.all([statePack, deletion]);

      expect(events).toEqual([
        "createStatePack:start",
        "createStatePack:end",
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
      async createStatePack() {
        calls += 1;
        if (calls === 1) {
          throw new Error("state pack boom");
        }
        return {
          pack: new Uint8Array(),
          commitSha: "1".repeat(40),
          ref: "refs/heads/main",
        };
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
      await expect(manager.createStatePack("agent@local")).rejects.toThrow(
        "state pack boom",
      );

      // The chain is not poisoned: the next op runs and resolves normally.
      const second = await manager.createStatePack("agent@local");
      expect(second.commitSha).toBe("1".repeat(40));
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
