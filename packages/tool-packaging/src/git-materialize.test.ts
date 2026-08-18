import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import { promises as fsp } from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import {
  DEFAULT_PACK_MATERIALIZATION_LIMITS,
  collectReachableObjects,
  indexPackIntoGitDir,
} from "@intx/storage-isogit/node";
import type { ToolPackageAssetSourceTree } from "@intx/types/tool-packages";

import { materializeGitEntry } from "./git-materialize";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fsp.mkdtemp(path.join(os.tmpdir(), "git-materialize-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

// Build a one-commit repo, pack it, and index the pack into a retained gitDir.
// Returns the gitDir plus the pinned commit and its (root) tree oid.
async function buildIndexedGitDir(): Promise<{
  gitDir: string;
  commitSha: string;
  rootTreeOid: string;
}> {
  const src = await tempDir();
  await git.init({ fs, dir: src, defaultBranch: "main" });
  await fsp.writeFile(path.join(src, "index.mjs"), "export const x = 1;\n");
  await git.add({ fs, dir: src, filepath: "index.mjs" });
  const commitSha = await git.commit({
    fs,
    dir: src,
    message: "t",
    author: { name: "t", email: "t@t.dev" },
  });
  const oids = await collectReachableObjects(src, commitSha);
  const { packfile } = await git.packObjects({ fs, dir: src, oids });
  if (packfile === undefined) {
    throw new Error("git-materialize test: packObjects returned no packfile");
  }

  const gitDir = await tempDir();
  await indexPackIntoGitDir(
    gitDir,
    packfile,
    commitSha,
    DEFAULT_PACK_MATERIALIZATION_LIMITS,
  );
  const { commit } = await git.readCommit({ fs, dir: gitDir, oid: commitSha });
  return { gitDir, commitSha, rootTreeOid: commit.tree };
}

describe("materializeGitEntry", () => {
  test("checks out the pinned subtree when the tree oid matches", async () => {
    const { gitDir, commitSha, rootTreeOid } = await buildIndexedGitDir();
    const tree: ToolPackageAssetSourceTree = {
      format: "source",
      commitSha,
      packageDir: ".",
      treeOid: rootTreeOid,
    };
    const instanceScratchDir = await tempDir();

    const { dir } = await materializeGitEntry({
      tree,
      name: "@wf/app",
      version: "1.0.0",
      gitDir,
      instanceScratchDir,
    });

    expect(await fsp.readFile(path.join(dir, "index.mjs"), "utf-8")).toBe(
      "export const x = 1;\n",
    );
  });

  test("fails loud when the subtree's tree oid does not match the frozen one", async () => {
    // The frozen `treeOid` is the content identity. A delivered pack whose
    // subtree hashes to something else means the bytes diverge from what was
    // approved -- the source-entry analog of an SRI mismatch.
    const { gitDir, commitSha } = await buildIndexedGitDir();
    const tree: ToolPackageAssetSourceTree = {
      format: "source",
      commitSha,
      packageDir: ".",
      treeOid: "0".repeat(40),
    };
    const instanceScratchDir = await tempDir();

    await expect(
      materializeGitEntry({
        tree,
        name: "@wf/app",
        version: "1.0.0",
        gitDir,
        instanceScratchDir,
      }),
    ).rejects.toThrow(/hashed to tree .*, not the frozen/);
  });

  test("fails loud when the pinned commit cannot be read from the gitDir", async () => {
    const { gitDir, rootTreeOid } = await buildIndexedGitDir();
    const tree: ToolPackageAssetSourceTree = {
      format: "source",
      commitSha: "0".repeat(40),
      packageDir: ".",
      treeOid: rootTreeOid,
    };
    const instanceScratchDir = await tempDir();

    await expect(
      materializeGitEntry({
        tree,
        name: "@wf/app",
        version: "1.0.0",
        gitDir,
        instanceScratchDir,
      }),
    ).rejects.toThrow(/could not be read at/);
  });
});
