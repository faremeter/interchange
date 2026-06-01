import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { applyAssetPack } from "./apply-asset-pack";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fsp.mkdtemp(path.join(os.tmpdir(), "apply-asset-pack-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

type BuiltPack = {
  pack: Uint8Array;
  commitSha: string;
};

async function buildAssetPack(
  files: Record<string, string>,
): Promise<BuiltPack> {
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

  // Walk the commit's reachable objects and produce a pack.
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
      if (entry.type === "tree") {
        await walkTree(entry.oid);
      }
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

describe("applyAssetPack", () => {
  test("materializes pack contents at <workspaceRoot>/<mountPath>/", async () => {
    const { pack, commitSha } = await buildAssetPack({
      "greet/SKILL.md": "---\nname: greet\ndescription: hi\n---\nbody\n",
      "greet/script.sh": "#!/bin/sh\necho hi\n",
    });

    const workspaceRoot = await tempDir();

    await applyAssetPack({
      workspaceRoot,
      mountPath: "skills/example/",
      pack,
      ref: "refs/heads/main",
      commitSha,
    });

    const mountDir = path.join(workspaceRoot, "skills", "example");
    const skillBody = await fsp.readFile(
      path.join(mountDir, "greet", "SKILL.md"),
      "utf-8",
    );
    expect(skillBody).toContain("name: greet");

    const scriptBody = await fsp.readFile(
      path.join(mountDir, "greet", "script.sh"),
      "utf-8",
    );
    expect(scriptBody).toContain("echo hi");
  });

  test("leaves sibling mounts untouched", async () => {
    const workspaceRoot = await tempDir();
    const siblingDir = path.join(workspaceRoot, "skills", "other");
    await fsp.mkdir(siblingDir, { recursive: true });
    await fsp.writeFile(path.join(siblingDir, "keep.txt"), "do-not-delete");

    const { pack, commitSha } = await buildAssetPack({
      "doc/SKILL.md": "---\nname: doc\ndescription: docs\n---\n",
    });

    await applyAssetPack({
      workspaceRoot,
      mountPath: "skills/example/",
      pack,
      ref: "refs/heads/main",
      commitSha,
    });

    const sibling = await fsp.readFile(
      path.join(siblingDir, "keep.txt"),
      "utf-8",
    );
    expect(sibling).toBe("do-not-delete");
  });

  test("does not leak a scratch .git into the workspace root", async () => {
    const { pack, commitSha } = await buildAssetPack({
      "a/SKILL.md": "---\nname: a\ndescription: a\n---\n",
    });

    const workspaceRoot = await tempDir();
    await applyAssetPack({
      workspaceRoot,
      mountPath: "skills/example/",
      pack,
      ref: "refs/heads/main",
      commitSha,
    });

    const entries = await fsp.readdir(workspaceRoot);
    // No leftover scratch dirs (they would start with .intx-asset-scratch-).
    const scratchLeftover = entries.filter((e) =>
      e.startsWith(".intx-asset-scratch-"),
    );
    expect(scratchLeftover).toEqual([]);
    // No stray .git at workspace root.
    expect(entries).not.toContain(".git");
  });

  test("throws asset_materialization_failed on missing commit", async () => {
    const { pack } = await buildAssetPack({
      "a/SKILL.md": "---\nname: a\ndescription: a\n---\n",
    });

    const workspaceRoot = await tempDir();
    const promise = applyAssetPack({
      workspaceRoot,
      mountPath: "skills/example/",
      pack,
      ref: "refs/heads/main",
      commitSha: "0".repeat(40),
    });
    await expect(promise).rejects.toThrow(/^asset_materialization_failed:/);
  });

  test("rejects path traversal in mountPath", async () => {
    const { pack, commitSha } = await buildAssetPack({
      "a/SKILL.md": "---\nname: a\ndescription: a\n---\n",
    });

    const workspaceRoot = await tempDir();
    await expect(
      applyAssetPack({
        workspaceRoot,
        mountPath: "skills/../escape/",
        pack,
        ref: "refs/heads/main",
        commitSha,
      }),
    ).rejects.toThrow(/^asset_materialization_failed:/);
  });

  // Guarding against the data-loss case where mountPath resolves to the
  // workspace root itself: the function clears `destDir` before writing,
  // so any path that normalizes to "." would wipe the entire workspace.
  test("rejects mountPath that resolves to workspace root", async () => {
    const { pack, commitSha } = await buildAssetPack({
      "a/SKILL.md": "---\nname: a\ndescription: a\n---\n",
    });

    const workspaceRoot = await tempDir();
    const sentinelDir = path.join(workspaceRoot, "skills", "keep");
    await fsp.mkdir(sentinelDir, { recursive: true });
    await fsp.writeFile(
      path.join(sentinelDir, "do-not-delete.txt"),
      "sentinel",
    );

    for (const offending of [".", "./", "./skills/foo", "skills/./foo"]) {
      await expect(
        applyAssetPack({
          workspaceRoot,
          mountPath: offending,
          pack,
          ref: "refs/heads/main",
          commitSha,
        }),
      ).rejects.toThrow(/^asset_materialization_failed:/);
    }

    // Sentinel survives every rejected call.
    const sentinel = await fsp.readFile(
      path.join(sentinelDir, "do-not-delete.txt"),
      "utf-8",
    );
    expect(sentinel).toBe("sentinel");
  });
});
