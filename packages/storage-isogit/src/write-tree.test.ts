import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { DEFAULT_PACK_MATERIALIZATION_LIMITS, writeTreeToDisk } from "./node";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fsp.mkdtemp(path.join(os.tmpdir(), "write-tree-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

async function commitTree(
  files: Record<string, { content: string; mode?: "100644" | "100755" }>,
): Promise<{ dir: string; treeOid: string }> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });
  for (const [rel, spec] of Object.entries(files)) {
    const abs = path.join(dir, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await fsp.writeFile(abs, spec.content);
    if (spec.mode === "100755") {
      await fsp.chmod(abs, 0o755);
    }
    await git.add({ fs, dir, filepath: rel });
  }
  const commitSha = await git.commit({
    fs,
    dir,
    message: "t",
    author: { name: "t", email: "t@t.dev" },
  });
  const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
  return { dir, treeOid: commit.tree };
}

describe("writeTreeToDisk", () => {
  test("preserves the exec bit: 100755 -> 0o755, others -> 0o644", async () => {
    const { dir, treeOid } = await commitTree({
      "nested/exec.sh": { content: "#!/bin/sh\n", mode: "100755" },
      "nested/plain.txt": { content: "plain\n", mode: "100644" },
    });
    const target = await tempDir();
    await writeTreeToDisk(
      dir,
      target,
      treeOid,
      DEFAULT_PACK_MATERIALIZATION_LIMITS,
    );

    const execStat = await fsp.stat(path.join(target, "nested", "exec.sh"));
    const plainStat = await fsp.stat(path.join(target, "nested", "plain.txt"));
    expect(execStat.mode & 0o777).toBe(0o755);
    expect(plainStat.mode & 0o777).toBe(0o644);
  });

  test("recurses nested subtrees and writes exact content", async () => {
    const { dir, treeOid } = await commitTree({
      "a/b/c/deep.txt": { content: "deep-bytes" },
      "top.txt": { content: "top-bytes" },
    });
    const target = await tempDir();
    await writeTreeToDisk(
      dir,
      target,
      treeOid,
      DEFAULT_PACK_MATERIALIZATION_LIMITS,
    );

    expect(
      await fsp.readFile(path.join(target, "a/b/c/deep.txt"), "utf-8"),
    ).toBe("deep-bytes");
    expect(await fsp.readFile(path.join(target, "top.txt"), "utf-8")).toBe(
      "top-bytes",
    );
  });

  test("throws on a submodule (commit) tree entry", async () => {
    const dir = await tempDir();
    await git.init({ fs, dir, defaultBranch: "main" });
    const treeOid = await git.writeTree({
      fs,
      dir,
      tree: [
        {
          mode: "160000",
          path: "submod",
          oid: "0".repeat(40),
          type: "commit",
        },
      ],
    });
    const target = await tempDir();
    await expect(
      writeTreeToDisk(
        dir,
        target,
        treeOid,
        DEFAULT_PACK_MATERIALIZATION_LIMITS,
      ),
    ).rejects.toThrow(
      /writeTreeToDisk: submodule reference at submod is not supported/,
    );
  });

  test("throws on a symlink (mode 120000) tree entry", async () => {
    const dir = await tempDir();
    await git.init({ fs, dir, defaultBranch: "main" });
    const blobOid = await git.writeBlob({
      fs,
      dir,
      blob: new TextEncoder().encode("../escape"),
    });
    const treeOid = await git.writeTree({
      fs,
      dir,
      tree: [
        {
          mode: "120000",
          path: "link",
          oid: blobOid,
          type: "blob",
        },
      ],
    });
    const target = await tempDir();
    await expect(
      writeTreeToDisk(
        dir,
        target,
        treeOid,
        DEFAULT_PACK_MATERIALIZATION_LIMITS,
      ),
    ).rejects.toThrow(/writeTreeToDisk: symlink at link is not supported/);
  });

  test("rejects a checkout whose cumulative bytes exceed the cap", async () => {
    const { dir, treeOid } = await commitTree({
      "big.txt": { content: "0123456789" }, // 10 bytes
    });
    const target = await tempDir();
    await expect(
      writeTreeToDisk(dir, target, treeOid, {
        ...DEFAULT_PACK_MATERIALIZATION_LIMITS,
        maxTreeBytes: 5,
      }),
    ).rejects.toThrow(/exceeds the 5-byte cap/);
  });

  test("rejects a checkout whose entry count exceeds the cap", async () => {
    const { dir, treeOid } = await commitTree({
      "a.txt": { content: "a" },
      "b.txt": { content: "b" },
    });
    const target = await tempDir();
    await expect(
      writeTreeToDisk(dir, target, treeOid, {
        ...DEFAULT_PACK_MATERIALIZATION_LIMITS,
        maxTreeEntries: 1,
      }),
    ).rejects.toThrow(/exceeds the 1-entry cap/);
  });
});
