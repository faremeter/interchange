import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { initAgentRepo } from "./init";
import { applyPack } from "./pack-receive";
import { collectReachableObjects } from "./object-walk";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "interchange-test-"),
  );
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

async function createPackFromRepo(
  sourceDir: string,
  oids: string[],
): Promise<Uint8Array> {
  const result = await git.packObjects({
    fs,
    dir: sourceDir,
    oids,
    write: false,
  });
  if (result.packfile === undefined) {
    throw new Error("packObjects returned no packfile");
  }
  return result.packfile;
}

async function makeSourceRepo(): Promise<{
  dir: string;
  commitSha: string;
  oids: string[];
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });

  const filePath = path.join(dir, "deploy", "prompt.txt");
  await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
  await fs.promises.writeFile(filePath, "You are a helpful agent.");
  await git.add({ fs, dir, filepath: "deploy/prompt.txt" });

  const commitSha = await git.commit({
    fs,
    dir,
    message: "Initial deploy",
    author: { name: "Test", email: "test@test.dev" },
  });

  const walkResult = await git.log({ fs, dir, depth: 1 });
  const entry = walkResult[0];
  if (entry === undefined) throw new Error("no commit");

  const oids = await collectReachableObjects(dir, commitSha);
  return { dir, commitSha, oids };
}

describe("applyPack", () => {
  test("applies a packfile and updates the ref", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(
      targetDir,
      pack,
      "refs/heads/deploy",
      source.commitSha,
      "test-transfer-1",
    );

    const resolved = await git.resolveRef({
      fs,
      dir: targetDir,
      ref: "refs/heads/deploy",
    });
    expect(resolved).toBe(source.commitSha);
  });

  test("cleans up temp files after success", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(
      targetDir,
      pack,
      "refs/heads/deploy",
      source.commitSha,
      "cleanup-test",
    );

    const packPath = path.join(
      targetDir,
      ".git",
      "pack-recv-cleanup-test.pack",
    );
    const idxPath = path.join(targetDir, ".git", "pack-recv-cleanup-test.idx");

    await expect(fs.promises.access(packPath)).rejects.toThrow();
    await expect(fs.promises.access(idxPath)).rejects.toThrow();
  });

  test("throws on sha mismatch", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await expect(
      applyPack(
        targetDir,
        pack,
        "refs/heads/deploy",
        "0000000000000000000000000000000000000000",
        "mismatch-test",
      ),
    ).rejects.toThrow("sha_mismatch");
  });

  test("cleans up temp files after failure", async () => {
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    // Write garbage as a pack — indexPack will fail
    const garbagePack = new Uint8Array([1, 2, 3, 4]);

    try {
      await applyPack(
        targetDir,
        garbagePack,
        "refs/heads/deploy",
        "abc123",
        "fail-cleanup",
      );
    } catch {
      // Expected to throw
    }

    const packPath = path.join(
      targetDir,
      ".git",
      "pack-recv-fail-cleanup.pack",
    );
    await expect(fs.promises.access(packPath)).rejects.toThrow();
  });
});
