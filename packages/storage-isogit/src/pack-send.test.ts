import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { createDeployPack } from "./pack-send";
import { applyPack } from "./pack-receive";
import { initAgentRepo } from "./init";

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

async function makeDeployRepo(): Promise<string> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });

  await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
  await fs.promises.writeFile(
    path.join(dir, "deploy", "prompt.txt"),
    "You are a helpful agent.",
  );
  await fs.promises.writeFile(
    path.join(dir, "deploy", "metadata.json"),
    JSON.stringify({ version: "1" }),
  );
  await git.add({ fs, dir, filepath: "deploy/prompt.txt" });
  await git.add({ fs, dir, filepath: "deploy/metadata.json" });
  await git.commit({
    fs,
    dir,
    message: "Initial deploy tree",
    author: { name: "Hub", email: "hub@interchange.dev" },
  });

  return dir;
}

describe("createDeployPack", () => {
  test("produces a packfile from a repo with a deploy tree", async () => {
    const sourceDir = await makeDeployRepo();
    const { pack, commitSha } = await createDeployPack(
      sourceDir,
      "refs/heads/main",
    );

    expect(pack.length).toBeGreaterThan(0);
    expect(commitSha.length).toBe(40);
  });

  test("produced pack can be applied to a fresh agent repo", async () => {
    const sourceDir = await makeDeployRepo();
    const { pack, commitSha } = await createDeployPack(
      sourceDir,
      "refs/heads/main",
    );

    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await applyPack(targetDir, pack, "refs/heads/deploy", commitSha, "t1");

    const resolved = await git.resolveRef({
      fs,
      dir: targetDir,
      ref: "refs/heads/deploy",
    });
    expect(resolved).toBe(commitSha);
  });

  test("throws for a nonexistent ref", async () => {
    const dir = await makeDeployRepo();
    await expect(
      createDeployPack(dir, "refs/heads/nonexistent"),
    ).rejects.toThrow();
  });
});
