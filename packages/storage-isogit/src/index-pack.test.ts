import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import {
  DEFAULT_PACK_MATERIALIZATION_LIMITS,
  collectReachableObjects,
  indexPackIntoGitDir,
} from "./node";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fsp.mkdtemp(path.join(os.tmpdir(), "index-pack-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

// Build a one-commit repo and return its commit sha plus a packfile carrying
// every object reachable from that commit.
async function buildPack(): Promise<{ commitSha: string; pack: Uint8Array }> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });
  await fsp.writeFile(path.join(dir, "package.json"), '{"name":"x"}\n');
  await git.add({ fs, dir, filepath: "package.json" });
  const commitSha = await git.commit({
    fs,
    dir,
    message: "t",
    author: { name: "t", email: "t@t.dev" },
  });
  const oids = await collectReachableObjects(dir, commitSha);
  const { packfile } = await git.packObjects({ fs, dir, oids });
  if (packfile === undefined) {
    throw new Error("index-pack test: packObjects returned no packfile");
  }
  return { commitSha, pack: packfile };
}

describe("indexPackIntoGitDir", () => {
  test("indexes a pack into a retained gitDir the commit can be read from", async () => {
    const { commitSha, pack } = await buildPack();
    const gitDir = await tempDir();

    await indexPackIntoGitDir(
      gitDir,
      pack,
      commitSha,
      DEFAULT_PACK_MATERIALIZATION_LIMITS,
    );

    // The retained object store serves the commit and its tree, so a later
    // checkout can walk it.
    const { commit } = await git.readCommit({
      fs,
      dir: gitDir,
      oid: commitSha,
    });
    expect(commit.tree.length).toBeGreaterThan(0);
  });

  test("throws when the pinned commit is absent from the pack", async () => {
    const { pack } = await buildPack();
    const gitDir = await tempDir();
    const absentSha = "0".repeat(40);

    await expect(
      indexPackIntoGitDir(
        gitDir,
        pack,
        absentSha,
        DEFAULT_PACK_MATERIALIZATION_LIMITS,
      ),
    ).rejects.toThrow(/not found in the pack/);
  });

  test("rejects a pack whose header declares more objects than the cap", async () => {
    // The one-file commit carries three objects (commit, tree, blob); a cap of
    // two rejects it from the header alone, before any object is indexed.
    const { commitSha, pack } = await buildPack();
    const gitDir = await tempDir();

    await expect(
      indexPackIntoGitDir(gitDir, pack, commitSha, {
        ...DEFAULT_PACK_MATERIALIZATION_LIMITS,
        maxPackObjects: 2,
      }),
    ).rejects.toThrow(/exceeding the 2-object cap/);
  });

  test("rejects a buffer that does not begin with the PACK magic", async () => {
    const gitDir = await tempDir();
    const notAPack = new Uint8Array(16); // 16 zero bytes: valid length, bad magic

    await expect(
      indexPackIntoGitDir(
        gitDir,
        notAPack,
        "0".repeat(40),
        DEFAULT_PACK_MATERIALIZATION_LIMITS,
      ),
    ).rejects.toThrow(/does not begin with the "PACK" magic/);
  });

  test("rejects a buffer shorter than the pack header", async () => {
    const gitDir = await tempDir();
    const truncated = new Uint8Array(8);

    await expect(
      indexPackIntoGitDir(
        gitDir,
        truncated,
        "0".repeat(40),
        DEFAULT_PACK_MATERIALIZATION_LIMITS,
      ),
    ).rejects.toThrow(/shorter than the 12-byte header/);
  });
});
