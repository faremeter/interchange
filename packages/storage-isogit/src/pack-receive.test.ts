import { afterEach, describe, expect, spyOn, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { initAgentRepo } from "./node";
import {
  removeOrphanPackIndexes,
  removeStagedPacks,
  stagePack,
  type StagedPack,
} from "./node";
import { collectReachableObjects } from "./node";

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

describe("stagePack", () => {
  function stagingDirOf(targetDir: string, transferId: string): string {
    return path.join(targetDir, ".git", "objects", "pack-staging", transferId);
  }

  async function packDirEntries(targetDir: string): Promise<string[]> {
    return fs.promises.readdir(path.join(targetDir, ".git", "objects", "pack"));
  }

  test("the view reads the staged objects and the repo's own fs does not", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "view");
    try {
      expect(staged.oids).toContain(source.commitSha);
      const viaView = await git.readCommit({
        fs: staged.fs,
        dir: targetDir,
        oid: source.commitSha,
      });
      expect(viaView.oid).toBe(source.commitSha);
      await expect(
        git.readCommit({ fs, dir: targetDir, oid: source.commitSha }),
      ).rejects.toThrow();
      expect(await packDirEntries(targetDir)).toEqual([]);
    } finally {
      await staged.discard();
    }
  });

  test("the view finds the staged pack through a relative gitdir", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "relative");
    try {
      const viaView = await git.readCommit({
        fs: staged.fs,
        gitdir: path.relative(process.cwd(), path.join(targetDir, ".git")),
        oid: source.commitSha,
      });
      expect(viaView.oid).toBe(source.commitSha);
    } finally {
      await staged.discard();
    }
  });

  test("publish makes the objects readable through the repo's own fs", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "publish");
    await staged.publish();

    const commit = await git.readCommit({
      fs,
      dir: targetDir,
      oid: source.commitSha,
    });
    expect(commit.oid).toBe(source.commitSha);
    expect((await packDirEntries(targetDir)).sort()).toEqual([
      "pack-recv-publish.idx",
      "pack-recv-publish.pack",
    ]);
    await expect(
      fs.promises.access(stagingDirOf(targetDir, "publish")),
    ).rejects.toThrow();
    // The view passes through after publish, so it lists the pack once.
    const viaView = await git.readCommit({
      fs: staged.fs,
      dir: targetDir,
      oid: source.commitSha,
    });
    expect(viaView.oid).toBe(source.commitSha);
  });

  test("a cache that loaded the staged pack through the view reuses it after publish", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);
    const cache = {};

    const staged = await stagePack(targetDir, pack, "cache-key");
    await git.readCommit({
      fs: staged.fs,
      dir: targetDir,
      cache,
      oid: source.commitSha,
    });
    await staged.publish();

    const packDir = path.join(targetDir, ".git", "objects", "pack");
    const spy = spyOn(fs.promises, "readFile");
    try {
      await git.readCommit({
        fs,
        dir: targetDir,
        cache,
        oid: source.commitSha,
      });
      const packReads = spy.mock.calls.filter(([target]) =>
        String(target).startsWith(packDir),
      );
      expect(packReads).toEqual([]);
    } finally {
      spy.mockRestore();
    }
  });

  test("unindexable bytes reject and leave no staging directory or pack", async () => {
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await expect(
      stagePack(targetDir, new Uint8Array([1, 2, 3, 4]), "unindexable"),
    ).rejects.toThrow();

    await expect(
      fs.promises.access(stagingDirOf(targetDir, "unindexable")),
    ).rejects.toThrow();
    expect(await packDirEntries(targetDir)).toEqual([]);
  });

  test("discard leaves no staging directory and nothing in objects/pack", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "discard");
    await staged.discard();
    await staged.discard();

    await expect(
      fs.promises.access(stagingDirOf(targetDir, "discard")),
    ).rejects.toThrow();
    expect(await packDirEntries(targetDir)).toEqual([]);
    await expect(
      git.readCommit({ fs: staged.fs, dir: targetDir, oid: source.commitSha }),
    ).rejects.toThrow();
    await expect(staged.publish()).rejects.toThrow(/cannot be published/);
    expect(await packDirEntries(targetDir)).toEqual([]);
  });

  test("discard after publish leaves the published pack in place", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "published");
    await staged.publish();
    await staged.discard();

    expect((await packDirEntries(targetDir)).sort()).toEqual([
      "pack-recv-published.idx",
      "pack-recv-published.pack",
    ]);
    expect(
      (await git.readCommit({ fs, dir: targetDir, oid: source.commitSha })).oid,
    ).toBe(source.commitSha);
  });

  test("removeStagedPacks removes every staged pack", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    await stagePack(targetDir, pack, "left-one");
    await stagePack(targetDir, pack, "left-two");
    await removeStagedPacks(targetDir);

    await expect(
      fs.promises.access(
        path.join(targetDir, ".git", "objects", "pack-staging"),
      ),
    ).rejects.toThrow();
    expect(await packDirEntries(targetDir)).toEqual([]);
  });

  test("removeOrphanPackIndexes removes only an index whose pack is gone", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);
    await (await stagePack(targetDir, pack, "kept")).publish();
    await (await stagePack(targetDir, pack, "lost")).publish();
    const packDir = path.join(targetDir, ".git", "objects", "pack");
    await fs.promises.rm(path.join(packDir, "pack-recv-lost.pack"));

    expect(await removeOrphanPackIndexes(targetDir)).toEqual([
      path.join(packDir, "pack-recv-lost.idx"),
    ]);
    expect((await packDirEntries(targetDir)).sort()).toEqual([
      "pack-recv-kept.idx",
      "pack-recv-kept.pack",
    ]);
    expect(
      (await git.readCommit({ fs, dir: targetDir, oid: source.commitSha })).oid,
    ).toBe(source.commitSha);
    expect(await removeOrphanPackIndexes(targetDir)).toEqual([]);
  });

  test("publish renames the completed pack before its index", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "rename-order");
    const originalRename = fs.promises.rename.bind(fs.promises);
    const renames: { from: string; to: string }[] = [];
    const renameSpy = spyOn(fs.promises, "rename").mockImplementation(
      async (from, to) => {
        await originalRename(from, to);
        renames.push({ from: from.toString(), to: to.toString() });
      },
    );

    try {
      await staged.publish();
    } finally {
      renameSpy.mockRestore();
    }

    const finalPack = path.join(
      targetDir,
      ".git",
      "objects",
      "pack",
      "pack-recv-rename-order.pack",
    );
    const finalIdx = finalPack.replace(/\.pack$/, ".idx");
    expect(renames).toEqual([
      {
        from: path.join(stagingDirOf(targetDir, "rename-order"), "pack.pack"),
        to: finalPack,
      },
      {
        from: path.join(stagingDirOf(targetDir, "rename-order"), "pack.idx"),
        to: finalIdx,
      },
    ]);
    await fs.promises.access(finalPack);
    await fs.promises.access(finalIdx);
    await expect(
      fs.promises.access(stagingDirOf(targetDir, "rename-order")),
    ).rejects.toThrow();
  });

  test.each(["pack", "idx"] as const)(
    "does not overwrite an existing final %s",
    async (extension) => {
      const source = await makeSourceRepo();
      const pack = await createPackFromRepo(source.dir, source.oids);
      const targetDir = await tempDir();
      await initAgentRepo(targetDir);

      const packDir = path.join(targetDir, ".git", "objects", "pack");
      const existingPath = path.join(
        packDir,
        `pack-recv-duplicate.${extension}`,
      );
      await fs.promises.mkdir(packDir, { recursive: true });
      await fs.promises.writeFile(existingPath, "existing");

      await expect(stagePack(targetDir, pack, "duplicate")).rejects.toThrow(
        /already published/,
      );
      expect(await fs.promises.readFile(existingPath, "utf8")).toBe("existing");
    },
  );

  test("rejects concurrent stages using the same transfer ID", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const originalIndexPack = git.indexPack.bind(git);
    let signalIndexingStarted: () => void = () => undefined;
    const indexingStarted = new Promise<void>((resolve) => {
      signalIndexingStarted = resolve;
    });
    let releaseIndexing: () => void = () => undefined;
    const indexingReleased = new Promise<void>((resolve) => {
      releaseIndexing = resolve;
    });
    let blockNextIndex = true;
    const indexPackSpy = spyOn(git, "indexPack").mockImplementation(
      async (args) => {
        if (blockNextIndex) {
          blockNextIndex = false;
          signalIndexingStarted();
          await indexingReleased;
        }
        return originalIndexPack(args);
      },
    );

    let firstStage: Promise<StagedPack> | undefined;
    try {
      firstStage = stagePack(targetDir, pack, "concurrent");
      await indexingStarted;

      await expect(stagePack(targetDir, pack, "concurrent")).rejects.toThrow(
        /already published or in progress/,
      );
      expect(indexPackSpy).toHaveBeenCalledTimes(1);

      releaseIndexing();
      await (await firstStage).publish();
    } finally {
      releaseIndexing();
      await firstStage?.catch(() => undefined);
      indexPackSpy.mockRestore();
    }
  });

  test("cleans up when index publication fails after the pack rename", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "partial");
    const originalRename = fs.promises.rename.bind(fs.promises);
    let renameCount = 0;
    const renameSpy = spyOn(fs.promises, "rename").mockImplementation(
      async (from, to) => {
        renameCount += 1;
        if (renameCount === 2) throw new Error("index publish failed");
        await originalRename(from, to);
      },
    );

    try {
      await expect(staged.publish()).rejects.toThrow("index publish failed");
    } finally {
      renameSpy.mockRestore();
    }

    const finalPack = path.join(
      targetDir,
      ".git",
      "objects",
      "pack",
      "pack-recv-partial.pack",
    );
    await expect(fs.promises.access(finalPack)).rejects.toThrow();
    await expect(
      fs.promises.access(finalPack.replace(/\.pack$/, ".idx")),
    ).rejects.toThrow();
    await expect(
      fs.promises.access(stagingDirOf(targetDir, "partial")),
    ).rejects.toThrow();
  });
});
