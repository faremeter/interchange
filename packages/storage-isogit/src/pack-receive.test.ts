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
import { createNodeIsogitRuntime } from "./node-runtime";
import { syncObjectsAndRefs } from "./sync-record";
import {
  PACK_COUNT_LIMIT,
  stagePack as stagePackOn,
  UNPACK_LIMIT,
} from "./pack-receive";
import { normalizeRuntime, type StorageRuntime } from "./runtime";
import { recordingRuntime } from "./sync.test-helpers";

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

// A repository whose one commit holds `files` files, so its pack holds
// `files` + 2 objects.
async function makeLargeSourceRepo(files: number): Promise<{
  dir: string;
  oids: string[];
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });
  const names = Array.from({ length: files }, (_, i) => `file-${String(i)}`);
  for (const name of names) {
    await fs.promises.writeFile(path.join(dir, name), name);
  }
  await git.add({ fs, dir, filepath: names });
  const commitSha = await git.commit({
    fs,
    dir,
    message: "Many files",
    author: { name: "Test", email: "test@test.dev" },
  });
  return { dir, oids: await collectReachableObjects(dir, commitSha) };
}

function stagingDirOf(targetDir: string, transferId: string): string {
  return path.join(targetDir, ".git", "objects", "pack-staging", transferId);
}

async function packDirEntries(targetDir: string): Promise<string[]> {
  return fs.promises.readdir(path.join(targetDir, ".git", "objects", "pack"));
}

function loosePath(targetDir: string, oid: string): string {
  return path.join(targetDir, ".git", "objects", oid.slice(0, 2), oid.slice(2));
}

describe("stagePack", () => {
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

  test("publish writes every object as a loose object and leaves no pack", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "publish");
    await staged.publish();

    for (const oid of source.oids) {
      await fs.promises.access(loosePath(targetDir, oid));
      const read = await git.readObject({ fs, dir: targetDir, oid });
      expect(read.oid).toBe(oid);
    }
    expect(await packDirEntries(targetDir)).toEqual([]);
    await expect(
      fs.promises.access(stagingDirOf(targetDir, "publish")),
    ).rejects.toThrow();
    const viaView = await git.readCommit({
      fs: staged.fs,
      dir: targetDir,
      oid: source.commitSha,
    });
    expect(viaView.oid).toBe(source.commitSha);
  });

  test("publish keeps an object the repo holds and syncs its directory", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);
    const held = loosePath(targetDir, source.commitSha);
    await fs.promises.mkdir(path.dirname(held), { recursive: true });
    await fs.promises.copyFile(loosePath(source.dir, source.commitSha), held);
    const { runtime, steps } = recordingRuntime(targetDir);
    // Synced once already, so the publish does not sweep every object
    // directory, and only `keep` makes it sync the held object's.
    await syncObjectsAndRefs(runtime, targetDir);
    steps.length = 0;

    const staged = await stagePackOn(runtime, targetDir, pack, "held");
    await staged.publish();

    const heldName = path.relative(targetDir, held);
    expect(steps.filter((step) => step.endsWith(` -> ${heldName}`))).toEqual(
      [],
    );
    expect(steps).toContain(`sync ${path.dirname(heldName)}`);
    expect(
      (await git.readCommit({ fs, dir: targetDir, oid: source.commitSha })).oid,
    ).toBe(source.commitSha);
  });

  test("a publish reads through a cache that loaded the staged pack, which still reads the commit after it", async () => {
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
    const spy = spyOn(fs.promises, "readFile");
    let packReads: string[];
    try {
      await staged.publish(cache);
      packReads = spy.mock.calls
        .map(([target]) => String(target))
        .filter((target) => target.endsWith(".pack"));
    } finally {
      spy.mockRestore();
    }
    expect(packReads).toEqual([]);

    const read = await git.readCommit({
      fs,
      dir: targetDir,
      cache,
      oid: source.commitSha,
    });
    expect(read.oid).toBe(source.commitSha);
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

  test("discard after publish leaves the published objects in place", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);

    const staged = await stagePack(targetDir, pack, "published");
    await staged.publish();
    await staged.discard();

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

  test("removeStagedPacks removes the temporary file of a ref write a crash cut short", async () => {
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);
    const gitdir = path.join(targetDir, ".git");
    const before = (await fs.promises.readdir(gitdir)).sort();
    await fs.promises.writeFile(
      path.join(gitdir, "ref-write-0123456789abcdef"),
      `${"e".repeat(40)}\n`,
    );

    await removeStagedPacks(targetDir);

    expect((await fs.promises.readdir(gitdir)).sort()).toEqual(before);
  });

  test("removeOrphanPackIndexes removes only an index whose pack is gone", async () => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);
    const packDir = path.join(targetDir, ".git", "objects", "pack");
    await fs.promises.mkdir(packDir, { recursive: true });
    for (const name of ["kept", "lost"]) {
      await fs.promises.writeFile(
        path.join(packDir, `pack-recv-${name}.pack`),
        pack,
      );
      await git.indexPack({
        fs,
        dir: targetDir,
        filepath: path.join(
          ".git",
          "objects",
          "pack",
          `pack-recv-${name}.pack`,
        ),
      });
    }
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
        /already in progress/,
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

  test("a failed publish removes the staging directory and cannot be published again", async () => {
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
        if (renameCount === 2) throw new Error("object rename failed");
        await originalRename(from, to);
      },
    );

    try {
      await expect(staged.publish()).rejects.toThrow("object rename failed");
    } finally {
      renameSpy.mockRestore();
    }

    await expect(
      fs.promises.access(stagingDirOf(targetDir, "partial")),
    ).rejects.toThrow();
    await expect(staged.publish()).rejects.toThrow(/cannot be published/);
  });
});

test("a publish syncs each object before it moves it into place, and every directory it touched before it returns", async () => {
  const source = await makeSourceRepo();
  const pack = await createPackFromRepo(source.dir, source.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  const { runtime, steps } = recordingRuntime(targetDir);
  // Synced once already, so the publish does not sweep every object
  // directory.
  await syncObjectsAndRefs(runtime, targetDir);
  steps.length = 0;

  const staged = await stagePackOn(runtime, targetDir, pack, "durable");
  await staged.publish();

  const objects = path.join(".git", "objects");
  const renames = steps.filter((step) => step.startsWith("rename "));
  expect(renames.map((step) => step.split(" -> ")[1]).sort()).toEqual(
    source.oids
      .map((oid) => path.join(objects, oid.slice(0, 2), oid.slice(2)))
      .sort(),
  );
  for (const rename of renames) {
    const temp = rename.slice("rename ".length).split(" -> ")[0];
    expect(steps.indexOf(`sync ${temp}`)).toBeLessThan(steps.indexOf(rename));
  }
  const lastRename = steps.indexOf(renames[renames.length - 1] ?? "");
  const fanouts = new Set(
    source.oids.map((oid) => path.join(objects, oid.slice(0, 2))),
  );
  for (const fanout of [objects, ...fanouts]) {
    expect(steps.lastIndexOf(`sync ${fanout}`)).toBeGreaterThan(lastRename);
  }
});

test("a failed publish waits for the objects in flight before it removes the staging directory", async () => {
  const source = await makeSourceRepo();
  const pack = await createPackFromRepo(source.dir, source.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  const stagingDir = stagingDirOf(targetDir, "in-flight");
  // The pack is smaller than what a publish writes at once, so all of its
  // objects are in flight together. The first rename fails once every
  // other object waits at its sync, and they go on only after every
  // reaction to that failure has run.
  const others = source.oids.length - 1;
  let syncs = 0;
  let inFlight = 0;
  let failed = false;
  const othersWaiting = Promise.withResolvers<undefined>();
  const released = Promise.withResolvers<undefined>();
  const node = createNodeIsogitRuntime();
  const base = normalizeRuntime({
    ...node,
    rename: async () => {
      if (failed) {
        inFlight -= 1;
      } else {
        failed = true;
        await othersWaiting.promise;
        setImmediate(() => released.resolve(undefined));
      }
      throw new Error("object rename failed");
    },
    sync: async (p) => {
      syncs += 1;
      if (syncs > 1) {
        inFlight += 1;
        if (inFlight === others) othersWaiting.resolve(undefined);
        await released.promise;
      }
      await node.sync?.(p);
    },
  });
  let inFlightAtRemove: number | undefined;
  const runtime: StorageRuntime = {
    ...base,
    fs: {
      ...base.fs,
      remove: (filepath, options) => {
        if (filepath === stagingDir) inFlightAtRemove ??= inFlight;
        return base.fs.remove(filepath, options);
      },
    },
  };

  const staged = await stagePackOn(runtime, targetDir, pack, "in-flight");
  await expect(staged.publish()).rejects.toThrow("object rename failed");

  expect(inFlightAtRemove).toBe(0);
  await expect(fs.promises.access(stagingDir)).rejects.toThrow();
});

test("a publish moves a pack of UNPACK_LIMIT objects into objects/pack, synced and index last", async () => {
  const source = await makeLargeSourceRepo(UNPACK_LIMIT - 2);
  expect(source.oids.length).toBe(UNPACK_LIMIT);
  const pack = await createPackFromRepo(source.dir, source.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  const { runtime, steps } = recordingRuntime(targetDir);

  await syncObjectsAndRefs(runtime, targetDir);
  steps.length = 0;
  const staged = await stagePackOn(runtime, targetDir, pack, "large");
  await staged.publish();

  const objects = path.join(".git", "objects");
  const staging = path.join(objects, "pack-staging", "large");
  const packDir = path.join(objects, "pack");
  expect(steps).toEqual([
    `sync ${path.join(staging, "pack.pack")}`,
    `sync ${path.join(staging, "pack.idx")}`,
    `rename ${path.join(staging, "pack.pack")} -> ${path.join(packDir, "pack-recv-large.pack")}`,
    `sync ${packDir}`,
    `rename ${path.join(staging, "pack.idx")} -> ${path.join(packDir, "pack-recv-large.idx")}`,
    `sync ${packDir}`,
    `sync ${objects}`,
  ]);
  for (const oid of source.oids) {
    expect((await git.readObject({ fs, dir: targetDir, oid })).oid).toBe(oid);
  }
});

test("a publish that moves a pack flushes on a runtime that only flushes", async () => {
  const source = await makeLargeSourceRepo(UNPACK_LIMIT - 2);
  const pack = await createPackFromRepo(source.dir, source.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  const { sync: _sync, ...node } = createNodeIsogitRuntime();
  const steps: string[] = [];
  const runtime = normalizeRuntime({
    ...node,
    rename: async (from, to) => {
      steps.push(`rename ${path.basename(to)}`);
      await node.rename(from, to);
    },
    flush: async () => {
      steps.push("flush");
    },
  });
  await syncObjectsAndRefs(runtime, targetDir);
  steps.length = 0;

  await (await stagePackOn(runtime, targetDir, pack, "flushed")).publish();

  expect(steps).toEqual([
    "rename pack-recv-flushed.pack",
    "rename pack-recv-flushed.idx",
    "flush",
  ]);
});

test("a publish unpacks a pack of UNPACK_LIMIT - 1 objects", async () => {
  const source = await makeLargeSourceRepo(UNPACK_LIMIT - 3);
  expect(source.oids.length).toBe(UNPACK_LIMIT - 1);
  const pack = await createPackFromRepo(source.dir, source.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);

  await (await stagePack(targetDir, pack, "small")).publish();

  expect(await packDirEntries(targetDir)).toEqual([]);
});

test("a failed publish of a large pack takes the pack back out", async () => {
  const source = await makeLargeSourceRepo(UNPACK_LIMIT - 2);
  const pack = await createPackFromRepo(source.dir, source.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  const node = createNodeIsogitRuntime();
  let renames = 0;
  const runtime = normalizeRuntime({
    ...node,
    rename: async (from, to) => {
      renames += 1;
      if (renames === 2) throw new Error("index rename failed");
      await node.rename(from, to);
    },
  });

  const staged = await stagePackOn(runtime, targetDir, pack, "failed");
  await expect(staged.publish()).rejects.toThrow("index rename failed");

  expect(await packDirEntries(targetDir)).toEqual([]);
});

test("a publish whose sync fails once the index lands keeps the pack and its index", async () => {
  const source = await makeLargeSourceRepo(UNPACK_LIMIT - 2);
  const pack = await createPackFromRepo(source.dir, source.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  const node = createNodeIsogitRuntime();
  let renames = 0;
  const runtime = normalizeRuntime({
    ...node,
    rename: async (from, to) => {
      renames += 1;
      await node.rename(from, to);
    },
    sync: async (p) => {
      if (renames === 2) throw new Error("directory sync failed");
      await node.sync?.(p);
    },
  });

  const staged = await stagePackOn(runtime, targetDir, pack, "late");
  await expect(staged.publish()).rejects.toThrow("directory sync failed");

  expect((await packDirEntries(targetDir)).sort()).toEqual([
    "pack-recv-late.idx",
    "pack-recv-late.pack",
  ]);
  for (const oid of source.oids) {
    expect((await git.readObject({ fs, dir: targetDir, oid })).oid).toBe(oid);
  }
});

test("the first publish into a repository in a process syncs its pack directory, and a later one does not", async () => {
  const first = await makeSourceRepo();
  const second = await makeSourceRepo();
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  // A runtime of its own stands for a process that has not published here.
  const { runtime, steps } = recordingRuntime(targetDir);
  const packDirSync = `sync ${path.join(".git", "objects", "pack")}`;
  const publish = async (repo: { dir: string; oids: string[] }, id: string) =>
    (
      await stagePackOn(
        runtime,
        targetDir,
        await createPackFromRepo(repo.dir, repo.oids),
        id,
      )
    ).publish();

  await publish(first, "first");
  expect(steps).toContain(packDirSync);
  steps.length = 0;
  await publish(second, "second");
  expect(steps).not.toContain(packDirSync);
});

test("the next publish after a pack directory sync failed syncs that directory before it returns", async () => {
  const before = await makeSourceRepo();
  const large = await makeLargeSourceRepo(UNPACK_LIMIT - 2);
  const after = await makeSourceRepo();
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  const { runtime, steps, failing } = recordingRuntime(targetDir);
  const packDir = path.join(targetDir, ".git", "objects", "pack");
  const stage = async (repo: { dir: string; oids: string[] }, id: string) =>
    stagePackOn(
      runtime,
      targetDir,
      await createPackFromRepo(repo.dir, repo.oids),
      id,
    );
  // This process has synced the pack directory once already.
  await (await stage(before, "before")).publish();
  // The second sync of objects/pack, once the index lands, fails.
  let packDirSyncs = 0;
  failing.sync = (p) => p === packDir && ++packDirSyncs === 2;
  await expect((await stage(large, "late")).publish()).rejects.toThrow(
    "sync failed",
  );
  failing.sync = () => false;

  steps.length = 0;
  await (await stage(after, "after")).publish();

  expect(steps).toContain(`sync ${path.join(".git", "objects", "pack")}`);
  expect(steps).toContain(`sync ${path.join(".git", "objects")}`);
});

test("a stage refuses a transfer ID whose pack the repository holds", async () => {
  const source = await makeLargeSourceRepo(UNPACK_LIMIT - 2);
  const pack = await createPackFromRepo(source.dir, source.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  await (await stagePack(targetDir, pack, "reused")).publish();

  await expect(stagePack(targetDir, pack, "reused")).rejects.toThrow(
    "already published",
  );
});

test.each(["pack", "idx"] as const)(
  "a stage refuses a transfer ID whose %s alone the repository holds",
  async (extension) => {
    const source = await makeSourceRepo();
    const pack = await createPackFromRepo(source.dir, source.oids);
    const targetDir = await tempDir();
    await initAgentRepo(targetDir);
    const existing = path.join(
      targetDir,
      ".git",
      "objects",
      "pack",
      `pack-recv-lone.${extension}`,
    );
    await fs.promises.mkdir(path.dirname(existing), { recursive: true });
    await fs.promises.writeFile(existing, "existing");

    await expect(stagePack(targetDir, pack, "lone")).rejects.toThrow(
      "already published",
    );
    expect(await fs.promises.readFile(existing, "utf8")).toBe("existing");
  },
);

test("a publish unpacks a large pack once the repository holds PACK_COUNT_LIMIT packs", async () => {
  const source = await makeLargeSourceRepo(UNPACK_LIMIT - 2);
  const pack = await createPackFromRepo(source.dir, source.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  for (let i = 0; i < PACK_COUNT_LIMIT; i += 1) {
    await (await stagePack(targetDir, pack, `kept${String(i)}`)).publish();
  }
  const packDir = path.join(targetDir, ".git", "objects", "pack");
  const indexes = async () =>
    (await fs.promises.readdir(packDir)).filter((n) => n.endsWith(".idx"));
  expect(await indexes()).toHaveLength(PACK_COUNT_LIMIT);

  await (await stagePack(targetDir, pack, "unpacked")).publish();

  expect(await indexes()).toHaveLength(PACK_COUNT_LIMIT);
  for (const oid of source.oids) {
    await fs.promises.access(loosePath(targetDir, oid));
  }
});

test("a publish reads the pack's objects from the pack, not the repository's other packs", async () => {
  const large = await makeLargeSourceRepo(UNPACK_LIMIT - 2);
  const largePack = await createPackFromRepo(large.dir, large.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  for (const id of ["kept0", "kept1"]) {
    await (await stagePack(targetDir, largePack, id)).publish();
  }
  const source = await makeSourceRepo();
  const staged = await stagePack(
    targetDir,
    await createPackFromRepo(source.dir, source.oids),
    "small",
  );

  const spy = spyOn(fs.promises, "readFile");
  let read: string[];
  try {
    await staged.publish();
  } finally {
    read = spy.mock.calls.map(([target]) => String(target));
    spy.mockRestore();
  }

  const packDir = path.join(targetDir, ".git", "objects", "pack");
  expect(read.filter((p) => p.startsWith(packDir + path.sep))).toEqual([]);
  for (const oid of source.oids) {
    await fs.promises.access(loosePath(targetDir, oid));
  }
});

test("a publish lists the pack directory once, however many objects it writes", async () => {
  const large = await makeLargeSourceRepo(UNPACK_LIMIT - 2);
  const largePack = await createPackFromRepo(large.dir, large.oids);
  const targetDir = await tempDir();
  await initAgentRepo(targetDir);
  await (await stagePack(targetDir, largePack, "kept")).publish();
  const source = await makeLargeSourceRepo(UNPACK_LIMIT / 2);
  const staged = await stagePack(
    targetDir,
    await createPackFromRepo(source.dir, source.oids),
    "unpacked",
  );
  expect(staged.oids.length).toBeLessThan(UNPACK_LIMIT);

  const packDir = path.join(targetDir, ".git", "objects", "pack");
  const spy = spyOn(fs.promises, "readdir");
  let listings: number;
  try {
    await staged.publish();
  } finally {
    listings = spy.mock.calls.filter(
      ([target]) => path.resolve(String(target)) === packDir,
    ).length;
    spy.mockRestore();
  }

  expect(listings).toBe(1);
});
