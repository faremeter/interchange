import { afterEach, expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import {
  collectReachableObjects,
  initAgentRepo,
  initRepo,
  stagePack,
  writeRefDurably,
} from "./node";
import { createDurableObjectWrites } from "./loose-objects";
import { createNodeIsogitRuntime } from "./node-runtime";
import { normalizeRuntime } from "./runtime";
import { syncObjectsAndRefs } from "./sync-record";
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

test("a durable write syncs each object before it moves it into place, and its directories in finish", async () => {
  const dir = await tempDir();
  await initAgentRepo(dir);
  const { runtime, steps } = recordingRuntime(dir);
  // This process synced the repository's object directories already.
  await syncObjectsAndRefs(runtime, dir);
  steps.length = 0;
  const durable = createDurableObjectWrites(runtime, dir);

  const blob = await git.writeBlob({
    fs: durable.fs,
    dir,
    blob: new TextEncoder().encode("event"),
  });
  const tree = await git.writeTree({
    fs: durable.fs,
    dir,
    tree: [{ mode: "100644", path: "event.json", oid: blob, type: "blob" }],
  });
  const beforeFinish = steps.length;
  await durable.finish();

  const objects = path.join(".git", "objects");
  const renames = steps.filter((step) => step.startsWith("rename "));
  expect(renames.map((step) => step.split(" -> ")[1]).sort()).toEqual(
    [blob, tree]
      .map((oid) => path.join(objects, oid.slice(0, 2), oid.slice(2)))
      .sort(),
  );
  for (const rename of renames) {
    const temp = rename.slice("rename ".length).split(" -> ")[0];
    expect(steps.indexOf(`sync ${temp}`)).toBeLessThan(steps.indexOf(rename));
  }
  expect(steps.slice(beforeFinish).sort()).toEqual(
    [objects, ...[blob, tree].map((oid) => path.join(objects, oid.slice(0, 2)))]
      .filter((entry, i, all) => all.indexOf(entry) === i)
      .map((entry) => `sync ${entry}`)
      .sort(),
  );
  const read = await git.readTree({ fs, dir, oid: tree });
  expect(read.tree.map((entry) => entry.oid)).toEqual([blob]);
});

test("a durable write keeps an object already in place and syncs its directory", async () => {
  const dir = await tempDir();
  await initAgentRepo(dir);
  const bytes = new TextEncoder().encode("event");
  const oid = await git.writeBlob({ fs, dir, blob: bytes });
  const objects = path.join(".git", "objects");
  const loose = path.join(dir, objects, oid.slice(0, 2), oid.slice(2));
  const before = await fs.promises.stat(loose);
  const { runtime, steps } = recordingRuntime(dir);
  await syncObjectsAndRefs(runtime, dir);
  steps.length = 0;
  const durable = createDurableObjectWrites(runtime, dir);

  expect(await git.writeBlob({ fs: durable.fs, dir, blob: bytes })).toBe(oid);
  await durable.finish();

  expect((await fs.promises.stat(loose)).ino).toBe(before.ino);
  expect(steps.sort()).toEqual(
    [objects, path.join(objects, oid.slice(0, 2))]
      .map((entry) => `sync ${entry}`)
      .sort(),
  );
});

test("the first write into a repository in a process syncs every object directory, and one after a failed write does too", async () => {
  const dir = await tempDir();
  await initAgentRepo(dir);
  const seeded = await git.writeBlob({
    fs,
    dir,
    blob: new TextEncoder().encode("from an earlier process"),
  });
  const { runtime, steps, failing } = recordingRuntime(dir);
  const objects = path.join(".git", "objects");
  const writeBlob = async (text: string) => {
    const durable = createDurableObjectWrites(runtime, dir);
    await git.writeBlob({
      fs: durable.fs,
      dir,
      blob: new TextEncoder().encode(text),
    });
    await durable.finish();
  };

  await writeBlob("first");
  expect(steps).toContain(`sync ${path.join(objects, seeded.slice(0, 2))}`);
  expect(steps).toContain(`sync ${path.join(objects, "pack")}`);

  steps.length = 0;
  await writeBlob("second");
  expect(steps).not.toContain(`sync ${path.join(objects, "pack")}`);

  failing.sync = () => true;
  await expect(writeBlob("third")).rejects.toThrow("sync failed");
  failing.sync = () => false;

  steps.length = 0;
  await writeBlob("fourth");
  expect(steps).toContain(`sync ${path.join(objects, "pack")}`);
});

test("a write dropped before finish leaves its directories to the next sync of the repository", async () => {
  const dir = await tempDir();
  await initAgentRepo(dir);
  const { runtime, steps } = recordingRuntime(dir);
  await syncObjectsAndRefs(runtime, dir);
  const objects = path.join(".git", "objects");
  const writeBlob = async (text: string, finish: boolean) => {
    const durable = createDurableObjectWrites(runtime, dir);
    const oid = await git.writeBlob({
      fs: durable.fs,
      dir,
      blob: new TextEncoder().encode(text),
    });
    if (finish) await durable.finish();
    return oid;
  };

  const dropped = await writeBlob("its commit failed validation", false);
  // Another fan-out directory, so only the record names the dropped one.
  let next = "next";
  while (
    (await git.hashBlob({ object: next })).oid.slice(0, 2) ===
    dropped.slice(0, 2)
  ) {
    next += "!";
  }
  steps.length = 0;
  await writeBlob(next, true);
  expect(steps).toContain(`sync ${path.join(objects, dropped.slice(0, 2))}`);

  steps.length = 0;
  await syncObjectsAndRefs(runtime, dir);
  expect(steps).toEqual([]);
});

test("a write that fails removes its temporary file", async () => {
  const dir = await tempDir();
  await initAgentRepo(dir);
  const { runtime, failing } = recordingRuntime(dir);
  const durable = createDurableObjectWrites(runtime, dir);

  failing.sync = () => true;
  await expect(
    git.writeBlob({
      fs: durable.fs,
      dir,
      blob: new TextEncoder().encode("its sync fails"),
    }),
  ).rejects.toThrow("sync failed");

  expect(
    await fs.promises.readdir(
      path.join(dir, ".git", "objects", "pack-staging"),
    ),
  ).toEqual([]);
});

// Inherited from a parent git process, `GIT_DIR` and its kin would point
// these commands at that repository.
const gitEnv = Object.fromEntries(
  Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_")),
);
const gitIn = (dir: string, args: string[]) =>
  execFileSync("git", ["-C", dir, ...args], {
    encoding: "utf8",
    env: gitEnv,
    stdio: ["ignore", "pipe", "pipe"],
  });
const author = { name: "Test", email: "test@test.dev" };

// isomorphic-git's own reads check each object's id, but only git itself
// proves the zlib framing and header are what every git reader expects.
test("git fsck accepts the loose objects a publish writes", async () => {
  const root = await tempDir();
  const source = path.join(root, "source");
  await git.init({ fs, dir: source, defaultBranch: "main" });
  await fs.promises.mkdir(path.join(source, "a", "b"), { recursive: true });
  await fs.promises.writeFile(path.join(source, "a", "b", "x.txt"), "hello\n");
  await fs.promises.writeFile(path.join(source, "empty"), "");
  await git.add({ fs, dir: source, filepath: ["a/b/x.txt", "empty"] });
  const first = await git.commit({ fs, dir: source, message: "one", author });
  await fs.promises.writeFile(
    path.join(source, "a", "b", "x.txt"),
    "hello world\n".repeat(200),
  );
  await git.add({ fs, dir: source, filepath: "a/b/x.txt" });
  const second = await git.commit({ fs, dir: source, message: "two", author });
  const { packfile } = await git.packObjects({
    fs,
    dir: source,
    // Each commit's own tree, as a sender's history pack carries them.
    oids: [
      ...new Set([
        ...(await collectReachableObjects(source, second)),
        ...(await collectReachableObjects(source, first)),
      ]),
    ],
    write: false,
  });
  if (packfile === undefined) throw new Error("packObjects returned no pack");

  const target = path.join(root, "target");
  await initRepo(target);
  await (await stagePack(target, packfile, "fsck")).publish();
  await writeRefDurably(target, "refs/heads/received", second);

  gitIn(target, ["fsck", "--full", "--strict"]);
  expect(gitIn(target, ["rev-parse", "received~1"]).trim()).toBe(first);
  expect(gitIn(target, ["count-objects", "-v"])).toContain("packs: 0");
});

test("git fsck accepts the objects of a durable write", async () => {
  const dir = await tempDir();
  await initRepo(dir);
  const runtime = normalizeRuntime(createNodeIsogitRuntime());
  const durable = createDurableObjectWrites(runtime, dir);
  const blob = await git.writeBlob({
    fs: durable.fs,
    dir,
    blob: new TextEncoder().encode("event\n"),
  });
  const tree = await git.writeTree({
    fs: durable.fs,
    dir,
    tree: [{ mode: "100644", path: "event.json", oid: blob, type: "blob" }],
  });
  const commit = await git.writeCommit({
    fs: durable.fs,
    dir,
    commit: {
      message: "event\n",
      tree,
      parent: [],
      author: { ...author, timestamp: 0, timezoneOffset: 0 },
      committer: { ...author, timestamp: 0, timezoneOffset: 0 },
    },
  });
  await durable.finish();
  await writeRefDurably(dir, "refs/heads/written", commit);

  gitIn(dir, ["fsck", "--full", "--strict"]);
  expect(gitIn(dir, ["cat-file", "-p", "written:event.json"])).toBe("event\n");
});
