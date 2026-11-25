import { afterEach, describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import git, { type CallbackFsClient, type FsClient } from "isomorphic-git";
import { createIsogitStorage } from "./index";
import { createNodeIsogitRuntime } from "./node-runtime";
import { normalizeRuntime, syncPaths, type IsogitRuntime } from "./runtime";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const dir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "isogit-runtime-"),
  );
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((dir) => fs.promises.rm(dir, { recursive: true, force: true })),
  );
});

function callbackFs(): CallbackFsClient {
  return {
    readFile: fs.readFile.bind(fs),
    writeFile: fs.writeFile.bind(fs),
    unlink: fs.unlink.bind(fs),
    readdir: fs.readdir.bind(fs),
    mkdir: fs.mkdir.bind(fs),
    rmdir: fs.rmdir.bind(fs),
    stat: fs.stat.bind(fs),
    lstat: fs.lstat.bind(fs),
    readlink: fs.readlink.bind(fs),
    symlink: fs.symlink.bind(fs),
    chmod: fs.chmod.bind(fs),
  };
}

function runtimeWith(fsClient: FsClient): IsogitRuntime {
  return {
    fs: fsClient,
    path,
    rename: (oldPath, newPath) => fs.promises.rename(oldPath, newPath),
  };
}

describe("normalizeRuntime", () => {
  test("normalizes promise filesystem operations", async () => {
    const dir = await tempDir();
    const runtime = normalizeRuntime(createNodeIsogitRuntime());
    const nested = path.join(dir, "one", "two");
    const filepath = path.join(nested, "value.txt");

    await runtime.fs.mkdir(nested, { recursive: true });
    await runtime.fs.writeFile(filepath, "promise client");

    expect(await runtime.fs.readTextFile(filepath)).toBe("promise client");
    await runtime.fs.remove(path.join(dir, "one"), {
      force: true,
      recursive: true,
    });
    await expect(runtime.fs.stat(filepath)).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  test("normalizes callback filesystem operations", async () => {
    const dir = await tempDir();
    const runtime = normalizeRuntime(runtimeWith(callbackFs()));
    const filepath = path.join(dir, "callback.txt");

    await runtime.fs.writeFile(filepath, "callback client");

    expect(await runtime.fs.readTextFile(filepath)).toBe("callback client");
  });

  test("preserves errors returned by the filesystem", async () => {
    const expected = Object.assign(new Error("permission denied"), {
      code: "EACCES",
    });
    const fsClient = callbackFs();
    fsClient.readFile = (
      _filepath: unknown,
      callback: (cause: unknown) => void,
    ) => {
      callback(expected);
    };
    const runtime = normalizeRuntime(runtimeWith(fsClient));

    await expect(runtime.fs.readFile("unreadable")).rejects.toBe(expected);
  });

  test("routes atomic rename through the host runtime", async () => {
    const calls: [string, string][] = [];
    const host = createNodeIsogitRuntime();
    const runtime = normalizeRuntime({
      ...host,
      rename: (oldPath, newPath) => {
        calls.push([oldPath, newPath]);
        return Promise.resolve();
      },
    });

    await runtime.fs.rename("staging.pack", "final.pack");

    expect(calls).toEqual([["staging.pack", "final.pack"]]);
  });
});

test("storage flushes after completed mutation boundaries", async () => {
  const dir = await tempDir();
  let flushes = 0;
  const storage = createIsogitStorage({
    ...createNodeIsogitRuntime(),
    flush: () => {
      flushes += 1;
      return Promise.resolve();
    },
  });

  await storage.initAgentRepo(dir);
  await storage.createAndSwitchBranch(dir, "flush-test");

  expect(flushes).toBe(3);

  await fs.promises.rm(path.join(dir, "state"), { recursive: true });
  await storage.initAgentRepo(dir);

  expect(flushes).toBe(4);
  await expect(fs.promises.stat(path.join(dir, "state"))).resolves.toBeTruthy();
});

test("flushes commit objects and the published branch ref exactly once", async () => {
  const dir = await tempDir();
  const refPath = path.join(dir, ".git", "refs", "heads", "main");
  const observedRefs: (string | null)[] = [];
  const storage = createIsogitStorage({
    ...createNodeIsogitRuntime(),
    flush: async () => {
      const ref = await fs.promises.readFile(refPath, "utf8").catch(() => null);
      observedRefs.push(ref?.trim() ?? null);
    },
  });
  const store = await storage.createIsogitStore(dir);
  const initialRef = (await fs.promises.readFile(refPath, "utf8")).trim();
  observedRefs.length = 0;

  await store.writeTurns([
    {
      role: "user",
      content: [{ type: "text", text: "durable before publication" }],
      timestamp: 1,
    },
  ]);
  const committed = await store.commit({ message: "durable commit" });

  expect(observedRefs).toEqual([initialRef, committed.hash]);
});

test("flushes a new branch before publishing it through HEAD", async () => {
  const dir = await tempDir();
  const headPath = path.join(dir, ".git", "HEAD");
  const branchPath = path.join(dir, ".git", "refs", "heads", "durable-branch");
  const observations: { branchExists: boolean; head: string }[] = [];
  const storage = createIsogitStorage({
    ...createNodeIsogitRuntime(),
    flush: async () => {
      const [head, branchExists] = await Promise.all([
        fs.promises.readFile(headPath, "utf8"),
        fs.promises
          .stat(branchPath)
          .then(() => true)
          .catch(() => false),
      ]);
      observations.push({ branchExists, head: head.trim() });
    },
  });

  await storage.initAgentRepo(dir);
  observations.length = 0;
  await storage.createAndSwitchBranch(dir, "durable-branch");

  expect(observations).toEqual([
    { branchExists: true, head: "ref: refs/heads/main" },
    { branchExists: true, head: "ref: refs/heads/durable-branch" },
  ]);
});

test.each(["add", "commit"] as const)(
  "git %s rejects an index it fails to read instead of reading it as empty",
  async (operation) => {
    const dir = await tempDir();
    const indexPath = path.join(dir, ".git", "index");
    let armed = false;
    const runtime = runtimeWith({
      promises: {
        ...fs.promises,
        async readFile(...args: Parameters<typeof fs.promises.readFile>) {
          if (armed && args[0] === indexPath) {
            throw Object.assign(new Error("injected index read failure"), {
              code: "EIO",
            });
          }
          return fs.promises.readFile(...args);
        },
      },
    });
    const store = await createIsogitStorage(runtime).createIsogitStore(dir);
    await store.writeBlob("output", new Uint8Array([1]));
    const head = (await store.commit({ message: "Committed output" })).hash;
    await fs.promises.writeFile(path.join(dir, "turns.jsonl"), "");
    const indexBefore = await fs.promises.readFile(indexPath);
    const gitFs = normalizeRuntime(runtime).fs.git;

    armed = true;
    await expect(
      operation === "add"
        ? git.add({ fs: gitFs, dir, filepath: "turns.jsonl" })
        : git.commit({
            fs: gitFs,
            dir,
            message: "Checkpoint",
            author: { name: "test", email: "test@example.com" },
          }),
    ).rejects.toThrow("injected index read failure");
    armed = false;
    expect(await fs.promises.readFile(indexPath)).toEqual(indexBefore);
    expect(await git.resolveRef({ fs, dir, ref: "HEAD" })).toBe(head);
  },
);

test("checkpoints and their rollback run through a callback filesystem client", async () => {
  const dir = await tempDir();
  let fail = false;
  const storage = createIsogitStorage({
    ...runtimeWith(callbackFs()),
    async flush() {
      if (!fail) return;
      fail = false;
      throw new Error("injected checkpoint flush failure");
    },
  });
  const store = await storage.createIsogitStore(dir);
  await store.writeBlob("first", new Uint8Array([1]));
  const first = await store.commit({ message: "First checkpoint" });
  await store.writeBlob("second", new Uint8Array([2]));

  fail = true;
  await expect(store.commit({ message: "Failed checkpoint" })).rejects.toThrow(
    "injected checkpoint flush failure",
  );
  const retried = await store.commit({ message: "Retry checkpoint" });
  expect(retried.parentHash).toBe(first.hash);
  expect(await git.listFiles({ fs, dir, ref: retried.hash })).toEqual([
    ".gitignore",
    "tool-output/first",
    "tool-output/second",
  ]);
});

describe("syncPaths", () => {
  const withSync = (sync: (p: string) => Promise<void>) =>
    normalizeRuntime({ ...createNodeIsogitRuntime(), sync });
  const paths = Array.from({ length: 40 }, (_, i) => `p${String(i)}`);

  test("syncs every path, at most 16 at once", async () => {
    let running = 0;
    let most = 0;
    const synced: string[] = [];
    await syncPaths(
      withSync(async (p) => {
        running += 1;
        most = Math.max(most, running);
        await Promise.resolve();
        running -= 1;
        synced.push(p);
      }),
      paths,
    );
    expect(most).toBe(16);
    expect(synced.sort()).toEqual([...paths].sort());
  });

  test("after a failure it starts no more, and throws once the running syncs settle", async () => {
    const started: string[] = [];
    const finished: string[] = [];
    const gate = Promise.withResolvers<undefined>();
    let finishedAtSettle: number | undefined;
    const syncing = syncPaths(
      withSync(async (p) => {
        started.push(p);
        if (p === "p3") throw new Error("sync failed");
        await gate.promise;
        finished.push(p);
      }),
      paths,
    ).finally(() => {
      finishedAtSettle = finished.length;
    });
    expect(started).toHaveLength(16);

    gate.resolve(undefined);
    await expect(syncing).rejects.toThrow("sync failed");
    expect(finishedAtSettle).toBe(15);
    expect(started).toHaveLength(16);
  });
});
