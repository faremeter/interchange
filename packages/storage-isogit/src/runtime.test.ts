import { afterEach, describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import type { CallbackFsClient, FsClient } from "isomorphic-git";
import { createIsogitStorage } from "./index";
import { createNodeIsogitRuntime } from "./node-runtime";
import { normalizeRuntime, type IsogitRuntime } from "./runtime";

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
