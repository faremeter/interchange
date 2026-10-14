import { afterEach, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import {
  UncertainRefPublicationError,
  createIndexRollback,
} from "./commit-helpers";
import { createIsogitStore } from "./node";
import { createNodeIsogitRuntime } from "./node-runtime";
import { withRepoDirLock } from "./repo-lock";
import { normalizeRuntime } from "./runtime";

const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(
    tempDirs
      .splice(0)
      .map((dir) => fs.promises.rm(dir, { recursive: true, force: true })),
  );
});

async function tempDir(): Promise<string> {
  const dir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "index-rollback-"),
  );
  tempDirs.push(dir);
  return dir;
}

test.each([
  ["snapshot write", "a-pending"],
  ["walk", "a-pending"],
  ["walk", "zz-pending"],
] as const)(
  "rollback repairs the index and flushes after a failed %s with %s staged",
  async (failure, pending) => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);
    const unreadableDir = path.join(dir, "tool-output", "z-unreadable");
    await fs.promises.mkdir(unreadableDir, { recursive: true });
    await fs.promises.writeFile(path.join(unreadableDir, "output"), "original");
    const checkpoint = await store.commit({ message: "Original output" });
    const { oid: treeOid } = await git.readTree({
      fs,
      dir,
      oid: checkpoint.hash,
      filepath: "tool-output/z-unreadable",
    });
    const treePath = path.join(
      dir,
      ".git",
      "objects",
      treeOid.slice(0, 2),
      treeOid.slice(2),
    );
    await store.writeBlob(pending, new Uint8Array([1]));
    await git.add({ fs, dir, filepath: `tool-output/${pending}` });

    const indexPath = path.join(dir, ".git", "index");
    const recoveryError = new Error(`injected ${failure} failure`);
    const flushError = new Error("injected recovery flush failure");
    let armed = false;
    let injected = false;
    let writeFaults = failure === "snapshot write" ? 1 : 0;
    let flushes = 0;
    const runtime = normalizeRuntime({
      ...createNodeIsogitRuntime(),
      fs: {
        promises: {
          ...fs.promises,
          async writeFile(...args: Parameters<typeof fs.promises.writeFile>) {
            if (armed && writeFaults > 0 && args[0] === indexPath) {
              writeFaults -= 1;
              injected = true;
              throw recoveryError;
            }
            await fs.promises.writeFile(...args);
          },
          async readFile(...args: Parameters<typeof fs.promises.readFile>) {
            if (armed && failure === "walk" && args[0] === treePath) {
              injected = true;
              throw recoveryError;
            }
            return fs.promises.readFile(...args);
          },
        },
      },
      async flush() {
        flushes += 1;
        throw flushError;
      },
    });

    await withRepoDirLock(runtime, dir, async () => {
      const rollback = await createIndexRollback(runtime, dir, ["tool-output"]);
      // Stage as a checkpoint would, so the rollback has an index to restore.
      await store.writeBlob("b-checkpoint", new Uint8Array([2]));
      await git.add({ fs, dir, filepath: "tool-output/b-checkpoint" });
      armed = true;
      const recoveryErrors = {
        "snapshot write": [recoveryError],
        walk: [expect.objectContaining({ code: "NotFoundError" })],
      }[failure];
      await expect(
        rollback(new Error("injected checkpoint failure")),
      ).rejects.toMatchObject({
        errors: [...recoveryErrors, { errors: [flushError] }],
      });
    });
    expect(injected).toBe(true);
    expect(flushes).toBe(1);
    expect(await git.listFiles({ fs, dir })).toEqual([
      ".gitignore",
      "tool-output/z-unreadable/output",
    ]);
    expect(await store.readBlob(pending)).toEqual(new Uint8Array([1]));
  },
);

test("rollback leaves the index unchanged when an uncertain ref write cannot be confirmed", async () => {
  const dir = await tempDir();
  const store = await createIsogitStore(dir);
  await store.writeBlob("committed", new Uint8Array([1]));
  await store.commit({ message: "Committed output" });
  await store.writeBlob("pending", new Uint8Array([2]));
  await git.add({ fs, dir, filepath: "tool-output/pending" });
  const indexPath = path.join(dir, ".git", "index");
  const indexBefore = await fs.promises.readFile(indexPath);

  const refPath = path.join(dir, ".git", "refs", "heads", "main");
  let armed = false;
  const runtime = normalizeRuntime({
    ...createNodeIsogitRuntime(),
    fs: {
      promises: {
        ...fs.promises,
        async readFile(...args: Parameters<typeof fs.promises.readFile>) {
          if (armed && args[0] === refPath) {
            throw Object.assign(new Error("injected ref read failure"), {
              code: "EIO",
            });
          }
          return fs.promises.readFile(...args);
        },
      },
    },
  });

  await withRepoDirLock(runtime, dir, async () => {
    const rollback = await createIndexRollback(runtime, dir, ["tool-output"]);
    armed = true;
    const failure = new UncertainRefPublicationError(
      "refs/heads/main",
      "1".repeat(40),
      new Error("injected ref flush failure"),
    );
    await expect(rollback(failure)).rejects.toMatchObject({
      errors: [expect.objectContaining({ code: "EIO" })],
    });
  });
  expect(await fs.promises.readFile(indexPath)).toEqual(indexBefore);
});

test("rollback leaves the index unchanged when HEAD is neither outcome of an uncertain ref write", async () => {
  const dir = await tempDir();
  const store = await createIsogitStore(dir);
  const initial = await git.resolveRef({ fs, dir, ref: "HEAD" });
  await store.writeBlob("committed", new Uint8Array([1]));
  await store.commit({ message: "Committed output" });
  await store.writeBlob("pending", new Uint8Array([2]));
  await git.add({ fs, dir, filepath: "tool-output/pending" });
  const indexPath = path.join(dir, ".git", "index");
  const indexBefore = await fs.promises.readFile(indexPath);

  const runtime = normalizeRuntime(createNodeIsogitRuntime());
  await withRepoDirLock(runtime, dir, async () => {
    const rollback = await createIndexRollback(runtime, dir, ["tool-output"]);
    await git.writeRef({
      fs,
      dir,
      ref: "refs/heads/main",
      value: initial,
      force: true,
    });
    const failure = new UncertainRefPublicationError(
      "refs/heads/main",
      "1".repeat(40),
      new Error("injected ref flush failure"),
    );
    await expect(rollback(failure)).rejects.toMatchObject({
      errors: [
        expect.objectContaining({
          message: expect.stringContaining("Could not confirm HEAD"),
        }),
      ],
    });
  });
  expect(await fs.promises.readFile(indexPath)).toEqual(indexBefore);
});

test("rollback leaves the index unchanged when an unborn branch's ref cannot be read after an uncertain write", async () => {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });
  await fs.promises.mkdir(path.join(dir, "tool-output"));
  await fs.promises.writeFile(path.join(dir, "tool-output", "spill"), "spill");
  await git.add({ fs, dir, filepath: "tool-output/spill" });
  const indexPath = path.join(dir, ".git", "index");
  const indexBefore = await fs.promises.readFile(indexPath);

  const refPath = path.join(dir, ".git", "refs", "heads", "main");
  let armed = false;
  const runtime = normalizeRuntime({
    ...createNodeIsogitRuntime(),
    fs: {
      promises: {
        ...fs.promises,
        async readFile(...args: Parameters<typeof fs.promises.readFile>) {
          if (armed && args[0] === refPath) {
            throw Object.assign(new Error("injected ref read failure"), {
              code: "EIO",
            });
          }
          return fs.promises.readFile(...args);
        },
      },
    },
  });

  await withRepoDirLock(runtime, dir, async () => {
    const rollback = await createIndexRollback(runtime, dir, ["tool-output"]);
    const published = await git.commit({
      fs,
      dir,
      message: "Published checkpoint",
      author: { name: "test", email: "test@example.com" },
    });
    armed = true;
    const failure = new UncertainRefPublicationError(
      "refs/heads/main",
      published,
      new Error("injected ref flush failure"),
    );
    await expect(rollback(failure)).rejects.toMatchObject({
      errors: [expect.objectContaining({ code: "EIO" })],
    });
  });
  expect(await fs.promises.readFile(indexPath)).toEqual(indexBefore);
});

test("rollback resets a nested checkpoint path and nothing beside it", async () => {
  const dir = await tempDir();
  await createIsogitStore(dir);
  const nestedDir = path.join(dir, "state", "checkpoint");
  await fs.promises.mkdir(nestedDir, { recursive: true });
  await fs.promises.writeFile(path.join(nestedDir, "committed"), "committed");
  await git.add({ fs, dir, filepath: "state/checkpoint/committed" });
  await git.commit({
    fs,
    dir,
    message: "Committed state",
    author: { name: "test", email: "test@example.com" },
  });
  await fs.promises.writeFile(path.join(nestedDir, "pending"), "pending");
  await fs.promises.writeFile(path.join(dir, "state", "beside"), "beside");
  await git.add({
    fs,
    dir,
    filepath: ["state/checkpoint/pending", "state/beside"],
  });

  const runtime = normalizeRuntime(createNodeIsogitRuntime());
  await withRepoDirLock(runtime, dir, async () => {
    const rollback = await createIndexRollback(runtime, dir, [
      "state/checkpoint",
    ]);
    await rollback(new Error("injected checkpoint failure"));
  });
  expect(await git.listFiles({ fs, dir })).toEqual([
    ".gitignore",
    "state/beside",
    "state/checkpoint/committed",
  ]);
});
