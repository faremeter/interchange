import { afterEach, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { RefNotDurableError, writeRefDurably } from "./commit-helpers";
import { initRepo } from "./init";
import { createDurableObjectWrites } from "./loose-objects";
import { initAgentRepo } from "./node";
import { createNodeIsogitRuntime } from "./node-runtime";
import { normalizeRuntime } from "./runtime";
import {
  holdSyncRecord,
  SYNC_RECORDS_MAX,
  syncObjectsAndRefs,
  syncRefs,
} from "./sync-record";
import { recordingRuntime, treeOf } from "./sync.test-helpers";

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

test("the first sync of a repository in a process syncs every object directory, then every ref", async () => {
  const dir = await tempDir();
  await initAgentRepo(dir);
  await writeRefDurably(
    normalizeRuntime(createNodeIsogitRuntime()),
    dir,
    "refs/heads/events",
    "e".repeat(40),
  );
  const { runtime, steps } = recordingRuntime(dir);

  await syncObjectsAndRefs(runtime, dir);

  const objects = path.join(".git", "objects");
  const refs = await treeOf(dir, path.join(dir, ".git", "refs"));
  expect(refs).toContain(path.join(".git", "refs", "heads", "events"));
  const lastObjectSync = steps.lastIndexOf(`sync ${objects}`);
  expect(steps.slice(0, lastObjectSync)).toContain(
    `sync ${path.join(objects, "pack")}`,
  );
  expect(steps.slice(lastObjectSync + 1).sort()).toEqual(
    refs.map((entry) => `sync ${entry}`).sort(),
  );

  steps.length = 0;
  await syncObjectsAndRefs(runtime, dir);
  expect(steps).toEqual([]);
});

test("a ref write that lands but cannot sync leaves the next sync to sync every ref, and no object directory", async () => {
  const dir = await tempDir();
  await initAgentRepo(dir);
  const { runtime, steps, failing } = recordingRuntime(dir);
  await syncObjectsAndRefs(runtime, dir);
  const heads = path.join(".git", "refs", "heads");

  failing.sync = (p) => p === path.join(dir, heads);
  await expect(
    writeRefDurably(runtime, dir, "refs/heads/events", "e".repeat(40)),
  ).rejects.toBeInstanceOf(RefNotDurableError);
  failing.sync = () => false;

  steps.length = 0;
  await syncObjectsAndRefs(runtime, dir);
  expect(steps).toContain(`sync ${path.join(heads, "events")}`);
  expect(steps).toContain(`sync ${heads}`);
  expect(steps).not.toContain(`sync ${path.join(".git", "objects", "pack")}`);
});

test("a ref sync syncs every ref the first time and after a failed ref write, and nothing else", async () => {
  const dir = await tempDir();
  await initAgentRepo(dir);
  const { runtime, steps, failing } = recordingRuntime(dir);
  const refs = await treeOf(dir, path.join(dir, ".git", "refs"));

  await syncRefs(runtime, dir);
  expect(steps.sort()).toEqual(refs.map((entry) => `sync ${entry}`).sort());

  steps.length = 0;
  await syncRefs(runtime, dir);
  expect(steps).toEqual([]);

  const heads = path.join(".git", "refs", "heads");
  failing.sync = (p) => p === path.join(dir, heads);
  await expect(
    writeRefDurably(runtime, dir, "refs/heads/events", "e".repeat(40)),
  ).rejects.toBeInstanceOf(RefNotDurableError);
  failing.sync = () => false;

  steps.length = 0;
  await syncRefs(runtime, dir);
  expect(steps).toContain(`sync ${path.join(heads, "events")}`);
  expect(steps).toContain(`sync ${heads}`);
  expect(
    steps.every((step) => step.startsWith(`sync ${path.join(".git", "refs")}`)),
  ).toBe(true);

  // syncRefs synced no object directory, so syncObjectsAndRefs lists them.
  steps.length = 0;
  await syncObjectsAndRefs(runtime, dir);
  expect(steps).toContain(`sync ${path.join(".git", "objects", "pack")}`);
  expect(steps).not.toContain(`sync ${heads}`);
});

test("after a failed sync, or once the repository is built again, the next sync lists the object directories again", async () => {
  const dir = await tempDir();
  await initAgentRepo(dir);
  const { runtime, steps } = recordingRuntime(dir);
  await syncObjectsAndRefs(runtime, dir);
  const packSync = `sync ${path.join(".git", "objects", "pack")}`;
  const dropWrite = async (text: string) => {
    const durable = createDurableObjectWrites(runtime, dir);
    return git.writeBlob({
      fs: durable.fs,
      dir,
      blob: new TextEncoder().encode(text),
    });
  };
  const objects = path.join(dir, ".git", "objects");

  const dropped = await dropWrite("its commit failed validation");
  await fs.promises.rm(path.join(objects, dropped.slice(0, 2)), {
    recursive: true,
  });
  await expect(syncObjectsAndRefs(runtime, dir)).rejects.toThrow(/ENOENT/);
  steps.length = 0;
  await syncObjectsAndRefs(runtime, dir);
  expect(steps).toContain(packSync);

  await dropWrite("dropped before the repository was removed");
  await fs.promises.rm(path.join(dir, ".git"), { recursive: true });
  await initRepo(runtime, dir);
  steps.length = 0;
  await syncObjectsAndRefs(runtime, dir);
  expect(steps).toContain(packSync);
});

// A runtime that records the paths it syncs without syncing them, and
// `count` repositories that hold only empty `objects/` and `refs/`.
async function manyRepos(count: number) {
  const root = await tempDir();
  const synced: string[] = [];
  const runtime = normalizeRuntime({
    ...createNodeIsogitRuntime(),
    sync: async (p) => {
      synced.push(p);
    },
  });
  const repos = Array.from({ length: count }, (_, i) =>
    path.join(root, `r${String(i)}`),
  );
  for (const repo of repos) {
    for (const sub of ["objects", "refs"]) {
      await fs.promises.mkdir(path.join(repo, ".git", sub), {
        recursive: true,
      });
    }
  }
  const fullSync = (repo: string) =>
    ["objects", "refs"].map((sub) => path.join(repo, ".git", sub));
  return { runtime, synced, repos, fullSync };
}

test("past the cap the record drops the repository used least recently, and never one a caller holds", async () => {
  const { runtime, synced, repos, fullSync } = await manyRepos(
    SYNC_RECORDS_MAX + 1,
  );
  const [held, used, oldest] = repos;
  if (held === undefined || used === undefined || oldest === undefined) {
    throw new Error("expected at least 3 repos");
  }
  await syncObjectsAndRefs(runtime, held);
  const release = holdSyncRecord(runtime, held);
  try {
    await syncObjectsAndRefs(runtime, used);
    await syncObjectsAndRefs(runtime, oldest);
    // Used again, so it is no longer the one used least recently.
    await syncObjectsAndRefs(runtime, used);
    for (const repo of repos.slice(3)) await syncObjectsAndRefs(runtime, repo);

    synced.length = 0;
    await syncObjectsAndRefs(runtime, held);
    await syncObjectsAndRefs(runtime, used);
    expect(synced).toEqual([]);
    await syncObjectsAndRefs(runtime, oldest);
    expect(synced).toEqual(fullSync(oldest));
  } finally {
    release();
  }
});

test("once holds that kept the record past the cap are released, it comes back down to the cap", async () => {
  const { runtime, synced, repos, fullSync } = await manyRepos(
    SYNC_RECORDS_MAX + 2,
  );
  const releases: (() => void)[] = [];
  for (const repo of repos.slice(0, SYNC_RECORDS_MAX + 1)) {
    await syncObjectsAndRefs(runtime, repo);
    releases.push(holdSyncRecord(runtime, repo));
  }
  for (const release of releases) release();
  const [, second] = repos;
  const last = repos.at(-1);
  if (second === undefined || last === undefined)
    throw new Error("expected at least 2 repos");

  await syncObjectsAndRefs(runtime, last);
  synced.length = 0;
  await syncObjectsAndRefs(runtime, second);
  expect(synced).toEqual(fullSync(second));
});
