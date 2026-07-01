// Adversarial correctness tests for writeTreeDelta / assembleTree. The
// oracle here is isomorphic-git's INDEX-based writeTree (git.add +
// git.commit), which is genuinely independent of assembleTree — unlike
// write-tree-delta.test.ts, whose full-replace comparison also runs
// through assembleTree, so a shared bug would pass both sides.

import { test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { createRepoStore } from "./store";
import type { AuthorizeFn, Principal, RepoId } from "./types";

const tempDirs: string[] = [];
let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises
      .rm(d, { recursive: true, force: true })
      .catch(() => undefined);
  }
});

const allowAll: AuthorizeFn = () => ({ allowed: true });
const principal: Principal = { kind: "test" };
const REF = "refs/heads/events";
const repoId: RepoId = { kind: "agent-state", id: "subject" };

function makeStore(dataDir: string) {
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: {
      "agent-state": {
        kind: "agent-state",
        directoryPrefix: "repos-under-test",
        validatePush: () => ({ ok: true }),
        onRefUpdated: () => undefined,
      },
    },
    authorize: allowAll,
  });
}

async function freshStore(label: string) {
  const dataDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), label));
  tempDirs.push(dataDir);
  return makeStore(dataDir);
}

async function committedTreeOid(
  store: ReturnType<typeof makeStore>,
): Promise<string> {
  const dir = store.getRepoDir(repoId);
  const sha = await git.resolveRef({ fs, dir, ref: REF });
  const { commit } = await git.readCommit({ fs, dir, oid: sha });
  return commit.tree;
}

// Independent oracle: materialize `files` in a scratch repo, stage each
// via git.add, and let isomorphic-git's index build the tree. This does
// not touch assembleTree at all.
async function canonicalTreeOid(
  label: string,
  files: Record<string, string>,
): Promise<string> {
  const dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), label));
  tempDirs.push(dir);
  await git.init({ fs, dir, defaultBranch: "main" });
  for (const [rel, content] of Object.entries(files)) {
    const full = path.join(dir, rel);
    await fs.promises.mkdir(path.dirname(full), { recursive: true });
    await fs.promises.writeFile(full, content);
    await git.add({ fs, dir, filepath: rel });
  }
  const sha = await git.commit({
    fs,
    dir,
    message: "oracle",
    author: { name: "o", email: "o@o" },
  });
  const { commit } = await git.readCommit({ fs, dir, oid: sha });
  return commit.tree;
}

async function readTreePaths(
  store: ReturnType<typeof makeStore>,
): Promise<Set<string>> {
  const dir = store.getRepoDir(repoId);
  const sha = await git.resolveRef({ fs, dir, ref: REF });
  const { commit } = await git.readCommit({ fs, dir, oid: sha });
  const out = new Set<string>();
  const walk = async (oid: string, prefix: string): Promise<void> => {
    const { tree } = await git.readTree({ fs, dir, oid });
    for (const e of tree) {
      const p = prefix === "" ? e.path : `${prefix}/${e.path}`;
      if (e.type === "tree") await walk(e.oid, p);
      else out.add(p);
    }
  };
  await walk(commit.tree, "");
  return out;
}

async function seed(
  store: ReturnType<typeof makeStore>,
  base: Record<string, string>,
) {
  await store.initRepo(repoId);
  await store.writeTree(principal, repoId, REF, {
    files: base,
    message: "seed",
  });
}

test("dequeue-shaped delta removes inbox entry and adds processing, OID == canonical git", async () => {
  const A = "addresses/a/";
  const base = {
    [`${A}inbox/10-m3.json`]: `{"messageId":"m3"}`,
    [`${A}consumed/m1.json`]: `{"receivedAt":1}`,
    [`${A}consumed/m2.json`]: `{"receivedAt":2}`,
    [`${A}watermark.json`]: `{"watermark":0}`,
  };
  const store = await freshStore("adv-dequeue-");
  await seed(store, base);
  await store.writeTreeDelta(principal, repoId, REF, {
    changedPathPrefixes: new Set([A]),
    message: "dequeue",
    computeDelta: async () => ({
      puts: { [`${A}processing/10-m3.json`]: `{"messageId":"m3"}` },
      deletes: [`${A}inbox/10-m3.json`],
    }),
  });
  const paths = await readTreePaths(store);
  expect(paths.has(`${A}inbox/10-m3.json`)).toBe(false); // removed, not carried
  expect(paths.has(`${A}processing/10-m3.json`)).toBe(true);
  expect(paths.has(`${A}consumed/m1.json`)).toBe(true);
  expect(paths.has(`${A}consumed/m2.json`)).toBe(true);

  const expected = {
    [`${A}processing/10-m3.json`]: `{"messageId":"m3"}`,
    [`${A}consumed/m1.json`]: `{"receivedAt":1}`,
    [`${A}consumed/m2.json`]: `{"receivedAt":2}`,
    [`${A}watermark.json`]: `{"watermark":0}`,
  };
  expect(await committedTreeOid(store)).toBe(
    await canonicalTreeOid("adv-dequeue-oracle-", expected),
  );
});

test("cascade delete empties processing/ and the whole address subtree, OID == canonical git", async () => {
  const A = "addresses/lonely/";
  const other = "addresses/keep/inbox/9-k1.json";
  const base = {
    [`${A}processing/10-m3.json`]: `{"messageId":"m3"}`,
    [other]: `{"messageId":"k1"}`,
  };
  const store = await freshStore("adv-cascade-");
  await seed(store, base);
  await store.writeTreeDelta(principal, repoId, REF, {
    changedPathPrefixes: new Set([A]),
    message: "prune-processing",
    computeDelta: async () => ({
      puts: {},
      deletes: [`${A}processing/10-m3.json`],
    }),
  });
  const paths = await readTreePaths(store);
  expect([...paths].some((p) => p.startsWith(A))).toBe(false);
  expect(paths.has(other)).toBe(true);

  const expected = { [other]: `{"messageId":"k1"}` };
  expect(await committedTreeOid(store)).toBe(
    await canonicalTreeOid("adv-cascade-oracle-", expected),
  );
});

test("put and delete at same exact path is rejected (delta_ambiguous)", async () => {
  const A = "addresses/a/";
  const base = {
    [`${A}inbox/10-m3.json`]: `{"old":true}`,
    [`${A}consumed/m1.json`]: `{"receivedAt":1}`,
  };
  const store = await freshStore("adv-h2a-");
  await seed(store, base);
  await expect(
    store.writeTreeDelta(principal, repoId, REF, {
      changedPathPrefixes: new Set([A]),
      message: "collide",
      computeDelta: async () => ({
        puts: { [`${A}inbox/10-m3.json`]: `{"new":true}` },
        deletes: [`${A}inbox/10-m3.json`],
      }),
    }),
  ).rejects.toThrow(/delta_ambiguous/);
});

test("put under a deleted subtree is rejected (delta_ambiguous)", async () => {
  const A = "addresses/a/";
  const base = {
    [`${A}inbox/10-m3.json`]: `{"old":true}`,
    [`${A}inbox/11-m4.json`]: `{"sibling":true}`,
    [`${A}consumed/m1.json`]: `{"receivedAt":1}`,
  };
  const store = await freshStore("adv-h2b-");
  await seed(store, base);
  await expect(
    store.writeTreeDelta(principal, repoId, REF, {
      changedPathPrefixes: new Set([A]),
      message: "delete-subtree-with-put",
      computeDelta: async () => ({
        puts: { [`${A}inbox/99-m9.json`]: `{"new":true}` },
        deletes: [`${A}inbox/`],
      }),
    }),
  ).rejects.toThrow(/delta_ambiguous/);
});

test("cascade prune emptying dir and parent == canonical git", async () => {
  const A = "addresses/solo/";
  const base = {
    [`${A}consumed/m1.json`]: `{"receivedAt":1}`,
    "addresses/other/consumed/m9.json": `{"receivedAt":9}`,
  };
  const store = await freshStore("adv-cascade2-");
  await seed(store, base);
  await store.writeTreeDelta(principal, repoId, REF, {
    changedPathPrefixes: new Set([A]),
    message: "prune-all",
    computeDelta: async () => ({
      puts: {},
      deletes: [`${A}consumed/m1.json`],
    }),
  });
  const expected = { "addresses/other/consumed/m9.json": `{"receivedAt":9}` };
  expect(await committedTreeOid(store)).toBe(
    await canonicalTreeOid("adv-cascade2-oracle-", expected),
  );
});

test("no-slash delete of a directory is rejected (delete_type_mismatch)", async () => {
  const base = {
    "foo/a.json": `{"in":"dir"}`,
    "keep.json": `{"keep":true}`,
  };
  const store = await freshStore("adv-del-dir-");
  await seed(store, base);
  await expect(
    store.writeTreeDelta(principal, repoId, REF, {
      changedPathPrefixes: new Set(["foo"]),
      message: "delete-dir-as-file",
      computeDelta: async () => ({
        puts: {},
        deletes: ["foo"],
      }),
    }),
  ).rejects.toThrow(/delete_type_mismatch/);
});

test("trailing-slash delete descending into a base blob is rejected (delete_type_mismatch)", async () => {
  const base = {
    foo: `{"is":"blob"}`,
    "keep.json": `{"keep":true}`,
  };
  const store = await freshStore("adv-del-blob-");
  await seed(store, base);
  await expect(
    store.writeTreeDelta(principal, repoId, REF, {
      changedPathPrefixes: new Set(["foo/bar/"]),
      message: "delete-blob-as-subtree",
      computeDelta: async () => ({
        puts: {},
        deletes: ["foo/bar/"],
      }),
    }),
  ).rejects.toThrow(/delete_type_mismatch/);
});

test("put descending into a base blob is not mislabeled a delete mismatch", async () => {
  const base = {
    foo: `{"is":"blob"}`,
    "keep.json": `{"keep":true}`,
  };
  const store = await freshStore("adv-file2dir-");
  await seed(store, base);
  const run = store.writeTreeDelta(principal, repoId, REF, {
    changedPathPrefixes: new Set(["foo/"]),
    message: "put-under-base-blob",
    computeDelta: async () => ({
      puts: { "foo/bar.json": `{"now":"dir"}` },
      deletes: [],
    }),
  });
  // The carve-out holds: a put-driven descent into a base blob is a
  // file-to-directory replacement, not a delete against the wrong base
  // type, so it must NOT raise delete_type_mismatch. The store cannot
  // complete the swap end-to-end today -- working-tree materialization
  // mkdir's over the base file and EEXISTs -- a separate, unreachable
  // limitation. The load-bearing assertion is the absence of
  // delete_type_mismatch; the EEXIST pins where the store gives out.
  await expect(run).rejects.toThrow();
  await run.catch((e: unknown) => {
    expect(String(e)).not.toContain("delete_type_mismatch");
    expect(String(e)).toContain("EEXIST");
  });
});

test("trailing-slash delete clearing a base directory still succeeds, OID == canonical git", async () => {
  const base = {
    "deploy/x.json": `{"x":1}`,
    "deploy/y.json": `{"y":2}`,
    "keep.json": `{"keep":true}`,
  };
  const store = await freshStore("adv-clear-dir-");
  await seed(store, base);
  await store.writeTreeDelta(principal, repoId, REF, {
    changedPathPrefixes: new Set(["deploy/"]),
    message: "clear-deploy",
    computeDelta: async () => ({
      puts: {},
      deletes: ["deploy/"],
    }),
  });
  const paths = await readTreePaths(store);
  expect([...paths].some((p) => p.startsWith("deploy/"))).toBe(false);
  expect(paths.has("keep.json")).toBe(true);

  const expected = { "keep.json": `{"keep":true}` };
  expect(await committedTreeOid(store)).toBe(
    await canonicalTreeOid("adv-clear-dir-oracle-", expected),
  );
});
