import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { createDeployPack, createNegotiatedPack } from "./pack-send";
import { collectReachableObjects } from "./object-walk";
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

const AUTHOR = { name: "Test", email: "test@test.dev" };

async function writeAndCommit(
  dir: string,
  files: { filepath: string; content: string }[],
  message: string,
): Promise<string> {
  for (const { filepath, content } of files) {
    const fullPath = path.join(dir, filepath);
    await fs.promises.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.promises.writeFile(fullPath, content);
    await git.add({ fs, dir, filepath });
  }
  return git.commit({ fs, dir, message, author: AUTHOR });
}

async function makeLinearRepo(): Promise<{
  dir: string;
  c1: string;
  c2: string;
  c3: string;
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });

  const c1 = await writeAndCommit(
    dir,
    [{ filepath: "a.txt", content: "a-v1" }],
    "first",
  );
  const c2 = await writeAndCommit(
    dir,
    [{ filepath: "a.txt", content: "a-v2" }],
    "second",
  );
  const c3 = await writeAndCommit(
    dir,
    [{ filepath: "a.txt", content: "a-v3" }],
    "third",
  );

  return { dir, c1, c2, c3 };
}

async function makeForkedRepo(): Promise<{
  dir: string;
  base: string;
  branchA: string;
  branchB: string;
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });

  const base = await writeAndCommit(
    dir,
    [{ filepath: "base.txt", content: "base" }],
    "base",
  );

  const branchA = await writeAndCommit(
    dir,
    [{ filepath: "a.txt", content: "alpha" }],
    "branch A",
  );

  // Reset working tree by removing branch-A artifact, then commit branch B
  // on top of base. We use git.commit with explicit parent via writeCommit
  // semantics through a fresh branch.
  await git.writeRef({
    fs,
    dir,
    ref: "refs/heads/main",
    value: base,
    force: true,
  });
  await fs.promises.rm(path.join(dir, "a.txt"), { force: true });
  await git.remove({ fs, dir, filepath: "a.txt" }).catch(() => undefined);

  const branchB = await writeAndCommit(
    dir,
    [{ filepath: "b.txt", content: "bravo" }],
    "branch B",
  );

  // Restore branch A as a named ref so callers can reach it.
  await git.writeRef({
    fs,
    dir,
    ref: "refs/heads/branch-a",
    value: branchA,
    force: true,
  });
  await git.writeRef({
    fs,
    dir,
    ref: "refs/heads/branch-b",
    value: branchB,
    force: true,
  });

  return { dir, base, branchA, branchB };
}

describe("createNegotiatedPack", () => {
  test("single want, no haves: includes every reachable object", async () => {
    const { dir, c3 } = await makeLinearRepo();

    const result = await createNegotiatedPack(dir, [c3], []);
    if (result === null) throw new Error("expected a non-empty pack");

    expect(result.oids).toContain(c3);
    // Three commits, three trees, three blobs (a.txt v1/v2/v3).
    expect(result.oids.length).toBe(9);
  });

  test("single want with HEAD have: produces an empty (null) pack", async () => {
    const { dir, c3 } = await makeLinearRepo();

    const result = await createNegotiatedPack(dir, [c3], [c3]);
    expect(result).toBeNull();
  });

  test("single want with ancestor have: subtracts ancestor's objects", async () => {
    const { dir, c1, c3 } = await makeLinearRepo();

    const full = await createNegotiatedPack(dir, [c3], []);
    if (full === null) throw new Error("expected full pack");
    const negotiated = await createNegotiatedPack(dir, [c3], [c1]);
    if (negotiated === null) throw new Error("expected negotiated pack");

    expect(negotiated.oids.length).toBeLessThan(full.oids.length);
    expect(negotiated.oids).toContain(c3);
    expect(negotiated.oids).not.toContain(c1);
  });

  test("multi-want with overlapping haves: union of reachable minus union of have-reachable", async () => {
    const { dir, base, branchA, branchB } = await makeForkedRepo();

    const result = await createNegotiatedPack(dir, [branchA, branchB], [base]);
    if (result === null) throw new Error("expected a non-empty pack");

    expect(result.oids).toContain(branchA);
    expect(result.oids).toContain(branchB);
    expect(result.oids).not.toContain(base);
  });

  test("includeSha filter drops oids that the predicate rejects", async () => {
    const { dir, c3 } = await makeLinearRepo();

    const seen: string[] = [];
    const result = await createNegotiatedPack(dir, [c3], [], (oid) => {
      seen.push(oid);
      return oid !== c3;
    });
    if (result === null) throw new Error("expected pack");

    expect(seen.length).toBeGreaterThan(0);
    expect(result.oids).not.toContain(c3);
  });

  test("throws when wants is empty", async () => {
    const { dir } = await makeLinearRepo();
    await expect(createNegotiatedPack(dir, [], [])).rejects.toThrow(
      "wants must be non-empty",
    );
  });

  test("silently ignores unknown haves", async () => {
    const { dir, c3 } = await makeLinearRepo();
    const bogusHave = "0".repeat(40);

    const result = await createNegotiatedPack(dir, [c3], [bogusHave]);
    if (result === null) throw new Error("expected pack");

    expect(result.oids).toContain(c3);
  });

  test("precomputed wantedObjects matches the from-scratch path byte-for-byte", async () => {
    // Callers that pre-walk reachable-from-wants for their own
    // purposes can hand the set in via `options.wantedObjects`. The
    // result must be identical to letting `createNegotiatedPack`
    // recompute the set internally — same OIDs in the same order,
    // same pack bytes — so the optimization is invisible to
    // downstream consumers.
    const { dir, c1, c3 } = await makeLinearRepo();

    const fromScratch = await createNegotiatedPack(dir, [c3], [c1]);
    if (fromScratch === null) throw new Error("expected pack");

    // Walk reachable-from-wants ourselves to construct the
    // precomputed set the caller would hand in. We use the same
    // helper `createNegotiatedPack` uses internally to keep the
    // contract obvious: precomputed must equal what the function
    // would have computed.
    const wantedObjects = new Set<string>();
    const chainHead = await git.readCommit({ fs, dir, oid: c3 });
    const queue: string[] = [c3];
    const seen = new Set<string>();
    while (queue.length > 0) {
      const oid = queue.shift();
      if (oid === undefined) break;
      if (seen.has(oid)) continue;
      seen.add(oid);
      const objs = await collectReachableObjects(dir, oid);
      for (const o of objs) wantedObjects.add(o);
      const c = oid === c3 ? chainHead : await git.readCommit({ fs, dir, oid });
      for (const p of c.commit.parent) {
        if (!seen.has(p)) queue.push(p);
      }
    }

    const precomputed = await createNegotiatedPack(dir, [c3], [c1], undefined, {
      wantedObjects,
    });
    if (precomputed === null) throw new Error("expected pack");

    expect(precomputed.oids).toEqual(fromScratch.oids);
    expect(precomputed.pack).toEqual(fromScratch.pack);
  });
});
