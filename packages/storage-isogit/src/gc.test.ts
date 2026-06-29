import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { initAgentRepo } from "./init";
import {
  applyPack,
  receivePackObjects,
  type CommitVerifier,
  type TreeValidator,
} from "./pack-receive";
import { collectReachableObjects } from "./object-walk";
import { repoDiskUsage } from "./repo-disk";
import { runGC } from "./gc";
import { IsogitStore } from "./store";

const author = { name: "Test", email: "test@test.dev" };

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "interchange-gc-test-"),
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

async function packFor(sourceDir: string, oids: string[]): Promise<Uint8Array> {
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

/**
 * A single-commit source repo. `tipOids` is the tip-tree reachable set, the
 * same shape a deploy pack carries (commit + tree + blobs, no ancestors).
 */
async function sourceRepo(files: Record<string, string>): Promise<{
  dir: string;
  tip: string;
  tipOids: string[];
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });
  for (const [filepath, content] of Object.entries(files)) {
    const full = path.join(dir, filepath);
    await fs.promises.mkdir(path.dirname(full), { recursive: true });
    await fs.promises.writeFile(full, content);
    await git.add({ fs, dir, filepath });
  }
  const tip = await git.commit({ fs, dir, message: "deploy", author });
  return { dir, tip, tipOids: await collectReachableObjects(dir, tip) };
}

/**
 * Commit a file onto `refs/heads/main` in a target repo, producing loose
 * objects and extending the main ancestry chain.
 */
async function commitOnMain(dir: string, name: string): Promise<string> {
  const full = path.join(dir, "state", name);
  await fs.promises.mkdir(path.dirname(full), { recursive: true });
  await fs.promises.writeFile(full, `content of ${name}`);
  await git.add({ fs, dir, filepath: `state/${name}` });
  return git.commit({ fs, dir, message: `add ${name}`, author });
}

async function deployOnto(
  dir: string,
  files: Record<string, string>,
  transferId: string,
): Promise<string> {
  const source = await sourceRepo(files);
  const pack = await packFor(source.dir, source.tipOids);
  await applyPack(dir, pack, "refs/heads/deploy", source.tip, transferId);
  return source.tip;
}

describe("runGC", () => {
  test("reclaims disk from superseded packs and loose objects across both refs", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);

    let mainTip = "";
    for (const name of ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"]) {
      mainTip = await commitOnMain(dir, name);
    }

    await deployOnto(dir, { "deploy/one.txt": "first deploy" }, "deploy-1");
    await deployOnto(dir, { "deploy/two.txt": "second deploy" }, "deploy-2");
    const deployTip = await deployOnto(
      dir,
      { "deploy/three.txt": "third deploy" },
      "deploy-3",
    );

    const rejectAll: TreeValidator = () => ({ ok: false, reason: "nope" });
    const rejectedSource = await sourceRepo({ "evil.txt": "bad" });
    const rejectedPack = await packFor(
      rejectedSource.dir,
      rejectedSource.tipOids,
    );
    await expect(
      receivePackObjects(
        dir,
        rejectedPack,
        "refs/heads/main",
        rejectedSource.tip,
        "reject-1",
        mainTip,
        rejectAll,
      ),
    ).rejects.toThrow(/^path_violation/);

    const unsignedSource = await sourceRepo({ "deploy/x.txt": "unsigned" });
    const unsignedPack = await packFor(
      unsignedSource.dir,
      unsignedSource.tipOids,
    );
    const verifier: CommitVerifier = () => true;
    await expect(
      applyPack(
        dir,
        unsignedPack,
        "refs/heads/deploy",
        unsignedSource.tip,
        "reject-2",
        verifier,
      ),
    ).rejects.toThrow(/^signature_unsigned/);

    const before = repoDiskUsage(dir);
    expect(before.packCount).toBeGreaterThan(1);
    expect(before.looseObjectCount).toBeGreaterThan(0);

    const result = await runGC(dir, { retention: "keep-history" });

    expect(result.reclaimedBytes).toBeGreaterThan(0);
    expect(result.after.packCount).toBe(1);
    expect(result.after.looseObjectCount).toBe(0);

    // Both diverging heads remain readable after the consolidation.
    expect((await git.readCommit({ fs, dir, oid: mainTip })).oid).toBe(mainTip);
    expect((await git.readCommit({ fs, dir, oid: deployTip })).oid).toBe(
      deployTip,
    );
  });

  test("preserves every object reachable from both diverging refs", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);

    const mainTip = await commitOnMain(dir, "main-state.txt");
    const deployTip = await deployOnto(
      dir,
      { "deploy/prompt.txt": "deploy content" },
      "deploy-1",
    );

    const mainBefore = new Set(await collectReachableObjects(dir, mainTip));
    const deployBefore = new Set(await collectReachableObjects(dir, deployTip));

    await runGC(dir, { retention: "tip-only" });

    // The full tree-reachable closure of both diverging heads survives the
    // consolidation; walking it re-reads every commit and tree, and the
    // enumerated object set is unchanged.
    expect(new Set(await collectReachableObjects(dir, mainTip))).toEqual(
      mainBefore,
    );
    expect(new Set(await collectReachableObjects(dir, deployTip))).toEqual(
      deployBefore,
    );
  });

  test("tip-only drops history while keep-history retains it", async () => {
    async function buildChain(): Promise<{
      dir: string;
      ancestor: string;
      tip: string;
    }> {
      const dir = await tempDir();
      await initAgentRepo(dir);
      const ancestor = await commitOnMain(dir, "old.txt");
      const tip = await commitOnMain(dir, "new.txt");
      return { dir, ancestor, tip };
    }

    const shallow = await buildChain();
    await runGC(shallow.dir, { retention: "tip-only" });
    expect(
      (await git.readCommit({ fs, dir: shallow.dir, oid: shallow.tip })).oid,
    ).toBe(shallow.tip);
    await expect(
      git.readCommit({ fs, dir: shallow.dir, oid: shallow.ancestor }),
    ).rejects.toThrow(shallow.ancestor);

    const deep = await buildChain();
    await runGC(deep.dir, { retention: "keep-history" });
    expect(
      (await git.readCommit({ fs, dir: deep.dir, oid: deep.ancestor })).oid,
    ).toBe(deep.ancestor);
  });

  test("keep-history tolerates dangling parents on the deploy ref", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);

    // A two-commit source whose tip carries a parent pointer; the pack only
    // contains the tip's tree-reachable objects, so the parent is absent on
    // disk once applied — the steady state of a real deploy ref.
    const source = await tempDir();
    await git.init({ fs, dir: source, defaultBranch: "main" });
    await fs.promises.writeFile(path.join(source, "a.txt"), "a");
    await git.add({ fs, dir: source, filepath: "a.txt" });
    await git.commit({ fs, dir: source, message: "first", author });
    await fs.promises.writeFile(path.join(source, "b.txt"), "b");
    await git.add({ fs, dir: source, filepath: "b.txt" });
    const deployTip = await git.commit({
      fs,
      dir: source,
      message: "second",
      author,
    });
    const tipOnly = await collectReachableObjects(source, deployTip);
    const pack = await packFor(source, tipOnly);
    await applyPack(dir, pack, "refs/heads/deploy", deployTip, "deploy-1");

    // The tip's parent is unresolvable; keep-history must not abort on it.
    const result = await runGC(dir, { retention: "keep-history" });
    expect(result.after.packCount).toBe(1);
    expect((await git.readCommit({ fs, dir, oid: deployTip })).oid).toBe(
      deployTip,
    );
  });

  test("serializes concurrent commits against GC without corruption", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);
    const store = new IsogitStore(dir);

    // Fire context commits and GC passes against the same repo concurrently.
    // Every commit and every GC acquires the per-directory lock, so they run
    // one-at-a-time and the object store is never observed mid-mutation.
    const ops: Promise<unknown>[] = [];
    for (let i = 0; i < 12; i += 1) {
      await fs.promises.writeFile(
        path.join(dir, "turns.jsonl"),
        `${JSON.stringify({ role: "user", content: [], timestamp: i })}\n`,
      );
      ops.push(store.commit({ message: `cycle ${i.toString()}` }));
      ops.push(runGC(dir, { retention: "tip-only" }));
    }
    await Promise.all(ops);

    // The repo is intact: HEAD resolves and walking its entire tree-reachable
    // closure re-reads every commit and tree without a missing object.
    const head = await git.resolveRef({ fs, dir, ref: "refs/heads/main" });
    const reachable = await collectReachableObjects(dir, head);
    expect(reachable.length).toBeGreaterThan(0);
  });
});
