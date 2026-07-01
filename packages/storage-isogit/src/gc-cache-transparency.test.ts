// The store threads one long-lived isomorphic-git `cache` object per repo
// through every git.* call. GC repacks the object store — consolidating
// loose objects into a fresh, differently-named pack and pruning the old
// one — behind that cache's back. These tests pin that the cache stays a
// pure accelerator across a repack: an object that migrates from a pruned
// pack into the new one still reads correctly under the SAME warm cache
// (because reads enumerate .idx files from disk, so a stranded parse of a
// pruned pack is never consulted), and an object GC genuinely drops
// surfaces NotFound rather than stale bytes.

import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { initAgentRepo } from "./init";
import { runGC } from "./gc";

const author = { name: "Test", email: "test@test.dev" };
const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), "gc-cache-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

function listIdx(dir: string): string[] {
  const p = path.join(dir, ".git", "objects", "pack");
  if (!fs.existsSync(p)) return [];
  return fs.readdirSync(p).filter((x) => x.endsWith(".idx"));
}

function countLoose(dir: string): number {
  const base = path.join(dir, ".git", "objects");
  if (!fs.existsSync(base)) return 0;
  let n = 0;
  for (const d of fs.readdirSync(base)) {
    if (!/^[0-9a-f]{2}$/.test(d)) continue;
    n += fs.readdirSync(path.join(base, d)).length;
  }
  return n;
}

async function commitFile(
  dir: string,
  name: string,
  content: string,
): Promise<string> {
  const full = path.join(dir, name);
  await fs.promises.mkdir(path.dirname(full), { recursive: true });
  await fs.promises.writeFile(full, content);
  await git.add({ fs, dir, filepath: name });
  return git.commit({ fs, dir, message: `add ${name}`, author });
}

async function commitFileOnRef(
  dir: string,
  branch: string,
  name: string,
  content: string,
): Promise<string> {
  await git.checkout({ fs, dir, ref: branch });
  const oid = await commitFile(dir, name, content);
  await git.checkout({ fs, dir, ref: "main" });
  return oid;
}

function packfileCacheKeys(cache: object): string[] {
  const packSym = Object.getOwnPropertySymbols(cache).find(
    (s) => s.toString() === "Symbol(PackfileCache)",
  );
  if (packSym === undefined) return [];
  const map: unknown = Reflect.get(cache, packSym);
  if (!(map instanceof Map)) return [];
  const keys: string[] = [];
  for (const k of map.keys()) {
    if (typeof k === "string") keys.push(k);
  }
  return keys;
}

describe("shared-cache repack transparency", () => {
  test("object in a pre-GC pack reads correctly after GC prunes that pack, under the SAME warm cache", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);

    const targetContent = "the-target-blob-payload-v1";
    let tip = await commitFile(dir, "state/target.txt", targetContent);
    const targetOid = await git.writeBlob({
      fs,
      dir,
      blob: Buffer.from(targetContent),
    });
    for (let i = 0; i < 5; i++) {
      tip = await commitFile(dir, `state/f${i}.txt`, `payload ${i}`);
    }

    // First GC consolidates all loose objects into pack #1 and prunes the
    // loose copies. The target now lives only in pack #1.
    await runGC(dir, { retention: "keep-history" });
    const idxBefore = listIdx(dir);
    const firstIdx = idxBefore[0];
    if (firstIdx === undefined) throw new Error("expected a pack .idx");
    expect(countLoose(dir)).toBe(0);

    // One shared, long-lived cache object — the exact shape the store
    // threads through every git.* call.
    const cache: object = {};

    // Warm the cache: read the target from the pre-GC pack, populating
    // isomorphic-git's PackfileCache keyed by the pre-GC .idx filename.
    const warm = await git.readBlob({ fs, dir, oid: targetOid, cache });
    expect(Buffer.from(warm.blob).toString()).toBe(targetContent);
    expect(packfileCacheKeys(cache).some((k) => k.includes(firstIdx))).toBe(
      true,
    );

    // More commits, then a second GC: repacks into a new pack #2 that
    // supersedes and prunes pack #1. The target migrates to pack #2.
    for (let i = 0; i < 4; i++) {
      tip = await commitFile(dir, `state/g${i}.txt`, `more ${i}`);
    }
    await runGC(dir, { retention: "keep-history" });

    const idxAfter = listIdx(dir);
    // The pre-GC pack is gone from disk, but its parsed index is still
    // stranded in the warm cache map.
    expect(idxAfter).not.toContain(firstIdx);
    expect(packfileCacheKeys(cache).some((k) => k.includes(firstIdx))).toBe(
      true,
    );

    // The test: read the target again under the SAME warm cache, after the
    // pack the cache indexed was pruned from disk.
    const reread = await git.readBlob({ fs, dir, oid: targetOid, cache });
    expect(Buffer.from(reread.blob).toString()).toBe(targetContent);

    // Exercise the readTree/readCommit path through the same cache too.
    const co = await git.readCommit({ fs, dir, oid: tip, cache });
    const tr = await git.readTree({ fs, dir, oid: co.commit.tree, cache });
    expect(tr.tree.length).toBeGreaterThan(0);
  });

  test("GC that drops a now-unreachable object surfaces NotFound, not stale bytes, under a warm cache", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);

    for (let i = 0; i < 3; i++) {
      await commitFile(dir, `state/f${i}.txt`, `keep ${i}`);
    }

    // A blob reachable only from a secondary ref.
    const orphanContent = "orphan-only-in-old-pack";
    const orphanOid = await git.writeBlob({
      fs,
      dir,
      blob: Buffer.from(orphanContent),
    });
    await git.branch({ fs, dir, ref: "scratch" });
    await commitFileOnRef(dir, "scratch", "state/orphan.txt", orphanContent);

    // Pack everything reachable into pack #1.
    await runGC(dir, { retention: "keep-history" });
    expect(countLoose(dir)).toBe(0);

    const cache: object = {};
    const warm = await git.readBlob({ fs, dir, oid: orphanOid, cache });
    expect(Buffer.from(warm.blob).toString()).toBe(orphanContent);

    // Drop the scratch ref so the orphan is unreachable, then GC.
    await git.deleteBranch({ fs, dir, ref: "scratch" });
    await commitFile(dir, "state/f9.txt", "keep 9");
    await runGC(dir, { retention: "keep-history" });

    // The object is genuinely gone — a NotFound throw, never stale bytes.
    let threw = false;
    try {
      const r = await git.readBlob({ fs, dir, oid: orphanOid, cache });
      expect(Buffer.from(r.blob).toString()).not.toBe(orphanContent);
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  });
});
