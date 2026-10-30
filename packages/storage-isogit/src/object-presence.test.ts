import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { createObjectPresence } from "./node";

const tempDirs: string[] = [];

async function tempRepo(): Promise<string> {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "object-presence-"));
  tempDirs.push(dir);
  await git.init({ fs, dir, defaultBranch: "main" });
  return dir;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

async function writeBlobs(dir: string, prefix: string, count: number) {
  const oids: string[] = [];
  for (let i = 0; i < count; i++) {
    oids.push(
      await git.writeBlob({
        fs,
        dir,
        blob: new TextEncoder().encode(`${prefix} ${String(i)}`),
      }),
    );
  }
  return oids;
}

// Pack blobs written in a scratch repo into `dir`, so `dir` holds them
// only in a pack and never loose.
async function packInto(dir: string, name: string, prefix: string) {
  const scratch = await tempRepo();
  const oids = await writeBlobs(scratch, prefix, 64);
  const { packfile } = await git.packObjects({ fs, dir: scratch, oids });
  if (packfile === undefined) throw new Error("packObjects returned no pack");
  const filepath = path.join(".git", "objects", "pack", `pack-${name}.pack`);
  await fsp.mkdir(path.join(dir, ".git", "objects", "pack"), {
    recursive: true,
  });
  await fsp.writeFile(path.join(dir, filepath), packfile);
  await git.indexPack({ fs, dir, filepath });
  return oids;
}

describe("createObjectPresence", () => {
  test("finds objects in every published pack and no others", async () => {
    const dir = await tempRepo();
    const first = await packInto(dir, "a", "first");
    const second = await packInto(dir, "b", "second");
    const scratch = await tempRepo();
    const absent = await writeBlobs(scratch, "absent", 64);

    const presence = createObjectPresence(dir);
    await presence.update();
    for (const oid of [...first, ...second]) {
      expect(await presence.holds(oid)).toBe(true);
    }
    for (const oid of absent) {
      expect(await presence.holds(oid)).toBe(false);
    }
  });

  test("an update reads a pack published since the one before", async () => {
    const dir = await tempRepo();
    const first = await packInto(dir, "a", "first");
    const presence = createObjectPresence(dir);
    await presence.update();
    const second = await packInto(dir, "b", "second");

    for (const oid of second) expect(await presence.holds(oid)).toBe(false);
    await presence.update();
    for (const oid of [...first, ...second]) {
      expect(await presence.holds(oid)).toBe(true);
    }
  });

  test("an update forgets the objects of a pack that is gone", async () => {
    const dir = await tempRepo();
    const first = await packInto(dir, "a", "first");
    const second = await packInto(dir, "b", "second");
    const presence = createObjectPresence(dir);
    await presence.update();
    for (const ext of ["pack", "idx"]) {
      await fsp.rm(path.join(dir, ".git", "objects", "pack", `pack-a.${ext}`));
    }

    await presence.update();
    for (const oid of first) expect(await presence.holds(oid)).toBe(false);
    for (const oid of second) expect(await presence.holds(oid)).toBe(true);
  });

  test("finds a loose object in a repository with no packs", async () => {
    const dir = await tempRepo();
    const [loose] = await writeBlobs(dir, "loose", 1);
    if (loose === undefined) throw new Error("no blob written");

    const presence = createObjectPresence(dir);
    await presence.update();
    expect(await presence.holds(loose)).toBe(true);
    expect(await presence.holds("0".repeat(40))).toBe(false);
  });

  test("rejects an object id that is not 40 hex digits", async () => {
    const presence = createObjectPresence(await tempRepo());
    await expect(presence.holds("abc")).rejects.toThrow(/not 40 hex digits/);
  });
});
