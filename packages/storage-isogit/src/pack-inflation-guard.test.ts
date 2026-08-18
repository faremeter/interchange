import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { collectReachableObjects } from "./node";
import { assertPackInflationWithinBounds } from "./pack-inflation-guard";
import { DEFAULT_PACK_MATERIALIZATION_LIMITS } from "./materialization-limits";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fsp.mkdtemp(path.join(os.tmpdir(), "pack-guard-test-"));
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

// Pack a one-commit repo whose single file has the given content.
async function packWithFile(content: string): Promise<Uint8Array> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });
  await fsp.writeFile(path.join(dir, "file.bin"), content);
  await git.add({ fs, dir, filepath: "file.bin" });
  const commitSha = await git.commit({
    fs,
    dir,
    message: "t",
    author: { name: "t", email: "t@t.dev" },
  });
  const oids = await collectReachableObjects(dir, commitSha);
  const { packfile } = await git.packObjects({ fs, dir, oids });
  if (packfile === undefined) {
    throw new Error("pack-guard test: packObjects returned no packfile");
  }
  return packfile;
}

describe("assertPackInflationWithinBounds", () => {
  test("accepts a normal pack and walks exactly to the trailer", async () => {
    // A tiny pack passes and the walk lands on the trailer; a misframed walk
    // would throw the ended-at/trailer error instead.
    const pack = await packWithFile("hello\n");
    await expect(
      assertPackInflationWithinBounds(
        pack,
        DEFAULT_PACK_MATERIALIZATION_LIMITS,
      ),
    ).resolves.toBeUndefined();
  });

  test("rejects an object that inflates past the per-object cap", async () => {
    // 200 KiB of zeros compresses to a tiny blob that inflates far past a
    // 1 KiB cap; the guard destroys the inflate the moment it is exceeded.
    const pack = await packWithFile("\0".repeat(200 * 1024));
    await expect(
      assertPackInflationWithinBounds(pack, {
        ...DEFAULT_PACK_MATERIALIZATION_LIMITS,
        maxObjectInflatedBytes: 1024,
      }),
    ).rejects.toThrow(/inflates past the 1024-byte per-object cap/);
  });

  test("rejects an over-count pack from the header before inflating", async () => {
    // The one-file commit packs three objects (commit, tree, blob); a cap of
    // two rejects it from the declared count alone, before the per-object walk.
    const pack = await packWithFile("hello\n");
    await expect(
      assertPackInflationWithinBounds(pack, {
        ...DEFAULT_PACK_MATERIALIZATION_LIMITS,
        maxPackObjects: 2,
      }),
    ).rejects.toThrow(/declares 3 objects, exceeding the 2-object cap/);
  });

  test("rejects a buffer too short to hold a header and trailer", async () => {
    await expect(
      assertPackInflationWithinBounds(
        new Uint8Array(16),
        DEFAULT_PACK_MATERIALIZATION_LIMITS,
      ),
    ).rejects.toThrow(/shorter than a header plus trailer/);
  });
});
