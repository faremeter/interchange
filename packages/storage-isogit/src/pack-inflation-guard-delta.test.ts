import { describe, test, expect, afterEach } from "bun:test";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { assertPackInflationWithinBounds } from "./pack-inflation-guard";
import { DEFAULT_PACK_MATERIALIZATION_LIMITS } from "./materialization-limits";

// The committed guard test only packs a fresh isomorphic-git repo, which
// produces NO delta objects, so the OFS_DELTA / REF_DELTA framing in
// assertPackInflationWithinBounds (the base-offset varint skip and the 20-byte
// ref-base skip) has zero regression coverage. A real pushed pack routinely
// carries deltas; a misframe there would spuriously reject a valid pack at the
// trailer-landing check. These tests build real delta packs with the git CLI.

const tmpDirs: string[] = [];
afterEach(async () => {
  await Promise.all(
    tmpDirs.splice(0).map((d) => fsp.rm(d, { recursive: true, force: true })),
  );
});

async function deltaPack(deltaBaseOffset: boolean): Promise<Uint8Array> {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), "guard-delta-"));
  tmpDirs.push(dir);
  const g = (args: string[], input?: string) =>
    execFileSync("git", args, {
      cwd: dir,
      input,
      maxBuffer: 1 << 28,
      stdio: ["pipe", "pipe", "pipe"],
    });
  g(["init", "-q"]);
  g(["config", "user.email", "t@t.dev"]);
  g(["config", "user.name", "t"]);
  for (let i = 1; i <= 12; i++) {
    const lines = Array.from(
      { length: i * 400 },
      (_, j) => `line ${String(j)} content here padding padding`,
    ).join("\n");
    await fsp.writeFile(path.join(dir, "f.txt"), lines);
    g(["add", "."]);
    g(["commit", "-qm", `c${String(i)}`]);
  }
  const objs = g(["rev-list", "--objects", "--all"])
    .toString()
    .split("\n")
    .map((l) => l.split(" ")[0])
    .filter(Boolean)
    .join("\n");
  const out = path.join(dir, "out");
  fs.mkdirSync(out);
  g(
    [
      "pack-objects",
      deltaBaseOffset ? "--delta-base-offset" : "--no-delta-base-offset",
      "--window=50",
      "--depth=50",
      path.join(out, "pack"),
    ],
    objs,
  );
  const idx = fs.readdirSync(out).find((f) => f.endsWith(".idx"));
  if (idx === undefined) throw new Error("no idx produced");
  const deltaRows = g(["verify-pack", "-v", path.join(out, idx)])
    .toString()
    .split("\n")
    .filter(
      (l) =>
        /^[0-9a-f]{40} (blob|tree|commit)\b/.test(l) &&
        l.split(/\s+/).length >= 7,
    ).length;
  expect(deltaRows).toBeGreaterThan(0); // guard against a vacuous (delta-free) pack
  const packName = fs.readdirSync(out).find((f) => f.endsWith(".pack"));
  if (packName === undefined) throw new Error("no pack produced");
  return new Uint8Array(fs.readFileSync(path.join(out, packName)));
}

describe("assertPackInflationWithinBounds delta framing", () => {
  test("accepts an OFS_DELTA pack and lands on the trailer", async () => {
    const pack = await deltaPack(true);
    await expect(
      assertPackInflationWithinBounds(
        pack,
        DEFAULT_PACK_MATERIALIZATION_LIMITS,
      ),
    ).resolves.toBeUndefined();
  });

  test("accepts a REF_DELTA pack and lands on the trailer", async () => {
    const pack = await deltaPack(false);
    await expect(
      assertPackInflationWithinBounds(
        pack,
        DEFAULT_PACK_MATERIALIZATION_LIMITS,
      ),
    ).resolves.toBeUndefined();
  });
});
