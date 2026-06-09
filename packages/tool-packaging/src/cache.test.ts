import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import ssri from "ssri";
import * as tar from "tar";

import { TarballIntegrityMismatchError, createTarballCache } from "./cache";

let scratch: string;

beforeEach(async () => {
  scratch = await fs.mkdtemp(path.join(os.tmpdir(), "tool-packaging-cache-"));
});

afterEach(async () => {
  await fs.rm(scratch, { recursive: true, force: true });
});

function makeBytes(seed: string): { bytes: Buffer; integrity: string } {
  const bytes = Buffer.from(`tarball-bytes-${seed}`);
  const integrity = ssri.fromData(bytes, { algorithms: ["sha512"] }).toString();
  return { bytes, integrity };
}

describe("createTarballCache", () => {
  test("rejects non-positive maxBytes", () => {
    expect(() => createTarballCache({ rootDir: scratch, maxBytes: 0 })).toThrow(
      /maxBytes must be positive/,
    );
  });
});

describe("get / put round-trip", () => {
  test("put stores bytes and get returns them", async () => {
    const cache = createTarballCache({ rootDir: scratch, maxBytes: 10_000 });
    const { bytes, integrity } = makeBytes("a");
    await cache.put(integrity, bytes);
    const fetched = await cache.get(integrity);
    expect(fetched).not.toBeNull();
    expect(fetched?.equals(bytes)).toBe(true);
  });

  test("get returns null on miss", async () => {
    const cache = createTarballCache({ rootDir: scratch, maxBytes: 10_000 });
    const { integrity } = makeBytes("not-stored");
    expect(await cache.get(integrity)).toBeNull();
  });

  test("put with mismatched bytes throws TarballIntegrityMismatchError", async () => {
    const cache = createTarballCache({ rootDir: scratch, maxBytes: 10_000 });
    const { integrity } = makeBytes("a");
    const wrongBytes = Buffer.from("not the right bytes");
    let caught: unknown;
    try {
      await cache.put(integrity, wrongBytes);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(TarballIntegrityMismatchError);
    expect(await cache.get(integrity)).toBeNull();
  });
});

describe("evict", () => {
  test("explicit evict removes the entry", async () => {
    const cache = createTarballCache({ rootDir: scratch, maxBytes: 10_000 });
    const { bytes, integrity } = makeBytes("a");
    await cache.put(integrity, bytes);
    expect(await cache.get(integrity)).not.toBeNull();
    await cache.evict(integrity);
    expect(await cache.get(integrity)).toBeNull();
  });

  test("evict on a missing entry is a no-op", async () => {
    const cache = createTarballCache({ rootDir: scratch, maxBytes: 10_000 });
    const { integrity } = makeBytes("never-stored");
    await cache.evict(integrity);
  });
});

describe("LRU eviction when over cap", () => {
  // The cap-driven sweep fires inside `extractTarball`, not inside
  // `put`. Sweeping in `put` would charge an entry's bytes-on-disk
  // total against `maxBytes` using only the tarball size — extraction
  // hasn't run yet, so the half-sized entry's true footprint is
  // not knowable. Tests below put + extractTarball each entry so the
  // sweep runs against the entry's full tarball + extracted total.
  test("oldest entries are evicted to bring total under maxBytes", async () => {
    const a = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "a", version: "1.0.0" }),
    });
    const b = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "b", version: "1.0.0" }),
    });
    const c = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "c", version: "1.0.0" }),
    });

    const cache = createTarballCache({
      rootDir: scratch,
      // Cap sized empirically: probe one entry's actual on-disk
      // footprint (tarball + extracted tree, including filesystem
      // overhead) and set the cap to 2.5x — between two-entry total
      // and three-entry total — so the third put+extract tips the
      // LRU sweep and evicts the oldest.
      maxBytes: Math.floor((await probeEntrySize(scratch)) * 2.5),
    });

    await cache.put(a.integrity, a.bytes);
    (await cache.extractTarball(a.integrity)).release();
    await new Promise((r) => setTimeout(r, 25));
    await cache.put(b.integrity, b.bytes);
    (await cache.extractTarball(b.integrity)).release();
    await new Promise((r) => setTimeout(r, 25));
    await cache.put(c.integrity, c.bytes);
    (await cache.extractTarball(c.integrity)).release();

    // After unpacking c, we are over cap; oldest (a) should be evicted.
    expect(await cache.get(a.integrity)).toBeNull();
    expect(await cache.get(b.integrity)).not.toBeNull();
    expect(await cache.get(c.integrity)).not.toBeNull();
  });

  test("a tarball larger than maxBytes survives its own extract-eviction sweep", async () => {
    // A single entry bigger than the cap would, without protection,
    // be the only candidate the LRU sweep finds: the just-written
    // tarball + just-extracted tree would be deleted before the
    // loader could read it back, and the next apply would re-fetch
    // and re-evict — perpetual churn. The just-written entry is
    // exempt from the sweep that its own extractTarball triggers.
    const big = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "big", version: "1.0.0" }),
      "payload.bin": "big-payload-".repeat(64),
    });

    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: 32, // far below big.bytes.length
    });

    await cache.put(big.integrity, big.bytes);
    (await cache.extractTarball(big.integrity)).release();

    // The entry survives the immediate sweep.
    expect(await cache.get(big.integrity)).not.toBeNull();
  });

  test("getting an entry updates atime so it survives eviction", async () => {
    const a = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "a", version: "1.0.0" }),
    });
    const b = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "b", version: "1.0.0" }),
    });
    const c = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "c", version: "1.0.0" }),
    });

    const cache = createTarballCache({
      rootDir: scratch,
      // Cap sized empirically: probe one entry's actual on-disk
      // footprint (tarball + extracted tree, including filesystem
      // overhead) and set the cap to 2.5x — between two-entry total
      // and three-entry total — so the third put+extract tips the
      // LRU sweep and evicts the oldest.
      maxBytes: Math.floor((await probeEntrySize(scratch)) * 2.5),
    });

    await cache.put(a.integrity, a.bytes);
    (await cache.extractTarball(a.integrity)).release();
    await new Promise((r) => setTimeout(r, 25));
    await cache.put(b.integrity, b.bytes);
    (await cache.extractTarball(b.integrity)).release();
    await new Promise((r) => setTimeout(r, 25));
    // Touch a so its atime is now newer than b's.
    await cache.get(a.integrity);
    await new Promise((r) => setTimeout(r, 25));
    await cache.put(c.integrity, c.bytes);
    (await cache.extractTarball(c.integrity)).release();

    // Now b should be the oldest and get evicted, not a.
    expect(await cache.get(a.integrity)).not.toBeNull();
    expect(await cache.get(b.integrity)).toBeNull();
    expect(await cache.get(c.integrity)).not.toBeNull();
  });

  test("cap-driven eviction defers the extraction reclaim while a reader holds the handle", async () => {
    // The cap-driven sweep removes the tarball blob immediately but
    // must defer the extraction-tree rm until every in-flight reader
    // releases — the same refcount/deferred-reclaim contract `evict`
    // honors. Without that gate, a concurrent `hardlinkTree` walk
    // against the LRU victim would observe ENOENT mid-readdir when
    // the sweep fires.
    const a = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "a", version: "1.0.0" }),
    });
    const b = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "b", version: "1.0.0" }),
    });

    // Cap big enough for one (a or b) plus a little slack; the second
    // put will trigger the sweep and evict the older entry.
    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: Math.max(a.bytes.length, b.bytes.length),
    });
    await cache.put(a.integrity, a.bytes);
    // Hold an extraction handle on `a` BEFORE the cap-driven sweep
    // would target it. The handle keeps the extraction tree alive
    // through the sweep.
    const handle = await cache.extractTarball(a.integrity);
    const dirBeforeEvict = handle.dir;
    // Verify the extraction tree exists right now.
    await fs.access(dirBeforeEvict);

    // Trigger the sweep: putting + extracting `b` pushes us over cap;
    // `a` is the older entry and gets evicted. The sweep fires inside
    // `extractTarball`, not `put`.
    await new Promise((r) => setTimeout(r, 25));
    await cache.put(b.integrity, b.bytes);
    (await cache.extractTarball(b.integrity)).release();

    // The tarball blob for `a` is gone (the sweep dropped it).
    expect(await cache.get(a.integrity)).toBeNull();
    // The extraction tree is still readable through the held handle
    // because reclaim was deferred behind the reader.
    await fs.access(dirBeforeEvict);

    // Releasing the handle clears the deferred reclaim; the
    // extraction tree is removed once the refcount hits zero.
    handle.release();
    // Give the deferred-reclaim microtask a tick to run.
    await new Promise((r) => setTimeout(r, 25));
    let extractionStillThere = true;
    try {
      await fs.access(dirBeforeEvict);
    } catch {
      extractionStillThere = false;
    }
    expect(extractionStillThere).toBe(false);
  });

  test("extractTarball advances atime so loader-pattern hits beat the LRU sweep", async () => {
    // The loader's hot path is `cache.has` + `cache.extractTarball` —
    // `cache.get` is not in that flow. `cache.get` explicitly calls
    // `fs.utimes` so LRU ordering reflects access even on
    // noatime/relatime mounts; `extractTarball` must do the same or
    // the LRU sweep orders entries by initial-put time, evicting
    // heavily-used integrities preferentially while idle ones
    // linger.
    const a = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "a", version: "1.0.0" }),
    });
    const b = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "b", version: "1.0.0" }),
    });
    const c = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "c", version: "1.0.0" }),
    });

    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: Math.floor((await probeEntrySize(scratch)) * 2.5),
    });

    await cache.put(a.integrity, a.bytes);
    (await cache.extractTarball(a.integrity)).release();
    await new Promise((r) => setTimeout(r, 25));
    await cache.put(b.integrity, b.bytes);
    (await cache.extractTarball(b.integrity)).release();
    await new Promise((r) => setTimeout(r, 25));

    // Touch `a` ONLY through the loader-shaped path (has + extract).
    // This is the access pattern `materialize` uses on a cache hit;
    // it must advance `a`'s atime so the next cap sweep treats `a`
    // as the most-recently-used.
    expect(await cache.has(a.integrity)).toBe(true);
    (await cache.extractTarball(a.integrity)).release();
    await new Promise((r) => setTimeout(r, 25));

    await cache.put(c.integrity, c.bytes);
    (await cache.extractTarball(c.integrity)).release();

    // `b` is the oldest by access pattern. If `extractTarball` does
    // not touch atime, the sweep evicts `a` instead because its put
    // time is still the oldest.
    expect(await cache.get(a.integrity)).not.toBeNull();
    expect(await cache.get(b.integrity)).toBeNull();
    expect(await cache.get(c.integrity)).not.toBeNull();
  });

  test("extractTarball's unpack path pins the refcount against a concurrent evict", async () => {
    // The invariant: IF `extractTarball` returns a handle, the
    // `handle.dir` it advertises must be readable until the handle's
    // `release` fires, regardless of what concurrent `cache.evict`
    // calls did in the meantime. Evict beating `extractTarball` to
    // the tarball-read step is documented behavior (the caller's
    // contract is `put` first), so an `extractTarball` rejection is
    // an acceptable race outcome — what is NOT acceptable is a
    // handle returned against a directory that has been reclaimed
    // out from under it. Race many extract/evict pairs and verify
    // every returned handle remains valid.
    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: 100_000_000,
    });
    for (let i = 0; i < 50; i += 1) {
      const fixture = await packFixtureTarball(scratch, {
        "package.json": JSON.stringify({
          name: `race-${String(i)}`,
          version: "1.0.0",
        }),
      });
      await cache.put(fixture.integrity, fixture.bytes);
      const [extractResult] = await Promise.allSettled([
        cache.extractTarball(fixture.integrity),
        cache.evict(fixture.integrity),
      ]);
      if (extractResult.status === "fulfilled") {
        const handle = extractResult.value;
        const dirStillThere = await fs
          .access(handle.dir)
          .then(() => true)
          .catch(() => false);
        expect(dirStillThere).toBe(true);
        handle.release();
      }
      // If extractTarball rejected (e.g. evict unlinked the tarball
      // before readFile could read it), that is a documented outcome
      // of the race the caller is expected to handle by re-`put`ing.
    }
  });
});

/**
 * Empirically measure the on-disk footprint of one cached entry
 * (tarball + extracted tree) so the LRU tests can size `maxBytes`
 * against the live filesystem's actual accounting. The probe builds
 * a sibling cache in a throwaway directory, puts a small tarball,
 * extracts it, and returns the cache's reported `size()`.
 */
async function probeEntrySize(scratch: string): Promise<number> {
  const probeRoot = await fs.mkdtemp(path.join(scratch, "probe-"));
  const probe = createTarballCache({ rootDir: probeRoot, maxBytes: 10_000 });
  const probeBytes = await packFixtureTarball(probeRoot, {
    "package.json": JSON.stringify({ name: "probe", version: "1.0.0" }),
  });
  await probe.put(probeBytes.integrity, probeBytes.bytes);
  (await probe.extractTarball(probeBytes.integrity)).release();
  const size = await probe.size();
  await fs.rm(probeRoot, { recursive: true, force: true });
  return size;
}

async function packFixtureTarball(
  cwd: string,
  files: Record<string, string>,
): Promise<{ bytes: Buffer; integrity: string }> {
  const staging = await fs.mkdtemp(path.join(cwd, "stage-"));
  const packageDir = path.join(staging, "package");
  await fs.mkdir(packageDir, { recursive: true });
  for (const [rel, contents] of Object.entries(files)) {
    const abs = path.join(packageDir, rel);
    await fs.mkdir(path.dirname(abs), { recursive: true });
    await fs.writeFile(abs, contents);
  }
  const tarballPath = path.join(staging, "out.tgz");
  await tar.create({ cwd: staging, gzip: true, file: tarballPath }, [
    "package",
  ]);
  const bytes = await fs.readFile(tarballPath);
  const integrity = ssri.fromData(bytes, { algorithms: ["sha512"] }).toString();
  return { bytes, integrity };
}

describe("extractTarball", () => {
  test("unpacks the tarball into a content-addressable directory", async () => {
    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: 10_000_000,
    });
    const { bytes, integrity } = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "foo", version: "1.0.0" }),
      "src/index.js": "export const x = 1;\n",
    });
    await cache.put(integrity, bytes);

    const handle = await cache.extractTarball(integrity);
    try {
      const pkg = await fs.readFile(
        path.join(handle.dir, "package.json"),
        "utf8",
      );
      expect(JSON.parse(pkg)).toEqual({ name: "foo", version: "1.0.0" });
      const body = await fs.readFile(
        path.join(handle.dir, "src/index.js"),
        "utf8",
      );
      expect(body).toBe("export const x = 1;\n");
    } finally {
      handle.release();
    }
  });

  test("idempotent: repeated calls return the same path without re-extracting", async () => {
    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: 10_000_000,
    });
    const { bytes, integrity } = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "foo", version: "1.0.0" }),
    });
    await cache.put(integrity, bytes);

    const first = await cache.extractTarball(integrity);
    const sentinel = path.join(first.dir, "sentinel");
    await fs.writeFile(sentinel, "marker");

    const second = await cache.extractTarball(integrity);
    expect(second.dir).toBe(first.dir);
    // The sentinel survives because the second call short-circuits on
    // the existing extraction rather than re-unpacking the tarball.
    expect(await fs.readFile(sentinel, "utf8")).toBe("marker");
    first.release();
    second.release();
  });

  test("throws when the cache has no bytes for the integrity", async () => {
    const cache = createTarballCache({ rootDir: scratch, maxBytes: 10_000 });
    let caught: unknown;
    try {
      await cache.extractTarball("sha512-deadbeef");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(String(caught)).toMatch(/sha512-deadbeef/);
  });

  test("evict removes both the tarball bytes and the extraction", async () => {
    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: 10_000_000,
    });
    const { bytes, integrity } = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "foo", version: "1.0.0" }),
    });
    await cache.put(integrity, bytes);
    const handle = await cache.extractTarball(integrity);
    expect(await fs.readdir(handle.dir)).toContain("package.json");
    handle.release();

    await cache.evict(integrity);
    expect(await cache.get(integrity)).toBeNull();
    let caught: unknown;
    try {
      await fs.access(handle.dir);
    } catch (err) {
      caught = err;
    }
    expect(caught).not.toBeUndefined();
  });

  test("evict defers extraction removal while a reader still holds an unreleased handle", async () => {
    // An in-flight `hardlinkTree` walk against an integrity's
    // extraction tree must not see the tree disappear mid-readdir when
    // another agent in the same sidecar process trips an
    // integrity-mismatch evict for the same integrity. The cache holds
    // a refcount per (integrity, extraction-dir) pair: `evict` deletes
    // the tarball blob immediately so a subsequent `extractTarball`
    // cannot reuse the on-disk extraction, but the physical removal of
    // the extraction tree is deferred until every outstanding
    // `release` from `extractTarball` has fired.
    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: 10_000_000,
    });
    const { bytes, integrity } = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "foo", version: "1.0.0" }),
      "src/index.js": "export const x = 1;\n",
    });
    await cache.put(integrity, bytes);

    const reader = await cache.extractTarball(integrity);
    expect(await fs.readdir(reader.dir)).toContain("package.json");

    // Evict races against the still-open reader. The tarball blob
    // disappears immediately so a fresh `extractTarball` cannot
    // short-circuit on the existing tree, but the tree itself must
    // remain on disk so the in-flight reader's walk continues to
    // succeed.
    await cache.evict(integrity);
    expect(await cache.get(integrity)).toBeNull();
    expect(await fs.readdir(reader.dir)).toContain("package.json");
    const body = await fs.readFile(
      path.join(reader.dir, "src/index.js"),
      "utf8",
    );
    expect(body).toBe("export const x = 1;\n");

    // Release the reader. The deferred reclaim runs after this, but
    // releaseExtraction's `fs.rm` is fire-and-forget; poll until the
    // tree disappears to avoid racing the test against the async
    // reclaim. Bound the poll so a real failure surfaces instead of
    // hanging the suite.
    reader.release();
    let cleared = false;
    for (let attempt = 0; attempt < 50; attempt++) {
      try {
        await fs.access(reader.dir);
      } catch {
        cleared = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(cleared).toBe(true);
  });

  test("refcount tracks each handle in isolation across sequential acquire/release cycles", async () => {
    // Each extractTarball acquires the refcount; each handle.release()
    // decrements it. Cycling through acquire → release → acquire →
    // release must leave the cache willing to hand out a fresh
    // handle, because every drop returned the count to zero cleanly
    // (rather than wedging it negative or accumulating leaked
    // references that would block a subsequent extractTarball from
    // re-extracting after an evict).
    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: 10_000_000,
    });
    const { bytes, integrity } = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "foo", version: "1.0.0" }),
    });
    await cache.put(integrity, bytes);
    const handle = await cache.extractTarball(integrity);
    handle.release();
    // The returned `release` is idempotent on the handle (it tracks
    // its own released-bit), so calling it again is a no-op.
    handle.release();
    // A fresh acquire after the refcount returned to zero must
    // succeed without observing leaked state from the prior handle.
    const second = await cache.extractTarball(integrity);
    second.release();
    const third = await cache.extractTarball(integrity);
    expect(third.dir).toBe(second.dir);
    third.release();
  });
});

describe("on-disk layout", () => {
  test("entries land under algorithm/<2-char>/<rest>/tarball.tgz", async () => {
    const cache = createTarballCache({ rootDir: scratch, maxBytes: 10_000 });
    const { bytes, integrity } = makeBytes("layout-check");
    await cache.put(integrity, bytes);

    const sha512Dir = path.join(scratch, "sha512");
    const algEntries = await fs.readdir(sha512Dir);
    expect(algEntries).toHaveLength(1);
    const shard = algEntries[0];
    expect(typeof shard).toBe("string");
    if (typeof shard !== "string") throw new Error("unreachable");
    expect(shard.length).toBe(2);

    const shardDir = path.join(sha512Dir, shard);
    const leafEntries = await fs.readdir(shardDir);
    expect(leafEntries).toHaveLength(1);
    const leaf = leafEntries[0];
    if (typeof leaf !== "string") throw new Error("unreachable");
    const file = path.join(shardDir, leaf, "tarball.tgz");
    const stat = await fs.stat(file);
    expect(stat.size).toBe(bytes.length);
  });
});

describe("sweepOrphans", () => {
  test("removes orphan tarball.tgz.tmp and extracted.tmp dirs left by crashed puts/extracts", async () => {
    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: 10_000_000,
    });
    const { bytes, integrity } = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "foo", version: "1.0.0" }),
    });
    await cache.put(integrity, bytes);
    const initialHandle = await cache.extractTarball(integrity);
    initialHandle.release();

    // Reach into the cache's on-disk layout to simulate orphans left
    // by a crash between staging and the final rename. The shard
    // layout is implementation-detail of the cache module, but the
    // sweep contract is the only behaviour worth asserting here, and
    // the only way to construct an orphan without monkey-patching the
    // module is to write one through the same layout it walks.
    const sha512Dir = path.join(scratch, "sha512");
    const shard = (await fs.readdir(sha512Dir))[0];
    if (typeof shard !== "string") throw new Error("unreachable");
    const shardDir = path.join(sha512Dir, shard);
    const leaf = (await fs.readdir(shardDir))[0];
    if (typeof leaf !== "string") throw new Error("unreachable");
    const entryDir = path.join(shardDir, leaf);

    const orphanPut = path.join(entryDir, "tarball.tgz.tmp.99999.abcd1234");
    const orphanExtract = path.join(entryDir, "extracted.tmp.99999.beef5678");
    await fs.writeFile(orphanPut, "partial");
    await fs.mkdir(orphanExtract, { recursive: true });
    await fs.writeFile(path.join(orphanExtract, "partial.txt"), "x");

    await cache.sweepOrphans();

    await expect(fs.access(orphanPut)).rejects.toThrow();
    await expect(fs.access(orphanExtract)).rejects.toThrow();
    // The canonical entry survives.
    expect(await cache.get(integrity)).not.toBeNull();
    await fs.access(path.join(entryDir, "extracted", "package.json"));
  });

  test("is a no-op when the cache root does not exist", async () => {
    const missingRoot = path.join(scratch, "never-created");
    const cache = createTarballCache({
      rootDir: missingRoot,
      maxBytes: 10_000_000,
    });
    await cache.sweepOrphans();
  });

  test("leaves non-tmp siblings of canonical entries alone", async () => {
    const cache = createTarballCache({
      rootDir: scratch,
      maxBytes: 10_000_000,
    });
    const { bytes, integrity } = await packFixtureTarball(scratch, {
      "package.json": JSON.stringify({ name: "foo", version: "1.0.0" }),
    });
    await cache.put(integrity, bytes);
    const handle = await cache.extractTarball(integrity);
    handle.release();
    await cache.sweepOrphans();
    expect(await cache.get(integrity)).not.toBeNull();
  });
});

describe("integrity-encoding rejection", () => {
  test("operations refuse a base64url-encoded integrity payload", async () => {
    // The cache's shard layout reserves `-` as an internal escape for
    // standard-base64's `/`. A base64url-encoded payload (using `-`
    // and `_` in place of `+` and `/`) would either collide with the
    // escape or write under a shard whose first two characters no
    // longer carry their standard-base64 meaning. Every operation
    // that runs the integrity through the path-deriving parser must
    // refuse the encoding loudly rather than producing colliding
    // on-disk paths.
    //
    // The check fires in `entryDir`, which `get`, `evict`, and
    // `extractTarball` all run before they touch the filesystem. Use
    // `get` here because it is the simplest entry point that hits
    // the parser without first matching the bytes against the
    // integrity (which the `put` path also does, and which would
    // fail first on synthesized input).
    const cache = createTarballCache({ rootDir: scratch, maxBytes: 10_000 });
    // A literal base64url payload: real ssri output is standard
    // base64, so the input has to be synthesized. Embed `_` and `-`
    // (the base64url-only characters) so the digit-shape check on
    // the payload fires.
    const base64urlIntegrity = `sha512-${"A".repeat(86)}_-`;
    let caught: unknown;
    try {
      await cache.get(base64urlIntegrity);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    if (caught instanceof Error) {
      expect(caught.message).toMatch(/not standard base64/);
    }
  });
});
