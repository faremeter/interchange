import git, { type FsClient } from "isomorphic-git";
import {
  pathExists,
  promiseFsMethods,
  syncPath,
  type StorageRuntime,
} from "./runtime";
import { hasCode } from "@intx/types/has-code";
import { REF_WRITE_PREFIX } from "./commit-helpers";
import { createLooseObjectWriter, stagingRootOf } from "./loose-objects";
import { forgetSyncRecord, syncObjectsAndRefs } from "./sync-record";

const SAFE_PATH_SEGMENT = /^[a-zA-Z0-9_-]+$/;

// How many objects a publish reads and writes at once.
const UNPACK_CONCURRENCY = 16;

/**
 * A pack with at least this many objects is published as a pack while
 * the repository holds fewer than `PACK_COUNT_LIMIT` packs. A synced file
 * per object pays off for the many small packs of a run's pushes, not for
 * one large push; git's receive-pack keeps a pack at its
 * `transfer.unpackLimit`, also 100.
 */
export const UNPACK_LIMIT = 100;

/**
 * A large pack stays a pack only while the repository holds fewer packs
 * than this. Nothing repacks, and a read of an object in a pack searches
 * every pack, so past this a large pack is unpacked too.
 */
export const PACK_COUNT_LIMIT = 8;

// Git's loose object format: the deflated "<type> <size>\0" header and
// content.
async function looseObjectBytes(
  type: string,
  content: Uint8Array,
): Promise<Uint8Array> {
  const header = new TextEncoder().encode(
    `${type} ${String(content.byteLength)}\0`,
  );
  const wrapped = new Uint8Array(header.byteLength + content.byteLength);
  wrapped.set(header);
  wrapped.set(content, header.byteLength);
  const stream = new Blob([wrapped])
    .stream()
    .pipeThrough(new CompressionStream("deflate"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/**
 * A received pack written and indexed outside `objects/pack/`, which
 * readers that use the repo's own filesystem do not find. `fs` is an
 * isomorphic-git client that also finds the staged objects, so a caller
 * can validate the pack before any other reader can see it. `publish`
 * makes the objects visible to every reader, as `stagePack` describes;
 * `discard` removes the pack. `publish` reads the objects through `cache`
 * when it gets one, so a cache that loaded the pack through `fs` serves
 * them.
 *
 * `fs` shows the staged `.idx` and `.pack` in `objects/pack/`, searched
 * before the repository's own packs, until `publish` returns or `discard`
 * runs, then passes every call through.
 *
 * `discard` is safe at any time. Once `publish` has started, `discard`
 * does nothing, so a caller can discard on every path without a check.
 * `publish` runs once, and only on a pack that is not discarded.
 */
export type StagedPack = {
  readonly oids: readonly string[];
  readonly fs: FsClient;
  publish(cache?: object): Promise<void>;
  discard(): Promise<void>;
};

/**
 * Stage a packfile for one receive and return its handle.
 *
 * The pack is written to `objects/pack-staging/<transferId>/` and indexed
 * there. isomorphic-git scans only `objects/pack/`, and only that one
 * level, so no reader of the repo sees the staged objects, and none can
 * observe the `.idx` that `git.indexPack` writes in place. The
 * per-transfer directory keeps concurrent receives apart. The caller
 * gives each receive of a repository a `transferId` no earlier receive
 * used, since a large pack is published under it.
 *
 * Unless the pack is large (below), `publish` writes every object of the
 * pack as a loose object instead of moving the pack into `objects/pack/`.
 * isomorphic-git finds a loose object by its path, so a read costs the
 * same however many pushes a repo has taken; with a pack per push, each
 * read lists, sorts and searches every pack. Each object is written to the
 * staging directory, synced, and renamed to `objects/<xx>/<rest>`, so a
 * reader never sees a partial object. After the renames, `objects/` and
 * each fan-out directory of the pack's objects are synced, so every object
 * is durable when `publish` returns on a runtime that syncs. An object the
 * repo already holds as a loose object is kept, as
 * `LooseObjectWriter.keep` describes.
 *
 * A pack of `UNPACK_LIMIT` objects or more, in a repository with fewer
 * than `PACK_COUNT_LIMIT` packs, is moved instead: the pack and its index
 * are synced, the pack is renamed into `objects/pack/` and that directory
 * synced, and only then the index, since a reader finds a pack by its
 * index. Then `objects/pack/` and `objects/` are synced.
 *
 * A throw before a moved pack's index lands takes the pack back out; a
 * throw after it leaves the pair in place. Every publish ends with
 * `syncObjectsAndRefs`. A throw or a crash can leave objects in place in
 * unsynced directories, and a crash can leave a pack without its index,
 * which no read uses. No ref names them yet, and a retried receive keeps
 * such an object and syncs its directory. The staging directory is removed
 * on a throw, on `discard`, and after `publish`; one left by a crash stays
 * until a caller runs `removeStagedPacks` on the repository.
 *
 * The caller serializes receives on `dir` (e.g. `withRepoLock` in
 * `packages/hub-sessions/src/repo-store/store.ts`).
 */
export async function stagePack(
  runtime: StorageRuntime,
  dir: string,
  pack: Uint8Array,
  transferId: string,
): Promise<StagedPack> {
  if (!SAFE_PATH_SEGMENT.test(transferId)) {
    throw new Error(
      `transferId contains unsafe characters: ${JSON.stringify(transferId)}`,
    );
  }

  const objectsDir = runtime.path.join(dir, ".git", "objects");
  const packDir = runtime.path.join(objectsDir, "pack");
  const stagingRoot = stagingRootOf(runtime, dir);
  const stagingDir = runtime.path.join(stagingRoot, transferId);

  const stagingPackPath = runtime.path.join(stagingDir, "pack.pack");
  const stagingIdxPath = stagingPackPath.replace(/\.pack$/, ".idx");
  const packName = `pack-recv-${transferId}.pack`;
  const idxName = `pack-recv-${transferId}.idx`;

  await runtime.fs.mkdir(packDir, { recursive: true });
  await runtime.fs.mkdir(stagingRoot, { recursive: true });
  if (
    (await pathExists(runtime, runtime.path.join(packDir, packName))) ||
    (await pathExists(runtime, runtime.path.join(packDir, idxName)))
  ) {
    throw new Error(
      `transferId "${transferId}" already published in ${dir}; callers must guarantee transferId uniqueness across all receives of a repository`,
    );
  }
  try {
    await runtime.fs.mkdir(stagingDir);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "EEXIST") {
      throw new Error(
        `transferId "${transferId}" already in progress in ${dir}; callers must guarantee transferId uniqueness across all receives of a repository`,
        { cause },
      );
    }
    throw cause;
  }
  // iso-git's indexPack takes a repo-root-relative filepath; derive it
  // from the absolute path rather than rebuilding the segment list so
  // the two stay coupled.
  const stagingFilepath = runtime.path.relative(dir, stagingPackPath);

  let oids: string[];
  try {
    await runtime.fs.writeFile(stagingPackPath, pack);
    const result = await git.indexPack({
      fs: runtime.fs.git,
      dir,
      filepath: stagingFilepath,
    });
    oids = result.oids;
  } catch (err) {
    // The rm is wrapped so a secondary cleanup failure does not mask the
    // original error.
    await runtime.fs
      .remove(stagingDir, { recursive: true, force: true })
      .catch(() => undefined);
    throw err;
  }

  let state: "staged" | "publishing" | "published" | "discarded" = "staged";
  const showsStaged = () => state === "staged" || state === "publishing";

  // isomorphic-git joins the gitdir and `objects/pack` for its listing
  // but concatenates them for its reads, and the gitdir can be
  // relative, so paths compare after `resolve`.
  const resolvedPackDir = runtime.path.resolve(packDir);
  // isomorphic-git searches packs in name order, and a repository an older
  // Hub wrote can hold many, so `fs` lists the staged pack under a name
  // that sorts first: a read of an object the pack brings then searches no
  // other pack.
  const viewIdxName = `!staged-${transferId}.idx`;
  const viewPackName = `!staged-${transferId}.pack`;
  const stagedPathOf = new Map([
    [
      runtime.path.resolve(runtime.path.join(packDir, viewIdxName)),
      stagingIdxPath,
    ],
    [
      runtime.path.resolve(runtime.path.join(packDir, viewPackName)),
      stagingPackPath,
    ],
  ]);
  const base = promiseFsMethods(runtime.fs.git);
  const isPackDir = (filepath: unknown): boolean =>
    typeof filepath === "string" &&
    runtime.path.resolve(filepath) === resolvedPackDir;
  const viewReaddir = async (...args: unknown[]) => {
    const entries = await base.readdir(...args);
    if (!showsStaged() || !isPackDir(args[0]) || !Array.isArray(entries)) {
      return entries;
    }
    return [...entries, viewIdxName, viewPackName];
  };
  const viewReadFile = async (...args: unknown[]) => {
    const [filepath, ...rest] = args;
    if (!showsStaged() || typeof filepath !== "string") {
      return base.readFile(...args);
    }
    const stagedPath = stagedPathOf.get(runtime.path.resolve(filepath));
    return stagedPath === undefined
      ? base.readFile(...args)
      : base.readFile(stagedPath, ...rest);
  };
  // isomorphic-git binds `promises` only when it is an enumerable own
  // property; otherwise it binds the client as a callback filesystem.
  const viewFs: FsClient = {
    promises: { ...base, readdir: viewReaddir, readFile: viewReadFile },
  };

  async function writeLooseObjects(cache: object): Promise<void> {
    const writer = createLooseObjectWriter(
      runtime,
      dir,
      runtime.path.join(stagingDir, "object-"),
    );
    // isomorphic-git lists `objects/pack/` for every object it reads. The
    // caller serializes receives on `dir`, so no pack lands or goes while
    // this publish runs, and one listing serves every read.
    let packListing: Promise<unknown> | undefined;
    const publishFs: FsClient = {
      promises: {
        ...base,
        readFile: viewReadFile,
        readdir: (...args: unknown[]) => {
          if (!isPackDir(args[0])) return viewReaddir(...args);
          packListing ??= viewReaddir(...args);
          return packListing;
        },
      },
    };
    let next = 0;
    let failed = false;
    const writeObject = async (oid: string) => {
      const loose = runtime.path.join(
        objectsDir,
        oid.slice(0, 2),
        oid.slice(2),
      );
      if (await pathExists(runtime, loose)) {
        writer.keep(oid);
        return;
      }
      const object = await git.readObject({
        fs: publishFs,
        dir,
        oid,
        cache,
        format: "content",
      });
      if (!(object.object instanceof Uint8Array)) {
        throw new Error(`object ${oid} did not read as bytes`);
      }
      await writer.write(
        oid,
        await looseObjectBytes(object.type, object.object),
      );
    };
    // A failure stops new writes, and the publish waits for the ones in
    // flight, so its cleanup of the staging directory does not race them.
    const worker = async () => {
      while (!failed && next < oids.length) {
        const oid = oids[next++];
        if (oid === undefined) continue;
        try {
          await writeObject(oid);
        } catch (cause) {
          failed = true;
          throw cause;
        }
      }
    };
    const results = await Promise.allSettled(
      Array.from({ length: Math.min(UNPACK_CONCURRENCY, oids.length) }, worker),
    );
    const failure = results.find(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );
    if (failure !== undefined) throw failure.reason;
    await writer.finish();
  }

  async function packCount(): Promise<number> {
    const names = await runtime.fs.readdir(packDir);
    return names.filter((name) => name.endsWith(".idx")).length;
  }

  async function movePack(): Promise<void> {
    const packPath = runtime.path.join(packDir, packName);
    const idxPath = runtime.path.join(packDir, idxName);
    await syncPath(runtime, stagingPackPath);
    await syncPath(runtime, stagingIdxPath);
    await runtime.fs.rename(stagingPackPath, packPath);
    try {
      await syncPath(runtime, packDir);
      await runtime.fs.rename(stagingIdxPath, idxPath);
    } catch (cause) {
      // No reader finds a pack without its index, so a publish that fails
      // before the index lands takes the pack back out.
      await runtime.fs.remove(packPath, { force: true }).catch(() => undefined);
      throw cause;
    }
    // Readers can list the pack from here on, so a failed sync leaves the
    // pair in place; the publish forgets the sync record, so the next
    // `syncObjectsAndRefs` here syncs it. Naming `objects/pack/` makes this
    // one sync it and flush, even when the record has nothing else unsynced.
    await syncObjectsAndRefs(runtime, dir, [packDir]);
  }

  return {
    oids,
    fs: viewFs,
    async publish(cache = {}) {
      if (state !== "staged") {
        throw new Error(
          `staged pack "${transferId}" in ${dir} cannot be published: it is ${state}`,
        );
      }
      state = "publishing";
      try {
        if (
          oids.length >= UNPACK_LIMIT &&
          (await packCount()) < PACK_COUNT_LIMIT
        ) {
          await movePack();
        } else {
          await writeLooseObjects(cache);
        }
      } catch (err) {
        forgetSyncRecord(runtime, dir);
        state = "discarded";
        await runtime.fs
          .remove(stagingDir, { recursive: true, force: true })
          .catch(() => undefined);
        throw err;
      }
      state = "published";
      // Every object is in place, so a failed cleanup only leaks disk
      // until `removeStagedPacks` runs.
      await runtime.fs
        .remove(stagingDir, { recursive: true, force: true })
        .catch(() => undefined);
    },
    async discard() {
      if (state !== "staged") return;
      // Set first, so the view stops showing a pack whose files the
      // removal below can leave half gone.
      state = "discarded";
      await runtime.fs.remove(stagingDir, { recursive: true, force: true });
    },
  };
}

/**
 * Remove every staged pack under `dir`, and the temporary files of any
 * unfinished durable object or ref write. Each lives only as long as the
 * receive or write that made it, so the caller must know that none is
 * live for `dir`, for example on the first receive for `dir` after a
 * process start.
 */
export async function removeStagedPacks(
  runtime: StorageRuntime,
  dir: string,
): Promise<void> {
  await runtime.fs.remove(stagingRootOf(runtime, dir), {
    recursive: true,
    force: true,
  });
  const gitdir = runtime.path.join(dir, ".git");
  const names = await runtime.fs.readdir(gitdir).catch((cause: unknown) => {
    if (hasCode(cause) && cause.code === "ENOENT") return [];
    throw cause;
  });
  for (const name of names) {
    if (name.startsWith(REF_WRITE_PREFIX)) {
      await runtime.fs.remove(runtime.path.join(gitdir, name), { force: true });
    }
  }
}

/**
 * Remove each pack index under `dir` whose pack is gone, and return the
 * paths removed. A publish syncs a pack's rename before it renames the
 * index, so such an index comes from a lost file, or from a pack rename a
 * crash lost before anything synced it. A read of an object that such an
 * index names fails as an internal error instead of finding no object, so
 * a caller removes the index before it reads the repository. The objects
 * it names went with the pack.
 */
export async function removeOrphanPackIndexes(
  runtime: StorageRuntime,
  dir: string,
): Promise<string[]> {
  const packDir = runtime.path.join(dir, ".git", "objects", "pack");
  let files: string[];
  try {
    files = await runtime.fs.readdir(packDir);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") return [];
    throw cause;
  }
  const listed = new Set(files);
  const removed: string[] = [];
  for (const file of files) {
    if (!file.endsWith(".idx")) continue;
    if (listed.has(`${file.slice(0, -".idx".length)}.pack`)) continue;
    const filepath = runtime.path.join(packDir, file);
    await runtime.fs.remove(filepath, { force: true });
    removed.push(filepath);
  }
  return removed;
}
