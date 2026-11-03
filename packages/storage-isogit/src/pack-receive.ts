import git, { type FsClient } from "isomorphic-git";
import { promiseFsMethods, type StorageRuntime } from "./runtime";
import { hasCode } from "@intx/types/has-code";

const SAFE_PATH_SEGMENT = /^[a-zA-Z0-9_-]+$/;

// Sibling of objects/pack/. Both directories share the storage namespace in
// which the runtime guarantees atomic rename operations.
const STAGING_DIR_NAME = "pack-staging";

function stagingRootOf(runtime: StorageRuntime, dir: string): string {
  return runtime.path.join(dir, ".git", "objects", STAGING_DIR_NAME);
}

// The names and paths a staged pack has in objects/pack/ once it is
// published. The staged view shows the pack under the same names.
function publishedPackPaths(
  runtime: StorageRuntime,
  dir: string,
  transferId: string,
): {
  packName: string;
  idxName: string;
  finalPackPath: string;
  finalIdxPath: string;
} {
  const packDir = runtime.path.join(dir, ".git", "objects", "pack");
  const packName = `pack-recv-${transferId}.pack`;
  const idxName = `pack-recv-${transferId}.idx`;
  return {
    packName,
    idxName,
    finalPackPath: runtime.path.join(packDir, packName),
    finalIdxPath: runtime.path.join(packDir, idxName),
  };
}

async function pathExists(
  runtime: StorageRuntime,
  filepath: string,
): Promise<boolean> {
  try {
    await runtime.fs.access(filepath);
    return true;
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") return false;
    throw cause;
  }
}

/**
 * A received pack written and indexed outside `objects/pack/`, which
 * readers that use the repo's own filesystem do not find. `fs` is an
 * isomorphic-git client that also finds the staged objects, so a caller
 * can validate the pack before any other reader can see it. `publish`
 * makes the pack visible to every reader; `discard` removes it.
 *
 * `fs` shows the staged `.idx` and `.pack` under the names `publish`
 * gives them. isomorphic-git keys a loaded pack by its `.idx` path, so a
 * cache that loaded the pack through `fs` reuses it after `publish`.
 * Once `publish` starts or `discard` runs, `fs` passes every call
 * through.
 *
 * `discard` is safe at any time. Once `publish` has started, `discard`
 * does nothing, so a caller can discard on every path without a check.
 * `publish` runs once, and only on a pack that is not discarded.
 */
export type StagedPack = {
  readonly oids: readonly string[];
  readonly fs: FsClient;
  publish(): Promise<void>;
  discard(): Promise<void>;
};

/**
 * Stage a packfile for one receive and return its handle.
 *
 * # Race being addressed
 *
 * `git.indexPack` writes the `.idx` file with a single non-atomic
 * `fs.write` (see `_indexPack` at
 * `node_modules/isomorphic-git/index.cjs:11953`). Pack discovery walks
 * `objects/pack/` enumerating `*.idx` files non-recursively (see
 * `readObjectPacked` at `node_modules/isomorphic-git/index.cjs:3394-3398`
 * and the sibling enumerator at `:9286-9290`) and, for each one,
 * derives the matching `.pack` filename and reads it. A concurrent
 * reader landing during the `.idx` write can observe either a truncated
 * buffer (surfacing as `TypeError: null is not an object (evaluating
 * 'this.buffer.slice')` inside iso-git's `BufferCursor`) or an `.idx`
 * that does not yet list the OID being looked for (surfacing as
 * `NotFoundError`).
 *
 * # Invariant maintained
 *
 * At every point at which an `.idx` is visible to a reader's directory
 * scan of `objects/pack/`, the `.pack` it references is fully written
 * and findable under the filename the reader will derive from the
 * `.idx`. Equivalently: readers see either no new pack at all, or the
 * fully-published `.pack` + `.idx` pair. There is no observable
 * intermediate state. A caller validates the pack through the staged
 * view, so a pack it rejects is never visible to other readers.
 *
 * # Strategy: stage outside the scanned directory
 *
 * The mid-write race is fundamental to `indexPack` writing its output
 * file in-place. The only way to keep readers from observing it is to
 * write the `.idx` somewhere readers do not scan. iso-git's enumerator
 * scans only the literal path `<gitdir>/objects/pack` and only
 * non-recursively (verified at the source line refs above), so any
 * sibling directory under `objects/` is invisible to discovery.
 *
 * We stage in `objects/pack-staging/<transferId>/`. The per-transfer
 * subdirectory keeps concurrent receives' temp pairs isolated from each
 * other. It shares a storage namespace with the final `objects/pack/`
 * directory, so the runtime's atomic rename contract applies between them.
 *
 * # Sequence (numbered for cross-reference with cleanup contract)
 *
 *   1. Create `objects/pack-staging/<transferId>/` and write the pack
 *      bytes to `pack.pack` inside it. Readers scanning
 *      `objects/pack/` do not see this file (different directory).
 *
 *   2. Run `git.indexPack` against the staging path — iso-git derives
 *      the `.idx` filename from the `.pack` filename and writes
 *      `pack.idx` next to it inside the staging directory. The
 *      non-atomic `.idx` write happens here but the file is in the
 *      staging directory, so concurrent readers scanning
 *      `objects/pack/` cannot observe the in-progress write.
 *      `stagePack` returns after this step.
 *
 *   3. `publish` (transitions readers from "no new pack" to "new pack
 *      visible"):
 *
 *        a. Atomically rename staging `.pack` -> final `.pack`. The new
 *           `.pack` now exists in `objects/pack/`. Readers scanning
 *           for `.idx` files still do not see the new pack (no `.idx`
 *           for it in `objects/pack/` yet). The staging `.pack`
 *           directory entry vanishes.
 *
 *        b. Atomically rename staging `.idx` -> final `.idx`. The new `.idx`
 *           appears atomically in `objects/pack/`; readers' next
 *           directory scan finds it and resolves the `.pack` written
 *           in 3a.
 *
 *        c. Remove the now-empty staging directory. `publish` does not
 *           flush: on a runtime that defines a flush, a caller that writes
 *           a ref to the pack flushes first.
 *
 *   4. On any throw in steps 1-2, and on `discard`, recursively remove
 *      the staging directory. Forced recursive removal tolerates
 *      partial states (e.g. throw mid-write before `.idx` exists, or
 *      throw inside `indexPack`).
 *
 * # Cleanup contract
 *
 * After `publish` returns, no staging files remain. After a throw in
 * steps 1-2 or after `discard`, the staging directory and any files
 * inside it are removed. A throw partway through publish (between 3a and
 * 3b) removes the staging directory and the orphan `.pack` in
 * `objects/pack/`, without masking the original publish error. A crash
 * leaves only a staging directory, which no reader scans. It stays until
 * a caller runs `removeStagedPacks` on the repository.
 *
 * # Scope
 *
 * This helper does not own the lock that serializes concurrent
 * receives on the same `dir`. Callers serialize at a higher layer
 * (e.g. `withRepoLock` in
 * `packages/hub-sessions/src/repo-store/store.ts`). The atomicity
 * guarantee here is against arbitrary `dir`-level isomorphic-git reads
 * issued from any code that shares the filesystem — including code
 * that does not consult the caller's lock (e.g. tests that call
 * `git.readCommit` directly).
 *
 * # Forward compatibility
 *
 * If isomorphic-git's `_indexPack` becomes atomic upstream (writes the
 * `.idx` via temp+rename internally), or if pack discovery moves to a
 * different mechanism, the publish half of this dance becomes redundant.
 * Validation before publish still needs a directory readers do not scan.
 * The cited source-line references above are the verification anchor
 * for the next reader deciding whether the dance is still needed.
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

  const packDir = runtime.path.join(dir, ".git", "objects", "pack");
  const stagingRoot = stagingRootOf(runtime, dir);
  const stagingDir = runtime.path.join(stagingRoot, transferId);

  const stagingPackPath = runtime.path.join(stagingDir, "pack.pack");
  const stagingIdxPath = stagingPackPath.replace(/\.pack$/, ".idx");
  const { packName, idxName, finalPackPath, finalIdxPath } = publishedPackPaths(
    runtime,
    dir,
    transferId,
  );

  await runtime.fs.mkdir(packDir, { recursive: true });
  await runtime.fs.mkdir(stagingRoot, { recursive: true });
  if (
    (await pathExists(runtime, finalPackPath)) ||
    (await pathExists(runtime, finalIdxPath))
  ) {
    throw new Error(
      `transferId "${transferId}" already published in ${dir}; callers must guarantee transferId uniqueness across all historical receives`,
    );
  }
  try {
    await runtime.fs.mkdir(stagingDir);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "EEXIST") {
      throw new Error(
        `transferId "${transferId}" already published or in progress in ${dir}; callers must guarantee transferId uniqueness across all historical receives`,
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
    // Step 1: write the pack bytes to the staging directory.
    await runtime.fs.writeFile(stagingPackPath, pack);
    // Step 2: index the pack. iso-git writes the .idx next to the
    // .pack — both stay inside the staging directory, invisible to
    // any reader scanning objects/pack/.
    const result = await git.indexPack({
      fs: runtime.fs.git,
      dir,
      filepath: stagingFilepath,
    });
    oids = result.oids;
  } catch (err) {
    // Step 4: cleanup before publish. Recursive rm handles every
    // partial state (write succeeded but indexPack threw, etc.). The
    // rm is wrapped so a secondary cleanup failure (permissions, I/O)
    // does not mask the original error.
    await runtime.fs
      .remove(stagingDir, { recursive: true, force: true })
      .catch(() => undefined);
    throw err;
  }

  let state: "staged" | "publishing" | "published" | "discarded" = "staged";

  // isomorphic-git joins the gitdir and `objects/pack` for its listing
  // but concatenates them for its reads, and the gitdir can be
  // relative, so paths compare after `resolve`.
  const resolvedPackDir = runtime.path.resolve(packDir);
  const stagedPathOf = new Map([
    [runtime.path.resolve(finalIdxPath), stagingIdxPath],
    [runtime.path.resolve(finalPackPath), stagingPackPath],
  ]);
  const base = promiseFsMethods(runtime.fs.git);
  // isomorphic-git binds `promises` only when it is an enumerable own
  // property; otherwise it binds the client as a callback filesystem.
  const viewFs: FsClient = {
    promises: {
      ...base,
      readdir: async (...args: unknown[]) => {
        const entries = await base.readdir(...args);
        const [filepath] = args;
        if (
          state !== "staged" ||
          typeof filepath !== "string" ||
          runtime.path.resolve(filepath) !== resolvedPackDir ||
          !Array.isArray(entries)
        ) {
          return entries;
        }
        return [...entries, idxName, packName];
      },
      readFile: (...args: unknown[]) => {
        const [filepath, ...rest] = args;
        const stagedPath =
          state === "staged" && typeof filepath === "string"
            ? stagedPathOf.get(runtime.path.resolve(filepath))
            : undefined;
        return stagedPath === undefined
          ? base.readFile(...args)
          : base.readFile(stagedPath, ...rest);
      },
    },
  };

  return {
    oids,
    fs: viewFs,
    async publish() {
      if (state !== "staged") {
        throw new Error(
          `staged pack "${transferId}" in ${dir} cannot be published: it is ${state}`,
        );
      }
      state = "publishing";
      // Step 3: atomic publish. The .pack rename (3a) and the .idx
      // rename (3b) are the two operations that determine
      // externally-observable state; a failure inside this block means
      // the publish is partial or absent, and the recovery rms below
      // clear the half-published pack from objects/pack/.
      // Staging-directory cleanup is NOT part of this block — see below.
      try {
        await runtime.fs.rename(stagingPackPath, finalPackPath); // 3a
        await runtime.fs.rename(stagingIdxPath, finalIdxPath); // 3b
      } catch (err) {
        state = "discarded";
        // Partial-publish recovery: if 3a succeeded and 3b failed we
        // leave an unindexed .pack in objects/pack/. The rm of
        // finalPackPath clears it; finalIdxPath cannot exist here (3b
        // never completed), so no rm is needed for it. Each recovery rm
        // is wrapped so a secondary failure (permissions, I/O) does not
        // mask the original publish error — the caller needs to see the
        // publish failure, not whatever the cleanup tripped on.
        await runtime.fs
          .remove(stagingDir, { recursive: true, force: true })
          .catch(() => undefined);
        await runtime.fs
          .remove(finalPackPath, { force: true })
          .catch(() => undefined);
        throw err;
      }
      state = "published";

      // Step 3c: staging-directory cleanup runs only after the pack is
      // fully published. A failure here is benign from the caller's
      // perspective — the pack is observable to readers, the publish
      // succeeded — so the rm is wrapped and swallowed. It MUST NOT live
      // inside the publish try/catch: doing so would let an
      // EACCES/EBUSY/EIO on the staging rm trigger the recovery rms above
      // and delete a pack that was already published and (potentially)
      // already read by concurrent observers. A leftover staging
      // directory on rm failure leaks disk until `removeStagedPacks`
      // runs; that is strictly preferable to retroactively destroying a
      // successful publish.
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
 * Remove every staged pack under `dir`. A staged pack lives only as long
 * as the receive that holds its handle, so the caller must know that no
 * handle for `dir` is live, for example on the first receive for `dir`
 * after a process start.
 */
export async function removeStagedPacks(
  runtime: StorageRuntime,
  dir: string,
): Promise<void> {
  await runtime.fs.remove(stagingRootOf(runtime, dir), {
    recursive: true,
    force: true,
  });
}

/**
 * Remove each pack index under `dir` whose pack is gone, and return the
 * paths removed. A publish renames the pack before its index, so no write
 * leaves an index without its pack; a lost rename or a lost file does. A read of an object that
 * such an index names fails as an internal error instead of finding no
 * object, so a caller removes the index before it reads the repository.
 * The objects it names went with the pack.
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
