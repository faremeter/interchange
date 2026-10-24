import git, { type FsClient } from "isomorphic-git";
import { readRawObject } from "./isogit-helpers";
import { withRepoDirLock } from "./repo-lock";
import {
  decodeUTF8,
  flushRuntime,
  promiseFsMethods,
  type StorageRuntime,
} from "./runtime";
import { hasCode } from "@intx/types/has-code";

/**
 * Verifies the signature embedded in a git commit object.
 *
 * Callers bind this to their verification implementation (e.g.
 * verifySSHSignature with the hub's public key). The storage layer does
 * not own key material.
 *
 * Returns true when the signature is valid. Should throw on malformed
 * input and return false on cryptographic failure.
 */
export type CommitVerifier = (
  payload: string,
  signature: string,
) => Promise<boolean>;

/**
 * Strip the gpgsig header from a raw git commit object, producing the
 * payload that was originally signed.
 *
 * isomorphic-git's readCommit().payload uses withoutSignature() which is
 * hardcoded to look for PGP armor markers. SSH signatures use different
 * markers, so the built-in reconstruction is wrong. This function works
 * with any signature format by parsing the header structure directly.
 */
function stripGpgsig(raw: string): string {
  const gpgsigIdx = raw.indexOf("\ngpgsig ");
  if (gpgsigIdx === -1) return raw;

  // The gpgsig header spans from "\ngpgsig " to the next header line that
  // does not start with a space. Continuation lines in git headers are
  // indented with a single leading space.
  let endIdx = gpgsigIdx + 1;
  while (endIdx < raw.length) {
    const nlIdx = raw.indexOf("\n", endIdx);
    if (nlIdx === -1) break;
    endIdx = nlIdx + 1;
    if (endIdx < raw.length && raw[endIdx] !== " ") break;
  }

  return raw.substring(0, gpgsigIdx) + "\n" + raw.substring(endIdx);
}

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

type TreeEntry = {
  type: "blob" | "tree" | "commit";
  mode: string;
  path: string;
  oid: string;
};

async function topLevelNames(
  gitFs: FsClient,
  dir: string,
  oid: string,
): Promise<Set<string>> {
  const { tree } = await git.readTree({ fs: gitFs, dir, oid });
  return new Set(tree.map((e) => e.path));
}

/**
 * Remove stale working-tree content and write the commit's tree to disk.
 *
 * Derives the set of deploy-managed top-level entries from the union of the
 * old and new commit trees, removes those entries, then writes the new tree.
 * Paths that never appear in any commit tree (e.g. .git, state, keys) are
 * never touched.
 *
 * NOTE: The rm-then-write sequence is not atomic. If writeTree fails after rm
 * succeeds (e.g. disk full), the working tree will be missing the cleared
 * paths. The ref is not updated in that case (caller writes ref after this
 * function returns), so a restart will re-read from the prior commit, but the
 * working tree will be stale until the next successful applyPack.
 *
 * Objects are read through `gitFs`, so a caller can check out a pack
 * that is still staged.
 */
async function checkoutTree(
  runtime: StorageRuntime,
  gitFs: FsClient,
  dir: string,
  commitSha: string,
  ref: string,
): Promise<void> {
  const { commit } = await git.readCommit({
    fs: gitFs,
    dir,
    oid: commitSha,
  });
  const { tree } = await git.readTree({
    fs: gitFs,
    dir,
    oid: commit.tree,
  });

  // Collect top-level names managed by deploy trees (new + previous).
  const managed = new Set(tree.map((e) => e.path));

  const prevSha = await git
    .resolveRef({ fs: gitFs, dir, ref })
    .catch(() => null);
  if (prevSha !== null) {
    const { commit: prev } = await git.readCommit({
      fs: gitFs,
      dir,
      oid: prevSha,
    });
    for (const name of await topLevelNames(gitFs, dir, prev.tree)) {
      managed.add(name);
    }
  }

  const existing = await runtime.fs.readdir(dir);
  for (const name of existing) {
    if (!managed.has(name)) continue;
    await runtime.fs.remove(runtime.path.join(dir, name), {
      recursive: true,
      force: true,
    });
  }

  await writeTreeEntries(runtime, gitFs, dir, dir, tree);
}

async function writeTreeEntries(
  runtime: StorageRuntime,
  gitFs: FsClient,
  repoDir: string,
  targetDir: string,
  entries: TreeEntry[],
): Promise<void> {
  for (const entry of entries) {
    const entryPath = runtime.path.join(targetDir, entry.path);
    if (entry.type === "tree") {
      await runtime.fs.mkdir(entryPath, { recursive: true });
      const { tree } = await git.readTree({
        fs: gitFs,
        dir: repoDir,
        oid: entry.oid,
      });
      await writeTreeEntries(runtime, gitFs, repoDir, entryPath, tree);
    } else if (entry.type === "blob") {
      const { blob } = await git.readBlob({
        fs: gitFs,
        dir: repoDir,
        oid: entry.oid,
      });
      await runtime.fs.writeFile(entryPath, blob, {
        mode: entry.mode === "100755" ? 0o755 : 0o644,
      });
    }
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
 *           flush: a caller that writes a ref to the pack flushes first,
 *           and one that publishes as part of a larger change (GC)
 *           flushes at its own commit point.
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
 * Apply a git packfile to a repository, check out the working tree, and
 * update the ref.
 *
 * Stages the pack and creates its .idx index outside .git/objects/pack/,
 * checks out the tree to the working directory through the staged view,
 * publishes the pack into .git/objects/pack/ (the standard location for
 * git packfiles), then updates `ref` to point at `expectedSha`. The ref
 * is written last so it never points at a commit whose working tree has
 * not been materialized.
 *
 * When `verifyCommit` is provided, the commit's embedded signature is
 * verified before the working tree is materialized. Throws with a message
 * prefixed by `"signature_unsigned"` if the commit has no signature, or
 * `"signature_invalid"` if verification fails. Only omit `verifyCommit`
 * for state packs that follow their own signing model.
 *
 * Throws if the expected commit is not found in the pack. A pack that
 * fails a check never enters .git/objects/pack/.
 *
 * The caller is responsible for ensuring `dir` is an initialized git repo.
 */
export async function applyPack(
  runtime: StorageRuntime,
  dir: string,
  pack: Uint8Array,
  ref: string,
  expectedSha: string,
  transferId: string,
  verifyCommit?: CommitVerifier,
): Promise<void> {
  // The deploy apply shares the agent repo's object store with the
  // reactor's context commits, the mail-audit commits, and GC. Hold the
  // per-directory lock across the stage, validation, checkout, publish,
  // and ref write so none of them interleave with this apply.
  await withRepoDirLock(runtime, dir, async () => {
    const staged = await stagePack(runtime, dir, pack, transferId);

    // Sidecar `applyPack` is the last line of defence against a
    // compromised hub or transport. Every check and the checkout read
    // the pack through the staged view, so a rejected pack is discarded
    // without ever entering objects/pack/, and a flood of
    // rejected-signature packs cannot accumulate there.
    try {
      if (!staged.oids.includes(expectedSha)) {
        throw new Error(
          `sha_mismatch: expected commit ${expectedSha} not found in pack`,
        );
      }

      if (verifyCommit !== undefined) {
        const { commit } = await git.readCommit({
          fs: staged.fs,
          dir,
          oid: expectedSha,
        });
        if (commit.gpgsig === undefined) {
          throw new Error(
            `signature_unsigned: commit ${expectedSha} has no signature`,
          );
        }

        // Reconstruct the signing payload from the raw object bytes.
        // readCommit().payload is unreliable for SSH signatures because
        // isogit's withoutSignature() only handles PGP armor markers.
        const { object: rawBytes } = await readRawObject(
          staged.fs,
          dir,
          expectedSha,
        );
        const payload = stripGpgsig(decodeUTF8(rawBytes));

        if (!(await verifyCommit(payload, commit.gpgsig))) {
          throw new Error(
            `signature_invalid: commit ${expectedSha} signature verification failed`,
          );
        }
      }

      // Ref is written last so it never references a commit whose working
      // tree has not been materialized.
      await checkoutTree(runtime, staged.fs, dir, expectedSha, ref);
    } catch (err) {
      await staged.discard().catch(() => undefined);
      throw err;
    }

    // Make the accepted pack durable before ref promotion. A later
    // failure may leave the promotion outcome uncertain, but every
    // possible ref value will resolve to a durable object store.
    await staged.publish();
    await flushRuntime(runtime);

    // Do not remove the pack after promotion begins: writeRef or the
    // final flush can fail after making the new ref observable.
    await git.writeRef({
      fs: runtime.fs.git,
      dir,
      ref,
      value: expectedSha,
      force: true,
    });
    await flushRuntime(runtime);
  });
}
