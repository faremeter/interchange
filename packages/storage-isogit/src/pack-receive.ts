import git from "isomorphic-git";
import { readRawObject } from "./isogit-helpers";
import { withRepoDirLock } from "./repo-lock";
import { decodeUTF8, flushRuntime, type StorageRuntime } from "./runtime";
import { hasCode } from "@intx/types";

/**
 * Verifies the signature embedded in a git commit object. Callers bind
 * this to their verification implementation (e.g. verifySSHSignature with
 * the hub's public key); the storage layer does not own key material.
 * Returns true when valid, throws on malformed input, false on crypto
 * failure.
 */
export type CommitVerifier = (
  payload: string,
  signature: string,
) => Promise<boolean>;

export type TreeValidatorResult = true | { ok: false; reason: string };

/**
 * Validates the contents of a commit's tree. Callers bind this to their
 * policy (e.g. state packs only contain entries named "state"). Return
 * `true` to accept; `{ ok: false, reason }` rejects with a reason the
 * substrate splices into its thrown `path_violation:` message. `false` is
 * accepted for back-compat as an opaque rejection.
 *
 * `topLevelPaths` lists names directly under the tree root. `readBlob`
 * reads any blob by repo-root-relative POSIX path. `listDir` enumerates
 * names directly under a tree-root-relative POSIX directory path (empty
 * string lists the root). Validators that only need path-level checks
 * can ignore `readBlob` and `listDir`.
 */
export type TreeValidator = (
  topLevelPaths: string[],
  readBlob: (path: string) => Promise<Uint8Array>,
  listDir: (path: string) => Promise<string[]>,
) => boolean | TreeValidatorResult | Promise<boolean | TreeValidatorResult>;

/**
 * Strip the gpgsig header from a raw git commit object, producing the
 * payload that was originally signed. isomorphic-git's withoutSignature()
 * is hardcoded to PGP armor markers, so its payload reconstruction is
 * wrong for SSH signatures; this parses the header structure directly.
 */
function stripGpgsig(raw: string): string {
  const gpgsigIdx = raw.indexOf("\ngpgsig ");
  if (gpgsigIdx === -1) return raw;

  // Header spans from "\ngpgsig " to the next header line that does not
  // start with a space; continuation lines are indented with one space.
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

// Sibling of objects/pack/; the runtime guarantees atomic renames within
// this storage namespace.
const STAGING_DIR_NAME = "pack-staging";

/**
 * Derive the final paths a publishPackAtomically call writes for a given
 * `transferId`, so callers that reject a published pack can unpublish it
 * without re-deriving the naming convention.
 */
function publishedPackPaths(
  runtime: StorageRuntime,
  dir: string,
  transferId: string,
): { finalPackPath: string; finalIdxPath: string } {
  const packDir = runtime.path.join(dir, ".git", "objects", "pack");
  const finalPackPath = runtime.path.join(
    packDir,
    `pack-recv-${transferId}.pack`,
  );
  const finalIdxPath = finalPackPath.replace(/\.pack$/, ".idx");
  return { finalPackPath, finalIdxPath };
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
 * Remove a previously-published `.pack` + `.idx` pair from objects/pack/,
 * used when post-publish validation fails. A reader that already loaded
 * the rejected pack can finish against those bytes; subsequent calls will
 * not discover the removed pair (iso-git retains no pack resources
 * between calls). Each removal is catch-wrapped so a secondary failure
 * does not mask the rejection the caller is about to throw.
 */
async function unpublishPack(
  runtime: StorageRuntime,
  dir: string,
  transferId: string,
): Promise<void> {
  const { finalPackPath, finalIdxPath } = publishedPackPaths(
    runtime,
    dir,
    transferId,
  );
  await runtime.fs
    .remove(finalPackPath, { force: true })
    .catch(() => undefined);
  await runtime.fs.remove(finalIdxPath, { force: true }).catch(() => undefined);
}

type TreeEntry = {
  type: "blob" | "tree" | "commit";
  mode: string;
  path: string;
  oid: string;
};

async function topLevelNames(
  runtime: StorageRuntime,
  dir: string,
  oid: string,
): Promise<Set<string>> {
  const { tree } = await git.readTree({ fs: runtime.fs.git, dir, oid });
  return new Set(tree.map((e) => e.path));
}

/**
 * Remove stale working-tree content and write the commit's tree to disk.
 * The managed top-level entries are the union of the old and new commit
 * trees; paths never in any commit tree (e.g. .git, state, keys) are
 * untouched.
 *
 * NOTE: rm-then-write is not atomic. If writeTree fails after rm succeeds
 * (e.g. disk full), the working tree is missing the cleared paths. The ref
 * is not updated in that case (the caller writes it after this returns), so
 * a restart re-reads from the prior commit and the tree is stale until the
 * next successful applyPack.
 */
async function checkoutTree(
  runtime: StorageRuntime,
  dir: string,
  commitSha: string,
  ref: string,
): Promise<void> {
  const { commit } = await git.readCommit({
    fs: runtime.fs.git,
    dir,
    oid: commitSha,
  });
  const { tree } = await git.readTree({
    fs: runtime.fs.git,
    dir,
    oid: commit.tree,
  });

  // Collect top-level names managed by deploy trees (new + previous).
  const managed = new Set(tree.map((e) => e.path));

  const prevSha = await git
    .resolveRef({ fs: runtime.fs.git, dir, ref })
    .catch(() => null);
  if (prevSha !== null) {
    const { commit: prev } = await git.readCommit({
      fs: runtime.fs.git,
      dir,
      oid: prevSha,
    });
    for (const name of await topLevelNames(runtime, dir, prev.tree)) {
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

  await writeTreeEntries(runtime, dir, dir, tree);
}

async function writeTreeEntries(
  runtime: StorageRuntime,
  repoDir: string,
  targetDir: string,
  entries: TreeEntry[],
): Promise<void> {
  for (const entry of entries) {
    const entryPath = runtime.path.join(targetDir, entry.path);
    if (entry.type === "tree") {
      await runtime.fs.mkdir(entryPath, { recursive: true });
      const { tree } = await git.readTree({
        fs: runtime.fs.git,
        dir: repoDir,
        oid: entry.oid,
      });
      await writeTreeEntries(runtime, repoDir, entryPath, tree);
    } else if (entry.type === "blob") {
      const { blob } = await git.readBlob({
        fs: runtime.fs.git,
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
 * Index a packfile and update a ref without materializing the working
 * tree; used by the hub to store state packs where only object history
 * matters. When `validateTree` is provided, the commit's top-level tree
 * entries are checked after indexing but before the ref is promoted;
 * rejection throws with a `"path_violation"` prefix.
 *
 * Returns the SHA the ref pointed at before this call (or `null` if the
 * ref did not exist). `expectedOldSha` enforces a compare-and-set: the
 * current ref value is read after indexing and compared, aborting with
 * `non_fast_forward:` on mismatch. Pass a SHA string to require the ref
 * points there; `null` to require it does not yet exist. The caller is
 * responsible for serializing concurrent updates to the same ref; this
 * primitive enforces the CAS check but does not own the lock.
 */
/**
 * Atomically publish a packfile into a git repository's pack directory.
 *
 * Race: `git.indexPack` writes the `.idx` with a single non-atomic
 * `fs.write` (`_indexPack` at index.cjs:11953). Pack discovery scans
 * `objects/pack/` non-recursively for `*.idx` files (`readObjectPacked`
 * at index.cjs:3394-3398 and the sibling enumerator at :9286-9290) and
 * derives the matching `.pack` from each. A reader landing mid-`.idx`
 * write can see a truncated buffer or an `.idx` that does not list the
 * OID it wants.
 *
 * Invariant: whenever an `.idx` is visible to a reader's scan, the
 * `.pack` it references is fully written under the derived filename.
 * Readers see either no new pack or the complete pair; no intermediate
 * state.
 *
 * Strategy: write the `.idx` where readers do not scan. iso-git scans
 * only the literal path `<gitdir>/objects/pack`, non-recursively, so
 * any sibling directory under `objects/` is invisible to discovery. We
 * stage in `objects/pack-staging/<transferId>/`; the per-transfer
 * subdirectory isolates concurrent receives, and the directory shares
 * the storage namespace with `objects/pack/`, so the runtime's atomic
 * rename contract applies between them.
 *
 * Sequence: 1) write the pack bytes to the staging directory; 2) run
 * `git.indexPack` against the staging path (the non-atomic `.idx` write
 * happens here, unobserved); 3) atomic publish -- a) rename staging
 * `.pack` to final `.pack`, b) rename staging `.idx` to final `.idx`
 * (this is the moment readers transition to the new pair), c) remove
 * the staging directory; 4) on any throw before publish completes,
 * recursively remove the staging directory.
 *
 * Cleanup contract: on success, no staging files remain. On throw before
 * publish, the staging directory is removed. A throw between 3a and 3b
 * leaves an unindexed `.pack` in `objects/pack/`; harmless because
 * iso-git ignores `.pack` files with no matching `.idx` (verified at
 * index.cjs:3394-3398), and the recovery path removes the orphan
 * without masking the original error.
 *
 * Validation timing: callers inspecting the pack's contents
 * (`git.readCommit`, `git.readTree`, signature verification) must do so
 * after this returns, because pack discovery does not find the pack
 * until step 3. Callers that reject the published pack call
 * `unpublishPack` to remove the pair; a concurrent reader that already
 * loaded it can finish against those bytes.
 *
 * Scope: this helper does not own the lock serializing concurrent
 * receives on the same `dir` (callers do, e.g. `withRepoLock`); its
 * guarantee covers arbitrary `dir`-level isomorphic-git reads from any
 * code sharing the filesystem, including code that ignores the lock
 * (e.g. tests calling `git.readCommit` directly).
 *
 * Forward compatibility: if isomorphic-git's `_indexPack` becomes
 * atomic upstream or pack discovery changes, this staging dance can
 * collapse back to writing directly into `objects/pack/`; the cited
 * line refs are the anchor for that decision.
 */
export async function publishPackAtomically(
  runtime: StorageRuntime,
  dir: string,
  pack: Uint8Array,
  transferId: string,
): Promise<string[]> {
  if (!SAFE_PATH_SEGMENT.test(transferId)) {
    throw new Error(
      `transferId contains unsafe characters: ${JSON.stringify(transferId)}`,
    );
  }

  const packDir = runtime.path.join(dir, ".git", "objects", "pack");
  const stagingRoot = runtime.path.join(
    dir,
    ".git",
    "objects",
    STAGING_DIR_NAME,
  );
  const stagingDir = runtime.path.join(stagingRoot, transferId);

  const stagingPackPath = runtime.path.join(stagingDir, "pack.pack");
  const stagingIdxPath = stagingPackPath.replace(/\.pack$/, ".idx");
  const { finalPackPath, finalIdxPath } = publishedPackPaths(
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
  // iso-git's indexPack takes a repo-root-relative filepath.
  const stagingFilepath = runtime.path.relative(dir, stagingPackPath);

  let oids: string[];
  try {
    // Steps 1-2: write the pack and index it, both inside the staging
    // directory, invisible to readers scanning objects/pack/.
    await runtime.fs.writeFile(stagingPackPath, pack);
    const result = await git.indexPack({
      fs: runtime.fs.git,
      dir,
      filepath: stagingFilepath,
    });
    oids = result.oids;
  } catch (err) {
    // Step 4: cleanup before publish; wrapped so a secondary cleanup
    // failure does not mask the original error.
    await runtime.fs
      .remove(stagingDir, { recursive: true, force: true })
      .catch(() => undefined);
    throw err;
  }

  // Step 3: atomic publish (3a pack rename, 3b idx rename). A failure
  // here means the publish is partial or absent; the recovery rms below
  // clear the half-published pack. Staging-directory cleanup is NOT part
  // of this block — see below.
  try {
    await runtime.fs.rename(stagingPackPath, finalPackPath); // 3a
    await runtime.fs.rename(stagingIdxPath, finalIdxPath); // 3b
  } catch (err) {
    // If 3a succeeded and 3b failed we leave an unindexed .pack in
    // objects/pack/; remove it (finalIdxPath cannot exist since 3b
    // never completed). Recovery rms are wrapped so a secondary
    // failure does not mask the publish error.
    await runtime.fs
      .remove(stagingDir, { recursive: true, force: true })
      .catch(() => undefined);
    await runtime.fs
      .remove(finalPackPath, { force: true })
      .catch(() => undefined);
    throw err;
  }

  // Step 3c: staging-directory cleanup runs only after the pack is
  // fully published, and the rm is wrapped and swallowed: a failure
  // here is benign, and it MUST NOT live inside the publish try/catch
  // or an EACCES/EBUSY/EIO on the staging rm would trigger the
  // recovery rms and delete an already-published (possibly already
  // read) pack. A leftover staging directory leaks disk until
  // external cleanup, which is preferable to destroying a successful
  // publish.
  await runtime.fs
    .remove(stagingDir, { recursive: true, force: true })
    .catch(() => undefined);

  return oids;
}

export async function receivePackObjects(
  runtime: StorageRuntime,
  dir: string,
  pack: Uint8Array,
  ref: string,
  expectedSha: string,
  transferId: string,
  expectedOldSha: string | null,
  validateTree?: TreeValidator,
): Promise<string | null> {
  const oids = await publishPackAtomically(runtime, dir, pack, transferId);
  let currentOldSha: string | null = null;

  // Post-publish validation runs inside a try so any rejection path
  // (sha mismatch, CAS non-fast-forward, tree validator) unpublishes
  // the pack before re-throwing. The immediate unpublish keeps a flood
  // of rejected packs from accumulating before write-path GC would
  // reclaim them.
  try {
    if (!oids.includes(expectedSha)) {
      throw new Error(
        `sha_mismatch: expected commit ${expectedSha} not found in pack`,
      );
    }

    currentOldSha = await git
      .resolveRef({ fs: runtime.fs.git, dir, ref })
      .catch(() => null);

    if (currentOldSha !== expectedOldSha) {
      const observed = currentOldSha === null ? "null" : currentOldSha;
      const expected = expectedOldSha === null ? "null" : expectedOldSha;
      throw new Error(
        `non_fast_forward: ref ${ref} expected ${expected} but found ${observed}`,
      );
    }

    if (validateTree !== undefined) {
      const { commit } = await git.readCommit({
        fs: runtime.fs.git,
        dir,
        oid: expectedSha,
      });
      const { tree } = await git.readTree({
        fs: runtime.fs.git,
        dir,
        oid: commit.tree,
      });
      // Surface every top-level entry — directories and files — to the
      // kind handler. The receivePack path used to filter to
      // directories only, which hid top-level files (e.g. an
      // `evil.exe` at the root) from handlers that reject anything
      // outside their allowlist; handlers distinguish file from
      // directory via `readBlob` / `listDir`.
      const topLevelPaths = tree.map((e) => e.path);
      const readBlob = async (relPath: string): Promise<Uint8Array> => {
        const segments = relPath.split("/");
        let currentTree = tree;
        for (let i = 0; i < segments.length - 1; i += 1) {
          const segment = segments[i];
          const entry = currentTree.find((e) => e.path === segment);
          if (entry === undefined || entry.type !== "tree") {
            throw new Error(
              `readBlob: path ${relPath} not found in commit ${expectedSha} tree`,
            );
          }
          const next = await git.readTree({
            fs: runtime.fs.git,
            dir,
            oid: entry.oid,
          });
          currentTree = next.tree;
        }
        const last = segments[segments.length - 1];
        const blobEntry = currentTree.find((e) => e.path === last);
        if (blobEntry === undefined || blobEntry.type !== "blob") {
          throw new Error(
            `readBlob: path ${relPath} not found in commit ${expectedSha} tree`,
          );
        }
        const { blob } = await git.readBlob({
          fs: runtime.fs.git,
          dir,
          oid: blobEntry.oid,
        });
        return blob;
      };
      const listDir = async (relPath: string): Promise<string[]> => {
        if (relPath === "") {
          return tree.map((e) => e.path);
        }
        let currentTree = tree;
        for (const segment of relPath.split("/")) {
          const entry = currentTree.find((e) => e.path === segment);
          if (entry === undefined || entry.type !== "tree") {
            throw new Error(
              `listDir: path ${relPath} is not a directory in commit ${expectedSha} tree`,
            );
          }
          const next = await git.readTree({
            fs: runtime.fs.git,
            dir,
            oid: entry.oid,
          });
          currentTree = next.tree;
        }
        return currentTree.map((e) => e.path);
      };
      const verdict = await validateTree(topLevelPaths, readBlob, listDir);
      if (verdict !== true) {
        const reason =
          typeof verdict === "object"
            ? verdict.reason
            : `commit ${expectedSha} tree contains disallowed paths: ${topLevelPaths.join(", ")}`;
        throw new Error(`path_violation: ${reason}`);
      }
    }

    // The pack must be durable before ref promotion begins: once writeRef
    // is attempted, an I/O error cannot prove whether the ref changed, so
    // later failures must retain the pack rather than risk a dangling ref.
    await flushRuntime(runtime);
  } catch (err) {
    await unpublishPack(runtime, dir, transferId);
    await flushRuntime(runtime);
    throw err;
  }

  await git.writeRef({
    fs: runtime.fs.git,
    dir,
    ref,
    value: expectedSha,
    force: true,
  });
  await flushRuntime(runtime);
  return currentOldSha;
}

/**
 * Apply a git packfile to a repository, check out the working tree, and
 * update the ref. Publishes the pack, creates the `.idx` via indexPack,
 * checks out the tree, then updates `ref` last so it never points at a
 * commit whose working tree has not been materialized.
 *
 * When `verifyCommit` is provided, the commit's embedded signature is
 * verified before materialization. Throws `"signature_unsigned"` if the
 * commit has no signature, `"signature_invalid"` if verification fails;
 * omit it only for state packs that follow their own signing model.
 * Throws if the expected commit is not in the pack.
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
  // The deploy apply shares the agent repo's object store with context
  // commits, mail-audit commits, and GC; hold the per-directory lock
  // across the whole apply so none interleave with it.
  await withRepoDirLock(runtime, dir, async () => {
    const oids = await publishPackAtomically(runtime, dir, pack, transferId);

    // Post-publish validation runs inside a try so any rejection path
    // (sha mismatch, missing signature, signature failure) unpublishes
    // the pack before re-throwing. Sidecar applyPack is the last line of
    // defence against a compromised hub or transport; the immediate
    // unpublish keeps rejected packs from accumulating before write-path
    // GC would reclaim them.
    try {
      if (!oids.includes(expectedSha)) {
        throw new Error(
          `sha_mismatch: expected commit ${expectedSha} not found in pack`,
        );
      }

      if (verifyCommit !== undefined) {
        const { commit } = await git.readCommit({
          fs: runtime.fs.git,
          dir,
          oid: expectedSha,
        });
        if (commit.gpgsig === undefined) {
          throw new Error(
            `signature_unsigned: commit ${expectedSha} has no signature`,
          );
        }

        // Reconstruct the signing payload from the raw object bytes;
        // readCommit().payload is unreliable for SSH signatures because
        // isogit's withoutSignature() only handles PGP armor markers.
        const { object: rawBytes } = await readRawObject(
          runtime,
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

      // Make the accepted pack durable before checkout and ref promotion;
      // a later failure may leave the promotion outcome uncertain, but
      // every possible ref value resolves to a durable object store.
      await flushRuntime(runtime);

      // Ref is written last so it never references a commit whose working
      // tree has not been materialized.
      await checkoutTree(runtime, dir, expectedSha, ref);
    } catch (err) {
      await unpublishPack(runtime, dir, transferId);
      await flushRuntime(runtime);
      throw err;
    }

    // Do not unpublish after promotion begins: writeRef or the final
    // flush can fail after making the new ref observable.
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
