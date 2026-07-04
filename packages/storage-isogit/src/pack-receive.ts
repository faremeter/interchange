import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { readRawObject } from "./isogit-helpers";
import { withRepoDirLock } from "./repo-lock";

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

export type TreeValidatorResult = true | { ok: false; reason: string };

/**
 * Validates the contents of a commit's tree.
 *
 * Callers bind this to their policy (e.g. state packs must only contain
 * entries named "state", asset packs must have a SKILL.md with valid
 * frontmatter). Return `true` when the tree is acceptable; return
 * `{ ok: false, reason }` to reject with a caller-supplied reason that
 * the substrate splices into its thrown `path_violation:` message.
 *
 * Returning `false` is also accepted for back-compatibility and is
 * treated as an opaque rejection without a reason.
 *
 * `topLevelPaths` lists the names directly under the tree root.
 * `readBlob` reads any blob in the tree by its repo-root-relative POSIX
 * path. `listDir` enumerates the names directly under a
 * tree-root-relative POSIX directory path (no trailing slash, no
 * leading slash); pass the empty string to list the root. Validators
 * that only need path-level checks can ignore `readBlob` and `listDir`.
 */
export type TreeValidator = (
  topLevelPaths: string[],
  readBlob: (path: string) => Promise<Uint8Array>,
  listDir: (path: string) => Promise<string[]>,
) => boolean | TreeValidatorResult | Promise<boolean | TreeValidatorResult>;

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

// Sibling of objects/pack/. Lives on the same filesystem so fs.link and
// fs.rename between staging and pack/ are atomic metadata operations.
const STAGING_DIR_NAME = "pack-staging";

/**
 * Derive the externally-visible final paths a publishPackAtomically call
 * will (or has already) written for a given `transferId`. Exported as a
 * helper so callers that reject a published pack can locate the files
 * to unpublish without re-deriving the naming convention.
 */
function publishedPackPaths(
  dir: string,
  transferId: string,
): { finalPackPath: string; finalIdxPath: string } {
  const packDir = path.join(dir, ".git", "objects", "pack");
  const finalPackPath = path.join(packDir, `pack-recv-${transferId}.pack`);
  const finalIdxPath = finalPackPath.replace(/\.pack$/, ".idx");
  return { finalPackPath, finalIdxPath };
}

/**
 * Remove a previously-published `.pack` + `.idx` pair from objects/pack/.
 *
 * Callers reject a published pack by calling this after their post-publish
 * validation fails (signature, tree-validator, sha mismatch, CAS
 * non-fast-forward, etc.). POSIX semantics mean unlink does not affect
 * file descriptors a concurrent reader has already opened, so a reader
 * mid-call against the rejected pack finishes its read against the
 * already-loaded bytes; subsequent calls will not see the pack at all.
 * Iso-git does not hold open file descriptors between calls (verified
 * by reading `readObjectPacked` at `index.cjs:3394-3398`), so the
 * unlink is safe against the standard pack-discovery path.
 *
 * Wrapped in `.catch(() => undefined)` per rm so a secondary failure
 * (permissions, I/O) does not mask whatever rejection the caller is
 * about to throw. A failed unlink leaks the rejected pack on disk
 * until external cleanup; that is acceptable because the caller is
 * already in an error path and the alternative is hiding the real
 * cause behind a cleanup throw.
 */
async function unpublishPack(dir: string, transferId: string): Promise<void> {
  const { finalPackPath, finalIdxPath } = publishedPackPaths(dir, transferId);
  await fs.promises.rm(finalPackPath, { force: true }).catch(() => undefined);
  await fs.promises.rm(finalIdxPath, { force: true }).catch(() => undefined);
}

type TreeEntry = {
  type: "blob" | "tree" | "commit";
  mode: string;
  path: string;
  oid: string;
};

async function topLevelNames(dir: string, oid: string): Promise<Set<string>> {
  const { tree } = await git.readTree({ fs, dir, oid });
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
 */
async function checkoutTree(
  dir: string,
  commitSha: string,
  ref: string,
): Promise<void> {
  const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
  const { tree } = await git.readTree({ fs, dir, oid: commit.tree });

  // Collect top-level names managed by deploy trees (new + previous).
  const managed = new Set(tree.map((e) => e.path));

  const prevSha = await git.resolveRef({ fs, dir, ref }).catch(() => null);
  if (prevSha !== null) {
    const { commit: prev } = await git.readCommit({ fs, dir, oid: prevSha });
    for (const name of await topLevelNames(dir, prev.tree)) {
      managed.add(name);
    }
  }

  const existing = await fs.promises.readdir(dir);
  for (const name of existing) {
    if (!managed.has(name)) continue;
    await fs.promises.rm(path.join(dir, name), {
      recursive: true,
      force: true,
    });
  }

  await writeTreeEntries(dir, dir, tree);
}

async function writeTreeEntries(
  repoDir: string,
  targetDir: string,
  entries: TreeEntry[],
): Promise<void> {
  for (const entry of entries) {
    const entryPath = path.join(targetDir, entry.path);
    if (entry.type === "tree") {
      await fs.promises.mkdir(entryPath, { recursive: true });
      const { tree } = await git.readTree({ fs, dir: repoDir, oid: entry.oid });
      await writeTreeEntries(repoDir, entryPath, tree);
    } else if (entry.type === "blob") {
      const { blob } = await git.readBlob({
        fs,
        dir: repoDir,
        oid: entry.oid,
      });
      await fs.promises.writeFile(entryPath, blob, {
        mode: entry.mode === "100755" ? 0o755 : 0o644,
      });
    }
  }
}

/**
 * Index a packfile and update a ref without materializing the working tree.
 *
 * Used by the hub to store state packs from sidecars where only the git
 * object history matters, not the working-tree files.
 *
 * When `validateTree` is provided, the commit's top-level tree entries
 * are checked after indexing but before the ref is promoted. Throws with
 * a `"path_violation"` prefix if the validator rejects the tree.
 *
 * Returns the SHA the ref pointed at before this call (or `null` if the
 * ref did not previously exist). Callers that drive `onRefUpdated`-style
 * hooks should feed this value to the hook rather than performing a
 * separate `resolveRef` read.
 *
 * `expectedOldSha` enforces a compare-and-set: the current ref value is
 * read after the pack is indexed, compared to `expectedOldSha`, and the
 * update aborts with `non_fast_forward:` on mismatch. Pass a SHA string
 * to require the ref currently points there; pass `null` to require
 * the ref does not yet exist. The caller is responsible for serializing
 * concurrent updates to the same ref; this primitive enforces the CAS
 * check but does not own the lock.
 */
/**
 * Atomically publish a packfile into a git repository's pack directory.
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
 * intermediate state.
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
 * other; the kernel guarantees that the staging directory and the
 * final `objects/pack/` are on the same filesystem (they share a
 * parent), so `fs.link` and `fs.rename` between them are atomic
 * metadata operations.
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
 *
 *   3. Atomic publish (transitions readers from "no new pack" to "new
 *      pack visible"):
 *
 *        a. `fs.link` staging `.pack` -> final `.pack`. The new
 *           `.pack` now exists in `objects/pack/`. Readers scanning
 *           for `.idx` files still do not see the new pack (no `.idx`
 *           for it in `objects/pack/` yet). The staging `.pack`
 *           directory entry is still present.
 *
 *        b. `fs.rename` staging `.idx` -> final `.idx`. The new `.idx`
 *           appears atomically in `objects/pack/`; readers' next
 *           directory scan finds it and resolves the `.pack` written
 *           in 3a. The staging directory's `.idx` entry vanishes;
 *           since nothing was reading from the staging directory in
 *           the first place, this transition is observable only to
 *           this function.
 *
 *        c. Remove the staging directory recursively (`unlink` of the
 *           remaining `.pack` plus `rmdir`). The final `.pack` inode
 *           persists via the link created in 3a.
 *
 *   4. On any throw before publish completes, recursively remove the
 *      staging directory. `fs.rm({recursive: true, force: true})`
 *      tolerates partial states (e.g. throw mid-write before `.idx`
 *      exists, or throw inside `indexPack`).
 *
 * # Cleanup contract
 *
 * On successful return, no staging files remain. On throw before
 * publish (steps 1-2), the staging directory and any files inside it
 * are removed. A throw partway through publish (between 3a and 3b)
 * leaves a linked-but-unindexed `.pack` in `objects/pack/`; this is
 * harmless (iso-git ignores `.pack` files with no matching `.idx`,
 * verified at `index.cjs:3394-3398`) and the recovery path on the next
 * call would re-write the same content. The cleanup-on-throw path
 * here attempts to remove the orphan `.pack` but does not mask the
 * original publish error.
 *
 * # Validation timing
 *
 * Callers that need to inspect the pack's contents (`git.readCommit`,
 * `git.readTree`, signature verification against raw object bytes)
 * must do so *after* this function returns: the pack lives in the
 * staging directory until publish, so iso-git's pack discovery does
 * not find it until step 3 completes.
 *
 * Callers that reject the published pack (signature failure, tree
 * validator rejection, sha mismatch, CAS non-fast-forward) call
 * `unpublishPack` to remove the published `.pack` + `.idx` pair from
 * `objects/pack/`. POSIX semantics let a concurrent reader that
 * already opened the rejected files finish its read against the
 * cached bytes, and iso-git does not hold open descriptors between
 * calls, so the unlink does not introduce the concurrent-read race
 * that the staging strategy exists to eliminate.
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
 * different mechanism, this staging dance becomes redundant and can
 * collapse back to writing directly into `objects/pack/`. The cited
 * source-line references above are the verification anchor for the
 * next reader deciding whether the dance is still needed.
 */
export async function publishPackAtomically(
  dir: string,
  pack: Uint8Array,
  transferId: string,
): Promise<string[]> {
  if (!SAFE_PATH_SEGMENT.test(transferId)) {
    throw new Error(
      `transferId contains unsafe characters: ${JSON.stringify(transferId)}`,
    );
  }

  const packDir = path.join(dir, ".git", "objects", "pack");
  const stagingRoot = path.join(dir, ".git", "objects", STAGING_DIR_NAME);
  const stagingDir = path.join(stagingRoot, transferId);

  await fs.promises.mkdir(packDir, { recursive: true });
  await fs.promises.mkdir(stagingDir, { recursive: true });

  const stagingPackPath = path.join(stagingDir, "pack.pack");
  const stagingIdxPath = stagingPackPath.replace(/\.pack$/, ".idx");
  const { finalPackPath, finalIdxPath } = publishedPackPaths(dir, transferId);
  // iso-git's indexPack takes a repo-root-relative filepath; derive it
  // from the absolute path rather than rebuilding the segment list so
  // the two stay coupled.
  const stagingFilepath = path.relative(dir, stagingPackPath);

  let oids: string[];
  try {
    // Step 1: write the pack bytes to the staging directory.
    await fs.promises.writeFile(stagingPackPath, pack);
    // Step 2: index the pack. iso-git writes the .idx next to the
    // .pack — both stay inside the staging directory, invisible to
    // any reader scanning objects/pack/.
    const result = await git.indexPack({
      fs,
      dir,
      filepath: stagingFilepath,
    });
    oids = result.oids;
  } catch (err) {
    // Step 4: cleanup before publish. Recursive rm handles every
    // partial state (write succeeded but indexPack threw, etc.). The
    // rm is wrapped so a secondary cleanup failure (permissions, I/O)
    // does not mask the original error.
    await fs.promises
      .rm(stagingDir, { recursive: true, force: true })
      .catch(() => undefined);
    throw err;
  }

  // Step 3: atomic publish. The .pack link (3a) and the .idx rename
  // (3b) are the two operations that determine externally-observable
  // state; a failure inside this block means the publish is partial
  // or absent, and the recovery rms below clear the half-published
  // pack from objects/pack/. Staging-directory cleanup is NOT part of
  // this block — see below.
  try {
    await fs.promises.link(stagingPackPath, finalPackPath); // 3a
    await fs.promises.rename(stagingIdxPath, finalIdxPath); // 3b
  } catch (err) {
    // EEXIST on the link means `finalPackPath` already exists, which
    // can only happen if a prior publishPackAtomically call on the
    // same `dir` used the same `transferId`. The contract is that
    // transferId is unique across all historical receives on `dir`;
    // both production callers honour this (hub uses crypto.randomUUID,
    // sidecar uses a per-process monotonic counter). Treat the EEXIST
    // as a programmer error and surface it cleanly without running
    // the recovery rms — those would destroy the earlier call's
    // published pack and break any reader holding it.
    if (
      err !== null &&
      typeof err === "object" &&
      "code" in err &&
      err.code === "EEXIST"
    ) {
      await fs.promises
        .rm(stagingDir, { recursive: true, force: true })
        .catch(() => undefined);
      throw new Error(
        `transferId "${transferId}" already published in ${dir}; callers must guarantee transferId uniqueness across all historical receives`,
        { cause: err },
      );
    }
    // Partial-publish recovery: if 3a succeeded and 3b failed we
    // leave a linked-but-unindexed .pack in objects/pack/. The rm of
    // finalPackPath clears it; finalIdxPath cannot exist here (3b
    // never completed), so no rm is needed for it. Each recovery rm
    // is wrapped so a secondary failure (permissions, I/O) does not
    // mask the original publish error — the caller needs to see the
    // publish failure, not whatever the cleanup tripped on.
    await fs.promises
      .rm(stagingDir, { recursive: true, force: true })
      .catch(() => undefined);
    await fs.promises.rm(finalPackPath, { force: true }).catch(() => undefined);
    throw err;
  }

  // Step 3c: staging-directory cleanup runs only after the pack is
  // fully published. A failure here is benign from the caller's
  // perspective — the pack is observable to readers, the publish
  // succeeded — so the rm is wrapped and swallowed. It MUST NOT live
  // inside the publish try/catch: doing so would let an
  // EACCES/EBUSY/EIO on the staging rm trigger the recovery rms above
  // and delete a pack that was already published and (potentially)
  // already read by concurrent observers. A leftover staging
  // directory on rm failure leaks disk until external cleanup; that
  // is strictly preferable to retroactively destroying a successful
  // publish.
  await fs.promises
    .rm(stagingDir, { recursive: true, force: true })
    .catch(() => undefined);

  return oids;
}

export async function receivePackObjects(
  dir: string,
  pack: Uint8Array,
  ref: string,
  expectedSha: string,
  transferId: string,
  expectedOldSha: string | null,
  validateTree?: TreeValidator,
): Promise<string | null> {
  const oids = await publishPackAtomically(dir, pack, transferId);

  // Post-publish validation runs inside a try so any rejection path
  // (sha mismatch, CAS non-fast-forward, tree validator) unpublishes
  // the pack before re-throwing. This removes a rejected pack
  // immediately, at the moment of rejection. Write-path GC (`runGC`)
  // also reclaims unreferenced packs, but only when a later accepted
  // write crosses its threshold; the immediate unpublish keeps a flood
  // of rejected packs from accumulating in the window before that.
  try {
    if (!oids.includes(expectedSha)) {
      throw new Error(
        `sha_mismatch: expected commit ${expectedSha} not found in pack`,
      );
    }

    const currentOldSha = await git
      .resolveRef({ fs, dir, ref })
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
        fs,
        dir,
        oid: expectedSha,
      });
      const { tree } = await git.readTree({
        fs,
        dir,
        oid: commit.tree,
      });
      // Surface every top-level tree entry — directories and files —
      // to the kind handler. The writeTree path already passes both;
      // the receivePack path used to filter to directories only, which
      // hid top-level files (e.g. an extra `evil.exe` at the root, or
      // a stray `package-registry.json`) from handlers that reject
      // anything outside their allowlist. Handlers that need to
      // distinguish file from directory inspect the entries
      // themselves via `readBlob` / `listDir`; widening here makes the
      // allowlist real on this path.
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
          const next = await git.readTree({ fs, dir, oid: entry.oid });
          currentTree = next.tree;
        }
        const last = segments[segments.length - 1];
        const blobEntry = currentTree.find((e) => e.path === last);
        if (blobEntry === undefined || blobEntry.type !== "blob") {
          throw new Error(
            `readBlob: path ${relPath} not found in commit ${expectedSha} tree`,
          );
        }
        const { blob } = await git.readBlob({ fs, dir, oid: blobEntry.oid });
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
          const next = await git.readTree({ fs, dir, oid: entry.oid });
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

    // Ref write happens after publish and validation so the ref never
    // references a commit whose pack is unpublished or whose tree was
    // rejected.
    await git.writeRef({ fs, dir, ref, value: expectedSha, force: true });
    return currentOldSha;
  } catch (err) {
    await unpublishPack(dir, transferId);
    throw err;
  }
}

/**
 * Apply a git packfile to a repository, check out the working tree, and
 * update the ref.
 *
 * Writes the pack to .git/objects/pack/ (the standard location for git
 * packfiles), creates the .idx index via indexPack, checks out the tree to
 * the working directory, then updates `ref` to point at `expectedSha`.
 * The ref is written last so it never points at a commit whose working tree
 * has not been materialized.
 *
 * When `verifyCommit` is provided, the commit's embedded signature is
 * verified before the working tree is materialized. Throws with a message
 * prefixed by `"signature_unsigned"` if the commit has no signature, or
 * `"signature_invalid"` if verification fails. Only omit `verifyCommit`
 * for state packs that follow their own signing model.
 *
 * Throws if the expected commit is not found in the pack.
 *
 * The caller is responsible for ensuring `dir` is an initialized git repo.
 */
export async function applyPack(
  dir: string,
  pack: Uint8Array,
  ref: string,
  expectedSha: string,
  transferId: string,
  verifyCommit?: CommitVerifier,
): Promise<void> {
  // The deploy apply shares the agent repo's object store with the
  // reactor's context commits, the mail-audit commits, and GC. Hold the
  // per-directory lock across the publish, validation, checkout, and ref
  // write so none of them interleave with this apply.
  await withRepoDirLock(dir, async () => {
    const oids = await publishPackAtomically(dir, pack, transferId);

    // Post-publish validation runs inside a try so any rejection path
    // (sha mismatch, missing signature, signature failure) unpublishes
    // the pack before re-throwing. Sidecar `applyPack` is the last line
    // of defence against a compromised hub or transport; the unpublish
    // removes a rejected pack at the moment of rejection, before the
    // reactor's write-path GC would next reclaim it, so a flood of
    // rejected-signature packs cannot accumulate in the meantime.
    try {
      if (!oids.includes(expectedSha)) {
        throw new Error(
          `sha_mismatch: expected commit ${expectedSha} not found in pack`,
        );
      }

      if (verifyCommit !== undefined) {
        const { commit } = await git.readCommit({
          fs,
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
        const { object: rawBytes } = await readRawObject(dir, expectedSha);
        const payload = stripGpgsig(new TextDecoder().decode(rawBytes));

        if (!(await verifyCommit(payload, commit.gpgsig))) {
          throw new Error(
            `signature_invalid: commit ${expectedSha} signature verification failed`,
          );
        }
      }

      // Checkout reads from the now-published pack; ref is written last
      // so it never references a commit whose working tree is not
      // materialized.
      await checkoutTree(dir, expectedSha, ref);
      await git.writeRef({ fs, dir, ref, value: expectedSha, force: true });
    } catch (err) {
      await unpublishPack(dir, transferId);
      throw err;
    }
  });
}
