import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { readRawObject } from "./isogit-helpers";

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
export type CommitVerifier = (payload: string, signature: string) => boolean;

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
 * path. Validators that only need path-level checks can ignore `readBlob`.
 */
export type TreeValidator = (
  topLevelPaths: string[],
  readBlob: (path: string) => Promise<Uint8Array>,
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
 * Paths that never appear in any commit tree (e.g. .git, agent.json, state,
 * keys) are never touched.
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
 * When `expectedOldSha` is supplied, the call performs a compare-and-set:
 * the current ref value is read after the pack is indexed, compared to
 * `expectedOldSha` (passing `null` asserts the ref does not yet exist),
 * and the update aborts with `non_fast_forward:` on mismatch. The caller
 * is responsible for serializing concurrent updates to the same ref;
 * this primitive enforces the CAS check but does not own the lock.
 */
export async function receivePackObjects(
  dir: string,
  pack: Uint8Array,
  ref: string,
  expectedSha: string,
  transferId: string,
  validateTree?: TreeValidator,
  expectedOldSha?: string | null,
): Promise<string | null> {
  if (!SAFE_PATH_SEGMENT.test(transferId)) {
    throw new Error(
      `transferId contains unsafe characters: ${JSON.stringify(transferId)}`,
    );
  }

  const packDir = path.join(dir, ".git", "objects", "pack");
  await fs.promises.mkdir(packDir, { recursive: true });

  const filename = `pack-recv-${transferId}.pack`;
  const filepath = path.join(".git", "objects", "pack", filename);
  const fullPath = path.join(dir, filepath);

  await fs.promises.writeFile(fullPath, pack);

  try {
    const { oids } = await git.indexPack({ fs, dir, filepath });

    if (!oids.includes(expectedSha)) {
      throw new Error(
        `sha_mismatch: expected commit ${expectedSha} not found in pack`,
      );
    }

    const currentOldSha = await git
      .resolveRef({ fs, dir, ref })
      .catch(() => null);

    if (expectedOldSha !== undefined) {
      if (currentOldSha !== expectedOldSha) {
        const observed = currentOldSha === null ? "null" : currentOldSha;
        const expected = expectedOldSha === null ? "null" : expectedOldSha;
        throw new Error(
          `non_fast_forward: ref ${ref} expected ${expected} but found ${observed}`,
        );
      }
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
      const verdict = await validateTree(topLevelPaths, readBlob);
      if (verdict !== true) {
        const reason =
          typeof verdict === "object"
            ? verdict.reason
            : `commit ${expectedSha} tree contains disallowed paths: ${topLevelPaths.join(", ")}`;
        throw new Error(`path_violation: ${reason}`);
      }
    }

    await git.writeRef({ fs, dir, ref, value: expectedSha, force: true });
    return currentOldSha;
  } catch (err) {
    await fs.promises.rm(fullPath, { force: true });
    const idxPath = fullPath.replace(/\.pack$/, ".idx");
    await fs.promises.rm(idxPath, { force: true });
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
  if (!SAFE_PATH_SEGMENT.test(transferId)) {
    throw new Error(
      `transferId contains unsafe characters: ${JSON.stringify(transferId)}`,
    );
  }

  const packDir = path.join(dir, ".git", "objects", "pack");
  await fs.promises.mkdir(packDir, { recursive: true });

  const filename = `pack-recv-${transferId}.pack`;
  const filepath = path.join(".git", "objects", "pack", filename);
  const fullPath = path.join(dir, filepath);

  await fs.promises.writeFile(fullPath, pack);

  try {
    const { oids } = await git.indexPack({ fs, dir, filepath });

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

      if (!verifyCommit(payload, commit.gpgsig)) {
        throw new Error(
          `signature_invalid: commit ${expectedSha} signature verification failed`,
        );
      }
    }

    await checkoutTree(dir, expectedSha, ref);
    await git.writeRef({ fs, dir, ref, value: expectedSha, force: true });
  } catch (err) {
    // Clean up pack and idx on failure so we don't leave corrupt state.
    await fs.promises.rm(fullPath, { force: true });
    const idxPath = fullPath.replace(/\.pack$/, ".idx");
    await fs.promises.rm(idxPath, { force: true });
    throw err;
  }
}
