import git from "isomorphic-git";

import type { StorageRuntime } from "./runtime";
import type { PackMaterializationLimits } from "./materialization-limits";

// A gitDir this helper produces holds exactly one pack, so the pack filename
// is a constant rather than a caller-supplied id.
const PACK_FILENAME = "pack-indexed.pack";

// A v2/v3 packfile opens with the 4-byte magic "PACK", a 4-byte version, and a
// 4-byte object count, all before any object data. The count lets us reject an
// object-count bomb from the header alone, before `git.indexPack` inflates a
// single object.
const PACK_HEADER_BYTES = 12;
const PACK_MAGIC = 0x5041_434b; // "PACK"

/**
 * Read and validate an untrusted pack's fixed header. Returns the declared
 * object count. Throws on a truncated header, a bad magic, or an unsupported
 * version, so a malformed pack fails loud here rather than deep inside
 * `git.indexPack`.
 */
function readPackObjectCount(pack: Uint8Array): number {
  if (pack.length < PACK_HEADER_BYTES) {
    throw new Error(
      `indexPackIntoGitDir: pack is ${String(pack.length)} bytes, shorter than the ${String(PACK_HEADER_BYTES)}-byte header`,
    );
  }
  const view = new DataView(pack.buffer, pack.byteOffset, pack.byteLength);
  const magic = view.getUint32(0, false);
  if (magic !== PACK_MAGIC) {
    throw new Error(
      `indexPackIntoGitDir: pack does not begin with the "PACK" magic`,
    );
  }
  const version = view.getUint32(4, false);
  if (version !== 2 && version !== 3) {
    throw new Error(
      `indexPackIntoGitDir: unsupported pack version ${String(version)}`,
    );
  }
  return view.getUint32(8, false);
}

/**
 * Initialize a `.git` under `gitDir` and index `pack` into its object store,
 * RETAINING the store so a caller can later read the trees and blobs the pack
 * carries (for example, check a subtree out via `writeTreeToDisk`). Asserts the
 * pinned `commitSha` is one of the objects the pack delivers, so a pack that
 * does not carry the expected commit fails loud rather than leaving a gitDir a
 * later checkout would fault on.
 *
 * This is the shared "index a pack into a gitDir and assert the commit is
 * present" step. `applyAssetPack` runs it against a scratch dir it discards
 * after copying files out; a source-asset delivery runs it against a durable
 * dir it keeps.
 */
export async function indexPackIntoGitDir(
  runtime: StorageRuntime,
  gitDir: string,
  pack: Uint8Array,
  commitSha: string,
  limits: PackMaterializationLimits,
): Promise<void> {
  // Bound the object count from the header BEFORE `git.indexPack` inflates a
  // single object -- an object-count bomb (millions of tiny objects) is
  // rejected here rather than exhausting memory during indexing.
  const objectCount = readPackObjectCount(pack);
  if (objectCount > limits.maxPackObjects) {
    throw new Error(
      `indexPackIntoGitDir: pack declares ${String(objectCount)} objects, exceeding the ${String(limits.maxPackObjects)}-object cap`,
    );
  }
  await runtime.fs.mkdir(gitDir, { recursive: true });
  await git.init({ fs: runtime.fs.git, dir: gitDir, defaultBranch: "main" });
  const packDir = runtime.path.join(gitDir, ".git", "objects", "pack");
  await runtime.fs.mkdir(packDir, { recursive: true });
  const relPackPath = runtime.path.join(
    ".git",
    "objects",
    "pack",
    PACK_FILENAME,
  );
  await runtime.fs.writeFile(runtime.path.join(gitDir, relPackPath), pack);
  const { oids } = await git.indexPack({
    fs: runtime.fs.git,
    dir: gitDir,
    filepath: relPackPath,
  });
  if (!oids.includes(commitSha)) {
    throw new Error(
      `indexPackIntoGitDir: expected commit ${commitSha} not found in the pack`,
    );
  }
}
