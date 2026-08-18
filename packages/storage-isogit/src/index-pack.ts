import git from "isomorphic-git";

import type { StorageRuntime } from "./runtime";
import type { PackMaterializationLimits } from "./materialization-limits";

// A gitDir this helper produces holds exactly one pack, so the pack filename
// is a constant rather than a caller-supplied id.
const PACK_FILENAME = "pack-indexed.pack";

// A v2/v3 packfile opens with a 4-byte magic, a 4-byte version, and a 4-byte
// object count, all before any object data. The count lets us reject an
// object-count bomb from the header alone. Magic/version and the object framing
// are validated by the node inflation guard (which walks every object) and by
// `git.indexPack` itself, so this reads only the count.
const PACK_HEADER_BYTES = 12;

/**
 * Read the declared object count from an untrusted pack's fixed header. Requires
 * the 12-byte header be present so the count read cannot fault.
 */
function readPackObjectCount(pack: Uint8Array): number {
  if (pack.length < PACK_HEADER_BYTES) {
    throw new Error(
      `indexPackIntoGitDir: pack is ${String(pack.length)} bytes, shorter than the ${String(PACK_HEADER_BYTES)}-byte header`,
    );
  }
  const view = new DataView(pack.buffer, pack.byteOffset, pack.byteLength);
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
