import fs from "node:fs";
import git from "isomorphic-git";
import { collectReachableObjects } from "./object-walk";

/**
 * Create a git packfile containing all objects reachable from a ref.
 *
 * Used by the hub to produce deploy packs for transfer to sidecars.
 * The caller sends the resulting bytes as chunked repo.pack.push frames.
 */
export async function createDeployPack(
  dir: string,
  ref: string,
): Promise<{ pack: Uint8Array; commitSha: string }> {
  const commitSha = await git.resolveRef({ fs, dir, ref });
  const oids = await collectReachableObjects(dir, commitSha);

  const result = await git.packObjects({
    fs,
    dir,
    oids,
    write: false,
  });
  if (result.packfile === undefined) {
    throw new Error(
      `packObjects returned no packfile for ref "${ref}" (${commitSha})`,
    );
  }

  return { pack: result.packfile, commitSha };
}
