import git from "isomorphic-git";
import { collectReachableObjects } from "./object-walk";
import { withRepoDirLock } from "./repo-lock";
import type { StorageRuntime } from "./runtime";

/**
 * Create a git packfile containing all objects reachable from a ref. Used
 * by the hub to produce deploy packs for transfer to sidecars, and by the
 * sidecar to produce state packs.
 */
export async function createDeployPack(
  runtime: StorageRuntime,
  dir: string,
  ref: string,
): Promise<{ pack: Uint8Array; commitSha: string }> {
  // Read under the per-directory lock so a concurrent GC pass cannot prune
  // a loose object out from under the reachability walk or the pack write.
  return withRepoDirLock(runtime, dir, async () => {
    const commitSha = await git.resolveRef({ fs: runtime.fs.git, dir, ref });
    const oids = await collectReachableObjects(runtime, dir, commitSha);

    const result = await git.packObjects({
      fs: runtime.fs.git,
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
  });
}

/**
 * Per-oid filter applied by `createNegotiatedPack` to the negotiated
 * object set (`true` keeps, `false` drops). Used by the upload-pack route
 * to suppress objects reachable only via refs the requester is not
 * permitted to fetch.
 */
export type IncludeShaPredicate = (sha: string) => boolean | Promise<boolean>;

async function collectCommitChain(
  runtime: StorageRuntime,
  dir: string,
  start: string,
): Promise<string[]> {
  const seen = new Set<string>();
  const queue: string[] = [start];
  while (queue.length > 0) {
    const oid = queue.shift();
    if (oid === undefined) break;
    if (seen.has(oid)) continue;
    seen.add(oid);
    const { commit } = await git.readCommit({ fs: runtime.fs.git, dir, oid });
    for (const parent of commit.parent) {
      if (!seen.has(parent)) queue.push(parent);
    }
  }
  return [...seen];
}

async function reachableFromCommits(
  runtime: StorageRuntime,
  dir: string,
  commits: readonly string[],
): Promise<Set<string>> {
  const reachable = new Set<string>();
  for (const commitOid of commits) {
    const chain = await collectCommitChain(runtime, dir, commitOid);
    for (const ancestor of chain) {
      if (reachable.has(ancestor)) continue;
      const objects = await collectReachableObjects(runtime, dir, ancestor);
      for (const oid of objects) {
        reachable.add(oid);
      }
    }
  }
  return reachable;
}

/**
 * Build a packfile from a multi-want, multi-have negotiation: objects
 * reachable from `wants` minus objects reachable from `haves`, filtered
 * per-oid by `includeSha`, packed via `git.packObjects`.
 *
 * `haves` may include OIDs that do not exist locally; unknown commits are
 * silently ignored, matching smart-HTTP semantics where the client may
 * advertise haves it has not verified the server has.
 *
 * Returns `null` when the resulting object set is empty — callers should
 * treat this as "everything you asked for, you already have."
 */
export type CreateNegotiatedPackOptions = {
  /**
   * Precomputed objects reachable from `wants`, supplied by callers that
   * already walked this set (the upload-pack route layer pre-walks the
   * allowed-ref tree for the bearer token's refPattern, then folds the
   * want walk into the same pass).
   *
   * Contract: when set, this MUST equal `reachableFromCommits(dir,
   * wants)`. A superset over-packs; a subset under-packs. Callers that
   * omit it pay one walk inside `createNegotiatedPack`; the byte output
   * is identical either way.
   */
  wantedObjects?: ReadonlySet<string>;
};

export async function createNegotiatedPack(
  runtime: StorageRuntime,
  dir: string,
  wants: readonly string[],
  haves: readonly string[],
  includeSha?: IncludeShaPredicate,
  options?: CreateNegotiatedPackOptions,
): Promise<{ pack: Uint8Array; oids: string[] } | null> {
  if (wants.length === 0) {
    throw new Error("createNegotiatedPack: wants must be non-empty");
  }

  const wantedObjects =
    options?.wantedObjects ?? (await reachableFromCommits(runtime, dir, wants));

  const knownHaves: string[] = [];
  for (const have of haves) {
    try {
      await git.readCommit({ fs: runtime.fs.git, dir, oid: have });
      knownHaves.push(have);
    } catch {
      // Unknown have — the client's advertised state is not present
      // locally; skip without failing the negotiation.
    }
  }

  const haveObjects = await reachableFromCommits(runtime, dir, knownHaves);

  const candidates: string[] = [];
  for (const oid of wantedObjects) {
    if (haveObjects.has(oid)) continue;
    candidates.push(oid);
  }

  let oids: string[];
  if (includeSha === undefined) {
    oids = candidates;
  } else {
    oids = [];
    for (const oid of candidates) {
      if (await includeSha(oid)) oids.push(oid);
    }
  }

  if (oids.length === 0) return null;

  const result = await git.packObjects({
    fs: runtime.fs.git,
    dir,
    oids,
    write: false,
  });
  if (result.packfile === undefined) {
    throw new Error(
      `packObjects returned no packfile for ${oids.length.toString()} oids`,
    );
  }

  return { pack: result.packfile, oids };
}
