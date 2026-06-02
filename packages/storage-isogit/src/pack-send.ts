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

/**
 * Predicate used by `createNegotiatedPack` to filter the set of object OIDs
 * actually placed in the resulting packfile. The walker computes the
 * full set of objects reachable from the wants and not reachable from any
 * have; the caller's `includeSha(sha)` then filters that set on a per-oid
 * basis. Returning `true` keeps the object; returning `false` drops it.
 *
 * Used by the upload-pack route to suppress objects that are reachable
 * only via refs the requester is not permitted to fetch (route-level
 * reachability enforcement).
 */
export type IncludeShaPredicate = (sha: string) => boolean | Promise<boolean>;

async function collectCommitChain(
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
    const { commit } = await git.readCommit({ fs, dir, oid });
    for (const parent of commit.parent) {
      if (!seen.has(parent)) queue.push(parent);
    }
  }
  return [...seen];
}

async function reachableFromCommits(
  dir: string,
  commits: readonly string[],
): Promise<Set<string>> {
  const reachable = new Set<string>();
  for (const commitOid of commits) {
    const chain = await collectCommitChain(dir, commitOid);
    for (const ancestor of chain) {
      if (reachable.has(ancestor)) continue;
      const objects = await collectReachableObjects(dir, ancestor);
      for (const oid of objects) {
        reachable.add(oid);
      }
    }
  }
  return reachable;
}

/**
 * Build a packfile from a multi-want, multi-have negotiation.
 *
 * The walker computes the set of objects reachable from any commit in
 * `wants`, then subtracts the set of objects reachable from any commit in
 * `haves`. The remaining objects — those the requester needs but doesn't
 * already have — are run through `includeSha(sha)` for per-oid filtering
 * (used by the upload-pack route to drop objects that are only reachable
 * via refs the requester is not permitted to fetch), and the surviving
 * OIDs are handed to `git.packObjects`.
 *
 * `haves` may include OIDs that don't exist locally; unknown commits are
 * silently ignored, matching the smart-HTTP semantics where the client
 * may advertise haves it hasn't actually verified the server has.
 *
 * Returns `null` when the resulting object set is empty (no objects to
 * send) — callers should treat this as "everything you asked for, you
 * already have."
 */
export type CreateNegotiatedPackOptions = {
  /**
   * Precomputed set of object OIDs reachable from `wants`. Supplied by
   * callers that already walked this set for their own purposes — the
   * upload-pack route layer pre-walks the allowed-ref tree to enforce
   * the bearer token's refPattern, then folds the want walk into the
   * same pass so the work is not duplicated here.
   *
   * Contract: when set, this MUST equal
   * `reachableFromCommits(dir, wants)`. A superset over-packs (sending
   * objects the client did not ask for); a subset under-packs
   * (omitting objects the client needs). Callers that omit this option
   * pay one walk inside `createNegotiatedPack`; either way the byte
   * output is identical for the same `(wants, haves, includeSha)`
   * triple.
   */
  wantedObjects?: ReadonlySet<string>;
};

export async function createNegotiatedPack(
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
    options?.wantedObjects ?? (await reachableFromCommits(dir, wants));

  const knownHaves: string[] = [];
  for (const have of haves) {
    try {
      await git.readCommit({ fs, dir, oid: have });
      knownHaves.push(have);
    } catch {
      // Unknown have — the client's advertised state is not present
      // locally. Skip without failing the negotiation.
    }
  }

  const haveObjects = await reachableFromCommits(dir, knownHaves);

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
    fs,
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
