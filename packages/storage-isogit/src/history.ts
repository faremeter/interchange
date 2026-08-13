import git from "isomorphic-git";
import type { ContextCommit } from "@intx/types/runtime";
import { AUTHOR } from "./init";
import { flushRuntime, type StorageRuntime } from "./runtime";

/**
 * Switch the working tree to the named branch. The branch must already exist.
 */
export async function switchBranch(
  runtime: StorageRuntime,
  dir: string,
  ref: string,
): Promise<void> {
  await git.checkout({ fs: runtime.fs.git, dir, ref });
  await flushRuntime(runtime);
}

/**
 * Create a new branch at HEAD and immediately switch to it.
 */
export async function createAndSwitchBranch(
  runtime: StorageRuntime,
  dir: string,
  name: string,
): Promise<void> {
  await git.branch({ fs: runtime.fs.git, dir, ref: name });
  // Persist the new branch before checkout overwrites the already-durable
  // HEAD body. A reload must never leave HEAD naming an absent branch.
  await flushRuntime(runtime);
  await git.checkout({ fs: runtime.fs.git, dir, ref: name });
  await flushRuntime(runtime);
}

/**
 * Return the name of the currently checked-out branch.
 */
export async function currentBranch(
  runtime: StorageRuntime,
  dir: string,
): Promise<string> {
  const branch = await git.currentBranch({ fs: runtime.fs.git, dir });
  if (branch === null || branch === undefined) {
    throw new Error("Repository is in detached HEAD state");
  }
  return branch;
}

/**
 * List all local branches.
 */
export async function listBranches(
  runtime: StorageRuntime,
  dir: string,
): Promise<string[]> {
  return git.listBranches({ fs: runtime.fs.git, dir });
}

/**
 * Return recent commits as ContextCommit entries.
 */
export async function logHistory(
  runtime: StorageRuntime,
  dir: string,
  limit = 10,
): Promise<ContextCommit[]> {
  const entries = await git.log({ fs: runtime.fs.git, dir, depth: limit });
  return entries.map((e) => {
    const base = {
      hash: e.oid,
      message: e.commit.message.trimEnd(),
      timestamp: e.commit.author.timestamp * 1000,
    };
    const parent = e.commit.parent[0];
    return parent !== undefined ? { ...base, parentHash: parent } : base;
  });
}

export { AUTHOR };
