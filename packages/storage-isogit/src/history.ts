import fs from "node:fs";
import git from "isomorphic-git";
import type { ContextCommit } from "@interchange/types/runtime";
import { AUTHOR } from "./init";

/**
 * Switch the working tree to the named branch. The branch must already exist.
 */
export async function switchBranch(dir: string, ref: string): Promise<void> {
  await git.checkout({ fs, dir, ref });
}

/**
 * Create a new branch at HEAD and immediately switch to it.
 */
export async function createAndSwitchBranch(
  dir: string,
  name: string,
): Promise<void> {
  await git.branch({ fs, dir, ref: name });
  await git.checkout({ fs, dir, ref: name });
}

/**
 * Return the name of the currently checked-out branch.
 */
export async function currentBranch(dir: string): Promise<string> {
  const branch = await git.currentBranch({ fs, dir });
  if (branch === null || branch === undefined) {
    throw new Error("Repository is in detached HEAD state");
  }
  return branch;
}

/**
 * List all local branches.
 */
export async function listBranches(dir: string): Promise<string[]> {
  return git.listBranches({ fs, dir });
}

/**
 * Return recent commits as ContextCommit entries.
 */
export async function logHistory(
  dir: string,
  limit = 10,
): Promise<ContextCommit[]> {
  const entries = await git.log({ fs, dir, depth: limit });
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
