import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import type { CommitSigner } from "./signer";
import { buildSigningArgs } from "./commit-helpers";

const AUTHOR = {
  name: "interchange-harness",
  email: "harness@interchange.local",
};

const HUB_AUTHOR = {
  name: "interchange-hub",
  email: "hub@interchange.local",
};

async function isGitRepo(dir: string): Promise<boolean> {
  return fs.promises
    .stat(path.join(dir, ".git"))
    .then(() => true)
    .catch(() => false);
}

/**
 * Initialize a git repository with a .gitignore and an empty initial commit.
 * Idempotent: safe to call on a directory that already contains a git repo.
 *
 * Used by the hub for repos that don't need sidecar-specific scaffolding.
 * isomorphic-git requires at least one commit before branching operations
 * work, so the initial commit is always created.
 *
 * When `signer` is supplied, the genesis commit is authored as
 * `interchange-hub` and signed via the callback (an SSH signature in the
 * gpgsig header). When `signer` is omitted, the harness-authored unsigned
 * genesis is produced — byte-for-byte identical to the historical behavior.
 */
export async function initRepo(
  dir: string,
  signer?: CommitSigner,
): Promise<void> {
  await fs.promises.mkdir(dir, { recursive: true });

  if (await isGitRepo(dir)) return;

  await git.init({ fs, dir, defaultBranch: "main" });

  await fs.promises.writeFile(path.join(dir, ".gitignore"), "keys/\n");
  await git.add({ fs, dir, filepath: ".gitignore" });

  const author = signer === undefined ? AUTHOR : HUB_AUTHOR;
  await git.commit({
    fs,
    dir,
    message: "Initialize repository",
    author,
    ...buildSigningArgs(signer),
  });
}

/**
 * Initialize a sidecar-side agent repository with the state/ directory
 * structure. Creates a single initial commit containing only `.gitignore`;
 * subsequent reactor cycles overwrite the per-cycle files (`turns.jsonl`,
 * `prompt.jsonl`, `response.jsonl`, `manifest.jsonl`, `metadata.json`) at the
 * repository root and commit them via `commit({ message })`.
 *
 * Idempotent: safe to call on a directory that already contains a git repo.
 */
export async function initAgentRepo(dir: string): Promise<void> {
  await fs.promises.mkdir(dir, { recursive: true });
  await fs.promises.mkdir(path.join(dir, "state"), { recursive: true });

  if (await isGitRepo(dir)) return;

  await git.init({ fs, dir, defaultBranch: "main" });

  await fs.promises.writeFile(path.join(dir, ".gitignore"), "keys/\n");
  await git.add({ fs, dir, filepath: ".gitignore" });

  await git.commit({
    fs,
    dir,
    message: "Initialize agent repository",
    author: AUTHOR,
  });
}

export { AUTHOR, HUB_AUTHOR };
