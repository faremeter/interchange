import git from "isomorphic-git";
import type { CommitSigner } from "./signer";
import { buildSigningArgs } from "./commit-helpers";
import { flushRuntime, type StorageRuntime } from "./runtime";

const AUTHOR = {
  name: "interchange-harness",
  email: "harness@interchange.local",
};

const HUB_AUTHOR = {
  name: "interchange-hub",
  email: "hub@interchange.local",
};

const DEFAULT_GITIGNORE = "keys/\n";

async function isGitRepo(
  runtime: StorageRuntime,
  dir: string,
): Promise<boolean> {
  return runtime.fs
    .stat(runtime.path.join(dir, ".git"))
    .then(() => true)
    .catch(() => false);
}

/**
 * Per-call options for `initRepo`.
 *
 *   - `signer`: enables a hub-authored signed genesis (gpgsig header
 *     populated via the callback). When omitted, the genesis is authored
 *     as the harness identity and unsigned.
 *   - `gitignore`: overrides the `.gitignore` body written into the
 *     genesis tree. When omitted, the default `keys/\n` body is used; the
 *     asset-route handler ships a richer body (OS/editor cruft, build
 *     output, `keys/`).
 */
export type InitRepoOpts = {
  signer?: CommitSigner;
  gitignore?: string;
};

/**
 * Initialize a git repository with a `.gitignore` and an empty initial
 * commit. Idempotent: safe on a directory that already contains a git
 * repo. isomorphic-git requires at least one commit before branching
 * operations work, so the initial commit is always created.
 */
export async function initRepo(
  runtime: StorageRuntime,
  dir: string,
  opts: InitRepoOpts = {},
): Promise<void> {
  await runtime.fs.mkdir(dir, { recursive: true });

  if (await isGitRepo(runtime, dir)) return;

  await git.init({ fs: runtime.fs.git, dir, defaultBranch: "main" });

  const gitignoreBody = opts.gitignore ?? DEFAULT_GITIGNORE;
  await runtime.fs.writeFile(
    runtime.path.join(dir, ".gitignore"),
    gitignoreBody,
  );
  await git.add({ fs: runtime.fs.git, dir, filepath: ".gitignore" });

  const author = opts.signer === undefined ? AUTHOR : HUB_AUTHOR;
  await git.commit({
    fs: runtime.fs.git,
    dir,
    message: "Initialize repository",
    author,
    ...buildSigningArgs(opts.signer),
  });
  await flushRuntime(runtime);
}

/**
 * Initialize a sidecar-side agent repository: creates the `state/`
 * directory and a single initial commit containing only `.gitignore`;
 * subsequent reactor cycles overwrite the per-cycle files at the repo
 * root and commit them via `commit({ message })`.
 *
 * Idempotent: safe on a directory that already contains a git repo.
 */
export async function initAgentRepo(
  runtime: StorageRuntime,
  dir: string,
): Promise<void> {
  await runtime.fs.mkdir(dir, { recursive: true });
  await runtime.fs.mkdir(runtime.path.join(dir, "state"), { recursive: true });

  if (await isGitRepo(runtime, dir)) {
    await flushRuntime(runtime);
    return;
  }

  await git.init({ fs: runtime.fs.git, dir, defaultBranch: "main" });

  await runtime.fs.writeFile(runtime.path.join(dir, ".gitignore"), "keys/\n");
  await git.add({ fs: runtime.fs.git, dir, filepath: ".gitignore" });

  await git.commit({
    fs: runtime.fs.git,
    dir,
    message: "Initialize agent repository",
    author: AUTHOR,
  });
  await flushRuntime(runtime);
}

export { AUTHOR, HUB_AUTHOR };
