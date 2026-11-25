import git from "isomorphic-git";
import { hasCode } from "@intx/types/has-code";
import type { CommitSigner } from "./signer";
import { buildSigningArgs } from "./commit-helpers";
import {
  flushRuntime,
  pathExists,
  syncPath,
  syncTree,
  type StorageRuntime,
} from "./runtime";
import { forgetSyncRecord } from "./sync-record";

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
  return pathExists(runtime, runtime.path.join(dir, ".git"));
}

// The directories `mkdir -p` creates for `dir`, outermost first.
async function missingDirs(
  runtime: StorageRuntime,
  dir: string,
): Promise<string[]> {
  const missing: string[] = [];
  let current = runtime.path.resolve(dir);
  while (!(await pathExists(runtime, current))) {
    missing.unshift(current);
    const parent = runtime.path.resolve(runtime.path.join(current, ".."));
    if (parent === current) break;
    current = parent;
  }
  return missing;
}

// `dir` and each directory above it, up to the one that holds `root`.
function holdersUpTo(
  runtime: StorageRuntime,
  dir: string,
  root: string,
): string[] {
  const top = runtime.path.resolve(root);
  const holders: string[] = [];
  let current = runtime.path.resolve(dir);
  while (current !== top) {
    holders.push(current);
    const parent = runtime.path.resolve(runtime.path.join(current, ".."));
    if (parent === current) throw new Error(`${dir} is not under ${root}`);
    current = parent;
  }
  return [...holders, top, runtime.path.resolve(runtime.path.join(top, ".."))];
}

// Written into `.git-init` before anything else, so a later call can tell
// a build a crashed call left from a `.git-init` that is not its own.
const BUILD_MARK = "intx-init";
const BUILD_MARK_TEXT =
  "@intx/storage-isogit initRepo builds a repository here\n";

// Remove what a crashed call left in `building`: a directory whose mark
// holds the mark's text, or one with nothing in it but a mark a crash cut
// short. Anything else there is not this code's, so the call refuses to
// touch it.
async function removeLeftBuild(
  runtime: StorageRuntime,
  building: string,
): Promise<void> {
  const refuse = (cause?: unknown) =>
    new Error(
      `${building} exists and is not a repository that initRepo was building`,
      { cause },
    );
  let names: string[];
  try {
    names = await runtime.fs.readdir(building);
  } catch (cause) {
    if (!hasCode(cause)) throw cause;
    if (cause.code === "ENOENT") return;
    // A file there is never a build.
    if (cause.code === "ENOTDIR") throw refuse(cause);
    throw cause;
  }
  if (names.length > 0) {
    if (!names.includes(BUILD_MARK)) throw refuse();
    const mark = await runtime.fs.readTextFile(
      runtime.path.join(building, BUILD_MARK),
    );
    const cutShort = names.length === 1 && BUILD_MARK_TEXT.startsWith(mark);
    if (mark !== BUILD_MARK_TEXT && !cutShort) throw refuse();
    // The mark goes last, once the rest is durably gone, so a crash part
    // way through leaves a build a later call still knows as its own.
    for (const name of names) {
      if (name === BUILD_MARK) continue;
      await runtime.fs.remove(runtime.path.join(building, name), {
        recursive: true,
        force: true,
      });
    }
    await syncPath(runtime, building);
    await flushRuntime(runtime);
    await runtime.fs.remove(runtime.path.join(building, BUILD_MARK));
  }
  await runtime.fs.remove(building);
}

// Per runtime and repository, the holders this process synced for it. A
// crash between the rename to `.git` and those syncs leaves a repository
// no later build would sync, so the first call in a process that finds one
// syncs them. A build drops the entry and the repository's sync record, so
// a repository built anew is not taken for the old one. Past the cap the
// map starts over, which costs only a repeat of the syncs.
const SYNCED_HOLDERS_MAX = 4096;
const syncedHolders = new WeakMap<StorageRuntime, Map<string, string>>();

const holdersKey = (holding: readonly string[]) => holding.join("\0");

function recordHoldersSynced(
  runtime: StorageRuntime,
  repo: string,
  holding: readonly string[],
): void {
  let synced = syncedHolders.get(runtime);
  if (synced === undefined) {
    synced = new Map();
    syncedHolders.set(runtime, synced);
  }
  if (synced.size >= SYNCED_HOLDERS_MAX) synced.clear();
  synced.set(repo, holdersKey(holding));
}

/**
 * Per-call options for `initRepo`.
 *
 *   - `signer`: enables a hub-authored signed genesis (gpgsig header
 *     populated via the callback). When omitted, the genesis is
 *     authored as the harness identity and unsigned.
 *   - `gitignore`: overrides the body written to `.gitignore` in the
 *     genesis tree. When omitted, the default `keys/\n` body is used,
 *     keeping the historical behaviour for every existing caller.
 *     The asset-route REST handler ships a richer body that includes
 *     OS/editor cruft, common build output, and `keys/`.
 *   - `durableRoot`: a directory that holds `dir`. Creating the
 *     repository then syncs every directory from `dir` up to the one
 *     that holds `durableRoot`, not only the ones this call made, so a
 *     directory that a crashed earlier call made is synced too. The
 *     first call in a process that finds the repository also syncs them.
 */
export type InitRepoOpts = {
  signer?: CommitSigner;
  gitignore?: string;
  durableRoot?: string;
};

/**
 * Initialize a git repository with a .gitignore and an empty initial commit.
 * Idempotent: safe to call on a directory that already contains a git repo.
 * Callers serialize calls for one `dir`, since a call replaces any
 * `.git-init` it finds.
 *
 * Used by the hub for repos that don't need sidecar-specific scaffolding.
 * isomorphic-git requires at least one commit before branching operations
 * work, so the initial commit is always created.
 *
 * The repository is built in `.git-init`, made durable whole, and renamed
 * to `.git`, so a crash never leaves a partial `.git`; the directories
 * that hold `.git` are synced after the rename (see `durableRoot`). The
 * next call replaces a `.git-init` a crash left and refuses one this code
 * did not make. The first call in a process that finds a repository syncs
 * its holders and flushes again, in case a crash or a failure came between
 * the rename and those steps.
 */
export async function initRepo(
  runtime: StorageRuntime,
  dir: string,
  opts: InitRepoOpts = {},
): Promise<void> {
  const repo = runtime.path.resolve(dir);
  const holding =
    opts.durableRoot === undefined
      ? [repo]
      : holdersUpTo(runtime, dir, opts.durableRoot);
  const created = await missingDirs(runtime, dir);
  await runtime.fs.mkdir(dir, { recursive: true });

  if (await isGitRepo(runtime, dir)) {
    if (syncedHolders.get(runtime)?.get(repo) !== holdersKey(holding)) {
      await Promise.all(holding.map((d) => syncPath(runtime, d)));
      await flushRuntime(runtime);
      recordHoldersSynced(runtime, repo, holding);
    }
    return;
  }

  syncedHolders.get(runtime)?.delete(repo);
  forgetSyncRecord(runtime, repo);
  const building = runtime.path.join(dir, ".git-init");
  await removeLeftBuild(runtime, building);
  await runtime.fs.mkdir(building);
  const mark = runtime.path.join(building, BUILD_MARK);
  await runtime.fs.writeFile(mark, BUILD_MARK_TEXT);
  // Durable before any Git file, so a crash never leaves Git files under
  // a mark that is missing or cut short, which no later call would remove.
  await syncPath(runtime, mark);
  await syncPath(runtime, building);
  await flushRuntime(runtime);
  await git.init({
    fs: runtime.fs.git,
    dir,
    gitdir: building,
    defaultBranch: "main",
  });

  const gitignoreBody = opts.gitignore ?? DEFAULT_GITIGNORE;
  await runtime.fs.writeFile(
    runtime.path.join(dir, ".gitignore"),
    gitignoreBody,
  );
  await git.add({
    fs: runtime.fs.git,
    dir,
    gitdir: building,
    filepath: ".gitignore",
  });

  const author = opts.signer === undefined ? AUTHOR : HUB_AUTHOR;
  await git.commit({
    fs: runtime.fs.git,
    dir,
    gitdir: building,
    message: "Initialize repository",
    author,
    ...buildSigningArgs(opts.signer),
  });
  await syncTree(runtime, building);
  await flushRuntime(runtime);
  await runtime.fs.rename(building, runtime.path.join(dir, ".git"));
  const outermost = created[0];
  const toSync = new Set([
    ...(outermost === undefined
      ? []
      : [runtime.path.resolve(runtime.path.join(outermost, ".."))]),
    ...created,
    ...holding,
  ]);
  await Promise.all([...toSync].map((d) => syncPath(runtime, d)));
  await flushRuntime(runtime);
  recordHoldersSynced(runtime, repo, holding);
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
