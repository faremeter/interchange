import git, { type WalkerIterate } from "isomorphic-git";

import { hasCode } from "@intx/types/has-code";
import { hexEncode } from "@intx/types/hex";

import type { CommitSigner } from "./signer";
import {
  decodeUTF8,
  flushRuntime,
  pathExists,
  syncPath,
  syncTree,
  type StorageRuntime,
} from "./runtime";
import { forgetRefsSynced } from "./sync-record";

export type SigningArgs = {
  onSign?: (args: { payload: string }) => Promise<{ signature: string }>;
  signingKey?: string;
};

export function buildSigningArgs(
  signer: CommitSigner | undefined,
): SigningArgs {
  if (signer === undefined) return {};
  return {
    signingKey: "sshsig",
    onSign: async ({ payload }) => ({ signature: await signer(payload) }),
  };
}

type GitCommitArgs = Parameters<typeof git.commit>[0];

type DurableCommitArgs = {
  message: string;
  author: NonNullable<GitCommitArgs["author"]>;
} & Pick<GitCommitArgs, "onSign" | "signingKey">;

/** A ref write was attempted, but its publication outcome is uncertain. */
export class UncertainRefPublicationError extends Error {
  readonly oid: string;
  readonly ref: string;

  constructor(ref: string, oid: string, cause: unknown) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    super(`Ref publication for ${ref} at ${oid} is uncertain: ${detail}`, {
      cause,
    });
    this.name = "UncertainRefPublicationError";
    this.oid = oid;
    this.ref = ref;
  }
}

/**
 * `writeRefDurably` renamed the ref into place but could not sync it:
 * readers already see `oid`, and a crash can still lose it. A caller
 * should finish the work the moved ref implies, then fail the write. The
 * write marked the repository's refs unsynced, so the next
 * `syncObjectsAndRefs` or `syncRefs` of the repository, such as a
 * retry's, syncs every ref.
 */
export class RefNotDurableError extends Error {
  readonly ref: string;
  readonly oid: string;

  constructor(ref: string, oid: string, cause: unknown) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    super(`Ref ${ref} names ${oid} but is not durable: ${detail}`, { cause });
    this.name = "RefNotDurableError";
    this.ref = ref;
    this.oid = oid;
  }
}

const OBJECT_ID = /^[0-9a-f]{40}$/;

// A ref write's temporary file in the git dir, which `removeStagedPacks`
// removes when a crash left it.
export const REF_WRITE_PREFIX = "ref-write-";

// The names `git check-ref-format` rejects, as isomorphic-git's `writeRef`
// checks them: `..`, `//`, a leading or trailing `.` or `/`, a component
// that starts or ends with `.` or ends in `.lock`, `@{`, a bare `@`, and
// any of `~^:?*[\`. `isValidRefName` also rejects control characters and
// spaces.
//
// Adapted from is-git-ref-name-valid, by way of isomorphic-git.
// SPDX-License-Identifier: MIT
// Copyright © Vincent Weevers
const BAD_REF_NAME = /(^|[/.])([/.]|$)|^@$|@\{|[~^:?*[\\]|\.lock(\/|$)/;

function isValidRefName(ref: string): boolean {
  if (BAD_REF_NAME.test(ref)) return false;
  for (const char of ref) {
    const code = char.charCodeAt(0);
    if (code <= 0x20 || code === 0x7f) return false;
  }
  return true;
}

/**
 * Point `ref`, a ref under `refs/`, at `oid` so that a crash leaves the old
 * value or the new one, never a torn file. The value is written to a file
 * in the git directory, which no ref listing reads, and renamed over the
 * ref. On a runtime that syncs, the value, the rename and any directory
 * the ref needed are durable when this returns. A write that fails after
 * its checks marks the repository's refs unsynced, as the ref may be in
 * place unsynced.
 */
export async function writeRefDurably(
  runtime: StorageRuntime,
  dir: string,
  ref: string,
  oid: string,
): Promise<void> {
  const segments = ref.split("/");
  if (
    segments[0] !== "refs" ||
    segments.length < 2 ||
    segments.some((s) => s === "" || s === "." || s === "..")
  ) {
    throw new Error(`${JSON.stringify(ref)} is not a ref under refs/`);
  }
  if (!isValidRefName(ref)) {
    throw new Error(`${JSON.stringify(ref)} is not a valid ref name`);
  }
  if (!OBJECT_ID.test(oid)) {
    throw new Error(`${JSON.stringify(oid)} is not an object id`);
  }
  try {
    const gitdir = runtime.path.join(dir, ".git");
    // A crash between making a directory and syncing it into its parent
    // leaves the directory but no ref, so a new ref syncs every directory it
    // needs, whoever made it.
    const isNew = !(await pathExists(
      runtime,
      runtime.path.join(gitdir, ...segments),
    ));
    let parent = gitdir;
    for (const segment of segments.slice(0, -1)) {
      const child = runtime.path.join(parent, segment);
      let made = true;
      try {
        await runtime.fs.mkdir(child);
      } catch (cause) {
        if (!hasCode(cause) || cause.code !== "EEXIST") throw cause;
        made = false;
      }
      if (made || isNew) await syncPath(runtime, parent);
      parent = child;
    }
    const temp = runtime.path.join(
      gitdir,
      `${REF_WRITE_PREFIX}${hexEncode(crypto.getRandomValues(new Uint8Array(8)))}`,
    );
    try {
      await runtime.fs.writeFile(temp, `${oid}\n`);
      await syncPath(runtime, temp);
      await runtime.fs.rename(temp, runtime.path.join(gitdir, ...segments));
    } catch (cause) {
      await runtime.fs.remove(temp, { force: true }).catch(() => undefined);
      throw cause;
    }
    try {
      await syncPath(runtime, parent);
      await flushRuntime(runtime);
    } catch (cause) {
      throw new RefNotDurableError(ref, oid, cause);
    }
  } catch (cause) {
    forgetRefsSynced(runtime, dir);
    throw cause;
  }
}

/**
 * Sync every file and directory of the repository's git dir, for a
 * repository that a store which did not sync may have written.
 */
export async function syncRepositoryDurably(
  runtime: StorageRuntime,
  dir: string,
): Promise<void> {
  await syncTree(runtime, runtime.path.join(dir, ".git"));
  await flushRuntime(runtime);
}

/** Restore selected index entries to the currently visible HEAD. */
export async function restoreIndexAfterFailedCommit(
  runtime: StorageRuntime,
  dir: string,
  filepaths: readonly string[],
): Promise<void> {
  const failures: unknown[] = [];
  for (const filepath of filepaths) {
    try {
      await git.resetIndex({ fs: runtime.fs.git, dir, filepath });
    } catch (cause) {
      failures.push(cause);
    }
  }
  try {
    await flushRuntime(runtime);
  } catch (cause) {
    failures.push(cause);
  }
  if (failures.length > 0) {
    throw new AggregateError(failures, "Could not restore the Git index");
  }
}

async function readIfPresent(runtime: StorageRuntime, filepath: string) {
  try {
    return await runtime.fs.readFile(filepath);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") return null;
    throw cause;
  }
}

// isomorphic-git reports a ref file that is empty or holds no oid, such as
// one truncated by a full disk, as missing, so confirm an unborn branch with
// reads of the ref files before trusting it.
async function resolveHead(runtime: StorageRuntime, dir: string) {
  try {
    return await git.resolveRef({ fs: runtime.fs.git, dir, ref: "HEAD" });
  } catch (cause) {
    if (!hasCode(cause) || cause.code !== "NotFoundError") throw cause;
  }
  const branch = await git.currentBranch({
    fs: runtime.fs.git,
    dir,
    fullname: true,
  });
  if (branch === undefined) {
    throw new Error("Could not resolve the detached HEAD");
  }
  const [loose, packed] = await Promise.all([
    readIfPresent(
      runtime,
      runtime.path.join(dir, ".git", ...branch.split("/")),
    ),
    readIfPresent(runtime, runtime.path.join(dir, ".git", "packed-refs")),
  ]);
  const packedHasBranch =
    packed !== null &&
    decodeUTF8(packed)
      .split("\n")
      .some((line) => line.split(" ")[1] === branch);
  if (loose !== null || packedHasBranch) {
    throw new Error(`HEAD's branch ${branch} exists but could not be read`);
  }
  return null;
}

// LightningFS persists a written file body at once but a rename only at the
// next flush, which may be the one that fails, so write in place. Skip an
// index that already matches: on a full disk, the likeliest reason a
// checkpoint fails, rewriting it could truncate it.
async function writeIndex(
  runtime: StorageRuntime,
  indexPath: string,
  contents: Uint8Array,
): Promise<void> {
  const current = await runtime.fs.readFile(indexPath).catch(() => null);
  if (
    current?.length === contents.length &&
    current.every((byte, i) => byte === contents[i])
  ) {
    return;
  }
  await runtime.fs.writeFile(indexPath, contents);
}

/**
 * Capture HEAD and the index before a checkpoint stages `paths`, and return
 * the rollback for a failed checkpoint. The rollback resets index entries
 * under `paths` to the HEAD the failure left visible, which also discards
 * staging there from before the checkpoint, or throws naming what it could
 * not repair. It restores the rest of the index only when HEAD did not move.
 * The caller holds the repository lock throughout and passes the failure
 * that stopped the checkpoint; any failure other than
 * UncertainRefPublicationError is taken to mean the ref was never written.
 */
export async function createIndexRollback(
  runtime: StorageRuntime,
  dir: string,
  paths: readonly string[],
): Promise<(failure: unknown) => Promise<void>> {
  const indexPath = runtime.path.join(dir, ".git", "index");
  const headBefore = await resolveHead(runtime, dir);
  const indexBefore = await runtime.fs
    .readFile(indexPath)
    .catch((cause: unknown) => {
      if (hasCode(cause) && cause.code === "ENOENT") return null;
      throw cause;
    });
  const inScope = (filepath: string) =>
    paths.some(
      (prefix) => filepath === prefix || filepath.startsWith(`${prefix}/`),
    );
  const leadsIntoScope = (filepath: string) =>
    paths.some((prefix) => prefix.startsWith(`${filepath}/`));

  return async (failure) => {
    const failures: unknown[] = [];
    const repairs: {
      filepath: string;
      headEntry: { oid: string; mode: number } | null;
    }[] = [];
    const unchanged = new Map<string, { oid: string; mode: number }>();
    const iterate: WalkerIterate = async (walk, children) => {
      for (const child of children) {
        try {
          await walk(child);
        } catch (cause) {
          failures.push(cause);
        }
      }
      return [];
    };
    try {
      let head = headBefore;
      if (failure instanceof UncertainRefPublicationError) {
        // Only a HEAD matching one of the write's two possible outcomes is
        // trusted.
        const visible = await resolveHead(runtime, dir);
        if (visible !== headBefore && visible !== failure.oid) {
          throw new Error(
            `Could not confirm HEAD as ${headBefore ?? "unborn"} or ${failure.oid} after an uncertain ref write; leaving the index unchanged`,
          );
        }
        head = visible;
      }
      // A failed ref flush can leave the checkpoint visible. Never replace
      // its index with the snapshot of an older HEAD. With no saved index,
      // the walk resets the staged entries instead: a removed file can
      // reappear without contents when the removal's flush fails.
      if (head === headBefore && indexBefore !== null) {
        try {
          await writeIndex(runtime, indexPath, indexBefore);
        } catch (cause) {
          failures.push(cause);
        }
      }

      // The snapshot may already contain interrupted staging. Determine its
      // repairs from Git metadata before a working-directory error can stop
      // traversal.
      await git.walk({
        fs: runtime.fs.git,
        dir,
        trees: [
          git.STAGE(),
          ...(head === null ? [] : [git.TREE({ ref: head })]),
        ],
        iterate,
        map: async (filepath, [staged, committed]) => {
          if (filepath === ".") return;
          if (!inScope(filepath)) {
            return leadsIntoScope(filepath) ? undefined : null;
          }
          const committedType = await committed?.type();
          const stagedType = await staged?.type();
          if (committedType !== "blob" && stagedType !== "blob") return;
          const headEntry =
            committedType === "blob" &&
            committed !== null &&
            committed !== undefined
              ? { oid: await committed.oid(), mode: await committed.mode() }
              : null;
          if (
            committedType !== stagedType ||
            headEntry?.oid !== (await staged?.oid()) ||
            headEntry?.mode !== (await staged?.mode())
          ) {
            repairs.push({ filepath, headEntry });
          } else if (headEntry !== null) {
            unchanged.set(filepath, headEntry);
          }
        },
      });

      // Entries already matching HEAD can still cache stale file stats.
      // Read their working bytes explicitly: same-sized overwrites within
      // a second can fool the stat cache. Keep the repairs above even if
      // this inspection fails.
      await git.walk({
        fs: runtime.fs.git,
        dir,
        trees: [git.WORKDIR()],
        iterate,
        map: async (filepath, [working]) => {
          if (filepath === ".") return;
          if (!inScope(filepath)) {
            return leadsIntoScope(filepath) ? undefined : null;
          }
          const headEntry = unchanged.get(filepath);
          if (
            headEntry === undefined ||
            working === null ||
            working === undefined
          )
            return;
          try {
            const content =
              (await working.type()) === "blob" &&
              (await working.mode()) === headEntry.mode
                ? await working.content()
                : undefined;
            if (
              content === undefined ||
              (await git.hashBlob({ object: content })).oid !== headEntry.oid
            ) {
              repairs.push({ filepath, headEntry });
            }
          } catch {
            // An unreadable working file cannot be compared, so rewrite the
            // entry with zeroed stats to make the next comparison read it.
            repairs.push({ filepath, headEntry });
          }
        },
      });
    } catch (cause) {
      failures.push(cause);
    }
    // Write HEAD's entry as the walk read it, with zeroed stats. resetIndex
    // would read HEAD and its tree again and turn a failed read into a
    // silent deletion.
    for (const { filepath, headEntry } of repairs) {
      try {
        await (headEntry === null
          ? git.updateIndex({
              fs: runtime.fs.git,
              dir,
              filepath,
              remove: true,
              force: true,
            })
          : git.updateIndex({
              fs: runtime.fs.git,
              dir,
              filepath,
              oid: headEntry.oid,
              mode: headEntry.mode,
              add: true,
            }));
      } catch (cause) {
        failures.push(cause);
      }
    }
    try {
      await flushRuntime(runtime);
    } catch (cause) {
      failures.push(cause);
    }
    if (failures.length > 0) {
      throw new AggregateError(failures, "Could not roll back the Git index");
    }
  };
}

/**
 * Create a commit without exposing its ref until the new objects are durable.
 *
 * LightningFS persists existing file bodies before its directory namespace.
 * Updating a live ref in the same phase that creates loose-object paths can
 * therefore leave a durable ref pointing at an absent object after a reload.
 * The first flush publishes the object namespace; only then is the ref moved.
 */
export async function commitDurably(
  runtime: StorageRuntime,
  dir: string,
  args: DurableCommitArgs,
): Promise<string> {
  const branch = await git.currentBranch({
    fs: runtime.fs.git,
    dir,
    fullname: true,
  });
  const oid = await git.commit({
    fs: runtime.fs.git,
    dir,
    ...args,
    noUpdateBranch: true,
  });

  await flushRuntime(runtime);
  const ref = branch ?? "HEAD";
  try {
    await git.writeRef({
      fs: runtime.fs.git,
      dir,
      ref,
      value: oid,
      force: true,
    });
    await flushRuntime(runtime);
  } catch (cause) {
    throw new UncertainRefPublicationError(ref, oid, cause);
  }
  return oid;
}
