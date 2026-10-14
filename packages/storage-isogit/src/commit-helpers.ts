import git from "isomorphic-git";

import { hasCode } from "@intx/types/has-code";

import type { CommitSigner } from "./signer";
import { decodeUTF8, flushRuntime, type StorageRuntime } from "./runtime";

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

// isomorphic-git reports a ref it fails to read as missing, so confirm an
// unborn branch with reads that keep their errors before trusting it.
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

  return async (failure) => {
    const failures: unknown[] = [];
    const repairs: {
      filepath: string;
      headEntry: { oid: string; mode: number } | null;
    }[] = [];
    const unexamined: string[] = [];
    try {
      let head = headBefore;
      if (failure instanceof UncertainRefPublicationError) {
        // isomorphic-git reports a failed ref read as a missing ref, so only
        // a HEAD matching one of the two possible outcomes is trusted.
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

      // A saved index can contain interrupted staging or cache old file stats.
      // Read working bytes explicitly: the Git stat cache can miss same-sized
      // overwrites made within a second. Only changed entries need resetting.
      await git.walk({
        fs: runtime.fs.git,
        dir,
        trees: [
          git.STAGE(),
          git.WORKDIR(),
          ...(head === null ? [] : [git.TREE({ ref: head })]),
        ],
        iterate: async (walk, children) => {
          for (const child of children) {
            try {
              await walk(child);
            } catch (cause) {
              failures.push(cause);
            }
          }
          return [];
        },
        map: async (filepath, [staged, working, committed]) => {
          if (filepath === ".") return;
          if (
            !paths.some(
              (prefix) =>
                filepath === prefix || filepath.startsWith(`${prefix}/`),
            )
          ) {
            return paths.some((prefix) => prefix.startsWith(`${filepath}/`))
              ? undefined
              : null;
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
            return;
          }
          // The entry already matches HEAD. Without a working file there are
          // no stale stats to invalidate, but isomorphic-git reports a failed
          // directory listing as empty, which hides files that still exist.
          // Report those rather than reset each one, since every reset
          // rewrites the whole index.
          if (working === null || working === undefined) {
            try {
              await runtime.fs.lstat(runtime.path.join(dir, filepath));
              unexamined.push(filepath);
            } catch (cause) {
              if (
                !hasCode(cause) ||
                (cause.code !== "ENOENT" && cause.code !== "ENOTDIR")
              ) {
                unexamined.push(filepath);
              }
            }
            return;
          }
          try {
            const content =
              (await working.type()) === "blob" &&
              (await working.mode()) === (await staged?.mode())
                ? await working.content()
                : undefined;
            if (
              content === undefined ||
              (await git.hashBlob({ object: content })).oid !==
                (await staged?.oid())
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
    const [firstUnexamined] = unexamined;
    if (firstUnexamined !== undefined) {
      failures.push(
        new Error(
          `Could not list ${String(unexamined.length)} working files starting with ${firstUnexamined}; their index stats may be stale`,
        ),
      );
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
