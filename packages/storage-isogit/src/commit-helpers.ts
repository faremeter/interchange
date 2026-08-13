import git from "isomorphic-git";

import type { CommitSigner } from "./signer";
import { flushRuntime, type StorageRuntime } from "./runtime";

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
