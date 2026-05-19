// Clone a contextDir into a sibling path and roll the clone's HEAD
// back to an older commit so a fresh agent can open it "rooted at"
// that earlier state.
//
// The two-step shape (copy the working tree, then move HEAD) is the
// closest isomorphic-git analogue of `git clone <local> <local> &&
// git -C <copy> reset --hard <hash>`. isomorphic-git's `clone` is
// designed for HTTP-fetched remotes and would require shimming the
// http transport for local-path sources; recursive `fs.cp` plus
// `git.checkout({ ref, force: true })` gets the same observable
// result with no extra moving parts. The function is exported so
// tests can exercise it independently of the CLI.

import * as fs from "node:fs";
import { cp, mkdir, rm } from "node:fs/promises";
import { dirname } from "node:path";

import git from "isomorphic-git";

export type CloneAndRewindOpts = {
  /** Existing contextDir managed by an `@intx/agent` instance. */
  sourceDir: string;
  /** Destination path; must not already exist. */
  destDir: string;
  /** Commit hash to roll the destination's HEAD to. */
  hash: string;
};

/**
 * Recursively copy `sourceDir` to `destDir` and then move the
 * destination's HEAD to `hash`. Throws if `destDir` already exists
 * (callers who want to overwrite must remove it themselves, so the
 * helper does not silently clobber a sibling directory).
 */
export async function cloneAndRewind(opts: CloneAndRewindOpts): Promise<void> {
  await mkdir(dirname(opts.destDir), { recursive: true });
  // `errorOnExist: true` paired with `force: false` makes the helper
  // refuse to overwrite a populated destination. The caller decides
  // when a stale rewound copy gets removed.
  await cp(opts.sourceDir, opts.destDir, {
    recursive: true,
    force: false,
    errorOnExist: true,
  });
  await git.checkout({
    fs,
    dir: opts.destDir,
    ref: opts.hash,
    force: true,
  });
}

/** Remove a previously-created rewind directory, ignoring missing paths. */
export async function clearRewindDir(destDir: string): Promise<void> {
  await rm(destDir, { recursive: true, force: true });
}
