// A RepoStore that implements only what a test declares it needs, and throws
// for everything else.
//
// The throw is the point. These stubs assert, by failing, that the code under
// them touches nothing beyond the named surface -- so the narrow ones are not
// an incomplete version of the broad ones, and substituting a working store
// would silently permit paths that currently fail loudly.
//
// Every stub serves its repo from a directory under `baseDir`: `getRepoDir`
// names it, and `openCommittedReads` reads it as the committed tree, since a
// stub keeps no git history.
//
// Only the two capability sets that were byte-identical across several files
// live here. The rest of this repo's stub stores are genuinely different
// fidelities, up to a 218-line in-memory implementation, and merging those
// would trade a real assertion for a smaller diff.

import fs from "node:fs/promises";
import path from "node:path";

import type { CommittedReads, RepoId, RepoStore } from "@intx/hub-sessions";

export type StubRepoStoreOpts = {
  /**
   * Permit `writeTreePreservingPrefix`, returning a fixed commit and no
   * newly-terminal runs. Off by default, so a test that does not ask for it
   * fails on the call rather than accepting it.
   */
  writeTree?: boolean;
};

export function createStubRepoStore(
  baseDir: string,
  opts: StubRepoStoreOpts = {},
): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async openCommittedReads(_principal, repoId) {
      return directoryReads(path.join(baseDir, repoId.kind, repoId.id));
    },
    ...(opts.writeTree === true
      ? {
          writeTreePreservingPrefix: async () => ({
            commitSha: "deadbeefcafef00d",
            newlyTerminalRuns: [],
          }),
        }
      : {}),
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; every method the opts did not enable surfaces a precise failure via the proxy
  return new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(
          `stub RepoStore: ${String(prop)} not implemented for this test`,
        );
      };
    },
  });
}

// A blob's oid is its repo-relative path.
function directoryReads(repoDir: string): CommittedReads {
  return {
    async listDir(relPath) {
      let dirents;
      try {
        dirents = await fs.readdir(path.join(repoDir, relPath), {
          withFileTypes: true,
        });
      } catch (cause) {
        if (
          cause instanceof Error &&
          "code" in cause &&
          cause.code === "ENOENT"
        ) {
          return [];
        }
        throw cause;
      }
      return dirents.map((dirent) => ({
        name: dirent.name,
        oid: path.join(relPath, dirent.name),
        type: dirent.isDirectory() ? ("tree" as const) : ("blob" as const),
      }));
    },
    readBlobByOid: (oid) => fs.readFile(path.join(repoDir, oid)),
    treeOid: async () => null,
  };
}
