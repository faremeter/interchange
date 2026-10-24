import git from "isomorphic-git";

import type {
  CommittedTreeEntry,
  KindHandler,
  Principal,
  RepoId,
} from "./types";

/** A tree as a map from repo-root-relative blob path to its bytes. */
export type TreeFiles = Readonly<Record<string, string | Uint8Array>>;

export type ValidatePushArgs = Parameters<KindHandler["validatePush"]>[0];

function bytesOf(body: string | Uint8Array): Uint8Array {
  return typeof body === "string" ? new TextEncoder().encode(body) : body;
}

// The names directly under `dir` and whether each one is a blob. A path
// that is a blob, or that no file is under, has no children.
function childrenOf(files: TreeFiles, dir: string): Map<string, boolean> {
  const prefix = dir === "" ? "" : `${dir}/`;
  const children = new Map<string, boolean>();
  for (const p of Object.keys(files)) {
    if (!p.startsWith(prefix)) continue;
    const rest = p.slice(prefix.length);
    if (rest.length === 0) continue;
    const slash = rest.indexOf("/");
    if (slash === -1) children.set(rest, true);
    else children.set(rest.slice(0, slash), false);
  }
  return children;
}

// The listing closures over one in-memory tree, with the store's
// semantics: an absent or blob path lists as empty. A blob carries its
// real git blob OID. A tree carries an OID hashed from its own listing,
// so equal subtrees share an OID as they do in git; it is not the git
// tree id.
function treeListings(files: TreeFiles): {
  listDir: (path: string) => Promise<string[]>;
  listDirOids: (path: string) => Promise<CommittedTreeEntry[]>;
} {
  const oids = new Map<string, Promise<string>>();
  const oidOf = (entryPath: string, isBlob: boolean): Promise<string> => {
    let oid = oids.get(entryPath);
    if (oid === undefined) {
      oid = isBlob
        ? blobOid(files, entryPath)
        : listDirOids(entryPath).then(async (entries) => {
            const listing = entries
              .map((e) => `${e.type} ${e.name} ${e.oid}`)
              .sort()
              .join("\n");
            return (await git.hashBlob({ object: listing })).oid;
          });
      oids.set(entryPath, oid);
    }
    return oid;
  };
  const listDirOids = async (dir: string): Promise<CommittedTreeEntry[]> =>
    Promise.all(
      Array.from(childrenOf(files, dir), async ([name, isBlob]) => {
        const entryPath = dir === "" ? name : `${dir}/${name}`;
        return {
          name,
          oid: await oidOf(entryPath, isBlob),
          type: isBlob ? ("blob" as const) : ("tree" as const),
        };
      }),
    );
  const listDir = async (dir: string): Promise<string[]> =>
    Array.from(childrenOf(files, dir).keys());
  return { listDir, listDirOids };
}

async function blobOid(files: TreeFiles, blobPath: string): Promise<string> {
  const body = files[blobPath];
  if (body === undefined) throw new Error(`no blob at ${blobPath}`);
  return (await git.hashBlob({ object: bytesOf(body) })).oid;
}

function topLevelsOf(files: TreeFiles): string[] {
  return Array.from(childrenOf(files, "").keys());
}

/**
 * Build the full `validatePush` args for an in-memory prospective tree
 * and, when `priorFiles` is given, a prior tree. With no `priorFiles`
 * the ref has no prior commit: prior reads return null and prior
 * listings are empty. Each call gets a fresh `validationScope`, so no
 * parse result carries over from another call. `topLevelTreePaths`
 * overrides the names derived from `files`.
 */
export function validatePushArgs(opts: {
  repoId: RepoId;
  ref: string;
  principal: Principal;
  files: TreeFiles;
  priorFiles?: TreeFiles | undefined;
  changedPathPrefixes?: ReadonlySet<string> | undefined;
  topLevelTreePaths?: string[];
}): ValidatePushArgs {
  const { files, priorFiles } = opts;
  const prospective = treeListings(files);
  const prior = treeListings(priorFiles ?? {});
  return {
    repoId: opts.repoId,
    ref: opts.ref,
    principal: opts.principal,
    topLevelTreePaths: opts.topLevelTreePaths ?? topLevelsOf(files),
    readBlob: async (blobPath) => {
      const body = files[blobPath];
      if (body === undefined)
        throw new Error(`readBlob: ${blobPath} not found`);
      return bytesOf(body);
    },
    listDir: prospective.listDir,
    listDirOids: prospective.listDirOids,
    priorReadBlob: async (blobPath) => {
      const body = priorFiles?.[blobPath];
      return body === undefined ? null : bytesOf(body);
    },
    priorListDir: prior.listDir,
    priorListDirOids: prior.listDirOids,
    changedPathPrefixes: opts.changedPathPrefixes,
    validationScope: {},
  };
}
