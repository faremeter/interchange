import fs from "node:fs";
import path from "node:path";
import git, { type TreeEntry } from "isomorphic-git";
import { collectReachableObjects } from "@intx/storage-isogit/node";

import type { TreeFiles } from "./repo-store/validate-push.test-helpers";

const AUTHOR = {
  name: "probe",
  email: "probe@example.com",
  timestamp: 1_700_000_000,
  timezoneOffset: 0,
};

/**
 * Author a commit on `ref` whose tree adds `files` on top of the index
 * left by `parent`, parented on `parent`. receivePack validates the tree
 * via validatePush, not the commit signature, so an unsigned source
 * commit is sufficient.
 */
export async function authorCommit(
  ref: string,
  dir: string,
  files: Record<string, string>,
  parent: string[],
  message: string,
): Promise<string> {
  for (const [rel, body] of Object.entries(files)) {
    const full = path.join(dir, rel);
    await fs.promises.mkdir(path.dirname(full), { recursive: true });
    await fs.promises.writeFile(full, body);
    await git.add({ fs, dir, filepath: rel });
  }
  return git.commit({
    fs,
    dir,
    message,
    author: { name: AUTHOR.name, email: AUTHOR.email },
    parent,
    ref,
  });
}

/**
 * Author a commit on `ref` whose tree is EXACTLY `files`: every
 * currently tracked path is removed first, so paths absent from `files`
 * are dropped. This is how a run deletion is expressed.
 */
export async function authorExactTree(
  ref: string,
  dir: string,
  files: Record<string, string>,
  parent: string[],
  message: string,
): Promise<string> {
  const tracked = await git.listFiles({ fs, dir });
  for (const rel of tracked) {
    await git.remove({ fs, dir, filepath: rel });
    await fs.promises.rm(path.join(dir, rel), { force: true });
  }
  return authorCommit(ref, dir, files, parent, message);
}

/** Pack every object reachable from `shas`. */
export async function packOids(
  dir: string,
  shas: string[],
): Promise<Uint8Array> {
  const oids = Array.from(
    new Set(
      (
        await Promise.all(shas.map((s) => collectReachableObjects(dir, s)))
      ).flat(),
    ),
  );
  return packObjectIds(dir, oids);
}

async function packObjectIds(dir: string, oids: string[]): Promise<Uint8Array> {
  const packResult = await git.packObjects({ fs, dir, oids, write: false });
  if (packResult.packfile === undefined) {
    throw new Error("git.packObjects returned no packfile");
  }
  return packResult.packfile;
}

type FixtureTree = {
  kind: "tree";
  children: Map<string, FixtureNode>;
  // null until the tree is written, and again after a change below it.
  oid: string | null;
};

type FixtureNode = { kind: "blob"; oid: string } | FixtureTree;

/**
 * A linear history built in a source repo for a pack receive. Each
 * commit writes only its changed blobs and the trees on their paths, and
 * names its tree directly, so a commit costs a fixed number of git
 * writes however long the history grows. Authoring through `git.add`
 * rewrites the whole index on every commit instead.
 */
export type PackFixture = {
  /** The commit ids, oldest first. */
  readonly commits: readonly string[];
  /**
   * Commit the current tree with `puts` written and `deletes` removed,
   * parented on the previous commit. A directory left empty is dropped.
   */
  commit(change: {
    puts?: TreeFiles;
    deletes?: readonly string[];
  }): Promise<string>;
  /** Pack every object of the first `count` commits. */
  pack(count: number): Promise<Uint8Array>;
};

export async function createPackFixture(
  dir: string,
  ref: string,
): Promise<PackFixture> {
  await git.init({ fs, dir, defaultBranch: "main" });
  const root: FixtureTree = { kind: "tree", children: new Map(), oid: null };
  const commits: string[] = [];
  // Each commit's new objects, so a pack of a prefix of the history
  // needs no walk of the trees.
  const objectsByCommit: string[][] = [];
  let pending: string[] = [];

  // The trees from the root down to the directory `segments` names,
  // created where absent, each marked for a rewrite.
  function treePath(segments: readonly string[]): FixtureTree[] {
    const chain: FixtureTree[] = [root];
    let node: FixtureTree = root;
    node.oid = null;
    for (const segment of segments) {
      let child = node.children.get(segment);
      if (child === undefined) {
        child = { kind: "tree", children: new Map(), oid: null };
        node.children.set(segment, child);
      }
      if (child.kind !== "tree") throw new Error(`${segment}: not a tree`);
      child.oid = null;
      chain.push(child);
      node = child;
    }
    return chain;
  }

  function splitPath(relPath: string): { dirs: string[]; name: string } {
    const dirs = relPath.split("/");
    const name = dirs.pop();
    if (name === undefined || name === "") throw new Error("empty path");
    return { dirs, name };
  }

  async function put(relPath: string, body: string | Uint8Array) {
    const { dirs, name } = splitPath(relPath);
    const oid = await git.writeBlob({
      fs,
      dir,
      blob: typeof body === "string" ? new TextEncoder().encode(body) : body,
    });
    pending.push(oid);
    treePath(dirs).at(-1)?.children.set(name, { kind: "blob", oid });
  }

  function remove(relPath: string) {
    const { dirs, name } = splitPath(relPath);
    const chain = treePath(dirs);
    chain.at(-1)?.children.delete(name);
    // Drop each directory the removal left empty, from the deepest up.
    // `chain[i]` is the directory `dirs[i - 1]` names.
    for (let i = chain.length - 1; i > 0; i--) {
      const segment = dirs[i - 1];
      if (segment === undefined) throw new Error("unreachable");
      if (chain[i]?.children.size === 0) chain[i - 1]?.children.delete(segment);
    }
  }

  async function writeTree(node: FixtureNode): Promise<string> {
    if (node.kind === "blob") return node.oid;
    if (node.oid !== null) return node.oid;
    const entries: TreeEntry[] = [];
    for (const [name, child] of node.children) {
      entries.push({
        mode: child.kind === "blob" ? "100644" : "040000",
        path: name,
        oid: await writeTree(child),
        type: child.kind,
      });
    }
    const oid = await git.writeTree({ fs, dir, tree: entries });
    pending.push(oid);
    node.oid = oid;
    return oid;
  }

  return {
    commits,
    async commit({ puts = {}, deletes = [] }) {
      for (const relPath of deletes) remove(relPath);
      for (const [relPath, body] of Object.entries(puts)) {
        await put(relPath, body);
      }
      const tree = await writeTree(root);
      const parent = commits[commits.length - 1];
      const oid = await git.commit({
        fs,
        dir,
        message: `commit ${String(commits.length)}`,
        author: AUTHOR,
        committer: AUTHOR,
        tree,
        parent: parent === undefined ? [] : [parent],
        ref,
      });
      pending.push(oid);
      commits.push(oid);
      objectsByCommit.push(pending);
      pending = [];
      return oid;
    },
    async pack(count) {
      const oids = new Set(objectsByCommit.slice(0, count).flat());
      return packObjectIds(dir, Array.from(oids));
    },
  };
}
