import { describe, test, expect } from "bun:test";

import { committedReadsToSourceTree } from "./committed-source-tree";
import type { CommittedReads, CommittedTreeEntry } from "./repo-store/types";

// A `CommittedReads` fake: `listDir` returns per-directory child entries, and
// `readBlobByOid` returns bytes keyed by oid. Directories addressed with the
// empty string are the root tree.
function fakeCommittedReads(spec: {
  dirs: Record<string, CommittedTreeEntry[]>;
  blobs: Record<string, string>;
  treeOids?: Record<string, string>;
}): CommittedReads {
  return {
    listDir: async (relPath) => spec.dirs[relPath] ?? [],
    readBlobByOid: async (oid) => {
      const body = spec.blobs[oid];
      if (body === undefined) throw new Error(`no blob for oid ${oid}`);
      return new TextEncoder().encode(body);
    },
    treeOid: async (relPath) => spec.treeOids?.[relPath] ?? null,
  };
}

const decode = (bytes: Uint8Array): string => new TextDecoder().decode(bytes);

describe("committedReadsToSourceTree", () => {
  test("reads a top-level blob by resolving its path against the root tree", async () => {
    const reads = committedReadsToSourceTree(
      fakeCommittedReads({
        dirs: {
          "": [{ name: "package.json", oid: "oid-root-pkg", type: "blob" }],
        },
        blobs: { "oid-root-pkg": '{"name":"@wf/root"}' },
      }),
    );

    expect(decode(await reads.readBlob("package.json"))).toBe(
      '{"name":"@wf/root"}',
    );
  });

  test("reads a nested blob by listing its immediate parent directory", async () => {
    const reads = committedReadsToSourceTree(
      fakeCommittedReads({
        dirs: {
          "packages/foo": [
            { name: "package.json", oid: "oid-foo-pkg", type: "blob" },
          ],
        },
        blobs: { "oid-foo-pkg": '{"name":"@wf/foo"}' },
      }),
    );

    expect(decode(await reads.readBlob("packages/foo/package.json"))).toBe(
      '{"name":"@wf/foo"}',
    );
  });

  test("fails loud when the path names a tree, not a blob", async () => {
    const reads = committedReadsToSourceTree(
      fakeCommittedReads({
        dirs: {
          "": [{ name: "packages", oid: "oid-tree", type: "tree" }],
        },
        blobs: {},
      }),
    );

    await expect(reads.readBlob("packages")).rejects.toThrow(/no blob at/);
  });

  test("fails loud when the blob is absent from the parent directory", async () => {
    const reads = committedReadsToSourceTree(
      fakeCommittedReads({ dirs: {}, blobs: {} }),
    );

    await expect(reads.readBlob("missing/package.json")).rejects.toThrow(
      /no blob at/,
    );
  });

  test("passes listDir and treeOid straight through", async () => {
    const children: CommittedTreeEntry[] = [
      { name: "package.json", oid: "o1", type: "blob" },
      { name: "src", oid: "o2", type: "tree" },
    ];
    const reads = committedReadsToSourceTree(
      fakeCommittedReads({
        dirs: { "packages/foo": children },
        blobs: {},
        treeOids: { "packages/foo": "tree-oid-foo" },
      }),
    );

    expect(await reads.listDir("packages/foo")).toEqual(children);
    expect(await reads.treeOid("packages/foo")).toBe("tree-oid-foo");
    expect(await reads.treeOid("absent")).toBeNull();
  });
});
