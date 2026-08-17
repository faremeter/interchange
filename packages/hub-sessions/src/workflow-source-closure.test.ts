import { describe, test, expect } from "bun:test";

import type { Packument, PackumentFetcher } from "@intx/tool-packaging";

import { resolveSourceWorkflowClosure } from "./workflow-source-closure";
import type { SourceTreeReads } from "./workflow-source-closure";
import type { CommittedTreeEntry } from "./repo-store/types";

// A git-tree read fixture: package.json blobs keyed by repo-relative path, and
// tree oids keyed by packageDir. `listDir` is derived from the blob paths.
function treeReads(spec: {
  packages: Record<string, { json: unknown; treeOid: string }>;
}): SourceTreeReads {
  const blobs = new Map<string, Uint8Array>();
  const oids = new Map<string, string>();
  for (const [dir, { json, treeOid }] of Object.entries(spec.packages)) {
    const blobPath = dir === "." ? "package.json" : `${dir}/package.json`;
    blobs.set(blobPath, new TextEncoder().encode(JSON.stringify(json)));
    oids.set(dir, treeOid);
  }
  return {
    async readBlob(path) {
      const bytes = blobs.get(path);
      if (bytes === undefined) throw new Error(`no blob at ${path}`);
      return bytes;
    },
    async listDir(dir): Promise<CommittedTreeEntry[]> {
      if (dir === "." || dir === "") {
        return [{ name: "package.json", oid: "root-pkg", type: "blob" }];
      }
      return [];
    },
    async treeOid(dir) {
      return oids.get(dir) ?? null;
    },
  };
}

function packument(name: string, version: string): Packument {
  return {
    name,
    versions: {
      [version]: {
        name,
        version,
        dist: {
          tarball: `https://registry.test/${name}/-/${name}-${version}.tgz`,
          integrity: `sha512-${name}-${version}`,
        },
      },
    },
  };
}

describe("resolveSourceWorkflowClosure", () => {
  test("resolves a single-package source workflow with an external dep", async () => {
    const reads = treeReads({
      packages: {
        ".": {
          json: {
            name: "@wf/app",
            version: "1.0.0",
            dependencies: { "left-pad": "^1.0.0" },
          },
          treeOid: "root-tree-oid",
        },
      },
    });
    const fetchPackument: PackumentFetcher = async (name) => {
      if (name === "left-pad") return packument("left-pad", "1.3.0");
      throw new Error(`no packument: ${name}`);
    };

    const manifest = await resolveSourceWorkflowClosure({
      source: {
        kind: "asset",
        assetId: "asset_wf",
        package: { format: "source", commitSha: "commit-abc" },
      },
      reads,
      // A distinctive name so the assertion below proves the caller-supplied
      // registry name (not an invented one) is what the external entry carries.
      registryName: "npm-corp",
      registryConfig: { url: "https://registry.test" },
      fetchPackument,
    });

    expect(manifest.topLevel).toEqual([{ name: "@wf/app", version: "1.0.0" }]);

    const byName = new Map(manifest.entries.map((e) => [e.name, e]));
    expect(byName.size).toBe(2);

    const app = byName.get("@wf/app");
    expect(app?.source).toEqual({
      kind: "asset",
      assetId: "asset_wf",
      package: {
        format: "source",
        commitSha: "commit-abc",
        packageDir: ".",
        treeOid: "root-tree-oid",
      },
    });

    const leftPad = byName.get("left-pad");
    expect(leftPad?.version).toBe("1.3.0");
    expect(leftPad?.source).toEqual({
      kind: "registry",
      registry: "npm-corp",
      integrity: "sha512-left-pad-1.3.0",
    });
  });

  test("resolves a self-contained source workflow with no external deps", async () => {
    const reads = treeReads({
      packages: {
        ".": {
          json: { name: "@wf/solo", version: "2.1.0" },
          treeOid: "solo-tree",
        },
      },
    });

    const manifest = await resolveSourceWorkflowClosure({
      source: {
        kind: "asset",
        assetId: "asset_solo",
        package: { format: "source", commitSha: "c1" },
      },
      reads,
      registryName: "npmjs",
      registryConfig: { url: "https://registry.test" },
    });

    expect(manifest.entries).toHaveLength(1);
    expect(manifest.entries[0]?.source).toEqual({
      kind: "asset",
      assetId: "asset_solo",
      package: {
        format: "source",
        commitSha: "c1",
        packageDir: ".",
        treeOid: "solo-tree",
      },
    });
  });

  test("rejects a workspaces monorepo (not yet supported)", async () => {
    const reads = treeReads({
      packages: {
        ".": {
          json: {
            name: "@wf/root",
            version: "1.0.0",
            workspaces: ["packages/*"],
          },
          treeOid: "root",
        },
      },
    });

    await expect(
      resolveSourceWorkflowClosure({
        source: {
          kind: "asset",
          assetId: "a",
          package: { format: "source", commitSha: "c" },
        },
        reads,
        registryName: "npmjs",
        registryConfig: { url: "https://registry.test" },
      }),
    ).rejects.toThrow(/monorepo workspaces are not yet supported/);
  });

  test("fails loud on a catalog: external specifier", async () => {
    const reads = treeReads({
      packages: {
        ".": {
          json: {
            name: "@wf/app",
            version: "1.0.0",
            dependencies: { "left-pad": "catalog:" },
          },
          treeOid: "root",
        },
      },
    });

    await expect(
      resolveSourceWorkflowClosure({
        source: {
          kind: "asset",
          assetId: "a",
          package: { format: "source", commitSha: "c" },
        },
        reads,
        registryName: "npmjs",
        registryConfig: { url: "https://registry.test" },
      }),
    ).rejects.toThrow(/catalog protocol, which is not yet supported/);
  });
});
