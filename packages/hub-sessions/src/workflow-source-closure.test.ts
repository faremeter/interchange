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

  test("fails loud on a catalog: specifier with no matching root catalog entry", async () => {
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
    ).rejects.toThrow(/declares no "catalog" entry for it/);
  });

  test("fails loud on a pnpm-workspace.yaml monorepo layout", async () => {
    // A pnpm root has no package.json `workspaces` field (members live in
    // pnpm-workspace.yaml), so it would otherwise be misread as a single
    // package. The resolver detects the file and rejects with a clear message.
    const reads: SourceTreeReads = {
      async readBlob(path) {
        if (path === "package.json") {
          return new TextEncoder().encode(
            JSON.stringify({ name: "@wf/root", private: true }),
          );
        }
        throw new Error(`no blob at ${path}`);
      },
      async listDir(dir): Promise<CommittedTreeEntry[]> {
        if (dir === "." || dir === "") {
          return [
            { name: "package.json", oid: "root-pkg", type: "blob" },
            { name: "pnpm-workspace.yaml", oid: "pnpm-ws", type: "blob" },
          ];
        }
        return [];
      },
      async treeOid() {
        return "root-tree";
      },
    };

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
    ).rejects.toThrow(/pnpm workspace layout is not supported/);
  });
});

// A richer tree fixture for monorepos: `package.json` blobs keyed by dir plus a
// derived directory listing, so `listDir(base)` returns the member subtrees the
// producer's glob expansion walks. Each dir gets a tree oid keyed by its path.
// `extraFiles` registers arbitrary blobs (e.g. a non-package directory a glob
// matches), so their parent directories surface in `listDir` without a
// package.json.
function monorepoReads(
  packages: Record<string, { json: unknown; treeOid?: string }>,
  extraFiles: Record<string, string> = {},
): SourceTreeReads {
  const blobs = new Map<string, Uint8Array>();
  const oids = new Map<string, string>();
  const dirChildren = new Map<string, Map<string, "tree" | "blob">>();
  const ensureDir = (d: string): Map<string, "tree" | "blob"> => {
    let m = dirChildren.get(d);
    if (m === undefined) {
      m = new Map();
      dirChildren.set(d, m);
    }
    return m;
  };
  const registerBlob = (blobPath: string): void => {
    const segments = blobPath.split("/");
    const fileName = segments.pop();
    if (fileName === undefined) return;
    let parent = "";
    for (const seg of segments) {
      ensureDir(parent).set(seg, "tree");
      parent = parent === "" ? seg : `${parent}/${seg}`;
      ensureDir(parent);
    }
    ensureDir(parent).set(fileName, "blob");
  };
  ensureDir("");
  for (const [dir, { json, treeOid }] of Object.entries(packages)) {
    const norm = dir === "." ? "" : dir;
    const blobPath = norm === "" ? "package.json" : `${norm}/package.json`;
    blobs.set(blobPath, new TextEncoder().encode(JSON.stringify(json)));
    oids.set(dir, treeOid ?? `tree-${dir}`);
    registerBlob(blobPath);
  }
  for (const [filePath, contents] of Object.entries(extraFiles)) {
    blobs.set(filePath, new TextEncoder().encode(contents));
    registerBlob(filePath);
  }
  return {
    async readBlob(path) {
      const bytes = blobs.get(path);
      if (bytes === undefined) throw new Error(`no blob at ${path}`);
      return bytes;
    },
    async listDir(dir): Promise<CommittedTreeEntry[]> {
      const norm = dir === "." ? "" : dir;
      const children = dirChildren.get(norm);
      if (children === undefined) return [];
      return [...children].map(([name, type]) => ({
        name,
        oid: `${norm}/${name}`,
        type,
      }));
    },
    async treeOid(dir) {
      return oids.get(dir) ?? null;
    },
  };
}

function resolveMonorepo(
  reads: SourceTreeReads,
  packageName: string,
  fetchPackument?: PackumentFetcher,
): ReturnType<typeof resolveSourceWorkflowClosure> {
  return resolveSourceWorkflowClosure({
    source: {
      kind: "asset",
      assetId: "asset_mono",
      package: { format: "source", commitSha: "commit-mono", packageName },
    },
    reads,
    registryName: "npmjs",
    registryConfig: { url: "https://registry.test" },
    ...(fetchPackument !== undefined ? { fetchPackument } : {}),
  });
}

describe("resolveSourceWorkflowClosure monorepo", () => {
  test("enumerates members and recurses a workspace:* dependency", async () => {
    const reads = monorepoReads({
      ".": {
        json: { name: "@wf/root", private: true, workspaces: ["packages/*"] },
      },
      "packages/app": {
        json: {
          name: "@wf/app",
          version: "1.0.0",
          dependencies: { "@wf/lib": "workspace:*" },
        },
        treeOid: "app-tree",
      },
      "packages/lib": {
        json: { name: "@wf/lib", version: "2.0.0" },
        treeOid: "lib-tree",
      },
    });

    const manifest = await resolveMonorepo(reads, "@wf/app");

    expect(manifest.topLevel).toEqual([{ name: "@wf/app", version: "1.0.0" }]);
    const byName = new Map(manifest.entries.map((e) => [e.name, e]));
    expect(byName.size).toBe(2);
    expect(byName.get("@wf/app")?.source).toEqual({
      kind: "asset",
      assetId: "asset_mono",
      package: {
        format: "source",
        commitSha: "commit-mono",
        packageDir: "packages/app",
        treeOid: "app-tree",
      },
    });
    expect(byName.get("@wf/lib")?.source).toEqual({
      kind: "asset",
      assetId: "asset_mono",
      package: {
        format: "source",
        commitSha: "commit-mono",
        packageDir: "packages/lib",
        treeOid: "lib-tree",
      },
    });
  });

  test("expands a bare catalog: dependency against the root catalog", async () => {
    const reads = monorepoReads({
      ".": {
        json: {
          name: "@wf/root",
          private: true,
          workspaces: ["packages/*"],
          catalog: { "left-pad": "^1.0.0" },
        },
      },
      "packages/app": {
        json: {
          name: "@wf/app",
          version: "1.0.0",
          dependencies: { "left-pad": "catalog:" },
        },
      },
    });
    const fetchPackument: PackumentFetcher = async (name) => {
      if (name === "left-pad") return packument("left-pad", "1.3.0");
      throw new Error(`no packument: ${name}`);
    };

    const manifest = await resolveMonorepo(reads, "@wf/app", fetchPackument);

    const leftPad = manifest.entries.find((e) => e.name === "left-pad");
    expect(leftPad?.version).toBe("1.3.0");
    expect(leftPad?.source.kind).toBe("registry");
  });

  test("fails loud when two members pin one external at conflicting ranges", async () => {
    const reads = monorepoReads({
      ".": { json: { name: "@wf/root", private: true, workspaces: ["p/*"] } },
      "p/app": {
        json: {
          name: "@wf/app",
          version: "1.0.0",
          dependencies: { "@wf/lib": "workspace:*", "left-pad": "^1.0.0" },
        },
      },
      "p/lib": {
        json: {
          name: "@wf/lib",
          version: "1.0.0",
          dependencies: { "left-pad": "^2.0.0" },
        },
      },
    });

    await expect(resolveMonorepo(reads, "@wf/app")).rejects.toThrow(
      /conflicting ranges/,
    );
  });

  test("fails loud on a member that depends on the workspace root", async () => {
    const reads = monorepoReads({
      ".": {
        json: { name: "@wf/root", private: true, workspaces: ["packages/*"] },
      },
      "packages/app": {
        json: {
          name: "@wf/app",
          version: "1.0.0",
          dependencies: { "@wf/root": "^1.0.0" },
        },
      },
    });

    await expect(resolveMonorepo(reads, "@wf/app")).rejects.toThrow(
      /depends on the workspace root/,
    );
  });

  test("fails loud on a named catalog specifier", async () => {
    const reads = monorepoReads({
      ".": {
        json: { name: "@wf/root", private: true, workspaces: ["packages/*"] },
      },
      "packages/app": {
        json: {
          name: "@wf/app",
          version: "1.0.0",
          dependencies: { "left-pad": "catalog:corp" },
        },
      },
    });

    await expect(resolveMonorepo(reads, "@wf/app")).rejects.toThrow(
      /named catalog/,
    );
  });

  test("fails loud on an unsupported workspaces glob", async () => {
    const reads = monorepoReads({
      ".": {
        json: { name: "@wf/root", private: true, workspaces: ["packages/**"] },
      },
      "packages/app": {
        json: { name: "@wf/app", version: "1.0.0" },
      },
    });

    await expect(resolveMonorepo(reads, "@wf/app")).rejects.toThrow(
      /unsupported workspaces glob/,
    );
  });

  test("fails loud on the object form of workspaces", async () => {
    const reads = monorepoReads({
      ".": {
        json: { name: "@wf/root", private: true, workspaces: { packages: [] } },
      },
    });

    await expect(resolveMonorepo(reads, "@wf/app")).rejects.toThrow(
      /object form.*is not supported/,
    );
  });

  test("requires a member to declare name and version", async () => {
    const reads = monorepoReads({
      ".": {
        json: { name: "@wf/root", private: true, workspaces: ["packages/*"] },
      },
      "packages/app": {
        json: { name: "@wf/app" },
      },
    });

    await expect(resolveMonorepo(reads, "@wf/app")).rejects.toThrow(
      /must declare string "name" and "version"/,
    );
  });

  test("skips a non-package directory a workspaces glob matches", async () => {
    // `packages/docs` matches `packages/*` but has no package.json; bun/yarn
    // skip it. Enumeration must not fault on it while still resolving the real
    // members.
    const reads = monorepoReads(
      {
        ".": {
          json: { name: "@wf/root", private: true, workspaces: ["packages/*"] },
        },
        "packages/app": {
          json: {
            name: "@wf/app",
            version: "1.0.0",
            dependencies: { "@wf/lib": "workspace:*" },
          },
        },
        "packages/lib": {
          json: { name: "@wf/lib", version: "2.0.0" },
        },
      },
      { "packages/docs/README.md": "# docs\n" },
    );

    const manifest = await resolveMonorepo(reads, "@wf/app");

    const names = new Set(manifest.entries.map((e) => e.name));
    expect(names).toEqual(new Set(["@wf/app", "@wf/lib"]));
  });
});
