// Hub-side workflow closure resolution over an in-process fixture registry.
//
// Mirrors the fixture-registry harness the tool-packaging end-to-end suite
// uses (`buildFixture` / `makePackumentFromFixtures` / the `PackumentFetcher`
// seam) but drives it through `resolveWorkflowClosure`, the workflow-source
// entry point. No sidecar and no network: the resolver reads synthetic
// packuments only. The test asserts the frozen closure carries concrete
// versions and integrity SRIs for the top-level package and its transitive
// dependency.
//
// Integrity is computed with `node:crypto` rather than `ssri` so the test
// depends only on packages already declared by `hub-sessions`; the SRI shape
// (`sha512-<base64 digest>`) is identical to what the real registry serves.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import * as tar from "tar";

import type { Packument, PackumentFetcher } from "@intx/tool-packaging";
import type { WorkflowDefinitionRegistrySource } from "@intx/types/workflow-sources";
import { getToolPackageSourceContentIdentity } from "@intx/types/tool-packages";

import { resolveWorkflowClosure } from "./workflow-closure-resolution";
import type { SourceTreeReads } from "./workflow-source-closure";
import type { CommittedTreeEntry } from "./repo-store/types";

let scratch: string;
let fixtureRoot: string;

beforeEach(async () => {
  scratch = await fs.mkdtemp(path.join(os.tmpdir(), "wf-closure-"));
  fixtureRoot = path.join(scratch, "fixtures");
  await fs.mkdir(fixtureRoot, { recursive: true });
});

afterEach(async () => {
  await fs.rm(scratch, { recursive: true, force: true });
});

interface Fixture {
  name: string;
  version: string;
  integrity: string;
  tarballUrl: string;
  dependencies: Record<string, string>;
  bytes: Uint8Array;
}

function sriFromBytes(bytes: Uint8Array): string {
  return `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
}

async function buildFixture(spec: {
  name: string;
  version: string;
  dependencies?: Record<string, string>;
}): Promise<Fixture> {
  const stagingDir = path.join(
    fixtureRoot,
    `${spec.name.replace("/", "_")}-${spec.version}`,
  );
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });

  const dependencies = spec.dependencies ?? {};
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: spec.name,
      version: spec.version,
      dependencies,
      interchange: { tools: "./tools.js" },
    }),
  );
  await fs.writeFile(
    path.join(packageDir, "tools.js"),
    "// stub; hub-side resolution never executes this\n",
  );

  const tarballPath = path.join(stagingDir, "out.tgz");
  await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
    "package",
  ]);
  const bytes = await fs.readFile(tarballPath);
  const tarballUrl = `https://registry.test/${spec.name}/-/${
    spec.name.startsWith("@") ? spec.name.split("/")[1] : spec.name
  }-${spec.version}.tgz`;
  return {
    name: spec.name,
    version: spec.version,
    integrity: sriFromBytes(bytes),
    tarballUrl,
    dependencies,
    bytes,
  };
}

/**
 * In-process `readBlob`/`listBlobs` over a synthetic `package-registry` asset
 * whose `tarballs/` directory holds the given tarballs, keyed by filename. This
 * is the asset-arm analog of the `fetchPackument` seam the registry tests use:
 * `AssetRegistrySource` lists `tarballs/`, reads each `.tgz`, and synthesizes
 * the packuments itself.
 */
function makeAssetReaders(tarballs: Record<string, Uint8Array>): {
  readBlob: (path: string) => Promise<Uint8Array>;
  listBlobs: (dir: string) => Promise<string[]>;
} {
  return {
    readBlob: async (blobPath) => {
      const filename = blobPath.replace(/^tarballs\//, "");
      const bytes = tarballs[filename];
      if (bytes === undefined) {
        throw new Error(`no blob at ${blobPath}`);
      }
      return bytes;
    },
    listBlobs: async (dir) => (dir === "tarballs" ? Object.keys(tarballs) : []),
  };
}

// A `SourceTreeReads` fake over `package.json` blobs keyed by packageDir and a
// tree oid per packageDir, the source-arm analog of `makeAssetReaders`. The
// git-tree resolver's own logic is unit-covered in
// `workflow-source-closure.test.ts`; this drives it through `resolveWorkflowClosure`.
function makeSourceTreeReads(
  packages: Record<string, { json: unknown; treeOid: string }>,
): SourceTreeReads {
  const blobs = new Map<string, Uint8Array>();
  const oids = new Map<string, string>();
  for (const [dir, { json, treeOid }] of Object.entries(packages)) {
    const blobPath = dir === "." ? "package.json" : `${dir}/package.json`;
    blobs.set(blobPath, new TextEncoder().encode(JSON.stringify(json)));
    oids.set(dir, treeOid);
  }
  return {
    readBlob: async (blobPath) => {
      const bytes = blobs.get(blobPath);
      if (bytes === undefined) throw new Error(`no blob at ${blobPath}`);
      return bytes;
    },
    listDir: async (): Promise<CommittedTreeEntry[]> => [],
    treeOid: async (dir) => oids.get(dir) ?? null,
  };
}

function makePackumentFromFixtures(fixtures: readonly Fixture[]): Packument[] {
  return fixtures.map((f) => ({
    name: f.name,
    versions: {
      [f.version]: {
        name: f.name,
        version: f.version,
        dist: {
          tarball: f.tarballUrl,
          integrity: f.integrity,
        },
        dependencies: f.dependencies,
      },
    },
  }));
}

describe("resolveWorkflowClosure", () => {
  test("resolves a registry-sourced workflow to concrete versions + SRIs", async () => {
    const dep = await buildFixture({
      name: "wf-fixture-dep",
      version: "1.2.0",
    });
    const top = await buildFixture({
      name: "wf-fixture-top",
      version: "1.0.0",
      dependencies: { [dep.name]: "^1.0.0" },
    });

    const packuments = makePackumentFromFixtures([top, dep]);
    const fetchPackument: PackumentFetcher = async (name) => {
      const p = packuments.find((q) => q.name === name);
      if (p === undefined) throw new Error(`no packument: ${name}`);
      return p;
    };

    const source: WorkflowDefinitionRegistrySource = {
      kind: "registry",
      registry: "fixture-registry",
    };

    const manifest = await resolveWorkflowClosure({
      source,
      pin: `${top.name}@${top.version}`,
      registryConfig: { url: "https://registry.test" },
      fetchPackument,
    });

    expect(manifest.topLevel).toEqual([
      { name: top.name, version: top.version },
    ]);

    const byName = new Map(manifest.entries.map((e) => [e.name, e]));
    expect(byName.size).toBe(2);

    const topEntry = byName.get(top.name);
    if (topEntry === undefined) throw new Error("missing top-level entry");
    expect(topEntry.version).toBe(top.version);
    expect(getToolPackageSourceContentIdentity(topEntry.source)).toBe(
      top.integrity,
    );
    expect(topEntry.source).toEqual({
      kind: "registry",
      registry: source.registry,
      integrity: top.integrity,
    });

    const depEntry = byName.get(dep.name);
    if (depEntry === undefined) throw new Error("missing transitive entry");
    expect(depEntry.version).toBe(dep.version);
    expect(getToolPackageSourceContentIdentity(depEntry.source)).toBe(
      dep.integrity,
    );
    expect(depEntry.source).toEqual({
      kind: "registry",
      registry: source.registry,
      integrity: dep.integrity,
    });
  });

  test("reuses parsePin to accept a range spec for the top-level pin", async () => {
    const only = await buildFixture({
      name: "wf-ranged",
      version: "2.3.4",
    });
    const packuments = makePackumentFromFixtures([only]);
    const fetchPackument: PackumentFetcher = async (name) => {
      const p = packuments.find((q) => q.name === name);
      if (p === undefined) throw new Error(`no packument: ${name}`);
      return p;
    };

    const manifest = await resolveWorkflowClosure({
      source: { kind: "registry", registry: "fixture-registry" },
      pin: `${only.name}@^2.0.0`,
      registryConfig: { url: "https://registry.test" },
      fetchPackument,
    });

    // A ranged pin freezes to the concrete version the registry advertises.
    expect(manifest.topLevel).toEqual([
      { name: only.name, version: only.version },
    ]);
    expect(manifest.entries).toHaveLength(1);
    const onlyEntry = manifest.entries[0];
    if (onlyEntry === undefined) throw new Error("missing entry");
    expect(onlyEntry.version).toBe(only.version);
    expect(getToolPackageSourceContentIdentity(onlyEntry.source)).toBe(
      only.integrity,
    );
  });

  test("resolves an asset-sourced workflow to a kind:asset entry", async () => {
    const top = await buildFixture({ name: "wf-asset-top", version: "1.0.0" });
    const tarballFile = "wf-asset-top-1.0.0.tgz";
    const { readBlob, listBlobs } = makeAssetReaders({
      [tarballFile]: top.bytes,
    });

    const manifest = await resolveWorkflowClosure({
      source: {
        kind: "asset",
        assetId: "asset_top",
        package: { format: "tarball" },
      },
      pin: `${top.name}@${top.version}`,
      readBlob,
      listBlobs,
    });

    expect(manifest.topLevel).toEqual([
      { name: top.name, version: top.version },
    ]);
    expect(manifest.entries).toHaveLength(1);
    const entry = manifest.entries[0];
    if (entry === undefined) throw new Error("missing entry");
    expect(entry.version).toBe(top.version);
    expect(getToolPackageSourceContentIdentity(entry.source)).toBe(
      top.integrity,
    );
    expect(entry.source).toEqual({
      kind: "asset",
      assetId: "asset_top",
      package: {
        format: "tarball",
        path: `tarballs/${tarballFile}`,
        integrity: top.integrity,
      },
    });
  });

  test("routes a source-format asset to the git-tree resolver", async () => {
    // The workflow package lives as a source subtree; its one external dep
    // resolves through the npm registry into a tarball-backed registry entry.
    const dep = await buildFixture({
      name: "wf-src-ext-dep",
      version: "1.4.0",
    });
    const packuments = makePackumentFromFixtures([dep]);
    const fetchPackument: PackumentFetcher = async (name) => {
      const p = packuments.find((q) => q.name === name);
      if (p === undefined) throw new Error(`no packument: ${name}`);
      return p;
    };

    const reads = makeSourceTreeReads({
      ".": {
        json: {
          name: "@wf/src-app",
          version: "3.0.0",
          dependencies: { [dep.name]: "^1.0.0" },
        },
        treeOid: "root-tree",
      },
    });

    const manifest = await resolveWorkflowClosure({
      source: {
        kind: "asset",
        assetId: "asset_src",
        package: { format: "source", commitSha: "commit-xyz" },
      },
      reads,
      registryName: "npmjs",
      registryConfig: { url: "https://registry.test" },
      fetchPackument,
    });

    expect(manifest.topLevel).toEqual([
      { name: "@wf/src-app", version: "3.0.0" },
    ]);
    const byName = new Map(manifest.entries.map((e) => [e.name, e]));
    expect(byName.size).toBe(2);

    // The workflow package is a source entry pinned to the tree oid.
    const app = byName.get("@wf/src-app");
    expect(app?.source).toEqual({
      kind: "asset",
      assetId: "asset_src",
      package: {
        format: "source",
        commitSha: "commit-xyz",
        packageDir: ".",
        treeOid: "root-tree",
      },
    });

    // The external dep is a registry entry with the tarball's SRI.
    const ext = byName.get(dep.name);
    expect(ext?.version).toBe(dep.version);
    expect(ext?.source.kind).toBe("registry");
    if (ext === undefined) throw new Error("missing external entry");
    expect(getToolPackageSourceContentIdentity(ext.source)).toBe(dep.integrity);
  });

  test("fails loud when the asset does not publish a declared dependency", async () => {
    // The workflow package declares a dependency the asset does not hold; the
    // single-source asset resolver has no npm fallback, so the walk must throw.
    const top = await buildFixture({
      name: "wf-asset-needs-dep",
      version: "1.0.0",
      dependencies: { "@external/absent": "^1.0.0" },
    });
    const { readBlob, listBlobs } = makeAssetReaders({
      "wf-asset-needs-dep-1.0.0.tgz": top.bytes,
    });

    await expect(
      resolveWorkflowClosure({
        source: {
          kind: "asset",
          assetId: "asset_top",
          package: { format: "tarball" },
        },
        pin: `${top.name}@${top.version}`,
        readBlob,
        listBlobs,
      }),
    ).rejects.toThrow(/no tarball publishing package "@external\/absent"/);
  });
});
