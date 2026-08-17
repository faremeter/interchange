import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";

import type { RegistryConfig, TarballFetcher } from "@intx/tool-packaging";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import { applyFrozenWorkflowClosure } from "./workflow-closure-apply";

const REGISTRY_NAME = "test-registry";
const WORKFLOW_PACKAGE_NAME = "@fixture/frozen-workflow";
const WORKFLOW_PACKAGE_VERSION = "1.4.2";
const WORKFLOW_ID = "fixture-closure-workflow";

// The pinned code the frozen closure carries: an ESM entry whose default
// export is a plain, envelope-valid workflow definition. It imports nothing so
// the closure needs no dependency entries, keeping the fixture hermetic while
// still exercising the real fetch -> SRI-verify -> extract -> layout -> import
// path.
const WORKFLOW_ENTRY_SOURCE = `export default {
  id: ${JSON.stringify(WORKFLOW_ID)},
  triggers: [],
  steps: { done: { kind: "sleep", id: "done", durationMs: 1 } },
  stepOrder: ["done"],
};
`;

let scratchRoot: string;
let cacheRoot: string;
let instanceDir: string;

beforeEach(async () => {
  scratchRoot = await fs.mkdtemp(
    path.join(os.tmpdir(), "sidecar-wf-closure-apply-"),
  );
  cacheRoot = path.join(scratchRoot, "cache");
  instanceDir = path.join(scratchRoot, "instance");
  await fs.mkdir(cacheRoot, { recursive: true });
  await fs.mkdir(instanceDir, { recursive: true });
});

afterEach(async () => {
  await fs.rm(scratchRoot, { recursive: true, force: true });
});

/**
 * Pack the fixture workflow package into an npm-style tarball (with the
 * `package/` prefix the loader strips) and return its bytes plus the SRI
 * integrity string the frozen manifest pins.
 */
async function packWorkflowFixture(): Promise<{
  bytes: Uint8Array;
  integrity: string;
}> {
  const stagingDir = path.join(scratchRoot, "fixture-source");
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: WORKFLOW_PACKAGE_NAME,
      version: WORKFLOW_PACKAGE_VERSION,
      type: "module",
      interchange: { workflow: "index.js" },
    }),
  );
  await fs.writeFile(path.join(packageDir, "index.js"), WORKFLOW_ENTRY_SOURCE);

  const tarballPath = path.join(stagingDir, "out.tgz");
  const proc = Bun.spawnSync([
    "tar",
    "-czf",
    tarballPath,
    "-C",
    stagingDir,
    "package",
  ]);
  if (!proc.success) {
    throw new Error(
      `tar failed to pack the fixture: ${new TextDecoder().decode(proc.stderr)}`,
    );
  }
  const bytes = await fs.readFile(tarballPath);
  const integrity = `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
  return { bytes, integrity };
}

function registries(): ReadonlyMap<string, RegistryConfig> {
  return new Map([[REGISTRY_NAME, { url: "https://registry.invalid" }]]);
}

describe("applyFrozenWorkflowClosure", () => {
  test("materializes the pinned closure and loads the pinned code", async () => {
    const fixture = await packWorkflowFixture();
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [
        {
          name: WORKFLOW_PACKAGE_NAME,
          version: WORKFLOW_PACKAGE_VERSION,
          source: {
            kind: "registry",
            registry: REGISTRY_NAME,
            integrity: fixture.integrity,
          },
        },
      ],
    };

    // Record every fetch so the test can prove the sidecar fetched EXACTLY the
    // frozen concrete version -- no packument round-trip, no re-resolution.
    const fetched: string[] = [];
    const fetchTarball: TarballFetcher = async (entry) => {
      fetched.push(`${entry.name}@${entry.version}`);
      return fixture.bytes;
    };

    const applied = await applyFrozenWorkflowClosure({
      source: { kind: "registry", registry: REGISTRY_NAME },
      closure: manifest,
      instanceDir,
      cacheRoot,
      cacheMaxBytes: 10_000_000,
      registryMaxTarballBytes: 10_000_000,
      registries: registries(),
      fetchTarball,
    });

    // The pinned code was evaluated to its validated definition.
    expect(applied.definition.id).toBe(WORKFLOW_ID);

    // The frozen closure was applied byte-for-byte: the workflow package's
    // package.json is present in the materialized store directory.
    const pkgJson = JSON.parse(
      await fs.readFile(path.join(applied.packageDir, "package.json"), "utf8"),
    );
    expect(pkgJson.name).toBe(WORKFLOW_PACKAGE_NAME);
    expect(pkgJson.version).toBe(WORKFLOW_PACKAGE_VERSION);

    // No re-resolution: the only fetch was for the exact pinned concrete
    // version the frozen closure named.
    expect(fetched).toEqual([
      `${WORKFLOW_PACKAGE_NAME}@${WORKFLOW_PACKAGE_VERSION}`,
    ]);
  });

  test("rejects fetched bytes whose SRI does not match the frozen closure", async () => {
    const fixture = await packWorkflowFixture();
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [
        {
          name: WORKFLOW_PACKAGE_NAME,
          version: WORKFLOW_PACKAGE_VERSION,
          // A pinned integrity that does not describe the returned bytes.
          source: {
            kind: "registry",
            registry: REGISTRY_NAME,
            integrity: `sha512-${createHash("sha512").update("tampered").digest("base64")}`,
          },
        },
      ],
    };
    const fetchTarball: TarballFetcher = async () => fixture.bytes;

    await expect(
      applyFrozenWorkflowClosure({
        source: { kind: "registry", registry: REGISTRY_NAME },
        closure: manifest,
        instanceDir,
        cacheRoot,
        cacheMaxBytes: 10_000_000,
        registryMaxTarballBytes: 10_000_000,
        registries: registries(),
        fetchTarball,
      }),
    ).rejects.toThrow(/integrity\.mismatch/);
  });

  test("rejects a closure that does not pin exactly one top-level package", async () => {
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };
    await expect(
      applyFrozenWorkflowClosure({
        source: { kind: "registry", registry: REGISTRY_NAME },
        closure: manifest,
        instanceDir,
        cacheRoot,
        cacheMaxBytes: 10_000_000,
        registryMaxTarballBytes: 10_000_000,
        registries: registries(),
      }),
    ).rejects.toThrow(/exactly one top-level pin/);
  });

  test("rejects a source registry the sidecar is not configured for", async () => {
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [
        {
          name: WORKFLOW_PACKAGE_NAME,
          version: WORKFLOW_PACKAGE_VERSION,
          source: {
            kind: "registry",
            registry: REGISTRY_NAME,
            integrity: `sha512-${createHash("sha512").update("x").digest("base64")}`,
          },
        },
      ],
    };
    await expect(
      applyFrozenWorkflowClosure({
        source: { kind: "registry", registry: "unconfigured-registry" },
        closure: manifest,
        instanceDir,
        cacheRoot,
        cacheMaxBytes: 10_000_000,
        registryMaxTarballBytes: 10_000_000,
        registries: registries(),
      }),
    ).rejects.toThrow(/is not in the sidecar registry config/);
  });

  test("materializes an asset-sourced closure from a mounted asset root", async () => {
    // This is the durable-store read path: the deploy checked the source asset
    // out at <assetRoot>/<mountPath>/, and both deploy and restore materialize
    // from there with no HTTP fetch. Stage the tarball as a plain file where the
    // loader resolves a kind:"asset" entry, then apply.
    const fixture = await packWorkflowFixture();
    const assetId = "asset_deploy";
    const mountPath = "source-assets/asset_deploy/";
    const tarballRel = "tarballs/wf.tgz";
    const assetRoot = path.join(scratchRoot, "asset-store");
    const tarballAbs = path.join(assetRoot, mountPath, tarballRel);
    await fs.mkdir(path.dirname(tarballAbs), { recursive: true });
    await fs.writeFile(tarballAbs, fixture.bytes);

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [
        {
          name: WORKFLOW_PACKAGE_NAME,
          version: WORKFLOW_PACKAGE_VERSION,
          source: {
            kind: "asset",
            assetId,
            package: {
              format: "tarball",
              path: tarballRel,
              integrity: fixture.integrity,
            },
          },
        },
      ],
    };

    const applied = await applyFrozenWorkflowClosure({
      source: { kind: "asset", assetId, package: { format: "tarball" } },
      closure: manifest,
      instanceDir,
      cacheRoot,
      cacheMaxBytes: 10_000_000,
      registryMaxTarballBytes: 10_000_000,
      registries: registries(),
      assetRoot,
      assetMounts: new Map([[assetId, mountPath]]),
      gitDirs: new Map(),
    });

    expect(applied.definition.id).toBe(WORKFLOW_ID);
  });

  test("fails closed on a source-format asset closure", async () => {
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [],
    };
    await expect(
      applyFrozenWorkflowClosure({
        source: {
          kind: "asset",
          assetId: "asset_abc",
          package: {
            format: "source",
            commitSha: "0123456789abcdef0123456789abcdef01234567",
          },
        },
        closure: manifest,
        instanceDir,
        cacheRoot,
        cacheMaxBytes: 10_000_000,
        registryMaxTarballBytes: 10_000_000,
        registries: registries(),
      }),
    ).rejects.toThrow(
      /source-format asset workflow closures cannot be materialized/,
    );
  });
});
