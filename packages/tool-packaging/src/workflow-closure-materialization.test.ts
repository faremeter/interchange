import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";

import type { WorkflowProbeRequestFrame } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import { createTarballCache, type TarballCache } from "./cache";
import type { TarballFetcher } from "./loader";
import { hostPlatform } from "./materialization-config";
import type { RegistryConfig } from "./resolver";
import {
  createWorkflowClosureMaterializer,
  type WorkflowClosureMaterializerConfig,
} from "./workflow-closure-materialization";

const REGISTRY_NAME = "test-registry";
const WORKFLOW_PACKAGE_NAME = "@fixture/wf-probe";
const WORKFLOW_PACKAGE_VERSION = "1.0.0";
const DEP_PACKAGE_NAME = "@fixture/dep";
const DEP_PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "index.js";

type AssetDelivery = WorkflowClosureMaterializerConfig["materializeAssets"];

// The pinned workflow entry writes a sentinel file as an import-time side
// effect. The materializer lays the closure out WITHOUT importing any author
// code, so the sentinel must never appear -- its absence proves no module was
// evaluated on the host. The path is baked into the source so the test does
// not read the process environment.
function workflowEntrySource(sentinelPath: string): string {
  return `
import { writeFileSync } from "node:fs";
writeFileSync(${JSON.stringify(sentinelPath)}, "imported");
export default {
  id: "probe-materializer-fixture",
  triggers: [],
  steps: { done: { kind: "sleep", id: "done", durationMs: 1 } },
  stepOrder: ["done"],
};
`;
}

let scratchRoot: string;
let cache: TarballCache;
let materializerScratch: string;
let fixtureSourceRoot: string;
let assetDeliveries: Parameters<AssetDelivery>[0][];

beforeEach(async () => {
  scratchRoot = await fs.mkdtemp(
    path.join(os.tmpdir(), "sidecar-wf-probe-materialize-"),
  );
  const cacheRoot = path.join(scratchRoot, "cache");
  materializerScratch = path.join(scratchRoot, "probe-closures");
  fixtureSourceRoot = path.join(scratchRoot, "fixture-source");
  assetDeliveries = [];
  await fs.mkdir(cacheRoot, { recursive: true });
  cache = createTarballCache({ rootDir: cacheRoot, maxBytes: 10_000_000 });
  await fs.mkdir(materializerScratch, { recursive: true });
  await fs.mkdir(fixtureSourceRoot, { recursive: true });
});

afterEach(async () => {
  await fs.rm(scratchRoot, { recursive: true, force: true });
});

/**
 * Pack a package directory into an npm-style tarball (with the `package/`
 * prefix the loader strips) and return its bytes plus the SRI integrity the
 * frozen manifest pins.
 */
async function packFixture(
  pkgJson: Record<string, unknown>,
  files: Record<string, string>,
): Promise<{ bytes: Uint8Array; integrity: string }> {
  const stagingDir = path.join(
    fixtureSourceRoot,
    `${String(pkgJson.name).replace("/", "_")}-${String(pkgJson.version)}`,
  );
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify(pkgJson),
  );
  for (const [rel, contents] of Object.entries(files)) {
    const dest = path.join(packageDir, rel);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    await fs.writeFile(dest, contents);
  }

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

/** Records the call and returns empty maps. Registry closures fetch over HTTP. */
function recordEmptyAssets(): AssetDelivery {
  return async (args) => {
    assetDeliveries.push(args);
    return { assetMounts: new Map(), gitDirs: new Map() };
  };
}

/**
 * Records the call and places `bytes` where a tarball `kind: "asset"` entry
 * resolves. Asset unpacking itself is owned by the caller-supplied delivery
 * function; this stub is the mount that function returns.
 */
function deliverMountedTarball(
  assetId: string,
  mountPath: string,
  tarballRel: string,
  bytes: Uint8Array,
): AssetDelivery {
  return async (args) => {
    assetDeliveries.push(args);
    const dest = path.join(args.assetRoot, mountPath, tarballRel);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    await fs.writeFile(dest, bytes);
    return {
      assetMounts: new Map([[assetId, mountPath]]),
      gitDirs: new Map(),
    };
  };
}

function materializerConfig(
  fetchTarball?: TarballFetcher,
  materializeAssets: AssetDelivery = recordEmptyAssets(),
) {
  return {
    cache,
    registryMaxTarballBytes: 10_000_000,
    maxAssetPayloadBytes: 50_000_000,
    registries: registries(),
    scratchRoot: materializerScratch,
    host: hostPlatform("linux", "x64"),
    materializeAssets,
    ...(fetchTarball !== undefined ? { fetchTarball } : {}),
  };
}

function probeFrame(
  closure: ToolPackageManifest,
  entry: string,
): WorkflowProbeRequestFrame {
  return {
    type: "workflow.probe.request",
    requestId: "req-1",
    source: { kind: "registry", registry: REGISTRY_NAME },
    closure,
    entry,
  };
}

function deliveredAsset(assetId: string, mountPath: string) {
  return {
    assetId,
    mountPath,
    pack: "cGFjaw==",
    ref: "refs/heads/main",
    commitSha: "a".repeat(40),
  };
}

describe("createWorkflowClosureMaterializer", () => {
  test("lays out the frozen closure, resolves node_modules, and imports no author code", async () => {
    const sentinelPath = path.join(scratchRoot, "import-sentinel");
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
        dependencies: { [DEP_PACKAGE_NAME]: DEP_PACKAGE_VERSION },
      },
      { [WORKFLOW_ENTRY]: workflowEntrySource(sentinelPath) },
    );
    const dep = await packFixture(
      {
        name: DEP_PACKAGE_NAME,
        version: DEP_PACKAGE_VERSION,
        type: "module",
      },
      { "index.js": "export const x = 1;\n" },
    );

    const closure: ToolPackageManifest = {
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
            integrity: workflow.integrity,
          },
        },
        {
          name: DEP_PACKAGE_NAME,
          version: DEP_PACKAGE_VERSION,
          source: {
            kind: "registry",
            registry: REGISTRY_NAME,
            integrity: dep.integrity,
          },
        },
      ],
    };

    const fetched: string[] = [];
    const fetchTarball: TarballFetcher = async (entry) => {
      fetched.push(`${entry.name}@${entry.version}`);
      if (entry.name === WORKFLOW_PACKAGE_NAME) return workflow.bytes;
      if (entry.name === DEP_PACKAGE_NAME) return dep.bytes;
      throw new Error(`unexpected fetch for ${entry.name}@${entry.version}`);
    };

    const materialize = createWorkflowClosureMaterializer(
      materializerConfig(fetchTarball),
    );
    const materialized = await materialize(probeFrame(closure, WORKFLOW_ENTRY));

    // The workflow package's own package.json is present in the laid-out
    // store directory.
    const pkgJson = JSON.parse(
      await fs.readFile(
        path.join(materialized.packageDir, "package.json"),
        "utf8",
      ),
    );
    expect(pkgJson.name).toBe(WORKFLOW_PACKAGE_NAME);
    expect(pkgJson.version).toBe(WORKFLOW_PACKAGE_VERSION);

    // The dependency resolves through the laid-out node_modules graph.
    const depPkgJson = JSON.parse(
      await fs.readFile(
        path.join(
          materialized.packageDir,
          "node_modules",
          "@fixture",
          "dep",
          "package.json",
        ),
        "utf8",
      ),
    );
    expect(depPkgJson.name).toBe(DEP_PACKAGE_NAME);

    // Both frozen entries were fetched exactly once, at their pinned
    // concrete versions.
    expect(fetched.sort()).toEqual([
      `${DEP_PACKAGE_NAME}@${DEP_PACKAGE_VERSION}`,
      `${WORKFLOW_PACKAGE_NAME}@${WORKFLOW_PACKAGE_VERSION}`,
    ]);

    // A registry closure delivers no assets. The materializer still asks.
    expect(assetDeliveries).toHaveLength(1);
    expect(assetDeliveries[0]?.assets).toEqual([]);
    expect(assetDeliveries[0]?.maxAssetPayloadBytes).toBe(50_000_000);

    // No author code ran: the entry module's import-time sentinel was
    // never written.
    await expect(fs.stat(sentinelPath)).rejects.toThrow();

    // Cleanup removes the ephemeral scratch tree.
    await materialized.cleanup();
    await expect(fs.stat(materialized.packageDir)).rejects.toThrow();
  });

  test("fails loud when the closure pins no top-level package", async () => {
    const materialize = createWorkflowClosureMaterializer(materializerConfig());
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    };
    await expect(
      materialize(probeFrame(closure, WORKFLOW_ENTRY)),
    ).rejects.toThrow(/exactly one top-level package/);
  });

  test("fails loud when the closure pins more than one top-level package", async () => {
    const materialize = createWorkflowClosureMaterializer(materializerConfig());
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
        { name: DEP_PACKAGE_NAME, version: DEP_PACKAGE_VERSION },
      ],
      entries: [],
    };
    await expect(
      materialize(probeFrame(closure, WORKFLOW_ENTRY)),
    ).rejects.toThrow(/exactly one top-level package/);
  });

  test("materializes an asset-sourced closure from the delivered asset mount", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      },
      {
        [WORKFLOW_ENTRY]: workflowEntrySource(
          path.join(scratchRoot, "unused-sentinel"),
        ),
      },
    );
    const tarballPath = "tarballs/wf-probe-1.0.0.tgz";
    const assetId = "asset_probe";
    const mountPath = "package-registries/fixture/";

    const closure: ToolPackageManifest = {
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
              path: tarballPath,
              integrity: workflow.integrity,
            },
          },
        },
      ],
    };

    const asset = deliveredAsset(assetId, mountPath);
    const materialize = createWorkflowClosureMaterializer(
      materializerConfig(
        undefined,
        deliverMountedTarball(assetId, mountPath, tarballPath, workflow.bytes),
      ),
    );
    const materialized = await materialize({
      type: "workflow.probe.request",
      requestId: "req-asset",
      source: { kind: "asset", assetId, package: { format: "tarball" } },
      closure,
      entry: WORKFLOW_ENTRY,
      assets: [asset],
    });

    try {
      // The workflow package was laid out from the mount the delivery
      // function returned, SRI-verified against the frozen closure entry.
      const pkgJson = JSON.parse(
        await fs.readFile(
          path.join(materialized.packageDir, "package.json"),
          "utf8",
        ),
      );
      expect(pkgJson.name).toBe(WORKFLOW_PACKAGE_NAME);
      expect(pkgJson.version).toBe(WORKFLOW_PACKAGE_VERSION);

      expect(assetDeliveries).toHaveLength(1);
      const delivery = assetDeliveries[0];
      expect(delivery?.assets).toEqual([asset]);
      expect(delivery?.closure).toBe(closure);
      expect(delivery?.maxAssetPayloadBytes).toBe(50_000_000);
      expect(delivery?.assetRoot.endsWith(`${path.sep}workspace`)).toBe(true);
      expect(delivery?.gitDirRoot.endsWith(`${path.sep}gitdirs`)).toBe(true);
    } finally {
      await materialized.cleanup();
    }
  });

  test("forwards the asset payload cap and propagates a delivery failure", async () => {
    const assetId = "asset_probe";
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [],
    };
    const materializeAssets: AssetDelivery = async (args) => {
      assetDeliveries.push(args);
      throw new Error("delivery rejected the asset payload");
    };

    const materialize = createWorkflowClosureMaterializer({
      ...materializerConfig(undefined, materializeAssets),
      maxAssetPayloadBytes: 16,
    });
    await expect(
      materialize({
        type: "workflow.probe.request",
        requestId: "req-cap",
        source: { kind: "asset", assetId, package: { format: "tarball" } },
        closure,
        entry: WORKFLOW_ENTRY,
        assets: [deliveredAsset(assetId, "package-registries/fixture/")],
      }),
    ).rejects.toThrow(/delivery rejected the asset payload/);
    expect(assetDeliveries).toHaveLength(1);
    expect(assetDeliveries[0]?.maxAssetPayloadBytes).toBe(16);
  });

  test("rejects an asset-delivered tarball that fails its pinned integrity", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      },
      {
        [WORKFLOW_ENTRY]: workflowEntrySource(
          path.join(scratchRoot, "unused-sentinel"),
        ),
      },
    );
    const assetId = "asset_probe";
    const tarballPath = "tarballs/wf-probe-1.0.0.tgz";
    const mountPath = "package-registries/fixture/";

    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [
        {
          name: WORKFLOW_PACKAGE_NAME,
          version: WORKFLOW_PACKAGE_VERSION,
          // A pinned integrity that does not describe the delivered tarball.
          source: {
            kind: "asset",
            assetId,
            package: {
              format: "tarball",
              path: tarballPath,
              integrity: `sha512-${createHash("sha512").update("tampered").digest("base64")}`,
            },
          },
        },
      ],
    };

    const materialize = createWorkflowClosureMaterializer(
      materializerConfig(
        undefined,
        deliverMountedTarball(assetId, mountPath, tarballPath, workflow.bytes),
      ),
    );
    await expect(
      materialize({
        type: "workflow.probe.request",
        requestId: "req-sri",
        source: { kind: "asset", assetId, package: { format: "tarball" } },
        closure,
        entry: WORKFLOW_ENTRY,
        assets: [deliveredAsset(assetId, mountPath)],
      }),
    ).rejects.toThrow(/did not match pinned integrity/);
  });

  test("rejects an asset source whose asset was not delivered", async () => {
    const assetId = "asset_probe";
    const closure: ToolPackageManifest = {
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
              path: "tarballs/wf.tgz",
              integrity: `sha512-${createHash("sha512").update("x").digest("base64")}`,
            },
          },
        },
      ],
    };
    const materialize = createWorkflowClosureMaterializer(materializerConfig());
    await expect(
      materialize({
        type: "workflow.probe.request",
        requestId: "req-undelivered",
        source: { kind: "asset", assetId, package: { format: "tarball" } },
        closure,
        entry: WORKFLOW_ENTRY,
        assets: [],
      }),
    ).rejects.toThrow(/was not among the delivered assets/);
  });

  test("forwards a repeated asset delivery and propagates the rejection", async () => {
    const assetId = "asset_probe";
    const closure: ToolPackageManifest = {
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
              path: "tarballs/wf-probe-1.0.0.tgz",
              integrity: "sha512-placeholder",
            },
          },
        },
      ],
    };
    const first = deliveredAsset(assetId, "package-registries/a/");
    const second = deliveredAsset(assetId, "package-registries/b/");
    const materializeAssets: AssetDelivery = async (args) => {
      assetDeliveries.push(args);
      throw new Error("duplicate delivery rejected");
    };
    const materialize = createWorkflowClosureMaterializer(
      materializerConfig(undefined, materializeAssets),
    );
    await expect(
      materialize({
        type: "workflow.probe.request",
        requestId: "req-dup",
        source: { kind: "asset", assetId, package: { format: "tarball" } },
        closure,
        entry: WORKFLOW_ENTRY,
        assets: [first, second],
      }),
    ).rejects.toThrow(/duplicate delivery rejected/);
    expect(assetDeliveries).toHaveLength(1);
    expect(assetDeliveries[0]?.assets).toEqual([first, second]);
  });

  test("fails loud when the frame entry disagrees with interchange.workflow", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      },
      {
        [WORKFLOW_ENTRY]: workflowEntrySource(
          path.join(scratchRoot, "unused-sentinel"),
        ),
      },
    );
    const closure: ToolPackageManifest = {
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
            integrity: workflow.integrity,
          },
        },
      ],
    };
    const fetchTarball: TarballFetcher = async () => workflow.bytes;

    const materialize = createWorkflowClosureMaterializer(
      materializerConfig(fetchTarball),
    );
    await expect(
      materialize(probeFrame(closure, "./does-not-match.js")),
    ).rejects.toThrow(
      /does not match the materialized package's interchange.workflow/,
    );
  });
});
