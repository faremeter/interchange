import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fsNode, { promises as fs } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { base64Encode } from "@intx/types";
import type { RegistryConfig, TarballFetcher } from "@intx/tool-packaging";
import type { WorkflowProbeRequestFrame } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import { createWorkflowClosureMaterializer } from "./workflow-closure-materialization";

const REGISTRY_NAME = "test-registry";
const WORKFLOW_PACKAGE_NAME = "@fixture/wf-probe";
const WORKFLOW_PACKAGE_VERSION = "1.0.0";
const DEP_PACKAGE_NAME = "@fixture/dep";
const DEP_PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "index.js";

// The pinned workflow entry writes a sentinel file as an import-time side
// effect. The materializer lays the closure out WITHOUT importing any author
// code, so the sentinel must never appear -- its absence proves no module was
// evaluated on the host.
const SENTINEL_ENV = "PROBE_MATERIALIZER_SENTINEL";
const WORKFLOW_ENTRY_SOURCE = `
import { writeFileSync } from "node:fs";
const sentinel = process.env[${JSON.stringify(SENTINEL_ENV)}];
if (sentinel !== undefined) writeFileSync(sentinel, "imported");
export default {
  id: "probe-materializer-fixture",
  triggers: [],
  steps: { done: { kind: "sleep", id: "done", durationMs: 1 } },
  stepOrder: ["done"],
};
`;

let scratchRoot: string;
let cacheRoot: string;
let materializerScratch: string;
let fixtureSourceRoot: string;

beforeEach(async () => {
  scratchRoot = await fs.mkdtemp(
    path.join(os.tmpdir(), "sidecar-wf-probe-materialize-"),
  );
  cacheRoot = path.join(scratchRoot, "cache");
  materializerScratch = path.join(scratchRoot, "probe-closures");
  fixtureSourceRoot = path.join(scratchRoot, "fixture-source");
  await fs.mkdir(cacheRoot, { recursive: true });
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

function materializerConfig(fetchTarball?: TarballFetcher) {
  return {
    cacheRoot,
    cacheMaxBytes: 10_000_000,
    registryMaxTarballBytes: 10_000_000,
    maxAssetPayloadBytes: 50_000_000,
    registries: registries(),
    scratchRoot: materializerScratch,
    ...(fetchTarball !== undefined ? { fetchTarball } : {}),
  };
}

/**
 * Build a git packfile whose single commit's tree holds `files`, mirroring the
 * `createPack` output the hub delivers for an asset. Returns the pack bytes and
 * the commit sha the probe frame pins.
 */
async function buildAssetPack(
  files: Record<string, Uint8Array>,
): Promise<{ pack: Uint8Array; commitSha: string }> {
  const sourceDir = path.join(fixtureSourceRoot, `asset-${randomUUID()}`);
  await fs.mkdir(sourceDir, { recursive: true });
  await git.init({ fs: fsNode, dir: sourceDir, defaultBranch: "main" });
  for (const [rel, content] of Object.entries(files)) {
    const abs = path.join(sourceDir, rel);
    await fs.mkdir(path.dirname(abs), { recursive: true });
    await fs.writeFile(abs, content);
    await git.add({ fs: fsNode, dir: sourceDir, filepath: rel });
  }
  const commitSha = await git.commit({
    fs: fsNode,
    dir: sourceDir,
    message: "asset",
    author: { name: "test", email: "test@test.dev" },
  });
  const oids = new Set<string>([commitSha]);
  const { commit } = await git.readCommit({
    fs: fsNode,
    dir: sourceDir,
    oid: commitSha,
  });
  oids.add(commit.tree);
  async function walkTree(treeOid: string): Promise<void> {
    const { tree } = await git.readTree({
      fs: fsNode,
      dir: sourceDir,
      oid: treeOid,
    });
    for (const entry of tree) {
      oids.add(entry.oid);
      if (entry.type === "tree") await walkTree(entry.oid);
    }
  }
  await walkTree(commit.tree);
  const result = await git.packObjects({
    fs: fsNode,
    dir: sourceDir,
    oids: [...oids],
    write: false,
  });
  if (result.packfile === undefined) {
    throw new Error("packObjects produced no packfile");
  }
  return { pack: result.packfile, commitSha };
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

describe("createWorkflowClosureMaterializer", () => {
  test("lays out the frozen closure, resolves node_modules, and imports no author code", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
        dependencies: { [DEP_PACKAGE_NAME]: DEP_PACKAGE_VERSION },
      },
      { [WORKFLOW_ENTRY]: WORKFLOW_ENTRY_SOURCE },
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

    const sentinelPath = path.join(scratchRoot, "import-sentinel");
    process.env[SENTINEL_ENV] = sentinelPath;
    try {
      const materialize = createWorkflowClosureMaterializer(
        materializerConfig(fetchTarball),
      );
      const materialized = await materialize(
        probeFrame(closure, WORKFLOW_ENTRY),
      );

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

      // No author code ran: the entry module's import-time sentinel was
      // never written.
      await expect(fs.stat(sentinelPath)).rejects.toThrow();

      // Cleanup removes the ephemeral scratch tree.
      await materialized.cleanup();
      await expect(fs.stat(materialized.packageDir)).rejects.toThrow();
    } finally {
      Reflect.deleteProperty(process.env, SENTINEL_ENV);
    }
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

  test("materializes an asset-sourced closure from an inline-delivered pack", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      },
      { [WORKFLOW_ENTRY]: WORKFLOW_ENTRY_SOURCE },
    );
    const tarballPath = "tarballs/wf-probe-1.0.0.tgz";
    const assetId = "asset_probe";
    const mountPath = "package-registries/fixture/";
    const { pack, commitSha } = await buildAssetPack({
      [tarballPath]: workflow.bytes,
    });

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

    const materialize = createWorkflowClosureMaterializer(materializerConfig());
    const materialized = await materialize({
      type: "workflow.probe.request",
      requestId: "req-asset",
      source: { kind: "asset", assetId, package: { format: "tarball" } },
      closure,
      entry: WORKFLOW_ENTRY,
      assets: [
        {
          assetId,
          mountPath,
          pack: base64Encode(pack),
          ref: "refs/heads/main",
          commitSha,
        },
      ],
    });

    try {
      // The workflow package was laid out from the asset-delivered tarball,
      // SRI-verified against the frozen closure entry -- no HTTP fetch.
      const pkgJson = JSON.parse(
        await fs.readFile(
          path.join(materialized.packageDir, "package.json"),
          "utf8",
        ),
      );
      expect(pkgJson.name).toBe(WORKFLOW_PACKAGE_NAME);
      expect(pkgJson.version).toBe(WORKFLOW_PACKAGE_VERSION);
    } finally {
      await materialized.cleanup();
    }
  });

  test("fails loud when the inline asset payload exceeds the cap", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      },
      { [WORKFLOW_ENTRY]: WORKFLOW_ENTRY_SOURCE },
    );
    const assetId = "asset_probe";
    const { pack, commitSha } = await buildAssetPack({
      "tarballs/wf-probe-1.0.0.tgz": workflow.bytes,
    });

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
              integrity: workflow.integrity,
            },
          },
        },
      ],
    };

    const materialize = createWorkflowClosureMaterializer({
      ...materializerConfig(),
      maxAssetPayloadBytes: 16,
    });
    await expect(
      materialize({
        type: "workflow.probe.request",
        requestId: "req-cap",
        source: { kind: "asset", assetId, package: { format: "tarball" } },
        closure,
        entry: WORKFLOW_ENTRY,
        assets: [
          {
            assetId,
            mountPath: "package-registries/fixture/",
            pack: base64Encode(pack),
            ref: "refs/heads/main",
            commitSha,
          },
        ],
      }),
    ).rejects.toThrow(/inline asset payload exceeds the 16-byte cap/);
  });

  test("rejects an asset-delivered tarball that fails its pinned integrity", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      },
      { [WORKFLOW_ENTRY]: WORKFLOW_ENTRY_SOURCE },
    );
    const assetId = "asset_probe";
    const tarballPath = "tarballs/wf-probe-1.0.0.tgz";
    const { pack, commitSha } = await buildAssetPack({
      [tarballPath]: workflow.bytes,
    });

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

    const materialize = createWorkflowClosureMaterializer(materializerConfig());
    await expect(
      materialize({
        type: "workflow.probe.request",
        requestId: "req-sri",
        source: { kind: "asset", assetId, package: { format: "tarball" } },
        closure,
        entry: WORKFLOW_ENTRY,
        assets: [
          {
            assetId,
            mountPath: "package-registries/fixture/",
            pack: base64Encode(pack),
            ref: "refs/heads/main",
            commitSha,
          },
        ],
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

  test("rejects a frame that delivers the same assetId twice", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      },
      { [WORKFLOW_ENTRY]: WORKFLOW_ENTRY_SOURCE },
    );
    const assetId = "asset_probe";
    const { pack, commitSha } = await buildAssetPack({
      "tarballs/wf-probe-1.0.0.tgz": workflow.bytes,
    });
    const encoded = base64Encode(pack);
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [],
    };
    const materialize = createWorkflowClosureMaterializer(materializerConfig());
    await expect(
      materialize({
        type: "workflow.probe.request",
        requestId: "req-dup",
        source: { kind: "asset", assetId, package: { format: "tarball" } },
        closure,
        entry: WORKFLOW_ENTRY,
        assets: [
          {
            assetId,
            mountPath: "package-registries/a/",
            pack: encoded,
            ref: "refs/heads/main",
            commitSha,
          },
          {
            assetId,
            mountPath: "package-registries/b/",
            pack: encoded,
            ref: "refs/heads/main",
            commitSha,
          },
        ],
      }),
    ).rejects.toThrow(/is delivered more than once/);
  });

  test("fails closed on a git-sourced probe frame", async () => {
    const materialize = createWorkflowClosureMaterializer(materializerConfig());
    const closure: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: WORKFLOW_PACKAGE_NAME, version: WORKFLOW_PACKAGE_VERSION },
      ],
      entries: [],
    };
    await expect(
      materialize({
        type: "workflow.probe.request",
        requestId: "req-1",
        source: {
          kind: "asset",
          assetId: "asset_abc",
          package: {
            format: "source",
            commitSha: "0123456789abcdef0123456789abcdef01234567",
          },
        },
        closure,
        entry: WORKFLOW_ENTRY,
      }),
    ).rejects.toThrow(
      /source-format asset probe closures cannot be materialized/,
    );
  });

  test("fails loud when the frame entry disagrees with interchange.workflow", async () => {
    const workflow = await packFixture(
      {
        name: WORKFLOW_PACKAGE_NAME,
        version: WORKFLOW_PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      },
      { [WORKFLOW_ENTRY]: WORKFLOW_ENTRY_SOURCE },
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
